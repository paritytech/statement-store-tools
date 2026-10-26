// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

//! A v2 DHT statement network under a stepped statement load from many accounts, with probes that
//! time how long a statement takes to reach its K XOR-closest nodes and check that it stays there.

pub mod load;
pub mod metrics;
pub mod node;
pub mod probes;

use anyhow::{anyhow, ensure, Result};
use clap::Parser;
use load::{Schedule, Step};
use log::{info, warn};
use sc_statement_store::test_utils::get_keypair;
use sp_core::{blake2_256, sr25519};
use sp_statement_store::Statement;
use std::{
	collections::HashSet,
	net::SocketAddr,
	sync::Arc,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, watch, Mutex};

/// Time the nodes get to advertise their new affinity filters before the load starts.
const FILTER_SETTLE: Duration = Duration::from_secs(10);

#[derive(Parser, Debug)]
#[command(name = "statement-dht-bench")]
#[command(about = "Load a v2 DHT statement network step by step and time statement delivery")]
pub struct Args {
	/// RPC WebSocket endpoints of every statement node in the network.
	#[arg(long, value_delimiter = ',', required = true)]
	pub rpc_endpoints: Vec<String>,

	/// Replication factor (K) the nodes run with.
	#[arg(long, default_value_t = 20)]
	pub replication_factor: usize,

	/// Statement allowance count each account was provisioned with.
	#[arg(long, default_value_t = 2_000)]
	pub allowance_max_count: u64,

	/// Load accounts: `setup-allowances` indices `0..load_accounts`.
	#[arg(long, default_value_t = 10_000)]
	pub load_accounts: u32,

	/// Load steps as `statements_per_second:seconds`, run in order. The load stops after the last
	/// one; the probes go on until the end of the run.
	#[arg(long, value_delimiter = ',', required = true)]
	pub load_steps: Vec<Step>,

	/// Statement payload size in bytes.
	#[arg(long, default_value_t = 1_000)]
	pub payload_size: usize,

	/// Validity of load statements in seconds.
	#[arg(long, default_value_t = 14_400)]
	pub load_expiry_secs: u64,

	/// Upper bound on load submissions awaiting a reply.
	#[arg(long, default_value_t = 4_096)]
	pub max_in_flight: usize,

	/// Probe accounts, the indices right after the load accounts.
	#[arg(long, default_value_t = 600)]
	pub probe_accounts: u32,

	/// Milliseconds between probes.
	#[arg(long, default_value_t = 200)]
	pub probe_interval_ms: u64,

	/// Topics the probes rotate over. Every node subscribes to those it is a replica of.
	#[arg(long, default_value_t = 200)]
	pub probe_topics: usize,

	/// Validity of probes in seconds.
	#[arg(long, default_value_t = 14_400)]
	pub probe_expiry_secs: u64,

	/// Seconds within which a probe must reach all K replicas.
	#[arg(long, default_value_t = 60)]
	pub probe_deadline_secs: u64,

	/// Run length in seconds: the load steps, then probes alone.
	#[arg(long, default_value_t = 6_000)]
	pub duration_secs: u64,

	/// Prometheus exposition address.
	#[arg(long, default_value = "0.0.0.0:9615")]
	pub metrics_addr: SocketAddr,
}

impl Args {
	fn validate(&self) -> Result<()> {
		let nodes = self.rpc_endpoints.len();
		ensure!(
			nodes > self.replication_factor,
			"{nodes} endpoints leave no non-replica to write probes through with K={}",
			self.replication_factor
		);
		ensure!(
			self.load_accounts > 0 && self.probe_accounts > 0,
			"the load and the probes need accounts"
		);
		ensure!(
			self.probe_topics > 0 && self.probe_interval_ms > 0,
			"the probes need topics and an interval"
		);
		let schedule = Schedule(self.load_steps.clone());
		ensure!(
			schedule.secs() <= self.duration_secs,
			"the load steps last {}s, longer than the {}s run",
			schedule.secs(),
			self.duration_secs
		);
		// Nothing may expire during the run: a node reaps at most 10,000 expired statements per
		// 31 s, far slower than this load adds new ones.
		ensure!(
			self.load_expiry_secs > self.duration_secs,
			"load statements expire after {}s, within the {}s run",
			self.load_expiry_secs,
			self.duration_secs
		);
		ensure!(
			self.probe_expiry_secs > self.duration_secs + self.probe_deadline_secs,
			"probes expire after {}s, before the final check",
			self.probe_expiry_secs
		);
		// Nothing may be evicted: a node holds K/N of an account's load, but all of a probe
		// account's probes, as such an account may write to one topic only.
		let load_share = self.replication_factor as f64 / nodes as f64;
		let probes = self.duration_secs * 1_000 / self.probe_interval_ms;
		for (class, statements, accounts, share) in [
			("load", schedule.total(), self.load_accounts, load_share),
			("probe", probes, self.probe_accounts, 1.0),
		] {
			let per_node = (statements as f64 / accounts as f64 * share).ceil() as u64;
			ensure!(
				per_node < self.allowance_max_count,
				"each {class} account would hold {per_node} statements per node, allowance {}",
				self.allowance_max_count
			);
		}
		Ok(())
	}
}

/// State every task reads: the nodes, the signing keys of every account and the metrics.
pub struct Bench {
	pub nodes: Vec<node::Node>,
	pub dht_keys: Vec<[u8; 32]>,
	pub accounts: Vec<sr25519::Pair>,
	pub metrics: metrics::Metrics,
	pub replication_factor: usize,
	pub run_id: u64,
}

/// A signed statement on `topic`, valid for `validity`, filled with `size` bytes. `seq` orders the
/// account's statements: the lowest one is evicted first once its allowance is reached.
pub fn build_statement(
	key: &sr25519::Pair,
	topic: [u8; 32],
	validity: Duration,
	seq: u32,
	size: usize,
) -> Statement {
	let mut statement = Statement::new();
	statement.set_expiry_from_parts(unix_now().saturating_add(validity.as_secs()) as u32, seq);
	statement.set_topic(0, topic.into());
	statement.set_plain_data(vec![0u8; size]);
	statement.sign_sr25519_private(key);
	statement
}

pub fn topic(run_id: u64, class: &str, n: u64) -> [u8; 32] {
	blake2_256(format!("statement-dht-{run_id}-{class}-{n}").as_bytes())
}

fn unix_now() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("system clock after 1970; qed")
		.as_secs()
}

pub async fn run(args: Args) -> Result<()> {
	args.validate()?;
	let nodes =
		futures::future::try_join_all(args.rpc_endpoints.iter().map(|e| node::Node::connect(e)))
			.await?;
	let dht_keys: Vec<[u8; 32]> = nodes.iter().map(|node| node.dht_key).collect();
	ensure!(
		dht_keys.iter().collect::<HashSet<_>>().len() == nodes.len(),
		"two endpoints lead to the same node"
	);

	let probe_start = args.load_accounts;
	let total = probe_start + args.probe_accounts;
	info!("Deriving {total} account keys");
	let accounts: Vec<sr25519::Pair> =
		tokio::task::spawn_blocking(move || (0..total).map(get_keypair).collect()).await?;

	let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
	let bench = Arc::new(Bench {
		nodes,
		dht_keys,
		accounts,
		metrics: metrics::Metrics::new()?,
		replication_factor: args.replication_factor,
		run_id,
	});
	let schedule = Schedule(args.load_steps.clone());

	let metrics_task = {
		let bench = bench.clone();
		let addr = args.metrics_addr;
		tokio::spawn(async move {
			if let Err(e) = bench.metrics.serve(addr).await {
				warn!("{e}");
			}
		})
	};

	// Subscribe while the stores are empty: each subscription makes the node's peers replay their
	// stores to it.
	let pool = Arc::new(probes::Pool::new(&bench, args.probe_topics));
	let tracker = Arc::new(Mutex::new(probes::Tracker::new(
		bench.nodes.len(),
		args.replication_factor,
		schedule.clone(),
	)));
	let (mut watchers, mut opened) = (Vec::new(), Vec::new());
	for node in pool.watched() {
		let (tx, rx) = oneshot::channel();
		watchers.push(tokio::spawn(probes::watch(
			bench.clone(),
			pool.clone(),
			tracker.clone(),
			node,
			tx,
		)));
		opened.push(rx);
	}
	for rx in opened {
		rx.await.map_err(|_| anyhow!("a probe watcher exited before subscribing"))??;
	}
	info!(
		"Run {run_id}: {} nodes, K={}, probes subscribed on {} nodes; load steps {} of {} B from \
		 {} accounts, then probes alone until {}s",
		bench.nodes.len(),
		args.replication_factor,
		pool.watched().count(),
		schedule
			.0
			.iter()
			.map(|s| format!("{}/s×{}s", s.rate, s.secs))
			.collect::<Vec<_>>()
			.join(", "),
		args.payload_size,
		args.load_accounts,
		args.duration_secs
	);
	tokio::time::sleep(FILTER_SETTLE).await;

	let started = Instant::now();
	let deadline = Duration::from_secs(args.probe_deadline_secs);
	let (stop_tx, stop_rx) = watch::channel(false);
	let tasks = vec![
		tokio::spawn(load::run(
			bench.clone(),
			load::Load {
				accounts: 0..args.load_accounts as usize,
				schedule: schedule.clone(),
				size: args.payload_size,
				validity: Duration::from_secs(args.load_expiry_secs),
				max_in_flight: args.max_in_flight,
			},
			started,
			stop_rx.clone(),
		)),
		tokio::spawn(probes::submit(
			bench.clone(),
			pool.clone(),
			tracker.clone(),
			probes::Probes {
				accounts: probe_start as usize..total as usize,
				interval: Duration::from_millis(args.probe_interval_ms),
				size: args.payload_size,
				validity: Duration::from_secs(args.probe_expiry_secs),
			},
			schedule,
			started,
			stop_rx,
		)),
	];
	// Settling outlives the writes by one deadline, so the last probes get the same deadline.
	let (settle_stop_tx, settle_stop_rx) = watch::channel(false);
	let settle = tokio::spawn(probes::settle(
		bench.clone(),
		pool.clone(),
		tracker.clone(),
		deadline,
		settle_stop_rx,
	));

	tokio::select! {
		_ = tokio::time::sleep(Duration::from_secs(args.duration_secs)) => {},
		_ = tokio::signal::ctrl_c() => info!("Interrupted"),
	}
	stop_tx.send(true).map_err(|_| anyhow!("every task exited before the stop"))?;
	for task in tasks {
		task.await?;
	}
	tokio::time::sleep(deadline).await;
	settle_stop_tx
		.send(true)
		.map_err(|_| anyhow!("probe settling exited before the stop"))?;
	settle.await?;
	tracker.lock().await.settle(&bench, &pool, Instant::now(), Duration::ZERO);
	let final_read = probes::final_check(&bench, &pool, &tracker).await;
	let (report, catch_up) = {
		let mut tracker = tracker.lock().await;
		(tracker.report(), tracker.catch_up_report(&bench, Instant::now()))
	};
	for watcher in watchers {
		watcher.abort();
	}
	metrics_task.abort();
	warn!(
		"Probe subscriptions closed: every watched node's affinity filter changes, so its peers \
		 replay their stores to it"
	);

	info!("Run {run_id} probe delivery by load step:\n{report}");
	if !catch_up.is_empty() {
		info!("Run {run_id} catch-up of the nodes that went offline:\n{catch_up}");
	}
	info!("Run {run_id} totals:\n{}", bench.metrics.summary()?);
	let violations = bench.metrics.violations();
	ensure!(violations == 0, "run {run_id} ended with {violations} violations");
	ensure!(final_read, "run {run_id}: the final check could not read every watched node");
	ensure!(bench.metrics.deliveries_checked() > 0, "run {run_id} checked no probe delivery");
	info!("Run {run_id} ended without violations");
	Ok(())
}
