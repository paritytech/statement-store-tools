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

//! A v2 DHT statement network under a constant statement load from many accounts, with probes,
//! late reads and periodic audits that check the DHT keeps every statement on exactly its K
//! XOR-closest nodes.

pub mod audit;
pub mod load;
pub mod metrics;
pub mod node;

use anyhow::{anyhow, ensure, Result};
use clap::Parser;
use log::{info, warn};
use sc_statement_store::test_utils::get_keypair;
use sp_core::{blake2_256, sr25519};
use sp_statement_store::Statement;
use std::{
	collections::HashSet,
	net::SocketAddr,
	sync::Arc,
	time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{watch, Mutex};

#[derive(Parser, Debug)]
#[command(name = "statement-dht-bench")]
#[command(about = "Load a v2 DHT statement network and audit statement placement")]
pub struct Args {
	/// RPC WebSocket endpoints of every statement node in the network.
	#[arg(long, value_delimiter = ',', required = true)]
	pub rpc_endpoints: Vec<String>,

	/// Replication factor (K) the nodes run with.
	#[arg(long, default_value_t = 20)]
	pub replication_factor: usize,

	/// Statement allowance count each account was provisioned with.
	#[arg(long, default_value_t = 5)]
	pub allowance_max_count: u64,

	/// Load accounts: `setup-allowances` indices `0..load_accounts`.
	#[arg(long, default_value_t = 10_000)]
	pub load_accounts: u32,

	/// Statements each load account submits per hour.
	#[arg(long, default_value_t = 600)]
	pub rate_per_hour: u32,

	/// Statement payload size in bytes.
	#[arg(long, default_value_t = 2_000)]
	pub payload_size: usize,

	/// Validity of load statements in seconds.
	#[arg(long, default_value_t = 600)]
	pub load_expiry_secs: u64,

	/// Upper bound on load submissions awaiting a reply.
	#[arg(long, default_value_t = 4_096)]
	pub max_in_flight: usize,

	/// Probe accounts, the indices right after the load accounts.
	#[arg(long, default_value_t = 600)]
	pub probe_accounts: u32,

	/// Milliseconds between placement probes.
	#[arg(long, default_value_t = 1_000)]
	pub probe_interval_ms: u64,

	/// Late-read accounts, the indices right after the probe accounts.
	#[arg(long, default_value_t = 10)]
	pub late_read_accounts: u32,

	/// Seconds between late reads. Each one changes the reader node's affinity filter, which
	/// makes its peers replay their stores to it, so keep them rare.
	#[arg(long, default_value_t = 900)]
	pub late_read_interval_secs: u64,

	/// Seconds a late reader waits after the write before it subscribes.
	#[arg(long, default_value_t = 20)]
	pub late_read_delay_secs: u64,

	/// Seconds a late reader waits for the statement once subscribed.
	#[arg(long, default_value_t = 60)]
	pub late_read_timeout_secs: u64,

	/// Validity of probe and late-read statements in seconds.
	#[arg(long, default_value_t = 1_800)]
	pub probe_expiry_secs: u64,

	/// Seconds between audits of every node store.
	#[arg(long, default_value_t = 600)]
	pub audit_interval_secs: u64,

	/// Age in seconds from which a probe must sit on exactly its K replicas.
	#[arg(long, default_value_t = 60)]
	pub placement_age_secs: u64,

	/// Age in seconds from which a probe is checked again on the same K replicas.
	#[arg(long, default_value_t = 300)]
	pub persistence_age_secs: u64,

	/// Seconds after a late reader unsubscribes by which its node must drop the statement.
	#[arg(long, default_value_t = 60)]
	pub drop_age_secs: u64,

	/// Run length in seconds.
	#[arg(long, default_value_t = 86_400)]
	pub duration_secs: u64,

	/// Prometheus exposition address.
	#[arg(long, default_value = "0.0.0.0:9615")]
	pub metrics_addr: SocketAddr,
}

impl Args {
	fn validate(&self) -> Result<()> {
		let nodes = self.rpc_endpoints.len();
		// A late read writes through the closest non-replica and reads on the farthest node.
		ensure!(
			nodes >= self.replication_factor + 2,
			"{nodes} endpoints leave no room for a late read with K={}",
			self.replication_factor
		);
		ensure!(
			self.rate_per_hour > 0 && self.load_accounts > 0,
			"the load needs accounts and a rate"
		);
		ensure!(
			self.probe_accounts > 0 && self.late_read_accounts > 0,
			"probes and late reads need accounts"
		);
		// Every check of a probe or late read lands at the first audit after its age is reached.
		let last_check =
			self.persistence_age_secs.max(self.drop_age_secs) + self.audit_interval_secs;
		ensure!(
			self.probe_expiry_secs > last_check,
			"probes and late reads expire after {}s, before their last check at up to {last_check}s",
			self.probe_expiry_secs
		);
		// A probe or late read must never be evicted by its own account's allowance.
		for (class, accounts, interval) in [
			("probe", self.probe_accounts, self.probe_interval_ms as f64 / 1000.0),
			("late-read", self.late_read_accounts, self.late_read_interval_secs as f64),
		] {
			let live = (self.probe_expiry_secs as f64 / (accounts as f64 * interval)).ceil() as u64;
			ensure!(
				live < self.allowance_max_count,
				"each {class} account would hold {live} live statements, allowance {}",
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
	let late_start = probe_start + args.probe_accounts;
	let total = late_start + args.late_read_accounts;
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
	let tracker = Arc::new(Mutex::new(audit::Tracker::default()));
	let (stop_tx, stop_rx) = watch::channel(false);

	let metrics_task = {
		let bench = bench.clone();
		let addr = args.metrics_addr;
		tokio::spawn(async move {
			if let Err(e) = bench.metrics.serve(addr).await {
				warn!("{e}");
			}
		})
	};

	let rate = args.load_accounts as f64 * args.rate_per_hour as f64 / 3600.0;
	info!(
		"Run {run_id}: {} nodes, K={}, {rate:.0} statements/s of {} B from {} accounts for {}s",
		bench.nodes.len(),
		args.replication_factor,
		args.payload_size,
		args.load_accounts,
		args.duration_secs
	);

	let probe_validity = Duration::from_secs(args.probe_expiry_secs);
	let checks = audit::Ages {
		placement: Duration::from_secs(args.placement_age_secs),
		persistence: Duration::from_secs(args.persistence_age_secs),
		drop: Duration::from_secs(args.drop_age_secs),
	};
	let tasks = vec![
		tokio::spawn(load::run(
			bench.clone(),
			load::Load {
				accounts: 0..args.load_accounts as usize,
				rate,
				size: args.payload_size,
				validity: Duration::from_secs(args.load_expiry_secs),
				max_in_flight: args.max_in_flight,
			},
			stop_rx.clone(),
		)),
		tokio::spawn(audit::probes(
			bench.clone(),
			tracker.clone(),
			probe_start as usize..late_start as usize,
			Duration::from_millis(args.probe_interval_ms),
			args.payload_size,
			probe_validity,
			stop_rx.clone(),
		)),
		tokio::spawn(audit::late_reads(
			bench.clone(),
			tracker.clone(),
			audit::LateReadConfig {
				accounts: late_start as usize..total as usize,
				interval: Duration::from_secs(args.late_read_interval_secs),
				delay: Duration::from_secs(args.late_read_delay_secs),
				timeout: Duration::from_secs(args.late_read_timeout_secs),
				size: args.payload_size,
				validity: probe_validity,
			},
			stop_rx.clone(),
		)),
		tokio::spawn(audit::audits(
			bench.clone(),
			tracker.clone(),
			Duration::from_secs(args.audit_interval_secs),
			checks.clone(),
			stop_rx,
		)),
	];

	tokio::select! {
		_ = tokio::time::sleep(Duration::from_secs(args.duration_secs)) => {},
		_ = tokio::signal::ctrl_c() => info!("Interrupted"),
	}
	stop_tx.send(true).map_err(|_| anyhow!("every task exited before the stop"))?;
	for task in tasks {
		task.await?;
	}
	let final_audit = audit::audit(&bench, &tracker, &checks).await;
	metrics_task.abort();

	info!("Run {run_id} totals:\n{}", bench.metrics.summary()?);
	let violations = bench.metrics.violations();
	ensure!(violations == 0, "run {run_id} ended with {violations} violations");
	ensure!(final_audit, "run {run_id}: the final audit could not read every node store");
	ensure!(bench.metrics.placements_checked() > 0, "run {run_id} checked no probe placement");
	info!("Run {run_id} ended without violations");
	Ok(())
}
