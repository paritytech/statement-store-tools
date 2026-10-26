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

//! Probes: statements on a fixed topic pool, written through non-replicas, that time delivery to
//! the K XOR-closest nodes and check that they stay there.
//!
//! Each node subscribes once, before the load, to the pool topics it is a replica of: a topic
//! subscription changes its affinity filter, and every change makes its peers replay their stores.
//!
//! - Delivery: all K replicas within the deadline. An offline replica is excused but owes the
//!   probe.
//! - Restart: a returning node's replay must hold every probe it had stored.
//! - Final: a probe gone from a replica is lost, one an online replica never stored is undelivered,
//!   one still owed is reported as catching up.

use super::{build_statement, load::Schedule, node::by_distance, topic, Bench};
use anyhow::Result;
use codec::Encode;
use futures::{stream, StreamExt};
use jsonrpsee::core::client::Subscription;
use log::{info, warn};
use sp_core::{blake2_256, hexdisplay::HexDisplay};
use sp_statement_store::{StatementEvent, SubmitResult, Topic, TopicFilter, MAX_ANY_TOPICS};
use std::{
	collections::{BTreeMap, HashMap, HashSet},
	fmt::Write as _,
	ops::Range,
	sync::Arc,
	time::{Duration, Instant},
};
use tokio::sync::{oneshot, watch, Mutex};

type Hash = [u8; 32];

/// Wait between attempts to subscribe again on a node that dropped its subscriptions.
const RESUBSCRIBE_BACKOFF: Duration = Duration::from_secs(1);
/// How often the probes past their deadline are settled.
const SETTLE_INTERVAL: Duration = Duration::from_secs(1);
/// How often the delivery latency of the last interval is logged.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(60);
/// Nodes read at once by the final check.
const FINAL_READ_CONCURRENCY: usize = 8;

/// The probe topics and the nodes the DHT places each one on.
pub struct Pool {
	topics: Vec<Hash>,
	/// The K closest nodes of each topic.
	replicas: Vec<Vec<usize>>,
	/// The other nodes of each topic, which its probes are written through.
	entries: Vec<Vec<usize>>,
	/// Per node, filters naming the topics it is a replica of.
	filters: Vec<Vec<TopicFilter>>,
}

impl Pool {
	pub fn new(bench: &Bench, size: usize) -> Self {
		let k = bench.replication_factor;
		let mut by_node: Vec<Vec<Topic>> = vec![Vec::new(); bench.nodes.len()];
		let (mut topics, mut replicas, mut entries) = (Vec::new(), Vec::new(), Vec::new());
		for n in 0..size as u64 {
			let topic = topic(bench.run_id, "probe", n);
			let order = by_distance(&bench.dht_keys, &topic);
			for &node in &order[..k] {
				by_node[node].push(topic.into());
			}
			replicas.push(order[..k].to_vec());
			entries.push(order[k..].to_vec());
			topics.push(topic);
		}
		let filters = by_node
			.iter()
			.map(|topics| {
				topics
					.chunks(MAX_ANY_TOPICS)
					.map(|chunk| {
						TopicFilter::MatchAny(
							chunk
								.to_vec()
								.try_into()
								.expect("chunks hold at most MAX_ANY_TOPICS; qed"),
						)
					})
					.collect()
			})
			.collect();
		Self { topics, replicas, entries, filters }
	}

	/// Nodes that are a replica of at least one probe topic.
	pub fn watched(&self) -> impl Iterator<Item = usize> + '_ {
		(0..self.filters.len()).filter(|&node| !self.filters[node].is_empty())
	}
}

struct Probe {
	topic: usize,
	entry: usize,
	submitted: Instant,
	/// The load step the probe was written in.
	step: Option<usize>,
	/// Replicas that stored it so far.
	arrived: HashSet<usize>,
}

/// Delivery of the probes written in one load step, latencies in seconds.
#[derive(Default)]
struct Latencies {
	probes: u64,
	complete: u64,
	replica: Vec<f64>,
	all: Vec<f64>,
}

impl Latencies {
	fn line(&mut self) -> String {
		self.replica.sort_by(f64::total_cmp);
		self.all.sort_by(f64::total_cmp);
		format!(
			"{} probes, {} on all replicas in time; all replicas p50 {} p90 {} p99 {} max {}; \
			 one replica p50 {} p99 {}",
			self.probes,
			self.complete,
			quantile(&self.all, 0.5),
			quantile(&self.all, 0.9),
			quantile(&self.all, 0.99),
			quantile(&self.all, 1.0),
			quantile(&self.replica, 0.5),
			quantile(&self.replica, 0.99),
		)
	}
}

fn quantile(sorted: &[f64], q: f64) -> String {
	match sorted.len() {
		0 => "-".to_string(),
		len => format!("{:.2}s", sorted[((len - 1) as f64 * q).round() as usize]),
	}
}

/// Steps in run order, idle last.
fn by_step(
	latencies: &mut BTreeMap<Option<usize>, Latencies>,
) -> Vec<(Option<usize>, &mut Latencies)> {
	let mut steps: Vec<_> = latencies.iter_mut().map(|(step, l)| (*step, l)).collect();
	steps.sort_by_key(|(step, _)| step.unwrap_or(usize::MAX));
	steps
}

/// A replica's catch-up since its last return.
struct CatchUp {
	back: Instant,
	outage: Duration,
	/// Probes owed since the return.
	owed: usize,
	/// When the last owed probe arrived.
	last: Option<Instant>,
}

pub struct Tracker {
	schedule: Schedule,
	replication_factor: usize,
	/// Probes inside their deadline.
	pending: HashMap<Hash, Probe>,
	/// Probes past their deadline or on all replicas: their topic.
	settled: HashMap<Hash, usize>,
	/// Per node, the probes its subscriptions reported stored.
	stored: Vec<HashSet<Hash>>,
	/// Per node, the probes it missed while offline and has not stored since.
	owed: Vec<HashSet<Hash>>,
	/// Per node, since when it is offline, and when it last came back.
	offline_since: Vec<Option<Instant>>,
	back_at: Vec<Option<Instant>>,
	catch_up: Vec<Option<CatchUp>>,
	/// Delivery by load step, over the run and over the current progress interval.
	run: BTreeMap<Option<usize>, Latencies>,
	interval: BTreeMap<Option<usize>, Latencies>,
}

impl Tracker {
	pub fn new(nodes: usize, replication_factor: usize, schedule: Schedule) -> Self {
		Self {
			schedule,
			replication_factor,
			pending: HashMap::new(),
			settled: HashMap::new(),
			stored: vec![HashSet::new(); nodes],
			owed: vec![HashSet::new(); nodes],
			offline_since: vec![None; nodes],
			back_at: vec![None; nodes],
			catch_up: (0..nodes).map(|_| None).collect(),
			run: BTreeMap::new(),
			interval: BTreeMap::new(),
		}
	}

	fn arrived(&mut self, bench: &Bench, node: usize, hash: Hash, now: Instant) {
		if !self.pending.contains_key(&hash) && !self.settled.contains_key(&hash) {
			return;
		}
		self.stored[node].insert(hash);
		if self.owed[node].remove(&hash) {
			if let Some(catch_up) = &mut self.catch_up[node] {
				catch_up.last = Some(now);
			}
		}
		let Some(probe) = self.pending.get_mut(&hash) else { return };
		if !probe.arrived.insert(node) {
			return;
		}
		let (step, complete) = (probe.step, probe.arrived.len() == self.replication_factor);
		let latency = now.saturating_duration_since(probe.submitted);
		let label = self.schedule.label(step);
		bench.metrics.replica_latency(&label, latency);
		for latencies in [&mut self.run, &mut self.interval] {
			latencies.entry(step).or_default().replica.push(latency.as_secs_f64());
		}
		if complete {
			let probe = self.pending.remove(&hash).expect("found above; qed");
			bench.metrics.all_replicas_latency(&label, latency);
			bench.metrics.check("delivery", "ok", 1);
			for latencies in [&mut self.run, &mut self.interval] {
				let latencies = latencies.entry(step).or_default();
				latencies.probes += 1;
				latencies.complete += 1;
				latencies.all.push(latency.as_secs_f64());
			}
			self.settled.insert(hash, probe.topic);
		}
	}

	/// Settle the probes written more than `deadline` before `now` that missed a replica.
	pub fn settle(&mut self, bench: &Bench, pool: &Pool, now: Instant, deadline: Duration) {
		let due: Vec<Hash> = self
			.pending
			.iter()
			.filter(|(_, probe)| now.saturating_duration_since(probe.submitted) >= deadline)
			.map(|(hash, _)| *hash)
			.collect();
		for hash in due {
			let probe = self.pending.remove(&hash).expect("collected from pending; qed");
			let (excused, missed) = self.unreached(&pool.replicas[probe.topic], &probe);
			let result = if missed.is_empty() { "excused" } else { "violation" };
			bench.metrics.check("delivery", result, 1);
			for &node in &excused {
				// An offline replica's debt is counted when it comes back.
				if self.owed[node].insert(hash) && self.offline_since[node].is_none() {
					if let Some(catch_up) = &mut self.catch_up[node] {
						catch_up.owed += 1;
					}
				}
			}
			for latencies in [&mut self.run, &mut self.interval] {
				latencies.entry(probe.step).or_default().probes += 1;
			}
			if !missed.is_empty() {
				warn!(
					"Probe {} on topic {} reached {}/{} replicas within {deadline:?}, written via {}: \
					 missing on {:?}, excused offline {:?}",
					HexDisplay::from(&hash),
					HexDisplay::from(&pool.topics[probe.topic]),
					probe.arrived.len(),
					self.replication_factor,
					bench.nodes[probe.entry].endpoint,
					endpoints(bench, &missed),
					endpoints(bench, &excused),
				);
			}
			self.settled.insert(hash, probe.topic);
		}
	}

	/// The `replicas` `probe` has not reached: those offline at some point since it was written,
	/// and the others.
	fn unreached(&self, replicas: &[usize], probe: &Probe) -> (Vec<usize>, Vec<usize>) {
		replicas
			.iter()
			.filter(|node| !probe.arrived.contains(node))
			.partition(|&&node| {
				self.offline_since[node].is_some() ||
					self.back_at[node].is_some_and(|back| back > probe.submitted)
			})
	}

	/// `node` is back. `held`, its store now, must include every probe it had stored.
	fn back_online(&mut self, bench: &Bench, node: usize, now: Instant, held: &HashSet<Hash>) {
		let outage = self.offline_since[node]
			.take()
			.map_or(Duration::ZERO, |since| now.saturating_duration_since(since));
		self.back_at[node] = Some(now);
		self.catch_up[node] =
			Some(CatchUp { back: now, outage, owed: self.owed[node].len(), last: None });
		let stored = &self.stored[node];
		let lost: Vec<&Hash> = stored.iter().filter(|hash| !held.contains(*hash)).collect();
		bench.metrics.check("restart", "ok", (stored.len() - lost.len()) as u64);
		bench.metrics.check("restart", "violation", lost.len() as u64);
		let endpoint = &bench.nodes[node].endpoint;
		match lost.first() {
			None => info!(
				"{endpoint} is back after {outage:.1?} with all {} probes it had stored",
				stored.len()
			),
			Some(first) => warn!(
				"{endpoint} is back after {outage:.1?} without {} of the {} probes it had stored, \
				 e.g. {}",
				lost.len(),
				stored.len(),
				HexDisplay::from(*first),
			),
		}
	}

	/// How each replica that came back caught up on the probes it missed while offline.
	pub fn catch_up_report(&self, bench: &Bench, now: Instant) -> String {
		self.catch_up
			.iter()
			.enumerate()
			.filter_map(|(node, catch_up)| {
				let CatchUp { back, outage, owed, last } = catch_up.as_ref()?;
				let endpoint = &bench.nodes[node].endpoint;
				let left = self.owed[node].len();
				Some(match (left, last) {
					(0, Some(last)) => format!(
						"  {endpoint}: back after {outage:.1?} offline, caught up on {owed} probes \
						 {:.1?} after its return",
						last.saturating_duration_since(*back)
					),
					(0, None) => {
						format!("  {endpoint}: back after {outage:.1?} offline, owed no probes")
					},
					(left, _) => format!(
						"  {endpoint}: back after {outage:.1?} offline, {left} of {owed} probes still \
						 missing {:.1?} after its return",
						now.saturating_duration_since(*back)
					),
				})
			})
			.collect::<Vec<_>>()
			.join("\n")
	}

	/// Delivery in the progress interval, which then starts over.
	fn progress(&mut self, bench: &Bench) -> String {
		let mut text = String::new();
		for (step, latencies) in by_step(&mut self.interval) {
			let _ = write!(text, "\n  {}: {}", self.schedule.label(step), latencies.line());
		}
		self.interval.clear();
		let _ = write!(text, "\n  {} violations so far", bench.metrics.violations());
		text
	}

	/// Delivery over the run, by load step.
	pub fn report(&mut self) -> String {
		by_step(&mut self.run)
			.into_iter()
			.map(|(step, latencies)| {
				format!("  {}: {}", self.schedule.label(step), latencies.line())
			})
			.collect::<Vec<_>>()
			.join("\n")
	}
}

fn endpoints<'a>(bench: &'a Bench, nodes: &[usize]) -> Vec<&'a str> {
	nodes.iter().map(|&node| bench.nodes[node].endpoint.as_str()).collect()
}

/// Keep `node`'s probe subscriptions open, resubscribing after a drop. `opened` reports the first
/// attempt.
pub async fn watch(
	bench: Arc<Bench>,
	pool: Arc<Pool>,
	tracker: Arc<Mutex<Tracker>>,
	node: usize,
	opened: oneshot::Sender<Result<()>>,
) {
	let mut opened = Some(opened);
	loop {
		match subscribe(&bench, &pool, node).await {
			Ok(subscriptions) => {
				let resubscribed = match opened.take() {
					Some(opened) => {
						let _ = opened.send(Ok(()));
						false
					},
					None => {
						bench.metrics.resubscribed();
						true
					},
				};
				follow(&bench, &tracker, node, subscriptions, resubscribed).await;
				tracker.lock().await.offline_since[node].get_or_insert_with(Instant::now);
				warn!(
					"{} dropped its probe subscriptions; subscribing again changes its affinity \
					 filter",
					bench.nodes[node].endpoint
				);
			},
			Err(e) => {
				if let Some(opened) = opened.take() {
					let _ = opened.send(Err(e));
					return;
				}
			},
		}
		tokio::time::sleep(RESUBSCRIBE_BACKOFF).await;
	}
}

async fn subscribe(
	bench: &Bench,
	pool: &Pool,
	node: usize,
) -> Result<Vec<Subscription<StatementEvent>>> {
	let mut subscriptions = Vec::new();
	for filter in &pool.filters[node] {
		subscriptions.push(bench.nodes[node].subscribe(filter.clone()).await?);
	}
	Ok(subscriptions)
}

/// Report the probes `subscriptions` deliver until one ends. After a resubscription, the replay
/// and the probes arriving with it are checked against what the node had stored.
async fn follow(
	bench: &Bench,
	tracker: &Mutex<Tracker>,
	node: usize,
	subscriptions: Vec<Subscription<StatementEvent>>,
	resubscribed: bool,
) {
	let mut replaying = subscriptions.len();
	let mut held = resubscribed.then(HashSet::new);
	let mut events =
		stream::select_all(subscriptions.into_iter().map(|subscription| {
			subscription.map(Some).chain(stream::once(async { None })).boxed()
		}));
	while let Some(Some(Ok(StatementEvent::NewStatements { statements, remaining }))) =
		events.next().await
	{
		let now = Instant::now();
		let mut tracker = tracker.lock().await;
		for statement in &statements {
			let hash = blake2_256(&statement.0);
			tracker.arrived(bench, node, hash, now);
			if let (true, Some(held)) = (replaying > 0, &mut held) {
				held.insert(hash);
			}
		}
		// Only the replay carries `remaining`, counting down to zero in each subscription.
		if remaining == Some(0) && replaying > 0 {
			replaying -= 1;
			if let (0, Some(held)) = (replaying, &held) {
				tracker.back_online(bench, node, now, held);
			}
		}
	}
}

pub struct Probes {
	pub accounts: Range<usize>,
	pub interval: Duration,
	pub size: usize,
	pub validity: Duration,
}

/// Write one probe per interval, rotating over the pool topics and, per topic, over the nodes that
/// are not its replicas.
pub async fn submit(
	bench: Arc<Bench>,
	pool: Arc<Pool>,
	tracker: Arc<Mutex<Tracker>>,
	probes: Probes,
	schedule: Schedule,
	started: Instant,
	mut stop: watch::Receiver<bool>,
) {
	let mut ticker = tokio::time::interval(probes.interval);
	let topics = pool.topics.len() as u64;
	for n in 0u64.. {
		tokio::select! {
			_ = ticker.tick() => {},
			_ = stop.changed() => return,
		}
		let topic = (n % topics) as usize;
		let entries = &pool.entries[topic];
		let entry = entries[((n / topics) % entries.len() as u64) as usize];
		let account = probes.accounts.start + (n % probes.accounts.len() as u64) as usize;
		let statement = build_statement(
			&bench.accounts[account],
			pool.topics[topic],
			probes.validity,
			n as u32,
			probes.size,
		);
		let hash = blake2_256(&statement.encode());
		// Tracked before the write, as a replica may report the probe before the writer replies.
		tracker.lock().await.pending.insert(
			hash,
			Probe {
				topic,
				entry,
				submitted: Instant::now(),
				step: schedule.step_at(started.elapsed()),
				arrived: HashSet::new(),
			},
		);
		let (bench, tracker) = (bench.clone(), tracker.clone());
		tokio::spawn(async move {
			let result = bench.nodes[entry].submit(&statement).await;
			bench.metrics.submit("probe", &result);
			let reply = match result {
				Ok(SubmitResult::New) => return,
				Ok(reply) => format!("{reply:?}"),
				Err(e) => format!("{e:#}"),
			};
			tracker.lock().await.pending.remove(&hash);
			warn!(
				"Probe {} not accepted by {}: {reply}",
				HexDisplay::from(&hash),
				bench.nodes[entry].endpoint
			);
		});
	}
}

/// Settle the probes past `deadline` every second and log the delivery of each progress interval.
pub async fn settle(
	bench: Arc<Bench>,
	pool: Arc<Pool>,
	tracker: Arc<Mutex<Tracker>>,
	deadline: Duration,
	mut stop: watch::Receiver<bool>,
) {
	let mut ticker = tokio::time::interval(SETTLE_INTERVAL);
	let mut logged = Instant::now();
	loop {
		tokio::select! {
			_ = ticker.tick() => {},
			_ = stop.changed() => return,
		}
		let now = Instant::now();
		let mut tracker = tracker.lock().await;
		tracker.settle(&bench, &pool, now, deadline);
		if now.saturating_duration_since(logged) >= PROGRESS_INTERVAL {
			logged = now;
			info!("Probe delivery in the last {PROGRESS_INTERVAL:?}:{}", tracker.progress(&bench));
		}
	}
}

/// Read every watched node's probes through a second subscription to the same topics, which
/// leaves its filter unchanged, and classify each settled probe per replica. Returns whether every
/// node could be read.
pub async fn final_check(bench: &Bench, pool: &Pool, tracker: &Mutex<Tracker>) -> bool {
	let reads = stream::iter(pool.watched())
		.map(|node| async move {
			let mut held = HashSet::new();
			for filter in &pool.filters[node] {
				bench.nodes[node]
					.read_store(filter.clone(), |statement| {
						held.insert(blake2_256(statement));
					})
					.await?;
			}
			Ok::<_, anyhow::Error>((node, held))
		})
		.buffer_unordered(FINAL_READ_CONCURRENCY)
		.collect::<Vec<_>>()
		.await;
	let tracker = tracker.lock().await;
	let (mut complete, mut ok, mut owed, mut undelivered) = (true, 0, 0, HashSet::new());
	for read in reads {
		let (node, held) = match read {
			Ok(read) => read,
			Err(e) => {
				complete = false;
				warn!("Final check incomplete: {e:#}");
				continue;
			},
		};
		let mut lost = Vec::new();
		for (hash, &topic) in &tracker.settled {
			if !pool.replicas[topic].contains(&node) {
				continue;
			}
			if held.contains(hash) {
				ok += 1;
			} else if tracker.stored[node].contains(hash) {
				lost.push(hash);
			} else if tracker.owed[node].contains(hash) {
				owed += 1;
			} else {
				undelivered.insert(*hash);
			}
		}
		bench.metrics.check("final", "lost", lost.len() as u64);
		if let Some(first) = lost.first() {
			warn!(
				"{} lost {} of the probes it had stored, e.g. {}",
				bench.nodes[node].endpoint,
				lost.len(),
				HexDisplay::from(*first)
			);
		}
	}
	bench.metrics.check("final", "ok", ok);
	bench.metrics.check("final", "catching_up", owed);
	bench.metrics.check("final", "undelivered", undelivered.len() as u64);
	if let Some(first) = undelivered.iter().next() {
		warn!(
			"{} probes never reached a replica that was online at their deadline, e.g. {}",
			undelivered.len(),
			HexDisplay::from(first)
		);
	}
	complete
}
