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

//! Probes, late reads and the audits that check them.
//!
//! Probes come from their own accounts at a low rate, so no allowance ever evicts them, and must
//! sit on exactly their K XOR-closest nodes once `placement` old and again once `persistence` old.
//! A late read writes through the closest non-replica, then subscribes on the farthest node: that
//! node holds no copy, as forwarding only moves a statement closer to its topic, so the statement
//! must come from the replicas through the reader's affinity filter. Once the reader unsubscribes,
//! its node must drop the statement within `drop`.
//!
//! Audits read every node store through a match-all subscription, which names no topic and leaves
//! the DHT untouched; a topic subscription would change the node's affinity filter.

use super::{build_statement, node::by_distance, topic, Bench};
use anyhow::{anyhow, Result};
use codec::Encode;
use futures::StreamExt;
use log::{info, warn};
use sp_core::{blake2_256, hexdisplay::HexDisplay};
use sp_statement_store::{StatementEvent, SubmitResult, TopicFilter};
use std::{
	collections::{HashMap, HashSet},
	ops::Range,
	sync::Arc,
	time::{Duration, Instant},
};
use tokio::sync::{watch, Mutex};

/// Node stores read at once during an audit.
const AUDIT_CONCURRENCY: usize = 8;

pub struct Probe {
	encoded: Vec<u8>,
	replicas: Vec<usize>,
	entry: usize,
	submitted: Instant,
	placement_checked: bool,
	persistence_checked: bool,
}

pub struct LateRead {
	encoded: Vec<u8>,
	reader: usize,
	released: Instant,
}

#[derive(Default)]
pub struct Tracker {
	probes: Vec<Probe>,
	reads: Vec<LateRead>,
}

#[derive(Clone)]
pub struct Ages {
	pub placement: Duration,
	pub persistence: Duration,
	pub drop: Duration,
}

pub async fn probes(
	bench: Arc<Bench>,
	tracker: Arc<Mutex<Tracker>>,
	accounts: Range<usize>,
	interval: Duration,
	size: usize,
	validity: Duration,
	mut stop: watch::Receiver<bool>,
) {
	let mut ticker = tokio::time::interval(interval);
	for n in 0u64.. {
		tokio::select! {
			_ = ticker.tick() => {},
			_ = stop.changed() => return,
		}
		let topic = topic(bench.run_id, "probe", n);
		let order = by_distance(&bench.dht_keys, &topic);
		let entry = (n % bench.nodes.len() as u64) as usize;
		let account = accounts.start + (n % accounts.len() as u64) as usize;
		let statement = build_statement(&bench.accounts[account], topic, validity, n as u32, size);
		let result = bench.nodes[entry].submit(&statement).await;
		bench.metrics.submit("probe", &result);
		if !matches!(result, Ok(SubmitResult::New)) {
			warn!("Probe not accepted by {}: {result:?}", bench.nodes[entry].endpoint);
			continue;
		}
		tracker.lock().await.probes.push(Probe {
			encoded: statement.encode(),
			replicas: order[..bench.replication_factor].to_vec(),
			entry,
			submitted: Instant::now(),
			placement_checked: false,
			persistence_checked: false,
		});
	}
}

pub struct LateReadConfig {
	pub accounts: Range<usize>,
	pub interval: Duration,
	pub delay: Duration,
	pub timeout: Duration,
	pub size: usize,
	pub validity: Duration,
}

enum ReadOutcome {
	Delivered(Duration),
	/// The reader's node already held the statement, so the read proved nothing about the DHT.
	LocalCopy,
	Missed,
}

pub async fn late_reads(
	bench: Arc<Bench>,
	tracker: Arc<Mutex<Tracker>>,
	reads: LateReadConfig,
	mut stop: watch::Receiver<bool>,
) {
	let reads = Arc::new(reads);
	let mut ticker = tokio::time::interval(reads.interval);
	for n in 0u64.. {
		tokio::select! {
			_ = ticker.tick() => {},
			_ = stop.changed() => return,
		}
		let topic = topic(bench.run_id, "late-read", n);
		let order = by_distance(&bench.dht_keys, &topic);
		let (entry, reader) = (order[bench.replication_factor], order[order.len() - 1]);
		let account = reads.accounts.start + (n % reads.accounts.len() as u64) as usize;
		let statement =
			build_statement(&bench.accounts[account], topic, reads.validity, n as u32, reads.size);
		let result = bench.nodes[entry].submit(&statement).await;
		bench.metrics.submit("late_read", &result);
		if !matches!(result, Ok(SubmitResult::New)) {
			warn!("Late read not accepted by {}: {result:?}", bench.nodes[entry].endpoint);
			continue;
		}
		let (bench, tracker, reads) = (bench.clone(), tracker.clone(), reads.clone());
		tokio::spawn(async move {
			tokio::time::sleep(reads.delay).await;
			let encoded = statement.encode();
			let outcome = wait_for_statement(&bench, reader, topic, &encoded, reads.timeout).await;
			let hash = HexDisplay::from(&blake2_256(&encoded)).to_string();
			let (entry, reader_endpoint) =
				(&bench.nodes[entry].endpoint, &bench.nodes[reader].endpoint);
			match outcome {
				Ok(ReadOutcome::Delivered(latency)) => {
					bench.metrics.late_read("delivered", Some(latency));
					tracker.lock().await.reads.push(LateRead {
						encoded,
						reader,
						released: Instant::now(),
					});
				},
				Ok(ReadOutcome::LocalCopy) => {
					bench.metrics.late_read("local_copy", None);
					warn!(
						"Late read {hash}: {reader_endpoint} already held it, written via {entry}"
					);
				},
				Ok(ReadOutcome::Missed) => {
					bench.metrics.late_read("missed", None);
					warn!(
						"Late read {hash}: {reader_endpoint} got nothing within {:?}, written via {entry}",
						reads.timeout
					);
				},
				Err(e) => {
					bench.metrics.late_read("error", None);
					warn!("Late read {hash} on {reader_endpoint} failed: {e:#}");
				},
			}
		});
	}
}

/// Subscribe on `reader` to the statement's topic and wait for the statement. The subscription
/// ends on return, which releases the reader node's affinity for the topic.
async fn wait_for_statement(
	bench: &Bench,
	reader: usize,
	topic: [u8; 32],
	encoded: &[u8],
	timeout: Duration,
) -> Result<ReadOutcome> {
	let filter = TopicFilter::MatchAny(
		vec![topic.into()]
			.try_into()
			.map_err(|_| anyhow!("one topic exceeds the filter bound"))?,
	);
	let started = Instant::now();
	let mut subscription = bench.nodes[reader].subscribe(filter).await?;
	let deadline = tokio::time::Instant::from_std(started + timeout);
	loop {
		let event = match tokio::time::timeout_at(deadline, subscription.next()).await {
			Err(_) => return Ok(ReadOutcome::Missed),
			Ok(None) => return Err(anyhow!("subscription closed")),
			Ok(Some(Err(e))) => return Err(anyhow!("subscription failed: {e}")),
			Ok(Some(Ok(event))) => event,
		};
		let StatementEvent::NewStatements { statements, remaining } = event;
		if statements.iter().any(|statement| statement.0 == encoded) {
			// Only the replay of the node's own store carries `remaining`.
			return Ok(match remaining {
				Some(_) => ReadOutcome::LocalCopy,
				None => ReadOutcome::Delivered(started.elapsed()),
			});
		}
	}
}

pub async fn audits(
	bench: Arc<Bench>,
	tracker: Arc<Mutex<Tracker>>,
	interval: Duration,
	ages: Ages,
	mut stop: watch::Receiver<bool>,
) {
	let start = tokio::time::Instant::now() + interval;
	let mut ticker = tokio::time::interval_at(start, interval);
	loop {
		tokio::select! {
			_ = ticker.tick() => {},
			_ = stop.changed() => return,
		}
		audit(&bench, &tracker, &ages).await;
	}
}

enum Check {
	Placement(usize),
	Persistence(usize),
	Drop(usize),
}

/// The checks due at `now`, by index into the tracker.
fn due_checks(tracker: &Tracker, now: Instant, ages: &Ages) -> Vec<Check> {
	let mut checks = Vec::new();
	for (idx, probe) in tracker.probes.iter().enumerate() {
		let age = now.saturating_duration_since(probe.submitted);
		if !probe.placement_checked && age >= ages.placement {
			checks.push(Check::Placement(idx));
		}
		if !probe.persistence_checked && age >= ages.persistence {
			checks.push(Check::Persistence(idx));
		}
	}
	for (idx, read) in tracker.reads.iter().enumerate() {
		if now.saturating_duration_since(read.released) >= ages.drop {
			checks.push(Check::Drop(idx));
		}
	}
	checks
}

/// Replicas without the statement and other nodes holding it.
fn placement_violations(
	replicas: &[usize],
	holders: &HashSet<usize>,
	nodes: usize,
) -> (Vec<usize>, Vec<usize>) {
	let missing = replicas.iter().copied().filter(|node| !holders.contains(node)).collect();
	let extra = (0..nodes)
		.filter(|node| holders.contains(node) && !replicas.contains(node))
		.collect();
	(missing, extra)
}

/// Read every node store and run the checks due now. A failed read leaves the checks for the next
/// audit and returns `false`.
///
/// The tracker stays unlocked while the stores are read, so probes and late reads go on. Only an
/// audit removes entries and audits never overlap, so the indices of the due checks stay valid.
pub async fn audit(bench: &Bench, tracker: &Mutex<Tracker>, ages: &Ages) -> bool {
	let started = Instant::now();
	let (checks, wanted) = {
		let tracker = tracker.lock().await;
		let checks = due_checks(&tracker, started, ages);
		let mut wanted: HashMap<Vec<u8>, usize> = HashMap::new();
		for check in &checks {
			let encoded = match *check {
				Check::Placement(idx) | Check::Persistence(idx) => &tracker.probes[idx].encoded,
				Check::Drop(idx) => &tracker.reads[idx].encoded,
			};
			let next = wanted.len();
			wanted.entry(encoded.clone()).or_insert(next);
		}
		(checks, wanted)
	};
	if checks.is_empty() {
		return true;
	}

	let wanted = &wanted;
	let reads = futures::stream::iter(0..bench.nodes.len())
		.map(|idx| async move {
			let mut found = Vec::new();
			bench.nodes[idx]
				.read_store(|statement| found.extend(wanted.get(statement).copied()))
				.await?;
			Ok::<_, anyhow::Error>((idx, found))
		})
		.buffer_unordered(AUDIT_CONCURRENCY)
		.collect::<Vec<_>>()
		.await;
	let mut holders = vec![HashSet::new(); wanted.len()];
	for read in reads {
		match read {
			Ok((node, found)) => {
				for statement in found {
					holders[statement].insert(node);
				}
			},
			Err(e) => {
				bench.metrics.audit(false);
				warn!("Audit postponed: {e:#}");
				return false;
			},
		}
	}

	let mut tracker = tracker.lock().await;
	for check in &checks {
		match *check {
			Check::Placement(idx) | Check::Persistence(idx) => {
				let name =
					if matches!(check, Check::Placement(_)) { "placement" } else { "persistence" };
				let probe = &tracker.probes[idx];
				let (missing, extra) = placement_violations(
					&probe.replicas,
					&holders[wanted[&probe.encoded]],
					bench.nodes.len(),
				);
				let exact = missing.is_empty() && extra.is_empty();
				bench.metrics.check(name, exact);
				if !exact {
					let endpoints = |nodes: &[usize]| -> Vec<&str> {
						nodes.iter().map(|&n| bench.nodes[n].endpoint.as_str()).collect()
					};
					warn!(
						"Probe {} {name} at {:?} old, written via {}: missing on {:?}, extra on {:?}",
						HexDisplay::from(&blake2_256(&probe.encoded)),
						started.saturating_duration_since(probe.submitted),
						bench.nodes[probe.entry].endpoint,
						endpoints(&missing),
						endpoints(&extra),
					);
				}
			},
			Check::Drop(idx) => {
				let read = &tracker.reads[idx];
				let retained = holders[wanted[&read.encoded]].contains(&read.reader);
				bench.metrics.check("drop", !retained);
				if retained {
					warn!(
						"Late read {}: {} still holds it {:?} after the subscription ended",
						HexDisplay::from(&blake2_256(&read.encoded)),
						bench.nodes[read.reader].endpoint,
						started.saturating_duration_since(read.released),
					);
				}
			},
		}
	}

	for check in &checks {
		match *check {
			Check::Placement(idx) => tracker.probes[idx].placement_checked = true,
			Check::Persistence(idx) => tracker.probes[idx].persistence_checked = true,
			Check::Drop(_) => {},
		}
	}
	// Drop exactly the reads `due_checks` picked at `started`.
	tracker
		.reads
		.retain(|read| read.released > started || started - read.released < ages.drop);
	tracker
		.probes
		.retain(|probe| !(probe.placement_checked && probe.persistence_checked));

	bench.metrics.audit(true);
	info!(
		"Audit of {} checks over {} nodes in {:?}, {} violations so far",
		checks.len(),
		bench.nodes.len(),
		started.elapsed(),
		bench.metrics.violations(),
	);
	true
}
