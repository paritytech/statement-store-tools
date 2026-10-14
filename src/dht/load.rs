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

//! The constant statement load: every account submits at the same rate, round-robin over the
//! nodes, each statement on a fresh topic and without a channel, so the account allowance evicts
//! the oldest one.

use super::{build_statement, topic, Bench};
use std::{
	ops::Range,
	sync::Arc,
	time::{Duration, Instant},
};
use tokio::sync::{watch, Semaphore};

/// How often the load tops up the statements due so far.
const PACE: Duration = Duration::from_millis(10);

pub struct Load {
	pub accounts: Range<usize>,
	/// Statements per second across all accounts.
	pub rate: f64,
	pub size: usize,
	pub validity: Duration,
	pub max_in_flight: usize,
}

pub async fn run(bench: Arc<Bench>, load: Load, mut stop: watch::Receiver<bool>) {
	let in_flight = Arc::new(Semaphore::new(load.max_in_flight));
	let mut ticker = tokio::time::interval(PACE);
	let started = Instant::now();
	let mut sent = 0u64;
	loop {
		tokio::select! {
			_ = ticker.tick() => {},
			_ = stop.changed() => return,
		}
		let due = (started.elapsed().as_secs_f64() * load.rate) as u64;
		let (resume, skipped) = resume_from(sent, due, load.rate);
		bench.metrics.load_skipped(skipped);
		sent = resume;
		while sent < due {
			let permit = in_flight
				.clone()
				.acquire_owned()
				.await
				.expect("the semaphore is never closed; qed");
			let n = sent;
			sent += 1;
			let bench = bench.clone();
			let account = load.accounts.start + (n % load.accounts.len() as u64) as usize;
			let (size, validity) = (load.size, load.validity);
			tokio::spawn(async move {
				let topic = topic(bench.run_id, "load", n);
				let statement =
					build_statement(&bench.accounts[account], topic, validity, n as u32, size);
				let node = &bench.nodes[(n % bench.nodes.len() as u64) as usize];
				let result = node.submit(&statement).await;
				bench.metrics.submit("load", &result);
				drop(permit);
			});
		}
	}
}

/// Where to resume from `sent` when `due` statements are due: a backlog beyond one second of load
/// is skipped instead of burst out, so the offered rate stays the configured one. Returns the
/// resume point and the number skipped.
fn resume_from(sent: u64, due: u64, rate: f64) -> (u64, u64) {
	let max_backlog = rate.ceil() as u64;
	match due.saturating_sub(sent).checked_sub(max_backlog) {
		Some(skipped) if skipped > 0 => (sent + skipped, skipped),
		_ => (sent, 0),
	}
}
