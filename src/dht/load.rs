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

//! The stepped statement load: the accounts submit in turn, round-robin over the nodes, each
//! statement on a fresh topic and without a channel. The rate follows the configured steps and
//! drops to zero after the last one.

use super::{build_statement, topic, Bench};
use anyhow::{anyhow, Context};
use std::{
	ops::Range,
	str::FromStr,
	sync::Arc,
	time::{Duration, Instant},
};
use tokio::sync::{watch, Semaphore};

/// How often the load tops up the statements due so far.
const PACE: Duration = Duration::from_millis(10);

/// A load step: `rate` statements per second across all accounts for `secs` seconds.
#[derive(Clone, Debug)]
pub struct Step {
	pub rate: u32,
	pub secs: u64,
}

impl FromStr for Step {
	type Err = anyhow::Error;

	/// Parse `rate:secs`.
	fn from_str(s: &str) -> anyhow::Result<Self> {
		let (rate, secs) =
			s.split_once(':').ok_or_else(|| anyhow!("load step {s:?} is not rate:secs"))?;
		Ok(Self {
			rate: rate.parse().with_context(|| format!("load step {s:?}: bad rate"))?,
			secs: secs.parse().with_context(|| format!("load step {s:?}: bad seconds"))?,
		})
	}
}

#[derive(Clone, Debug)]
pub struct Schedule(pub Vec<Step>);

impl Schedule {
	/// The index of the step `elapsed` into the load, `None` after the last step.
	pub fn step_at(&self, elapsed: Duration) -> Option<usize> {
		let mut end = Duration::ZERO;
		self.0.iter().position(|step| {
			end += Duration::from_secs(step.secs);
			elapsed < end
		})
	}

	/// The rate `elapsed` into the load, zero after the last step.
	pub fn rate_at(&self, elapsed: Duration) -> u32 {
		self.step_at(elapsed).map_or(0, |idx| self.0[idx].rate)
	}

	/// How reports name a step: its number and rate, or `idle` after the load.
	pub fn label(&self, step: Option<usize>) -> String {
		match step {
			Some(idx) => format!("{}:{}/s", idx + 1, self.0[idx].rate),
			None => "idle".to_string(),
		}
	}

	/// Statements due `elapsed` into the load.
	pub fn due_at(&self, elapsed: Duration) -> u64 {
		let mut left = elapsed.as_secs_f64();
		let mut due = 0.0;
		for step in &self.0 {
			let secs = left.min(step.secs as f64);
			due += secs * step.rate as f64;
			left -= secs;
		}
		due as u64
	}

	pub fn total(&self) -> u64 {
		self.0.iter().map(|step| step.rate as u64 * step.secs).sum()
	}

	pub fn secs(&self) -> u64 {
		self.0.iter().map(|step| step.secs).sum()
	}
}

pub struct Load {
	pub accounts: Range<usize>,
	pub schedule: Schedule,
	pub size: usize,
	pub validity: Duration,
	pub max_in_flight: usize,
}

pub async fn run(bench: Arc<Bench>, load: Load, started: Instant, mut stop: watch::Receiver<bool>) {
	let in_flight = Arc::new(Semaphore::new(load.max_in_flight));
	let mut ticker = tokio::time::interval(PACE);
	let mut sent = 0u64;
	loop {
		tokio::select! {
			_ = ticker.tick() => {},
			_ = stop.changed() => return,
		}
		let elapsed = started.elapsed();
		let due = load.schedule.due_at(elapsed);
		let (resume, skipped) = resume_from(sent, due, load.schedule.rate_at(elapsed) as f64);
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
