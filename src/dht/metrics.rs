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

//! Prometheus metrics of `statement-dht-bench`.

use anyhow::Result;
use sp_statement_store::SubmitResult;
use std::{net::SocketAddr, time::Duration};
use substrate_prometheus_endpoint::{
	prometheus::{Encoder, TextEncoder},
	register, Counter, CounterVec, Histogram, HistogramOpts, Opts, Registry, U64,
};

pub struct Metrics {
	registry: Registry,
	submit: CounterVec<U64>,
	load_skipped: Counter<U64>,
	checks: CounterVec<U64>,
	late_reads: CounterVec<U64>,
	late_read_seconds: Histogram,
	audits: CounterVec<U64>,
}

fn counter_vec(
	registry: &Registry,
	name: &str,
	help: &str,
	labels: &[&str],
) -> Result<CounterVec<U64>> {
	Ok(register(CounterVec::new(Opts::new(name, help), labels)?, registry)?)
}

impl Metrics {
	pub fn new() -> Result<Self> {
		let registry = Registry::new();
		Ok(Self {
			submit: counter_vec(
				&registry,
				"statement_dht_submit_total",
				"Statements submitted, by traffic class and node reply.",
				&["class", "result"],
			)?,
			load_skipped: register(
				Counter::new(
					"statement_dht_load_skipped_total",
					"Load statements skipped because the bench fell more than a second behind.",
				)?,
				&registry,
			)?,
			checks: counter_vec(
				&registry,
				"statement_dht_checks_total",
				"Checks of node stores (placement, persistence, drop), by check and result.",
				&["check", "result"],
			)?,
			late_reads: counter_vec(
				&registry,
				"statement_dht_late_reads_total",
				"Late reads, by result.",
				&["result"],
			)?,
			late_read_seconds: register(
				Histogram::with_opts(
					HistogramOpts::new(
						"statement_dht_late_read_seconds",
						"Time from the late reader's subscription to the statement's arrival.",
					)
					.buckets(vec![0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0]),
				)?,
				&registry,
			)?,
			audits: counter_vec(
				&registry,
				"statement_dht_audits_total",
				"Audits of every node store, by result.",
				&["result"],
			)?,
			registry,
		})
	}

	pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
		substrate_prometheus_endpoint::init_prometheus(addr, self.registry.clone())
			.await
			.map_err(|e| anyhow::anyhow!("prometheus exposition server failed: {e}"))
	}

	pub fn submit(&self, class: &str, result: &Result<SubmitResult>) {
		let result = match result {
			Ok(SubmitResult::New) => "new",
			Ok(SubmitResult::Known) => "known",
			Ok(SubmitResult::KnownExpired) => "known_expired",
			Ok(SubmitResult::Rejected(_)) => "rejected",
			Ok(SubmitResult::Invalid(_)) => "invalid",
			Ok(SubmitResult::InternalError(_)) => "internal_error",
			Err(_) => "rpc_error",
		};
		self.submit.with_label_values(&[class, result]).inc();
	}

	pub fn load_skipped(&self, count: u64) {
		self.load_skipped.inc_by(count);
	}

	pub fn check(&self, check: &str, ok: bool) {
		self.checks
			.with_label_values(&[check, if ok { "ok" } else { "violation" }])
			.inc();
	}

	pub fn late_read(&self, result: &str, latency: Option<Duration>) {
		self.late_reads.with_label_values(&[result]).inc();
		if let Some(latency) = latency {
			self.late_read_seconds.observe(latency.as_secs_f64());
		}
	}

	pub fn audit(&self, ok: bool) {
		self.audits.with_label_values(&[if ok { "ok" } else { "failed" }]).inc();
	}

	/// Every series but the histogram buckets, one `name{labels} value` per line.
	pub fn summary(&self) -> Result<String> {
		let mut text = Vec::new();
		TextEncoder::new().encode(&self.registry.gather(), &mut text)?;
		Ok(String::from_utf8(text)?
			.lines()
			.filter(|line| !line.starts_with('#') && !line.contains("_bucket{"))
			.collect::<Vec<_>>()
			.join("\n"))
	}

	/// Probes checked for placement, whatever the result.
	pub fn placements_checked(&self) -> u64 {
		["ok", "violation"]
			.iter()
			.map(|result| self.checks.with_label_values(&["placement", result]).get())
			.sum()
	}

	/// Outcomes that fail the run: a failed store check, and a late read that did not come
	/// through the DHT.
	pub fn violations(&self) -> u64 {
		let checks = ["placement", "persistence", "drop"]
			.iter()
			.map(|check| self.checks.with_label_values(&[check, "violation"]).get())
			.sum::<u64>();
		let reads = ["missed", "local_copy", "error"]
			.iter()
			.map(|result| self.late_reads.with_label_values(&[result]).get())
			.sum::<u64>();
		checks + reads
	}
}
