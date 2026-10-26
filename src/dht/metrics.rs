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
	register, Counter, CounterVec, HistogramOpts, HistogramVec, Opts, Registry, U64,
};

const LATENCY_BUCKETS: [f64; 12] =
	[0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0, 20.0, 30.0, 60.0];

pub struct Metrics {
	registry: Registry,
	submit: CounterVec<U64>,
	load_skipped: Counter<U64>,
	checks: CounterVec<U64>,
	resubscriptions: Counter<U64>,
	replica_seconds: HistogramVec,
	all_replicas_seconds: HistogramVec,
}

fn counter_vec(
	registry: &Registry,
	name: &str,
	help: &str,
	labels: &[&str],
) -> Result<CounterVec<U64>> {
	Ok(register(CounterVec::new(Opts::new(name, help), labels)?, registry)?)
}

fn latency(registry: &Registry, name: &str, help: &str) -> Result<HistogramVec> {
	Ok(register(
		HistogramVec::new(
			HistogramOpts::new(name, help).buckets(LATENCY_BUCKETS.to_vec()),
			&["step"],
		)?,
		registry,
	)?)
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
				"Probe checks (delivery, restart, final), by check and result.",
				&["check", "result"],
			)?,
			resubscriptions: register(
				Counter::new(
					"statement_dht_resubscriptions_total",
					"Probe subscriptions opened again after a node dropped them.",
				)?,
				&registry,
			)?,
			replica_seconds: latency(
				&registry,
				"statement_dht_probe_replica_seconds",
				"Time from a probe's submission to its storage on one replica, by load step.",
			)?,
			all_replicas_seconds: latency(
				&registry,
				"statement_dht_probe_all_replicas_seconds",
				"Time from a probe's submission to its storage on all K replicas, by load step.",
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

	pub fn check(&self, check: &str, result: &str, count: u64) {
		self.checks.with_label_values(&[check, result]).inc_by(count);
	}

	pub fn resubscribed(&self) {
		self.resubscriptions.inc();
	}

	pub fn replica_latency(&self, step: &str, latency: Duration) {
		self.replica_seconds.with_label_values(&[step]).observe(latency.as_secs_f64());
	}

	pub fn all_replicas_latency(&self, step: &str, latency: Duration) {
		self.all_replicas_seconds
			.with_label_values(&[step])
			.observe(latency.as_secs_f64());
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

	/// Probes checked for delivery, whatever the result.
	pub fn deliveries_checked(&self) -> u64 {
		["ok", "excused", "violation"]
			.iter()
			.map(|result| self.checks.with_label_values(&["delivery", result]).get())
			.sum()
	}

	/// Outcomes that fail the run: a failed probe check, and a statement a node refused to store.
	pub fn violations(&self) -> u64 {
		let checks = [
			("delivery", "violation"),
			("restart", "violation"),
			("final", "lost"),
			("final", "undelivered"),
		]
		.iter()
		.map(|(check, result)| self.checks.with_label_values(&[check, result]).get())
		.sum::<u64>();
		let rejected = ["load", "probe"]
			.iter()
			.map(|class| self.submit.with_label_values(&[class, "rejected"]).get())
			.sum::<u64>();
		checks + rejected
	}
}
