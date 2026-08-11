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

//! Prometheus metrics for the `statement-ops-bench` `loop` subcommand.
//!
//! [`PrometheusMetrics`] is a concrete recorder (no trait, no `Option<_>`
//! plumbing) constructed once at startup and threaded into each phase
//! function. Phases call `observe_*` / `inc_*` methods at each sample site.
//! Tests construct a throwaway recorder with [`PrometheusMetrics::for_tests`].
//!
//! [`serve`] is a thin wrapper around `substrate_prometheus_endpoint::
//! init_prometheus` that binds a hyper exposition server to a socket address.

use anyhow::{Context, Result};
use std::{net::SocketAddr, time::Duration};
use substrate_prometheus_endpoint::{
	register, CounterVec, HistogramOpts, HistogramVec, Opts, Registry, U64,
};

/// Default histogram buckets in seconds. Covers ~5ms to 10s, suitable for
/// statement-store RPC latency.
pub const DEFAULT_BUCKETS_SECS: &[f64] =
	&[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

const OUTCOME_LABEL: &str = "outcome";
const OUTCOME_SUCCESS: &str = "success";
const OUTCOME_FAILURE: &str = "failure";
const OUTCOME_COMPLETED: &str = "completed";
const OUTCOME_FAILED: &str = "failed";

/// Outcome of a single sample, mapped to the `outcome` label value.
#[derive(Debug, Clone, Copy)]
pub enum Outcome {
	Success,
	Failure,
}

impl Outcome {
	fn as_label(self) -> &'static str {
		match self {
			Self::Success => OUTCOME_SUCCESS,
			Self::Failure => OUTCOME_FAILURE,
		}
	}
}

/// Outcome of a whole loop iteration. Distinct from [`Outcome`] so phase-
/// level and loop-level labels don't share semantics by accident.
#[derive(Debug, Clone, Copy)]
pub enum LoopOutcome {
	Completed,
	Failed,
}

impl LoopOutcome {
	fn as_label(self) -> &'static str {
		match self {
			Self::Completed => OUTCOME_COMPLETED,
			Self::Failed => OUTCOME_FAILED,
		}
	}
}

/// Concrete Prometheus recorder shared by all phases of one loop run.
pub struct PrometheusMetrics {
	registry: Registry,
	submit_hist: HistogramVec,
	prop_submit_hist: HistogramVec,
	prop_full_hist: HistogramVec,
	subscribe_hist: HistogramVec,
	submit_ctr: CounterVec<U64>,
	prop_ctr: CounterVec<U64>,
	subscribe_ctr: CounterVec<U64>,
	iterations_ctr: CounterVec<U64>,
	reconnects_ctr: CounterVec<U64>,
}

impl PrometheusMetrics {
	/// Construct a new recorder, registering every metric family on a fresh
	/// [`Registry`]. Fails only if a name collision is somehow produced — in
	/// practice this only errors if `buckets` is empty (`HistogramOpts`
	/// rejects an empty bucket list).
	pub fn new(buckets: Vec<f64>) -> Result<Self> {
		let registry = Registry::new();

		let submit_hist = register(
			HistogramVec::new(
				HistogramOpts::new(
					"statement_ops_submit_duration_seconds",
					"`statement_submit` RPC duration observed by the submit phase.",
				)
				.buckets(buckets.clone()),
				&["endpoint"],
			)
			.context("failed to build submit_duration histogram")?,
			&registry,
		)
		.context("failed to register submit_duration histogram")?;

		let prop_submit_hist = register(
			HistogramVec::new(
				HistogramOpts::new(
					"statement_ops_propagation_submit_duration_seconds",
					"Submit-RPC portion of the propagation phase, per submit endpoint.",
				)
				.buckets(buckets.clone()),
				&["endpoint"],
			)
			.context("failed to build prop_submit histogram")?,
			&registry,
		)
		.context("failed to register prop_submit histogram")?;

		let prop_full_hist = register(
			HistogramVec::new(
				HistogramOpts::new(
					"statement_ops_propagation_full_duration_seconds",
					"Full submit→receive latency for each (submit, subscribe) endpoint pair.",
				)
				.buckets(buckets.clone()),
				&["submit_endpoint", "subscribe_endpoint"],
			)
			.context("failed to build prop_full histogram")?,
			&registry,
		)
		.context("failed to register prop_full histogram")?;

		let subscribe_hist = register(
			HistogramVec::new(
				HistogramOpts::new(
					"statement_ops_subscribe_read_duration_seconds",
					"Retrieval latency for the subscribe phase's read step, per endpoint.",
				)
				.buckets(buckets),
				&["endpoint"],
			)
			.context("failed to build subscribe_read histogram")?,
			&registry,
		)
		.context("failed to register subscribe_read histogram")?;

		let submit_ctr = register(
			CounterVec::new(
				Opts::new(
					"statement_ops_submit_total",
					"Total submit-phase attempts, partitioned by outcome.",
				),
				&["endpoint", OUTCOME_LABEL],
			)
			.context("failed to build submit_total counter")?,
			&registry,
		)
		.context("failed to register submit_total counter")?;

		let prop_ctr = register(
			CounterVec::new(
				Opts::new(
					"statement_ops_propagation_total",
					"Total propagation-phase attempts per pair, partitioned by outcome.",
				),
				&["submit_endpoint", "subscribe_endpoint", OUTCOME_LABEL],
			)
			.context("failed to build propagation_total counter")?,
			&registry,
		)
		.context("failed to register propagation_total counter")?;

		let subscribe_ctr = register(
			CounterVec::new(
				Opts::new(
					"statement_ops_subscribe_total",
					"Total subscribe-phase read attempts, partitioned by outcome.",
				),
				&["endpoint", OUTCOME_LABEL],
			)
			.context("failed to build subscribe_total counter")?,
			&registry,
		)
		.context("failed to register subscribe_total counter")?;

		let iterations_ctr = register(
			CounterVec::new(
				Opts::new(
					"statement_ops_loop_iterations_total",
					"Total loop iterations, partitioned by outcome.",
				),
				&[OUTCOME_LABEL],
			)
			.context("failed to build iterations_total counter")?,
			&registry,
		)
		.context("failed to register iterations_total counter")?;

		let reconnects_ctr = register(
			CounterVec::new(
				Opts::new(
					"statement_ops_reconnects_total",
					"Per-iteration reconnect attempts when --new-connection-per-iteration is on.",
				),
				&[OUTCOME_LABEL],
			)
			.context("failed to build reconnects_total counter")?,
			&registry,
		)
		.context("failed to register reconnects_total counter")?;

		Ok(Self {
			registry,
			submit_hist,
			prop_submit_hist,
			prop_full_hist,
			subscribe_hist,
			submit_ctr,
			prop_ctr,
			subscribe_ctr,
			iterations_ctr,
			reconnects_ctr,
		})
	}

	/// Convenience constructor for tests — uses the default bucket set.
	#[cfg(test)]
	pub fn for_tests() -> Self {
		Self::new(DEFAULT_BUCKETS_SECS.to_vec()).expect("default buckets always valid")
	}

	/// Return the registry — used to spawn the exposition HTTP server.
	pub fn registry(&self) -> &Registry {
		&self.registry
	}

	// ---- submit phase --------------------------------------------------

	pub fn observe_submit(&self, endpoint: &str, dur: Duration) {
		self.submit_hist.with_label_values(&[endpoint]).observe(dur.as_secs_f64());
	}

	pub fn inc_submit(&self, endpoint: &str, outcome: Outcome) {
		self.submit_ctr.with_label_values(&[endpoint, outcome.as_label()]).inc();
	}

	// ---- propagation phase ---------------------------------------------

	pub fn observe_prop_submit(&self, endpoint: &str, dur: Duration) {
		self.prop_submit_hist.with_label_values(&[endpoint]).observe(dur.as_secs_f64());
	}

	pub fn observe_prop_full(
		&self,
		submit_endpoint: &str,
		subscribe_endpoint: &str,
		dur: Duration,
	) {
		self.prop_full_hist
			.with_label_values(&[submit_endpoint, subscribe_endpoint])
			.observe(dur.as_secs_f64());
	}

	pub fn inc_prop(&self, submit_endpoint: &str, subscribe_endpoint: &str, outcome: Outcome) {
		self.prop_ctr
			.with_label_values(&[submit_endpoint, subscribe_endpoint, outcome.as_label()])
			.inc();
	}

	// ---- subscribe phase -----------------------------------------------

	pub fn observe_subscribe_read(&self, endpoint: &str, dur: Duration) {
		self.subscribe_hist.with_label_values(&[endpoint]).observe(dur.as_secs_f64());
	}

	pub fn inc_subscribe(&self, endpoint: &str, outcome: Outcome) {
		self.subscribe_ctr.with_label_values(&[endpoint, outcome.as_label()]).inc();
	}

	// ---- loop-level ----------------------------------------------------

	pub fn inc_iteration(&self, outcome: LoopOutcome) {
		self.iterations_ctr.with_label_values(&[outcome.as_label()]).inc();
	}

	pub fn inc_reconnect(&self, outcome: Outcome) {
		self.reconnects_ctr.with_label_values(&[outcome.as_label()]).inc();
	}
}

/// Parse a comma-separated bucket list, e.g. `"0.005,0.01,0.025"`. Buckets
/// must be finite, strictly positive, and strictly increasing. An empty
/// string falls back to [`DEFAULT_BUCKETS_SECS`].
pub fn parse_buckets(input: &str) -> Result<Vec<f64>> {
	let trimmed = input.trim();
	if trimmed.is_empty() {
		return Ok(DEFAULT_BUCKETS_SECS.to_vec());
	}
	let mut out = Vec::new();
	for raw in trimmed.split(',') {
		let part = raw.trim();
		let v: f64 = part.parse().with_context(|| format!("invalid bucket value '{part}'"))?;
		anyhow::ensure!(v.is_finite(), "bucket '{part}' is not finite");
		anyhow::ensure!(v > 0.0, "bucket '{part}' must be strictly positive");
		if let Some(&last) = out.last() {
			anyhow::ensure!(v > last, "buckets must be strictly increasing (saw {last} then {v})",);
		}
		out.push(v);
	}
	anyhow::ensure!(!out.is_empty(), "at least one bucket value required");
	Ok(out)
}

/// Spawn an HTTP exposition server on `addr` serving `/metrics` from the
/// recorder's registry. The returned future runs until the underlying
/// hyper server exits (typically: never, until cancelled).
pub async fn serve(addr: SocketAddr, registry: Registry) -> Result<()> {
	substrate_prometheus_endpoint::init_prometheus(addr, registry)
		.await
		.map_err(|e| anyhow::anyhow!("prometheus exposition server failed: {e}"))
}

#[cfg(test)]
mod tests {
	use super::*;
	use substrate_prometheus_endpoint::prometheus::{Encoder, TextEncoder};

	fn encode(m: &PrometheusMetrics) -> String {
		let mut buf = Vec::new();
		TextEncoder::new().encode(&m.registry().gather(), &mut buf).expect("encode");
		String::from_utf8(buf).expect("utf8")
	}

	#[test]
	fn parse_buckets_default_on_empty() {
		assert_eq!(parse_buckets("").unwrap(), DEFAULT_BUCKETS_SECS.to_vec());
		assert_eq!(parse_buckets("   ").unwrap(), DEFAULT_BUCKETS_SECS.to_vec());
	}

	#[test]
	fn parse_buckets_happy_path() {
		assert_eq!(parse_buckets("0.01,0.1,1.0").unwrap(), vec![0.01, 0.1, 1.0]);
		// whitespace and trailing-spaces tolerated
		assert_eq!(parse_buckets(" 0.01 , 0.1 ").unwrap(), vec![0.01, 0.1]);
	}

	#[test]
	fn parse_buckets_rejects_non_increasing() {
		assert!(parse_buckets("0.1,0.05").is_err());
		assert!(parse_buckets("0.1,0.1").is_err());
	}

	#[test]
	fn parse_buckets_rejects_non_positive() {
		assert!(parse_buckets("0").is_err());
		assert!(parse_buckets("-0.1").is_err());
	}

	#[test]
	fn parse_buckets_rejects_garbage() {
		assert!(parse_buckets("not_a_number").is_err());
		assert!(parse_buckets("inf").is_err());
		assert!(parse_buckets("nan").is_err());
	}

	#[test]
	fn registered_families_appear_in_encoded_output() {
		let m = PrometheusMetrics::for_tests();
		// Trigger one observation/inc per family so each family has a series.
		m.observe_submit("ws://a", Duration::from_millis(10));
		m.inc_submit("ws://a", Outcome::Success);
		m.observe_prop_submit("ws://a", Duration::from_millis(11));
		m.observe_prop_full("ws://a", "ws://b", Duration::from_millis(20));
		m.inc_prop("ws://a", "ws://b", Outcome::Success);
		m.observe_subscribe_read("ws://a", Duration::from_millis(5));
		m.inc_subscribe("ws://a", Outcome::Success);
		m.inc_iteration(LoopOutcome::Completed);
		m.inc_reconnect(Outcome::Success);

		let txt = encode(&m);
		for expected in [
			"statement_ops_submit_duration_seconds",
			"statement_ops_propagation_submit_duration_seconds",
			"statement_ops_propagation_full_duration_seconds",
			"statement_ops_subscribe_read_duration_seconds",
			"statement_ops_submit_total",
			"statement_ops_propagation_total",
			"statement_ops_subscribe_total",
			"statement_ops_loop_iterations_total",
			"statement_ops_reconnects_total",
		] {
			assert!(txt.contains(expected), "expected family {expected} in:\n{txt}");
		}
	}

	#[test]
	fn histogram_bucket_counts_match_observation_count() {
		let m = PrometheusMetrics::new(vec![0.01, 0.1, 1.0]).unwrap();
		for _ in 0..5 {
			m.observe_submit("ws://a", Duration::from_millis(5)); // bucket 0.01
		}
		for _ in 0..3 {
			m.observe_submit("ws://a", Duration::from_millis(500)); // bucket 1.0
		}

		let txt = encode(&m);
		// 5 fall in 0.01, 5+3 == 8 in 0.1+infty; specifically all 8 are <= +Inf.
		// We assert the +Inf bucket equals 8 (total count).
		let inf_line = txt
			.lines()
			.find(|l| {
				l.starts_with("statement_ops_submit_duration_seconds_bucket") &&
					l.contains("le=\"+Inf\"")
			})
			.expect("+Inf bucket present");
		assert!(inf_line.ends_with(" 8"), "unexpected +Inf bucket line: {inf_line}");

		let count_line = txt
			.lines()
			.find(|l| l.starts_with("statement_ops_submit_duration_seconds_count"))
			.expect("count line present");
		assert!(count_line.ends_with(" 8"), "unexpected count line: {count_line}");
	}

	#[test]
	fn counter_increments_record_separate_outcome_series() {
		let m = PrometheusMetrics::for_tests();
		m.inc_submit("ws://a", Outcome::Success);
		m.inc_submit("ws://a", Outcome::Success);
		m.inc_submit("ws://a", Outcome::Failure);

		let txt = encode(&m);
		assert!(
			txt.contains("statement_ops_submit_total{endpoint=\"ws://a\",outcome=\"success\"} 2")
		);
		assert!(
			txt.contains("statement_ops_submit_total{endpoint=\"ws://a\",outcome=\"failure\"} 1")
		);
	}
}
