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

//! `query` subcommand: list the statements currently held by each node,
//! sorted by the time they will expire (soonest first).
//!
//! The public statement RPC exposes no dump/get method, so the only way to
//! read store contents is `statement_subscribeStatement`: the subscription
//! first replays every matching statement already in the store (the "initial
//! dump", batches with a `remaining` countdown), then turns into a live
//! stream. This command collects exactly the initial dump and unsubscribes at
//! the boundary, yielding a point-in-time snapshot — statements arriving
//! after the subscription opened are not included, and on a busy node the
//! snapshot is best-effort (server-side subscription buffers may drop
//! messages if the client lags far behind).
//!
//! With `--topic` the filter is a single-topic `MatchAll`; without it,
//! `TopicFilter::Any` lists the whole store.

use crate::ops::{
	common::{collect_initial_dump, hex_full, single_topic_filter, Clock, SystemClock},
	rpc::StatementRpc,
};
use anyhow::Result;
use codec::Decode;
use log::{info, warn};
use sp_statement_store::{Statement, TopicFilter};
use std::{sync::Arc, time::Duration};

/// Static configuration for the `query` subcommand.
#[derive(Clone)]
pub struct QueryConfig {
	/// If `Some`, only statements carrying this topic are listed (single-topic
	/// `MatchAll` filter); otherwise every statement in the store is listed
	/// (`TopicFilter::Any`).
	pub topic: Option<[u8; 32]>,
	/// Per-event timeout while collecting the initial dump, in ms. Bounds the
	/// gap between consecutive dump events, not the total dump duration.
	pub drain_timeout_ms: u64,
}

impl QueryConfig {
	pub fn validate(&self) -> Result<()> {
		anyhow::ensure!(self.drain_timeout_ms > 0, "--drain-timeout-ms must be > 0");
		Ok(())
	}
}

/// Decoded, display-oriented view of one statement from the dump.
#[derive(Debug, Clone)]
pub struct StatementSummary {
	pub hash: [u8; 32],
	pub account: Option<[u8; 32]>,
	pub topics: Vec<[u8; 32]>,
	/// Unix seconds at which the statement expires (upper half of `expiry()`).
	pub expiry_secs: u32,
	/// Sequence number (lower half of `expiry()`); tiebreaker within a second.
	pub seq: u32,
	pub data_len: usize,
	pub channel: Option<[u8; 32]>,
}

impl StatementSummary {
	fn from_statement(statement: &Statement) -> Self {
		Self {
			hash: statement.hash(),
			account: statement.account_id(),
			topics: statement.topics().iter().map(|t| t.0).collect(),
			expiry_secs: statement.get_expiration_timestamp_secs(),
			seq: statement.expiry() as u32,
			data_len: statement.data_len(),
			channel: statement.channel(),
		}
	}
}

#[derive(Debug, Clone)]
pub struct QueryEndpointReport {
	pub endpoint: String,
	/// Sorted ascending by `(expiry_secs, seq)` — soonest to expire first.
	pub statements: Vec<StatementSummary>,
	/// Dump entries that failed to SCALE-decode (skipped, never fatal).
	pub undecodable: usize,
	/// Subscribe/drain error, if the endpoint could not be queried.
	pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryReport {
	pub per_endpoint: Vec<QueryEndpointReport>,
}

impl QueryReport {
	/// True when every endpoint failed (used by the binary for exit-code policy).
	pub fn all_failed(&self) -> bool {
		!self.per_endpoint.is_empty() && self.per_endpoint.iter().all(|r| r.error.is_some())
	}
}

/// Render the filter for log lines: `any` or the full `0x`-prefixed topic.
pub fn filter_label(topic: &Option<[u8; 32]>) -> String {
	match topic {
		Some(t) => format!("0x{}", hex_full(t)),
		None => "any".to_string(),
	}
}

fn filter_for(topic: Option<[u8; 32]>) -> Result<TopicFilter> {
	match topic {
		Some(t) => single_topic_filter(t),
		None => Ok(TopicFilter::Any),
	}
}

pub async fn run_query(
	endpoints: &[(String, Arc<dyn StatementRpc>)],
	clock: &dyn Clock,
	config: &QueryConfig,
) -> Result<QueryReport> {
	config.validate()?;

	let mut per_endpoint = Vec::with_capacity(endpoints.len());
	for (endpoint, rpc) in endpoints {
		let report = query_endpoint(endpoint, rpc.as_ref(), config).await;
		log_endpoint_report(clock, config, &report);
		per_endpoint.push(report);
	}

	Ok(QueryReport { per_endpoint })
}

async fn query_endpoint(
	endpoint: &str,
	rpc: &dyn StatementRpc,
	config: &QueryConfig,
) -> QueryEndpointReport {
	let outcome: Result<(Vec<StatementSummary>, usize)> = async {
		let filter = filter_for(config.topic)?;
		let mut stream = rpc.subscribe_topic(filter).await?;
		let drain_to = Duration::from_millis(config.drain_timeout_ms);
		let dump = collect_initial_dump(&mut stream, drain_to).await?;
		// Snapshot only: dropping the stream unsubscribes, so live events past
		// the dump boundary are never consumed.
		drop(stream);

		let mut statements = Vec::with_capacity(dump.len());
		let mut undecodable = 0usize;
		for encoded in &dump {
			match Statement::decode(&mut &encoded.0[..]) {
				Ok(statement) => statements.push(StatementSummary::from_statement(&statement)),
				Err(_) => undecodable += 1,
			}
		}
		statements.sort_by_key(|s| (s.expiry_secs, s.seq));
		Ok((statements, undecodable))
	}
	.await;

	match outcome {
		Ok((statements, undecodable)) => QueryEndpointReport {
			endpoint: endpoint.to_string(),
			statements,
			undecodable,
			error: None,
		},
		Err(e) => QueryEndpointReport {
			endpoint: endpoint.to_string(),
			statements: Vec::new(),
			undecodable: 0,
			error: Some(e.to_string()),
		},
	}
}

/// `45` → `"45s"`, `95` → `"1m35s"`, `3661` → `"1h1m1s"`, `90061` → `"1d1h1m1s"`.
fn format_duration_secs(secs: u64) -> String {
	let days = secs / 86_400;
	let hours = (secs % 86_400) / 3_600;
	let minutes = (secs % 3_600) / 60;
	let seconds = secs % 60;
	let mut out = String::new();
	if days > 0 {
		out.push_str(&format!("{days}d"));
	}
	if hours > 0 || !out.is_empty() {
		out.push_str(&format!("{hours}h"));
	}
	if minutes > 0 || !out.is_empty() {
		out.push_str(&format!("{minutes}m"));
	}
	out.push_str(&format!("{seconds}s"));
	out
}

/// Human-readable time to (or since) expiry. The "expired" branch guards
/// against node/client clock skew and statements that expire between the dump
/// and the report.
fn format_remaining(now_secs: u64, expiry_secs: u32) -> String {
	let expiry = expiry_secs as u64;
	if expiry > now_secs {
		format!("expires in {}", format_duration_secs(expiry - now_secs))
	} else if expiry < now_secs {
		format!("expired {} ago", format_duration_secs(now_secs - expiry))
	} else {
		"expires now".to_string()
	}
}

fn opt_hex(bytes: &Option<[u8; 32]>) -> String {
	match bytes {
		Some(b) => format!("0x{}", hex_full(b)),
		None => "none".to_string(),
	}
}

fn log_endpoint_report(clock: &dyn Clock, config: &QueryConfig, r: &QueryEndpointReport) {
	let filter = filter_label(&config.topic);
	if let Some(e) = &r.error {
		warn!("query endpoint={} filter={filter} failed: {e}", r.endpoint);
		return;
	}
	info!(
		"query endpoint={} filter={filter} n={} undecodable={} (sorted by expiry, soonest first)",
		r.endpoint,
		r.statements.len(),
		r.undecodable,
	);
	if r.statements.is_empty() {
		info!("  (no matching statements)");
		return;
	}
	let now = clock.now_unix_secs();
	for (i, s) in r.statements.iter().enumerate() {
		let topics = s
			.topics
			.iter()
			.map(|t| format!("0x{}", hex_full(t)))
			.collect::<Vec<_>>()
			.join(",");
		info!(
			"  [{i}] hash=0x{} expires_at={} ({}) seq={} data_len={} account={} channel={} topics=[{topics}]",
			hex_full(&s.hash),
			s.expiry_secs,
			format_remaining(now, s.expiry_secs),
			s.seq,
			s.data_len,
			opt_hex(&s.account),
			opt_hex(&s.channel),
		);
	}
}

/// Convenience: run with the system clock.
pub async fn run_query_with_system_clock(
	endpoints: &[(String, Arc<dyn StatementRpc>)],
	config: &QueryConfig,
) -> Result<QueryReport> {
	run_query(endpoints, &SystemClock, config).await
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ops::{
		common::{build_statement, FixedClock},
		rpc::MockRpc,
	};
	use codec::Encode;
	use sc_statement_store::test_utils::get_keypair;
	use sp_core::Bytes;
	use sp_statement_store::StatementEvent;

	fn built(topic: [u8; 32], expiry_secs: u32, seq: u32) -> Statement {
		// Channel varies with (expiry, seq) so test statements never collide.
		let mut channel = [0u8; 32];
		channel[..4].copy_from_slice(&expiry_secs.to_le_bytes());
		channel[4..8].copy_from_slice(&seq.to_le_bytes());
		build_statement(&get_keypair(0), topic, channel, expiry_secs, seq, vec![0u8; 32])
	}

	fn encoded(topic: [u8; 32], expiry_secs: u32, seq: u32) -> Bytes {
		Bytes(built(topic, expiry_secs, seq).encode())
	}

	fn dump_event(statements: Vec<Bytes>, remaining: Option<u32>) -> StatementEvent {
		StatementEvent::NewStatements { statements, remaining }
	}

	fn make_mock() -> (String, Arc<dyn StatementRpc>, MockRpc) {
		let m = MockRpc::new();
		let dynm: Arc<dyn StatementRpc> = Arc::new(m.clone());
		("ep".to_string(), dynm, m)
	}

	fn cfg(topic: Option<[u8; 32]>) -> QueryConfig {
		QueryConfig { topic, drain_timeout_ms: 500 }
	}

	const CLOCK: FixedClock = FixedClock(3_000_000);

	#[tokio::test]
	async fn all_statements_sorted_by_expiry() {
		let (name, rpc, mock) = make_mock();
		let topic = [0x11u8; 32];
		mock.push_subscribe_events(vec![Ok(dump_event(
			vec![encoded(topic, 3_000, 0), encoded(topic, 1_000, 0), encoded(topic, 2_000, 0)],
			Some(0),
		))]);
		let report = run_query(&[(name, rpc)], &CLOCK, &cfg(None)).await.unwrap();
		let r = &report.per_endpoint[0];
		assert!(r.error.is_none());
		assert_eq!(r.undecodable, 0);
		let expiries: Vec<_> = r.statements.iter().map(|s| s.expiry_secs).collect();
		assert_eq!(expiries, vec![1_000, 2_000, 3_000]);
	}

	#[tokio::test]
	async fn same_expiry_sorted_by_seq() {
		let (name, rpc, mock) = make_mock();
		let topic = [0x22u8; 32];
		mock.push_subscribe_events(vec![Ok(dump_event(
			vec![encoded(topic, 1_000, 5), encoded(topic, 1_000, 1)],
			Some(0),
		))]);
		let report = run_query(&[(name, rpc)], &CLOCK, &cfg(None)).await.unwrap();
		let seqs: Vec<_> = report.per_endpoint[0].statements.iter().map(|s| s.seq).collect();
		assert_eq!(seqs, vec![1, 5]);
	}

	#[tokio::test]
	async fn multi_event_dump_is_merged_and_globally_sorted() {
		let (name, rpc, mock) = make_mock();
		let topic = [0x33u8; 32];
		mock.push_subscribe_events(vec![
			Ok(dump_event(vec![encoded(topic, 3_000, 0), encoded(topic, 1_000, 0)], Some(1))),
			Ok(dump_event(vec![encoded(topic, 2_000, 0)], Some(0))),
		]);
		let report = run_query(&[(name, rpc)], &CLOCK, &cfg(None)).await.unwrap();
		let expiries: Vec<_> =
			report.per_endpoint[0].statements.iter().map(|s| s.expiry_secs).collect();
		assert_eq!(expiries, vec![1_000, 2_000, 3_000]);
	}

	#[tokio::test]
	async fn no_topic_uses_any_filter() {
		let (name, rpc, mock) = make_mock();
		mock.push_subscribe_events(vec![Ok(dump_event(vec![], Some(0)))]);
		run_query(&[(name, rpc)], &CLOCK, &cfg(None)).await.unwrap();
		let filters = mock.captured_filters();
		assert_eq!(filters.len(), 1);
		assert!(
			matches!(filters[0], TopicFilter::Any),
			"expected Any filter, got {:?}",
			filters[0]
		);
	}

	#[tokio::test]
	async fn topic_uses_single_match_all_filter() {
		let (name, rpc, mock) = make_mock();
		mock.push_subscribe_events(vec![Ok(dump_event(vec![], Some(0)))]);
		let topic = [0x42u8; 32];
		run_query(&[(name, rpc)], &CLOCK, &cfg(Some(topic))).await.unwrap();
		match &mock.captured_filters()[0] {
			TopicFilter::MatchAll(ts) => {
				assert_eq!(ts.len(), 1);
				assert_eq!(ts[0].0, topic);
			},
			other => panic!("expected MatchAll filter, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn undecodable_bytes_counted_not_fatal() {
		let (name, rpc, mock) = make_mock();
		mock.push_subscribe_events(vec![Ok(dump_event(
			vec![Bytes(vec![0xff, 0x00]), encoded([0x44u8; 32], 1_000, 0)],
			Some(0),
		))]);
		let report = run_query(&[(name, rpc)], &CLOCK, &cfg(None)).await.unwrap();
		let r = &report.per_endpoint[0];
		assert!(r.error.is_none());
		assert_eq!(r.statements.len(), 1);
		assert_eq!(r.undecodable, 1);
	}

	#[tokio::test]
	async fn empty_dump_is_ok() {
		let (name, rpc, mock) = make_mock();
		mock.push_subscribe_events(vec![Ok(dump_event(vec![], Some(0)))]);
		let report = run_query(&[(name, rpc)], &CLOCK, &cfg(None)).await.unwrap();
		let r = &report.per_endpoint[0];
		assert!(r.error.is_none());
		assert!(r.statements.is_empty());
		assert!(!report.all_failed());
	}

	#[tokio::test]
	async fn live_events_after_boundary_are_ignored() {
		let (name, rpc, mock) = make_mock();
		let topic = [0x55u8; 32];
		mock.push_subscribe_events(vec![
			Ok(dump_event(vec![encoded(topic, 1_000, 0)], Some(0))),
			// Live event past the dump boundary: must not be consumed.
			Ok(dump_event(vec![encoded(topic, 2_000, 0)], None)),
		]);
		let report = run_query(&[(name, rpc)], &CLOCK, &cfg(None)).await.unwrap();
		let r = &report.per_endpoint[0];
		assert_eq!(r.statements.len(), 1);
		assert_eq!(r.statements[0].expiry_secs, 1_000);
	}

	#[tokio::test]
	async fn subscribe_error_recorded_and_remaining_endpoints_still_run() {
		let (a_name, a_rpc, a_mock) = make_mock();
		let (b_name, b_rpc, b_mock) = make_mock();
		a_mock.push_subscribe_error("denied");
		b_mock.push_subscribe_events(vec![Ok(dump_event(
			vec![encoded([0x66u8; 32], 1_000, 0)],
			Some(0),
		))]);
		let report = run_query(&[(a_name, a_rpc), (b_name, b_rpc)], &CLOCK, &cfg(None))
			.await
			.unwrap();
		assert_eq!(report.per_endpoint.len(), 2);
		assert!(report.per_endpoint[0].error.as_ref().unwrap().contains("denied"));
		assert!(report.per_endpoint[1].error.is_none());
		assert_eq!(report.per_endpoint[1].statements.len(), 1);
		assert!(!report.all_failed());
	}

	#[tokio::test]
	async fn all_failed_when_every_endpoint_errors() {
		let (a_name, a_rpc, a_mock) = make_mock();
		let (b_name, b_rpc, b_mock) = make_mock();
		a_mock.push_subscribe_error("down");
		b_mock.push_subscribe_error("down too");
		let report = run_query(&[(a_name, a_rpc), (b_name, b_rpc)], &CLOCK, &cfg(None))
			.await
			.unwrap();
		assert!(report.all_failed());
	}

	#[tokio::test(start_paused = true)]
	async fn drain_timeout_is_recorded_as_error() {
		let (name, rpc, mock) = make_mock();
		mock.push_subscribe_pending();
		let config = QueryConfig { topic: None, drain_timeout_ms: 50 };
		let endpoints = vec![(name, rpc)];
		let fut = run_query(&endpoints, &CLOCK, &config);
		tokio::pin!(fut);
		let (report, _) = tokio::join!(fut, async {
			tokio::time::advance(Duration::from_millis(200)).await;
		});
		let r = &report.unwrap().per_endpoint[0];
		assert!(r.error.as_ref().unwrap().contains("Initial dump timed out"));
	}

	#[tokio::test]
	async fn summary_fields_extracted() {
		let (name, rpc, mock) = make_mock();
		let topic = [0x77u8; 32];
		let statement = built(topic, 5_000, 9);
		mock.push_subscribe_events(vec![Ok(dump_event(vec![Bytes(statement.encode())], Some(0)))]);
		let report = run_query(&[(name, rpc)], &CLOCK, &cfg(Some(topic))).await.unwrap();
		let s = &report.per_endpoint[0].statements[0];
		assert_eq!(s.hash, statement.hash());
		assert_eq!(s.account, statement.account_id());
		assert!(s.account.is_some(), "signed statement must have an account");
		assert_eq!(s.topics, vec![topic]);
		assert_eq!(s.expiry_secs, 5_000);
		assert_eq!(s.seq, 9);
		assert_eq!(s.data_len, 32);
		assert_eq!(s.channel, statement.channel());
		assert!(s.channel.is_some());
	}

	#[tokio::test]
	async fn validate_rejects_zero_drain_timeout() {
		let config = QueryConfig { topic: None, drain_timeout_ms: 0 };
		assert!(config.validate().is_err());
		let endpoints: Vec<(String, Arc<dyn StatementRpc>)> = vec![];
		assert!(run_query(&endpoints, &CLOCK, &config).await.is_err());
	}

	#[test]
	fn format_remaining_cases() {
		assert_eq!(format_remaining(1_000, 1_095), "expires in 1m35s");
		assert_eq!(format_remaining(1_012, 1_000), "expired 12s ago");
		assert_eq!(format_remaining(1_000, 1_000), "expires now");
	}

	#[test]
	fn format_duration_secs_cases() {
		assert_eq!(format_duration_secs(45), "45s");
		assert_eq!(format_duration_secs(95), "1m35s");
		assert_eq!(format_duration_secs(3_661), "1h1m1s");
		assert_eq!(format_duration_secs(90_061), "1d1h1m1s");
		assert_eq!(format_duration_secs(0), "0s");
		assert_eq!(format_duration_secs(3_600), "1h0m0s");
	}

	#[test]
	fn filter_label_cases() {
		assert_eq!(filter_label(&None), "any");
		assert_eq!(filter_label(&Some([0xABu8; 32])), format!("0x{}", "ab".repeat(32)));
	}
}
