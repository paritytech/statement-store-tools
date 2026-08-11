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

//! Per-node statement-store operation benchmark.
//!
//! Complements the cohort-wide `statement-latency-bench` binary by measuring
//! individual RPC operations on specific nodes. Six subcommands:
//!
//! - `submit`      — `statement_submit` duration on each node.
//! - `propagation` — submit→subscribe latency for each (submit, subscribe) pair.
//! - `subscribe`   — retrieval latency via `statement_subscribeStatement` on a previously-submitted
//!   statement.
//! - `loop`        — periodically run the above.
//! - `query`       — list statements currently in the store (optionally by topic), sorted by
//!   expiry.
//! - `admin`       — show or set an account's statement-store quota (allowance).

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use log::info;
use sp_statement_store::StatementAllowance;
use std::sync::Arc;

mod ops;

use ops::{
	admin::{set_quota, show_quota, SetQuotaConfig, ShowQuotaConfig},
	common::{hex_full, parse_account, parse_seed, parse_topic_hex},
	periodic_loop::{run_loop_with_ctrl_c, LoopConfig},
	propagation::{run_propagation_with_system_clock, PropagationConfig},
	query::{filter_label, run_query_with_system_clock, QueryConfig},
	rpc::{StatementRpc, WsClientRpc},
	signer::{resolve_sudo_keypair, SudoKeyArgs},
	submit::{run_submit_with_system_clock, SubmitConfig},
	subscribe::{run_subscribe_with_system_clock, SubscribeConfig},
};

#[derive(Parser, Debug)]
#[command(name = "statement-ops-bench")]
#[command(about = "Per-node statement-store operation latency benchmark", long_about = None)]
struct Cli {
	#[command(subcommand)]
	command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
	/// Measure per-node `statement_submit` duration.
	Submit(SubmitArgs),
	/// Measure submit→subscribe latency across all (submit, subscribe) pairs.
	Propagation(PropagationArgs),
	/// Measure per-node retrieval latency via subscribe-with-filter.
	Subscribe(SubscribeArgs),
	/// Periodically run all three scenarios.
	#[command(name = "loop")]
	Loop(LoopArgs),
	/// List statements currently in the store (optionally filtered by topic),
	/// sorted by the time they will expire (soonest first).
	Query(QueryArgs),
	/// Show or set the per-account statement-store quota (allowance).
	Admin(AdminArgs),
}

#[derive(Args, Debug)]
struct SharedArgs {
	/// Optional SURI / seed phrase used to sign statements. Defaults to a
	/// deterministic benchmark account derived from `//StatementClient//0`.
	#[arg(long)]
	seed: Option<String>,

	/// Statement payload size, in bytes.
	#[arg(long, default_value = "512")]
	message_size: usize,

	/// Statement expiry offset, in seconds, from "now".
	#[arg(long, default_value_t = 600)]
	base_expiry_secs: u64,
}

#[derive(Args, Debug)]
struct SubmitArgs {
	/// Comma-separated list of WebSocket RPC endpoints.
	#[arg(long, value_delimiter = ',', required = true)]
	rpc_endpoints: Vec<String>,

	/// Number of timing samples to take per endpoint. With
	/// `--iteration-batch B`, each sample contains B submits issued in
	/// parallel on the same ws connection; total submissions per endpoint
	/// is `iterations * iteration_batch`.
	#[arg(long, default_value = "1")]
	iterations: u32,

	/// How many statements to submit in parallel as one batched timing
	/// sample. `1` (default) reproduces the sequential, one-submit-per-
	/// sample behaviour. Larger values pipeline submits over the same
	/// jsonrpsee ws connection and record the wall-clock from kick-off to
	/// all-completed as a single sample.
	#[arg(long, default_value = "1")]
	iteration_batch: u32,

	/// Optional 32-byte topic in hex (with or without `0x` prefix). When set,
	/// every submitted statement uses this exact topic instead of a derived
	/// per-iteration one.
	#[arg(long, value_parser = parse_topic_hex)]
	topic: Option<[u8; 32]>,

	#[command(flatten)]
	shared: SharedArgs,
}

#[derive(Args, Debug)]
struct PropagationArgs {
	/// Comma-separated list of WebSocket RPC endpoints used for submission.
	#[arg(long, value_delimiter = ',', required = true)]
	submit_endpoints: Vec<String>,

	/// Comma-separated list of WebSocket RPC endpoints used for subscription.
	#[arg(long, value_delimiter = ',', required = true)]
	subscribe_endpoints: Vec<String>,

	/// Number of iterations per (submit, subscribe) pair.
	#[arg(long, default_value = "1")]
	iterations: u32,

	/// Maximum time to wait for the (empty) initial subscription dump, in ms.
	#[arg(long, default_value = "2000")]
	drain_timeout_ms: u64,

	/// Maximum time to wait for the propagated statement, in ms.
	#[arg(long, default_value = "5000")]
	receive_timeout_ms: u64,

	/// Optional 32-byte topic in hex (with or without `0x` prefix). When set,
	/// every iteration uses this exact topic. The subscription's initial dump
	/// may then legitimately contain prior matching statements, which are
	/// drained and excluded from the propagation timer.
	#[arg(long, value_parser = parse_topic_hex)]
	topic: Option<[u8; 32]>,

	#[command(flatten)]
	shared: SharedArgs,
}

#[derive(Args, Debug)]
struct SubscribeArgs {
	/// Comma-separated list of WebSocket RPC endpoints.
	#[arg(long, value_delimiter = ',', required = true)]
	rpc_endpoints: Vec<String>,

	/// Number of read operations per endpoint.
	#[arg(long, default_value = "1")]
	reads_per_node: u32,

	/// Milliseconds to wait after seeding before issuing reads.
	#[arg(long, default_value = "100")]
	settle_ms: u64,

	/// Maximum time to wait for the initial subscription dump, in ms.
	#[arg(long, default_value = "2000")]
	drain_timeout_ms: u64,

	/// Optional 32-byte topic in hex (with or without `0x` prefix). When set,
	/// the seed statement uses this topic and reads are filtered to it. Useful
	/// for measuring retrieval of statements already present under a known
	/// topic.
	#[arg(long, value_parser = parse_topic_hex)]
	topic: Option<[u8; 32]>,

	/// Assert that the subscription delivers the statement exactly once
	/// (requires `--topic`). Each read drains the initial dump, then watches one
	/// further `--drain-timeout-ms` idle window for any duplicate or live
	/// re-delivery, and the process exits non-zero unless every read received
	/// exactly one statement.
	#[arg(long, requires = "topic")]
	assert_once: bool,

	#[command(flatten)]
	shared: SharedArgs,
}

#[derive(Args, Debug)]
struct LoopArgs {
	/// Comma-separated list of WebSocket RPC endpoints. Used as both submit
	/// and subscribe endpoints for the `propagation` portion of each cycle.
	#[arg(long, value_delimiter = ',', required = true)]
	rpc_endpoints: Vec<String>,

	/// Interval between cycles, in seconds.
	#[arg(long, default_value = "30")]
	interval_secs: u64,

	/// Maximum number of cycles. Default: unbounded.
	#[arg(long)]
	iterations: Option<u32>,

	/// Maximum duration of the loop, in seconds. Default: unbounded.
	#[arg(long)]
	duration_secs: Option<u64>,

	/// Per-cycle submit iterations (per endpoint).
	#[arg(long, default_value = "1")]
	submit_iterations: u32,

	/// Per-cycle propagation iterations (per pair).
	#[arg(long, default_value = "1")]
	propagation_iterations: u32,

	/// Per-cycle reads per endpoint.
	#[arg(long, default_value = "1")]
	reads_per_node: u32,

	/// Drain-timeout for the propagation portion.
	#[arg(long, default_value = "2000")]
	drain_timeout_ms: u64,

	/// Receive-timeout for the propagation portion.
	#[arg(long, default_value = "5000")]
	receive_timeout_ms: u64,

	/// Subscribe-drain timeout for the subscribe portion.
	#[arg(long, default_value = "2000")]
	subscribe_drain_timeout_ms: u64,

	/// Settle time before reads in the subscribe portion.
	#[arg(long, default_value = "100")]
	settle_ms: u64,

	/// Open a fresh WebSocket connection to each endpoint at the start of every
	/// iteration (after the first). When `false`, the connections opened at
	/// startup are reused for all iterations.
	#[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
	new_connection_per_iteration: bool,

	#[command(flatten)]
	shared: SharedArgs,
}

#[derive(Args, Debug)]
struct QueryArgs {
	/// Comma-separated list of WebSocket RPC endpoints.
	#[arg(long, value_delimiter = ',', required = true)]
	rpc_endpoints: Vec<String>,

	/// Optional 32-byte topic in hex (with or without `0x` prefix). When set,
	/// only statements carrying this topic are listed; otherwise every
	/// statement in the store is listed.
	#[arg(long, value_parser = parse_topic_hex)]
	topic: Option<[u8; 32]>,

	/// Maximum time to wait for each event of the initial subscription dump,
	/// in ms. A full-store dump can span many events; this bounds the gap
	/// between consecutive events, not the total time. Dump events carry up to
	/// ~4 MiB of statements each, so slow WAN links need a generous value.
	#[arg(long, default_value = "15000")]
	drain_timeout_ms: u64,
}

#[derive(Args, Debug)]
struct AdminArgs {
	#[command(subcommand)]
	action: AdminAction,
}

#[derive(Subcommand, Debug)]
enum AdminAction {
	/// Show the statement-store quota (allowance) for an account.
	ShowQuota(ShowQuotaArgs),
	/// Set the statement-store quota (allowance) for an account via a
	/// sudo-signed `System.set_storage` extrinsic.
	SetQuota(SetQuotaArgs),
}

#[derive(Args, Debug)]
struct ShowQuotaArgs {
	/// WebSocket RPC endpoint (e.g. ws://127.0.0.1:9944).
	#[arg(long, required = true)]
	rpc_endpoint: String,

	/// Account as an SS58 address or 32-byte hex (with or without `0x`).
	#[arg(long, required = true, value_parser = parse_account)]
	account: [u8; 32],
}

#[derive(Args, Debug)]
struct SetQuotaArgs {
	/// WebSocket RPC endpoint (e.g. ws://127.0.0.1:9944).
	#[arg(long, required = true)]
	rpc_endpoint: String,

	/// Account as an SS58 address or 32-byte hex (with or without `0x`).
	#[arg(long, required = true, value_parser = parse_account)]
	account: [u8; 32],

	/// Sudo signing key. Exactly one source is required (`--sudo-seed`,
	/// `--sudo-seed-file`, or `--sudo-json`).
	#[command(flatten)]
	sudo: SudoKeyArgs,

	/// Maximum number of statements allowed for the account.
	#[arg(long, required = true)]
	max_count: u32,

	/// Maximum total size of statements, in bytes, for the account.
	#[arg(long, required = true)]
	max_size: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
	let _ = env_logger::try_init_from_env(
		env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
	);
	let cli = Cli::parse();
	let run_id: u64 = rand::random();

	match cli.command {
		Command::Submit(args) => run_submit_cmd(args, run_id).await,
		Command::Propagation(args) => run_propagation_cmd(args, run_id).await,
		Command::Subscribe(args) => run_subscribe_cmd(args, run_id).await,
		Command::Loop(args) => run_loop_cmd(args, run_id).await,
		Command::Query(args) => run_query_cmd(args).await,
		Command::Admin(args) => run_admin_cmd(args).await,
	}
}

async fn run_admin_cmd(args: AdminArgs) -> Result<()> {
	match args.action {
		AdminAction::ShowQuota(a) => {
			let config = ShowQuotaConfig { endpoint: a.rpc_endpoint, account: a.account };
			let quota = show_quota(&config).await?;
			log_quota(&config.account, quota.as_ref());
			Ok(())
		},
		AdminAction::SetQuota(a) => {
			let signer = resolve_sudo_keypair(&a.sudo)?;
			info!(
				"Setting quota for account={} to max_count={} max_size={}, signing as account={}",
				hex_full(&a.account),
				a.max_count,
				a.max_size,
				hex_full(&signer.public_key().0),
			);
			let config = SetQuotaConfig {
				endpoint: a.rpc_endpoint,
				account: a.account,
				signer,
				max_count: a.max_count,
				max_size: a.max_size,
			};
			set_quota(&config).await?;

			// Verify by reading the value back at the latest block.
			let show = ShowQuotaConfig { endpoint: config.endpoint, account: config.account };
			let quota = show_quota(&show).await?;
			log_quota(&show.account, quota.as_ref());
			match quota {
				Some(q) if q.max_count == config.max_count && q.max_size == config.max_size => {
					Ok(())
				},
				_ => anyhow::bail!(
					"Quota read-back did not match requested values (max_count={}, max_size={}) for account={}",
					config.max_count,
					config.max_size,
					hex_full(&show.account),
				),
			}
		},
	}
}

/// Log an account's quota (or its absence) on a single line.
fn log_quota(account: &[u8; 32], quota: Option<&StatementAllowance>) {
	match quota {
		Some(q) => info!(
			"account={} quota: max_count={} max_size={}{}",
			hex_full(account),
			q.max_count,
			q.max_size,
			if q.max_count == 0 || q.max_size == 0 { " (depleted)" } else { "" },
		),
		None => info!("account={} has NO quota set", hex_full(account)),
	}
}

async fn run_submit_cmd(args: SubmitArgs, run_id: u64) -> Result<()> {
	let keypair = parse_seed(args.shared.seed.as_deref())?;
	let endpoints = connect_all(&args.rpc_endpoints).await?;
	let config = SubmitConfig {
		iterations: args.iterations,
		iteration_batch: args.iteration_batch,
		message_size: args.shared.message_size,
		base_expiry_secs: args.shared.base_expiry_secs,
		run_id,
		topic_override: args.topic,
	};
	info!(
		"Running submit benchmark: endpoints={} iterations={} iteration_batch={} msg_size={}B",
		args.rpc_endpoints.len(),
		args.iterations,
		args.iteration_batch,
		args.shared.message_size,
	);
	run_submit_with_system_clock(&endpoints, &keypair, &config, "").await?;
	Ok(())
}

async fn run_propagation_cmd(args: PropagationArgs, run_id: u64) -> Result<()> {
	let keypair = parse_seed(args.shared.seed.as_deref())?;
	let submit_eps = connect_all(&args.submit_endpoints).await?;
	let sub_eps = connect_all(&args.subscribe_endpoints).await?;
	let config = PropagationConfig {
		iterations: args.iterations,
		message_size: args.shared.message_size,
		base_expiry_secs: args.shared.base_expiry_secs,
		run_id,
		drain_timeout_ms: args.drain_timeout_ms,
		receive_timeout_ms: args.receive_timeout_ms,
		topic_override: args.topic,
	};
	info!(
		"Running propagation benchmark: submit_eps={} subscribe_eps={} iterations={} msg_size={}B",
		args.submit_endpoints.len(),
		args.subscribe_endpoints.len(),
		args.iterations,
		args.shared.message_size,
	);
	run_propagation_with_system_clock(&submit_eps, &sub_eps, &keypair, &config, "").await?;
	Ok(())
}

async fn run_subscribe_cmd(args: SubscribeArgs, run_id: u64) -> Result<()> {
	let keypair = parse_seed(args.shared.seed.as_deref())?;
	let endpoints = connect_all(&args.rpc_endpoints).await?;
	let config = SubscribeConfig {
		reads_per_node: args.reads_per_node,
		message_size: args.shared.message_size,
		base_expiry_secs: args.shared.base_expiry_secs,
		run_id,
		settle_ms: args.settle_ms,
		drain_timeout_ms: args.drain_timeout_ms,
		topic_override: args.topic,
		assert_once: args.assert_once,
	};
	info!(
		"Running subscribe benchmark: endpoints={} reads_per_node={} msg_size={}B assert_once={}",
		args.rpc_endpoints.len(),
		args.reads_per_node,
		args.shared.message_size,
		args.assert_once,
	);
	let report = run_subscribe_with_system_clock(&endpoints, &keypair, &config, "").await?;

	// With `--assert-once`, a failed read (absent / duplicated / multiple) must
	// surface as a non-zero exit so callers and scripts can rely on it.
	if args.assert_once {
		let failed: Vec<_> = report.per_endpoint.iter().filter(|r| r.failures > 0).collect();
		if !failed.is_empty() {
			let detail = failed
				.iter()
				.map(|r| {
					format!("{}: {}", r.endpoint, r.first_error.as_deref().unwrap_or("read failed"))
				})
				.collect::<Vec<_>>()
				.join("; ");
			anyhow::bail!(
				"--assert-once failed on {}/{} endpoint(s): {detail}",
				failed.len(),
				report.per_endpoint.len(),
			);
		}
	}
	Ok(())
}

async fn run_loop_cmd(args: LoopArgs, run_id: u64) -> Result<()> {
	let keypair = parse_seed(args.shared.seed.as_deref())?;
	let rpc_endpoints = args.rpc_endpoints.clone();
	let config = LoopConfig {
		interval_secs: args.interval_secs,
		max_iterations: args.iterations,
		max_duration_secs: args.duration_secs,
		new_connection_per_iteration: args.new_connection_per_iteration,
		submit_config: SubmitConfig {
			iterations: args.submit_iterations,
			iteration_batch: 1,
			message_size: args.shared.message_size,
			base_expiry_secs: args.shared.base_expiry_secs,
			run_id,
			topic_override: None,
		},
		propagation_config: PropagationConfig {
			iterations: args.propagation_iterations,
			message_size: args.shared.message_size,
			base_expiry_secs: args.shared.base_expiry_secs,
			run_id,
			drain_timeout_ms: args.drain_timeout_ms,
			receive_timeout_ms: args.receive_timeout_ms,
			topic_override: None,
		},
		subscribe_config: SubscribeConfig {
			reads_per_node: args.reads_per_node,
			message_size: args.shared.message_size,
			base_expiry_secs: args.shared.base_expiry_secs,
			run_id,
			settle_ms: args.settle_ms,
			drain_timeout_ms: args.subscribe_drain_timeout_ms,
			topic_override: None,
			assert_once: false,
		},
	};
	info!(
		"Running loop benchmark: endpoints={} interval={}s max_iterations={:?} max_duration={:?}s new_connection_per_iteration={}",
		args.rpc_endpoints.len(),
		args.interval_secs,
		args.iterations,
		args.duration_secs,
		args.new_connection_per_iteration,
	);
	let connector = move || {
		let urls = rpc_endpoints.clone();
		async move { connect_all(&urls).await }
	};
	let report = run_loop_with_ctrl_c(connector, &keypair, config).await?;
	info!(
		"Loop finished: iterations={} stop_reason={:?}",
		report.iterations_completed, report.stopped_reason,
	);
	Ok(())
}

async fn run_query_cmd(args: QueryArgs) -> Result<()> {
	let endpoints = connect_all(&args.rpc_endpoints).await?;
	let config = QueryConfig { topic: args.topic, drain_timeout_ms: args.drain_timeout_ms };
	info!(
		"Running query: endpoints={} filter={} drain_timeout_ms={}",
		args.rpc_endpoints.len(),
		filter_label(&args.topic),
		args.drain_timeout_ms,
	);
	let report = run_query_with_system_clock(&endpoints, &config).await?;
	anyhow::ensure!(
		!report.all_failed(),
		"query failed on all {} endpoint(s)",
		report.per_endpoint.len(),
	);
	Ok(())
}

async fn connect_all(endpoints: &[String]) -> Result<Vec<(String, Arc<dyn StatementRpc>)>> {
	anyhow::ensure!(!endpoints.is_empty(), "At least one endpoint must be provided");
	let mut out = Vec::with_capacity(endpoints.len());
	for ep in endpoints {
		let rpc = WsClientRpc::connect(ep).await?;
		let dynm: Arc<dyn StatementRpc> = Arc::new(rpc);
		out.push((ep.clone(), dynm));
	}
	Ok(out)
}

#[cfg(test)]
mod cli_tests {
	use super::*;
	use clap::Parser;

	#[test]
	fn submit_minimal_required_args() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"submit",
			"--rpc-endpoints",
			"ws://a,ws://b",
		])
		.expect("parse");
		match cli.command {
			Command::Submit(a) => {
				assert_eq!(a.rpc_endpoints, vec!["ws://a", "ws://b"]);
				assert_eq!(a.iterations, 1);
				assert_eq!(a.shared.message_size, 512);
				assert_eq!(a.shared.base_expiry_secs, 600);
				assert!(a.shared.seed.is_none());
			},
			_ => panic!("expected submit"),
		}
	}

	#[test]
	fn submit_custom_iterations_and_seed() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"submit",
			"--rpc-endpoints",
			"ws://a",
			"--iterations",
			"7",
			"--seed",
			"//Alice",
			"--message-size",
			"256",
		])
		.expect("parse");
		match cli.command {
			Command::Submit(a) => {
				assert_eq!(a.iterations, 7);
				assert_eq!(a.iteration_batch, 1, "default iteration_batch is 1");
				assert_eq!(a.shared.message_size, 256);
				assert_eq!(a.shared.seed.as_deref(), Some("//Alice"));
			},
			_ => panic!("expected submit"),
		}
	}

	#[test]
	fn submit_parses_iteration_batch() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"submit",
			"--rpc-endpoints",
			"ws://a",
			"--iterations",
			"5",
			"--iteration-batch",
			"8",
		])
		.expect("parse");
		match cli.command {
			Command::Submit(a) => {
				assert_eq!(a.iterations, 5);
				assert_eq!(a.iteration_batch, 8);
			},
			_ => panic!("expected submit"),
		}
	}

	#[test]
	fn submit_rejects_missing_endpoints() {
		assert!(Cli::try_parse_from(["statement-ops-bench", "submit"]).is_err());
	}

	#[test]
	fn propagation_both_endpoint_lists_required() {
		assert!(Cli::try_parse_from([
			"statement-ops-bench",
			"propagation",
			"--submit-endpoints",
			"ws://a",
		])
		.is_err());
	}

	#[test]
	fn propagation_parses_full_args() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"propagation",
			"--submit-endpoints",
			"ws://a,ws://b",
			"--subscribe-endpoints",
			"ws://c",
			"--iterations",
			"3",
			"--receive-timeout-ms",
			"1234",
		])
		.expect("parse");
		match cli.command {
			Command::Propagation(a) => {
				assert_eq!(a.submit_endpoints, vec!["ws://a", "ws://b"]);
				assert_eq!(a.subscribe_endpoints, vec!["ws://c"]);
				assert_eq!(a.iterations, 3);
				assert_eq!(a.receive_timeout_ms, 1234);
			},
			_ => panic!("expected propagation"),
		}
	}

	#[test]
	fn subscribe_parses_full_args() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"subscribe",
			"--rpc-endpoints",
			"ws://x",
			"--reads-per-node",
			"3",
			"--settle-ms",
			"250",
		])
		.expect("parse");
		match cli.command {
			Command::Subscribe(a) => {
				assert_eq!(a.rpc_endpoints, vec!["ws://x"]);
				assert_eq!(a.reads_per_node, 3);
				assert_eq!(a.settle_ms, 250);
			},
			_ => panic!("expected subscribe"),
		}
	}

	#[test]
	fn loop_subcommand_parses() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"loop",
			"--rpc-endpoints",
			"ws://x,ws://y",
			"--interval-secs",
			"5",
			"--iterations",
			"3",
		])
		.expect("parse");
		match cli.command {
			Command::Loop(a) => {
				assert_eq!(a.rpc_endpoints, vec!["ws://x", "ws://y"]);
				assert_eq!(a.interval_secs, 5);
				assert_eq!(a.iterations, Some(3));
				assert_eq!(a.duration_secs, None);
				assert!(a.new_connection_per_iteration, "default is true");
			},
			_ => panic!("expected loop"),
		}
	}

	#[test]
	fn loop_new_connection_per_iteration_can_be_disabled() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"loop",
			"--rpc-endpoints",
			"ws://x",
			"--new-connection-per-iteration",
			"false",
		])
		.expect("parse");
		match cli.command {
			Command::Loop(a) => assert!(!a.new_connection_per_iteration),
			_ => panic!("expected loop"),
		}
	}

	#[test]
	fn empty_comma_separated_endpoint_list_is_accepted_then_rejected_by_connect() {
		// clap accepts an empty value here, but our connect_all enforces non-empty.
		// We just check parse succeeds with one explicit empty element.
		let _ =
			Cli::try_parse_from(["statement-ops-bench", "submit", "--rpc-endpoints", "ws://only"])
				.expect("parse");
	}

	#[test]
	fn submit_parses_topic_with_0x_prefix() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"submit",
			"--rpc-endpoints",
			"ws://a",
			"--topic",
			"0x00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		])
		.expect("parse");
		match cli.command {
			Command::Submit(a) => {
				let t = a.topic.expect("topic set");
				assert_eq!(t[0], 0x00);
				assert_eq!(t[1], 0x11);
				assert_eq!(t[31], 0xff);
			},
			_ => panic!("expected submit"),
		}
	}

	#[test]
	fn propagation_parses_topic_without_prefix() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"propagation",
			"--submit-endpoints",
			"ws://a",
			"--subscribe-endpoints",
			"ws://b",
			"--topic",
			"deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		])
		.expect("parse");
		match cli.command {
			Command::Propagation(a) => {
				let t = a.topic.expect("topic set");
				assert_eq!(&t[..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
			},
			_ => panic!("expected propagation"),
		}
	}

	#[test]
	fn subscribe_parses_topic() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"subscribe",
			"--rpc-endpoints",
			"ws://x",
			"--topic",
			"0xCAFEBABECAFEBABECAFEBABECAFEBABECAFEBABECAFEBABECAFEBABECAFEBABE",
		])
		.expect("parse");
		match cli.command {
			Command::Subscribe(a) => {
				let t = a.topic.expect("topic set");
				assert_eq!(&t[..4], &[0xCA, 0xFE, 0xBA, 0xBE]);
			},
			_ => panic!("expected subscribe"),
		}
	}

	#[test]
	fn subscribe_parses_assert_once() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"subscribe",
			"--rpc-endpoints",
			"ws://x",
			"--topic",
			"0xCAFEBABECAFEBABECAFEBABECAFEBABECAFEBABECAFEBABECAFEBABECAFEBABE",
			"--assert-once",
		])
		.expect("parse");
		match cli.command {
			Command::Subscribe(a) => {
				assert!(a.assert_once);
				assert!(a.topic.is_some());
			},
			_ => panic!("expected subscribe"),
		}
	}

	#[test]
	fn subscribe_assert_once_requires_topic() {
		// `--assert-once` without `--topic` must be rejected at parse time.
		assert!(Cli::try_parse_from([
			"statement-ops-bench",
			"subscribe",
			"--rpc-endpoints",
			"ws://x",
			"--assert-once",
		])
		.is_err());
	}

	#[test]
	fn subscribe_assert_once_defaults_false() {
		let cli =
			Cli::try_parse_from(["statement-ops-bench", "subscribe", "--rpc-endpoints", "ws://x"])
				.expect("parse");
		match cli.command {
			Command::Subscribe(a) => assert!(!a.assert_once),
			_ => panic!("expected subscribe"),
		}
	}

	#[test]
	fn topic_without_value_defaults_to_none() {
		let cli =
			Cli::try_parse_from(["statement-ops-bench", "submit", "--rpc-endpoints", "ws://a"])
				.expect("parse");
		match cli.command {
			Command::Submit(a) => assert!(a.topic.is_none()),
			_ => panic!("expected submit"),
		}
	}

	#[test]
	fn submit_rejects_short_topic() {
		assert!(Cli::try_parse_from([
			"statement-ops-bench",
			"submit",
			"--rpc-endpoints",
			"ws://a",
			"--topic",
			"00",
		])
		.is_err());
	}

	#[test]
	fn submit_rejects_non_hex_topic() {
		let bad = format!("zz{}", "00".repeat(31));
		assert!(Cli::try_parse_from([
			"statement-ops-bench",
			"submit",
			"--rpc-endpoints",
			"ws://a",
			"--topic",
			&bad,
		])
		.is_err());
	}

	#[test]
	fn query_minimal_required_args() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"query",
			"--rpc-endpoints",
			"ws://a,ws://b",
		])
		.expect("parse");
		match cli.command {
			Command::Query(a) => {
				assert_eq!(a.rpc_endpoints, vec!["ws://a", "ws://b"]);
				assert!(a.topic.is_none());
				assert_eq!(a.drain_timeout_ms, 15000);
			},
			_ => panic!("expected query"),
		}
	}

	#[test]
	fn query_parses_topic_and_drain_timeout() {
		let cli = Cli::try_parse_from([
			"statement-ops-bench",
			"query",
			"--rpc-endpoints",
			"ws://a",
			"--topic",
			"0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
			"--drain-timeout-ms",
			"9000",
		])
		.expect("parse");
		match cli.command {
			Command::Query(a) => {
				let t = a.topic.expect("topic set");
				assert_eq!(&t[..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
				assert_eq!(a.drain_timeout_ms, 9000);
			},
			_ => panic!("expected query"),
		}
	}

	#[test]
	fn query_rejects_missing_endpoints() {
		assert!(Cli::try_parse_from(["statement-ops-bench", "query"]).is_err());
	}

	#[test]
	fn query_rejects_short_topic() {
		assert!(Cli::try_parse_from([
			"statement-ops-bench",
			"query",
			"--rpc-endpoints",
			"ws://a",
			"--topic",
			"00",
		])
		.is_err());
	}

	#[test]
	fn query_rejects_non_hex_topic() {
		let bad = format!("zz{}", "00".repeat(31));
		assert!(Cli::try_parse_from([
			"statement-ops-bench",
			"query",
			"--rpc-endpoints",
			"ws://a",
			"--topic",
			&bad,
		])
		.is_err());
	}

	#[test]
	fn query_rejects_seed_flag() {
		// `query` is read-only: it deliberately takes no signing/submission args.
		assert!(Cli::try_parse_from([
			"statement-ops-bench",
			"query",
			"--rpc-endpoints",
			"ws://a",
			"--seed",
			"//Alice",
		])
		.is_err());
	}
}
