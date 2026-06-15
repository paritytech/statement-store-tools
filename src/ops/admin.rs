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

//! `admin` subcommand helpers: show and set the per-account statement-store
//! quota (allowance).
//!
//! The quota lives in unhashed runtime storage under the key
//! `b":statement_allowance:" ++ account_id`, holding a SCALE-encoded
//! [`StatementAllowance`]. It is *read* with the standard `state_getStorage`
//! RPC (no signer required) and *written* with a `Sudo(System.set_storage)`
//! extrinsic — there is no `state_setStorage` RPC — mirroring the `setup.rs`
//! binary.

use anyhow::{anyhow, Context, Result};
use codec::{Decode, Encode};
use jsonrpsee::{core::client::ClientT, rpc_params, ws_client::WsClientBuilder};
use sc_statement_store::subxt_client::{get_account_nonce, submit_extrinsic, CustomConfig};
use sp_core::Bytes;
use sp_statement_store::{statement_allowance_key, StatementAllowance};
use subxt::{
	ext::scale_value::{value, Value},
	OnlineClient,
};
use subxt_signer::sr25519::Keypair as SubxtKeypair;

/// Inputs for [`show_quota`].
pub struct ShowQuotaConfig {
	/// WebSocket RPC endpoint (e.g. `ws://127.0.0.1:9944`).
	pub endpoint: String,
	/// 32-byte account id whose quota to read.
	pub account: [u8; 32],
}

/// Inputs for [`set_quota`].
pub struct SetQuotaConfig {
	/// WebSocket RPC endpoint (e.g. `ws://127.0.0.1:9944`).
	pub endpoint: String,
	/// 32-byte account id whose quota to set.
	pub account: [u8; 32],
	/// Resolved keypair used to sign the `Sudo(System.set_storage)` extrinsic.
	pub signer: SubxtKeypair,
	/// Maximum number of statements allowed for the account.
	pub max_count: u32,
	/// Maximum total size of statements, in bytes, for the account.
	pub max_size: u32,
}

/// Read an account's statement-store quota via the `state_getStorage` RPC.
///
/// Returns `Ok(None)` when no allowance has been set for the account (the
/// storage key is absent), which is distinct from an allowance explicitly set
/// to zero (`Some(StatementAllowance { max_count: 0, max_size: 0 })`).
pub async fn show_quota(config: &ShowQuotaConfig) -> Result<Option<StatementAllowance>> {
	let key: Bytes = statement_allowance_key(config.account).into();
	let client = WsClientBuilder::default()
		.build(&config.endpoint)
		.await
		.with_context(|| format!("Failed to connect to {}", config.endpoint))?;

	let raw: Option<Bytes> = client
		.request("state_getStorage", rpc_params![key])
		.await
		.map_err(|e| anyhow!("state_getStorage failed on {}: {e}", config.endpoint))?;

	match raw {
		Some(bytes) if !bytes.0.is_empty() => {
			let allowance = StatementAllowance::decode(&mut &bytes.0[..])
				.context("Failed to decode StatementAllowance from storage value")?;
			Ok(Some(allowance))
		},
		_ => Ok(None),
	}
}

/// Set an account's statement-store quota by submitting a
/// `Sudo(System.set_storage { items: [(key, allowance)] })` extrinsic and
/// waiting for it to be finalized.
pub async fn set_quota(config: &SetQuotaConfig) -> Result<()> {
	let client = OnlineClient::<CustomConfig>::from_insecure_url_with_config(
		CustomConfig::default(),
		&config.endpoint,
	)
	.await
	.with_context(|| format!("Failed to connect to {}", config.endpoint))?;

	let allowance = StatementAllowance::new(config.max_count, config.max_size);
	let key = statement_allowance_key(config.account);
	let items = vec![Value::unnamed_composite([
		Value::from_bytes(key),
		Value::from_bytes(allowance.encode()),
	])];
	let tx =
		subxt::tx::dynamic("Sudo", "sudo", vec![value! { System(set_storage { items: items }) }]);

	let sudo_account_id =
		<SubxtKeypair as subxt::transactions::Signer<CustomConfig>>::account_id(&config.signer);
	let nonce = get_account_nonce(&client, &sudo_account_id).await?;
	submit_extrinsic(&client, &tx, &config.signer, nonce).await?;
	Ok(())
}
