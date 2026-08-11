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

//! Resolve a sudo signing keypair from any of the common formats an operator
//! might already have their key in, so they don't have to reformat it:
//!
//! - `--sudo-seed <SURI>`      — inline secret URI (`//Alice`, a mnemonic, a `0x`-hex seed,
//!   `…///password`, derivations). Also read from the `STATEMENT_SUDO_SEED` environment variable.
//! - `--sudo-seed-file <PATH>` — a file holding such a SURI, or a substrate node keystore key file
//!   (which is just a JSON-quoted SURI string).
//! - `--sudo-json <PATH>`      — a Polkadot-JS encrypted JSON account backup, unlocked with a
//!   password (`--sudo-password` / `STATEMENT_SUDO_PASSWORD` / `--sudo-password-file` /
//!   `--sudo-password-interactive`).
//!
//! All paths converge on a single `subxt_signer::sr25519::Keypair`.

use anyhow::{anyhow, bail, Context, Result};
use clap::{ArgGroup, Args};
use std::{fs, path::PathBuf, str::FromStr};
use subxt_signer::{sr25519::Keypair as SubxtKeypair, SecretUri};

/// Sudo signing-key options. Exactly one key source is required; the password
/// options apply only to `--sudo-json`.
#[derive(Args, Debug)]
#[command(group(
	ArgGroup::new("sudo_key")
		.required(true)
		.args(["sudo_seed", "sudo_seed_file", "sudo_json"]),
))]
pub struct SudoKeyArgs {
	/// Sudo seed/SURI (e.g. "//Alice", a mnemonic phrase, or a `0x`-hex seed).
	#[arg(long, env = "STATEMENT_SUDO_SEED")]
	pub sudo_seed: Option<String>,

	/// Path to a file containing a sudo seed/SURI, or a substrate node keystore
	/// key file (a JSON-quoted SURI string).
	#[arg(long)]
	pub sudo_seed_file: Option<PathBuf>,

	/// Path to a Polkadot-JS encrypted JSON account backup. Requires a password.
	#[arg(long)]
	pub sudo_json: Option<PathBuf>,

	/// Password for `--sudo-json` (also read from `STATEMENT_SUDO_PASSWORD`).
	#[arg(long, env = "STATEMENT_SUDO_PASSWORD")]
	pub sudo_password: Option<String>,

	/// Read the `--sudo-json` password from a file (a single trailing newline is
	/// stripped).
	#[arg(long)]
	pub sudo_password_file: Option<PathBuf>,

	/// Prompt for the `--sudo-json` password interactively (hidden input).
	#[arg(long)]
	pub sudo_password_interactive: bool,
}

/// Resolve the configured key source into a signing keypair.
pub fn resolve_sudo_keypair(args: &SudoKeyArgs) -> Result<SubxtKeypair> {
	if let Some(suri) = &args.sudo_seed {
		return keypair_from_suri(suri);
	}
	if let Some(path) = &args.sudo_seed_file {
		let contents = fs::read_to_string(path)
			.with_context(|| format!("Failed to read sudo seed file {}", path.display()))?;
		return keypair_from_keystore_contents(&contents);
	}
	if let Some(path) = &args.sudo_json {
		let json = fs::read_to_string(path)
			.with_context(|| format!("Failed to read Polkadot-JS JSON {}", path.display()))?;
		let password = resolve_password(args)?;
		return keypair_from_pjs_json(&json, &password);
	}
	// Unreachable while the clap `sudo_key` group is `required`, but stay safe.
	bail!("no sudo key provided: pass --sudo-seed, --sudo-seed-file, or --sudo-json")
}

/// Resolve the password used to unlock a `--sudo-json` keystore.
fn resolve_password(args: &SudoKeyArgs) -> Result<String> {
	if let Some(password) = &args.sudo_password {
		return Ok(password.clone());
	}
	if let Some(path) = &args.sudo_password_file {
		let raw = fs::read_to_string(path)
			.with_context(|| format!("Failed to read password file {}", path.display()))?;
		// Strip a single trailing newline (CRLF or LF) added by editors; keep
		// any other characters verbatim since passwords may contain spaces.
		let trimmed = raw.strip_suffix('\n').unwrap_or(&raw);
		let trimmed = trimmed.strip_suffix('\r').unwrap_or(trimmed);
		return Ok(trimmed.to_string());
	}
	if args.sudo_password_interactive {
		return rpassword::prompt_password("Keystore password: ")
			.context("Failed to read password from prompt");
	}
	bail!(
		"--sudo-json requires a password: provide --sudo-password, STATEMENT_SUDO_PASSWORD, \
		 --sudo-password-file, or --sudo-password-interactive"
	)
}

/// Build a keypair from an inline secret URI.
fn keypair_from_suri(suri: &str) -> Result<SubxtKeypair> {
	let uri =
		SecretUri::from_str(suri.trim()).map_err(|e| anyhow!("Invalid sudo seed URI: {e}"))?;
	SubxtKeypair::from_uri(&uri).map_err(|e| anyhow!("Failed to derive sudo keypair: {e}"))
}

/// Build a keypair from the contents of a seed file or a substrate node
/// keystore key file. A node keystore file stores the SURI as a JSON string
/// (e.g. `"//Alice"`); anything else is treated as a raw SURI.
fn keypair_from_keystore_contents(contents: &str) -> Result<SubxtKeypair> {
	let trimmed = contents.trim();
	let suri = if trimmed.starts_with('"') {
		serde_json::from_str::<String>(trimmed)
			.context("Failed to parse node keystore file as a JSON-encoded SURI string")?
	} else {
		trimmed.to_string()
	};
	keypair_from_suri(&suri)
}

/// Build a keypair from a Polkadot-JS encrypted JSON account backup.
fn keypair_from_pjs_json(json: &str, password: &str) -> Result<SubxtKeypair> {
	subxt_signer::polkadot_js_compat::decrypt_json(json, password)
		.map_err(|e| anyhow!("Failed to decrypt Polkadot-JS JSON keystore: {e}"))
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Canonical `//Alice` sr25519 public key (== account id).
	const ALICE: [u8; 32] = [
		0xd4, 0x35, 0x93, 0xc7, 0x15, 0xfd, 0xd3, 0x1c, 0x61, 0x14, 0x1a, 0xbd, 0x04, 0xa9, 0x9f,
		0xd6, 0x82, 0x2c, 0x85, 0x58, 0x85, 0x4c, 0xcd, 0xe3, 0x9a, 0x56, 0x84, 0xe7, 0xa5, 0x6d,
		0xa2, 0x7d,
	];

	/// Polkadot-JS encrypted JSON backup for Alice, password `whoisalice`.
	/// Taken verbatim from the `subxt-signer` `polkadot_js_compat` test vector.
	const ALICE_PJS_JSON: &str = r#"
		{
		  "encoded": "DumgApKCTqoCty1OZW/8WS+sgo6RdpHhCwAkA2IoDBMAgAAAAQAAAAgAAAB6IG/q24EeVf0JqWqcBd5m2tKq5BlyY84IQ8oamLn9DZe9Ouhgunr7i36J1XxUnTI801axqL/ym1gil0U8440Qvj0lFVKwGuxq38zuifgoj0B3Yru0CI6QKEvQPU5xxj4MpyxdSxP+2PnTzYao0HDH0fulaGvlAYXfqtU89xrx2/z9z7IjSwS3oDFPXRQ9kAdDebtyCVreZ9Otw9v3",
		  "encoding": {
		    "content": ["pkcs8", "sr25519"],
		    "type": ["scrypt", "xsalsa20-poly1305"],
		    "version": "3"
		  },
		  "address": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
		  "meta": { "genesisHash": "", "name": "Alice", "whenCreated": 1718265838755 }
		}
	"#;

	#[test]
	fn suri_resolves_to_alice() {
		assert_eq!(keypair_from_suri("//Alice").unwrap().public_key().0, ALICE);
		// Surrounding whitespace is tolerated.
		assert_eq!(keypair_from_suri("  //Alice\n").unwrap().public_key().0, ALICE);
	}

	#[test]
	fn keystore_contents_raw_and_json_quoted_resolve_to_alice() {
		// Raw SURI in a file.
		assert_eq!(keypair_from_keystore_contents("//Alice").unwrap().public_key().0, ALICE);
		// Substrate node keystore form: a JSON-quoted SURI string.
		assert_eq!(keypair_from_keystore_contents("\"//Alice\"").unwrap().public_key().0, ALICE);
		// Trailing newline (as written by editors / `serde_json::to_writer`) is tolerated.
		assert_eq!(keypair_from_keystore_contents("\"//Alice\"\n").unwrap().public_key().0, ALICE);
	}

	#[test]
	fn pjs_json_decrypts_to_alice() {
		assert_eq!(
			keypair_from_pjs_json(ALICE_PJS_JSON, "whoisalice").unwrap().public_key().0,
			ALICE
		);
	}

	#[test]
	fn pjs_json_wrong_password_errors() {
		assert!(keypair_from_pjs_json(ALICE_PJS_JSON, "not-the-password").is_err());
	}

	#[test]
	fn garbage_suri_errors() {
		assert!(keypair_from_suri("definitely not a valid suri %%%").is_err());
	}
}
