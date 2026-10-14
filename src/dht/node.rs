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

//! Statement nodes as the bench sees them: an RPC client and the key the DHT ranks the node by.

use anyhow::{anyhow, Context, Result};
use codec::Encode;
use jsonrpsee::{
	core::client::{ClientT, Subscription, SubscriptionClientT},
	rpc_params,
	ws_client::{WsClient, WsClientBuilder},
};
use sc_network_types::PeerId;
use sp_core::{blake2_256, Bytes};
use sp_statement_store::{Statement, StatementEvent, SubmitResult, TopicFilter};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::sync::RwLock;

/// Longest gap between two batches of a store replay before the read is abandoned.
const REPLAY_STALL: Duration = Duration::from_secs(60);

pub struct Node {
	pub endpoint: String,
	/// `blake2_256` of the node's peer id bytes, the key its DHT replica rank derives from.
	pub dht_key: [u8; 32],
	client: RwLock<Arc<WsClient>>,
}

async fn connect(endpoint: &str) -> Result<WsClient> {
	WsClientBuilder::default()
		.max_concurrent_requests(10_000)
		.build(endpoint)
		.await
		.with_context(|| format!("failed to connect to {endpoint}"))
}

impl Node {
	pub async fn connect(endpoint: &str) -> Result<Self> {
		let client = connect(endpoint).await?;
		let peer_id: String = client
			.request("system_localPeerId", rpc_params![])
			.await
			.with_context(|| format!("system_localPeerId failed on {endpoint}"))?;
		let peer_id = PeerId::from_str(&peer_id)
			.map_err(|e| anyhow!("{endpoint} reported an invalid peer id {peer_id}: {e:?}"))?;
		Ok(Self {
			endpoint: endpoint.to_string(),
			dht_key: blake2_256(&peer_id.to_bytes()),
			client: RwLock::new(Arc::new(client)),
		})
	}

	/// The node's client, reconnected first if the connection dropped: a run lasts a day.
	async fn client(&self) -> Result<Arc<WsClient>> {
		let current = self.client.read().await.clone();
		if current.is_connected() {
			return Ok(current);
		}
		let mut client = self.client.write().await;
		if !client.is_connected() {
			*client = Arc::new(connect(&self.endpoint).await?);
		}
		Ok(client.clone())
	}

	pub async fn submit(&self, statement: &Statement) -> Result<SubmitResult> {
		let encoded: Bytes = statement.encode().into();
		self.client()
			.await?
			.request("statement_submit", rpc_params![encoded])
			.await
			.map_err(|e| anyhow!("statement_submit failed on {}: {e}", self.endpoint))
	}

	pub async fn subscribe(&self, filter: TopicFilter) -> Result<Subscription<StatementEvent>> {
		self.client()
			.await?
			.subscribe(
				"statement_subscribeStatement",
				rpc_params![filter],
				"statement_unsubscribeStatement",
			)
			.await
			.map_err(|e| anyhow!("statement_subscribeStatement failed on {}: {e}", self.endpoint))
	}

	/// Pass every statement stored on the node to `visit`. A match-all filter names no topic, so
	/// the read leaves the node's advertised affinity, and with it the DHT, untouched.
	pub async fn read_store(&self, mut visit: impl FnMut(&[u8])) -> Result<()> {
		let mut subscription = self.subscribe(TopicFilter::Any).await?;
		loop {
			let event = tokio::time::timeout(REPLAY_STALL, subscription.next())
				.await
				.map_err(|_| anyhow!("store replay stalled on {}", self.endpoint))?
				.ok_or_else(|| {
					anyhow!("subscription closed during the replay on {}", self.endpoint)
				})?
				.map_err(|e| anyhow!("store replay failed on {}: {e}", self.endpoint))?;
			let StatementEvent::NewStatements { statements, remaining } = event;
			// Statements arriving live carry no `remaining`; only the replay counts down to zero.
			let Some(remaining) = remaining else { continue };
			for statement in &statements {
				visit(&statement.0);
			}
			if remaining == 0 {
				return Ok(());
			}
		}
	}
}

/// Node indices by the XOR distance of their key to `topic`, closest first: the order the DHT
/// ranks replicas in. Distinct keys never tie.
pub fn by_distance(keys: &[[u8; 32]], topic: &[u8; 32]) -> Vec<usize> {
	let mut order: Vec<usize> = (0..keys.len()).collect();
	order.sort_by_cached_key(|&idx| -> [u8; 32] {
		std::array::from_fn(|i| keys[idx][i] ^ topic[i])
	});
	order
}
