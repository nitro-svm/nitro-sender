use std::sync::Arc;

use solana_epoch_info::EpochInfo;
use solana_program::clock::DEFAULT_MS_PER_SLOT;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::{
    sync::{RwLock, watch},
    task::JoinHandle,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn};

use crate::messages::BlockMessage;

/// Watches the Solana blockchain for new blocks and broadcasts updates via a channel.
#[derive(Clone)]
pub struct BlockWatcher {
    block_message_tx: watch::Sender<BlockMessage>,
    rpc_client: Arc<RpcClient>,
    head: Arc<RwLock<BlockMessage>>,
    cancellation_token: CancellationToken,
}

impl BlockWatcher {
    /// Creates a new [`BlockWatcher`] instance along with a [`watch::Receiver`] to receive
    /// [`BlockMessage`] updates.
    pub fn new(
        rpc_client: Arc<RpcClient>,
        cancellation_token: CancellationToken,
    ) -> (Arc<Self>, watch::Receiver<BlockMessage>) {
        let head = BlockMessage::default();
        let (block_message_tx, mut block_message_rx) = watch::channel(head);
        block_message_rx.mark_unchanged();
        (
            Arc::new(Self {
                block_message_tx,
                rpc_client,
                head: Arc::new(RwLock::new(head)),
                cancellation_token,
            }),
            block_message_rx,
        )
    }

    /// Gets the latest block information from the RPC client.
    async fn get_block_info(&self) -> Option<BlockMessage> {
        let (blockhash, last_valid_block_height) = self
            .rpc_client
            .get_latest_blockhash_with_commitment(self.rpc_client.commitment())
            .await
            .inspect_err(|e| {
                warn!("failed to get latest blockhash: {e}");
            })
            .ok()?;

        let EpochInfo { block_height, .. } = self
            .rpc_client
            .get_epoch_info_with_commitment(self.rpc_client.commitment())
            .await
            .inspect_err(|e| {
                warn!("failed to get epoch info: {e}");
            })
            .ok()?;

        Some(BlockMessage {
            blockhash,
            last_valid_block_height,
            block_height,
        })
    }

    #[instrument(skip(self), name = "[block-watcher]")]
    pub async fn update_loop(&self) {
        // This will never equal the new slot, so the first update is always broadcast.
        let mut ticker = tokio::time::interval(Duration::from_millis(DEFAULT_MS_PER_SLOT));

        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    // If we received a shutdown signal, exit the loop.
                    info!("received shutdown signal, exiting block watcher");
                    break;
                }
                _ = self.block_message_tx.closed() => {
                    // If the channel is closed, exit the loop.
                    break;
                }
                _ = ticker.tick() => {
                    let Some(new_update) = self.get_block_info().await else {
                        warn!("failed to get block info, retrying");
                        continue;
                    };

                    if new_update == *self.head.read().await {
                        warn!("skipping duplicate block update: {new_update:?}");
                        continue;
                    }

                    self.head.write().await.clone_from(&new_update);

                    if let Err(e) = self.block_message_tx.send(new_update) {
                        warn!("failed to send block update: {e}");
                        break;
                    }
                }
            };
        }

        warn!("shutting down block watcher");
    }

    /// Spawns an independent task that periodically checks the latest blockhash and epoch info using
    /// the Solana RPC client, and broadcasts it as a [`BlockMessage`] on the given channel.
    pub fn spawn(self: &Arc<Self>) -> JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            this.update_loop().await;
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;

    use async_trait::async_trait;
    use solana_client::{
        rpc_client::RpcClientConfig,
        rpc_request::RpcRequest,
        rpc_response::{Response, RpcBlockhash, RpcResponseContext},
        rpc_sender::{RpcSender, RpcTransportStats},
    };
    use solana_epoch_info::EpochInfo;
    use solana_program::hash::Hash;
    use solana_rpc_client::mock_sender::MockSender;
    use solana_rpc_client_api::client_error::Result as SolanaResult;
    use tokio::time::Instant;
    use tracing::Level;

    use super::*;

    /// This is essentially an integration test of the full lifecycle of the block watcher.
    #[tokio::test(start_paused = true)]
    async fn test_block_watcher() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .try_init();

        // Use the paused current time as a reference point so the rest of the test doesn't depend
        // on the current time.
        let initial_time = Instant::now();

        let client = Arc::new(RpcClient::new_sender(
            // This sender is implemented below.
            MockBlockSender {
                sender: MockSender::new("succeeds"),
                initial_time,
                max_slot: 3,
            },
            RpcClientConfig::default(),
        ));
        let cancellation_token = CancellationToken::new();
        let (block_watcher, mut rx) = BlockWatcher::new(client.clone(), cancellation_token.clone());
        let handle = block_watcher.spawn();

        // Checking the value half a slot later should give a new value.
        tokio::time::sleep_until(initial_time + Duration::from_millis(DEFAULT_MS_PER_SLOT / 2))
            .await;
        assert_eq!(
            *rx.borrow_and_update(),
            BlockMessage {
                blockhash: Hash::default(),
                last_valid_block_height: 150,
                block_height: 0
            }
        );

        // Checking the value one slot later (and a bit) should give a new value.
        tokio::time::sleep_until(initial_time + Duration::from_millis(DEFAULT_MS_PER_SLOT + 1))
            .await;
        assert_eq!(
            *rx.borrow_and_update(),
            BlockMessage {
                blockhash: Hash::default(),
                last_valid_block_height: 151,
                block_height: 1
            }
        );

        // Checking the value two slots later should skip the intermediate value.
        tokio::time::sleep_until(initial_time + Duration::from_millis(3 * DEFAULT_MS_PER_SLOT + 1))
            .await;
        assert_eq!(
            *rx.borrow_and_update(),
            BlockMessage {
                blockhash: Hash::default(),
                // Note: Not 152 and 2.
                last_valid_block_height: 153,
                block_height: 3
            }
        );

        // The sender is set up to keep returning slot 3 forever, never slot 4, so the watcher
        // shouldn't send any updates after this point and this should time out.
        tokio::time::timeout_at(
            initial_time + Duration::from_millis(6 * DEFAULT_MS_PER_SLOT + 1),
            rx.changed(),
        )
        .await
        // The err being unwrapped is the timeout error.
        .unwrap_err();

        // Drop the receiver to trigger the watcher to exit.
        drop(rx);
        handle.await.unwrap();
    }
    struct MockBlockSender {
        sender: MockSender,
        initial_time: Instant,
        max_slot: u64,
    }

    #[async_trait]
    impl RpcSender for MockBlockSender {
        async fn send(
            &self,
            request: RpcRequest,
            params: serde_json::Value,
        ) -> SolanaResult<serde_json::Value> {
            // For this test it's fine to pretend that slots and blocks are the same thing.
            let slot = (Instant::now().duration_since(self.initial_time).as_millis()
                / DEFAULT_MS_PER_SLOT as u128) as u64;
            let slot = min(slot, self.max_slot);
            if let RpcRequest::GetLatestBlockhash = request {
                Ok(serde_json::to_value(Response {
                    context: RpcResponseContext {
                        slot,
                        api_version: None,
                    },
                    value: RpcBlockhash {
                        blockhash: Hash::default().to_string(),
                        last_valid_block_height: slot + 150,
                    },
                })?)
            } else if let RpcRequest::GetEpochInfo = request {
                Ok(serde_json::to_value(EpochInfo {
                    epoch: 0,
                    slot_index: slot,
                    slots_in_epoch: 256,
                    absolute_slot: slot,
                    block_height: slot,
                    transaction_count: Some(123),
                })?)
            } else {
                self.sender.send(request, params).await
            }
        }

        fn get_transport_stats(&self) -> RpcTransportStats {
            self.sender.get_transport_stats()
        }

        fn url(&self) -> String {
            self.sender.url()
        }
    }
}
