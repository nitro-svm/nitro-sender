use std::sync::Arc;

use solana_program::clock::DEFAULT_MS_PER_SLOT;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::{sync::watch, task::JoinHandle, time::Duration};
use tracing::{info, warn};

use crate::messages::BlockMessage;

async fn get_block_info(client: &RpcClient) -> Option<BlockMessage> {
    let (blockhash, last_valid_block_height) = client
        .get_latest_blockhash_with_commitment(client.commitment())
        .await
        .inspect_err(|e| {
            warn!("failed to get latest blockhash: {e}");
        })
        .ok()?;

    let epoch_info = client
        .get_epoch_info_with_commitment(client.commitment())
        .await
        .inspect_err(|e| {
            warn!("failed to get epoch info: {e}");
        })
        .ok()?;

    Some(BlockMessage {
        blockhash,
        last_valid_block_height,
        block_height: epoch_info.block_height,
    })
}

/// Spawns an independent task that periodically checks the latest blockhash and epoch info using
/// the Solana RPC client, and broadcasts it as a [`BlockMessage`] on the given channel.
///
/// The task will exit if there are no receivers alive. This will happen when the
/// [transaction confirmer](`super::transaction_confirmer::spawn_transaction_confirmer`) and
/// [transaction sender](`super::transaction_sender::spawn_transaction_sender`) tasks have both exited.
pub fn spawn_block_watcher(
    blockdata_tx: watch::Sender<BlockMessage>,
    shutdown_signal: watch::Receiver<()>,
    rpc_client: Arc<RpcClient>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // This will never equal the new slot, so the first update is always broadcast.
        let mut last_update = BlockMessage::default();
        let mut ticker = tokio::time::interval(Duration::from_millis(DEFAULT_MS_PER_SLOT));
        let mut shutdown_signal = shutdown_signal;

        loop {
            tokio::select! {
                _ = shutdown_signal.changed() => {
                    // If we received a shutdown signal, exit the loop.
                    info!("received shutdown signal, exiting block watcher");
                    break;
                }
                _ = blockdata_tx.closed() => {
                    // If the channel is closed, exit the loop.
                    break;
                }
                _ = ticker.tick() => {
                    let Some(new_update) = get_block_info(&rpc_client).await else {
                        // If we failed to get the block info, just try again on the next tick.
                        continue;
                    };

                    if new_update == last_update {
                        continue;
                    }

                    last_update = new_update;
                    if let Err(e) = blockdata_tx.send(new_update) {
                        warn!("failed to send block update: {e}");
                        break;
                    }
                }
            };
        }

        warn!("shutting down block watcher");
    })
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

        // Dummy initial value to distinguish it from what the MockBlockSender returns.
        let initial_value = BlockMessage {
            blockhash: Hash::default(),
            last_valid_block_height: 1234,
            block_height: 5678,
        };
        let (tx, mut rx) = watch::channel(initial_value);
        let client = Arc::new(RpcClient::new_sender(
            // This sender is implemented below.
            MockBlockSender {
                sender: MockSender::new("succeeds"),
                initial_time,
                max_slot: 3,
            },
            RpcClientConfig::default(),
        ));
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let handle = spawn_block_watcher(tx, shutdown_rx, client);

        // Checking the value straight away should return the initial value.
        assert_eq!(*rx.borrow_and_update(), initial_value);

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
