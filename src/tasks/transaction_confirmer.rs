use std::{option::Option, sync::Arc};

use solana_client::rpc_client::SerializableTransaction;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_transaction_status::UiTransactionEncoding;
use tokio::{
    sync::{RwLock, mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, trace, warn};

use crate::{
    messages::{BlockMessage, ConfirmTransactionMessage, SendTransactionMessage, StatusMessage},
    transaction::TransactionStatus,
};

#[derive(Debug, thiserror::Error)]
pub enum ConfirmError {
    #[error("channel error: {0}")]
    ChannelError(#[from] tokio::sync::watch::error::RecvError),

    #[error("confirmation channel closed")]
    ConfirmChannelClosed,

    #[error("sender channel closed")]
    SenderChannelClosed,
}

pub type ConfirmResult<T = ()> = Result<T, ConfirmError>;

/// A task managing transaction confirmations.
#[derive(Clone)]
pub struct Confirmer {
    rpc_client: Arc<RpcClient>,
    block_message_rx: watch::Receiver<BlockMessage>,
    sender_tx: mpsc::UnboundedSender<SendTransactionMessage>,
    confirmer_tx: mpsc::UnboundedSender<ConfirmTransactionMessage>,
    confirmer_rx: Arc<RwLock<mpsc::UnboundedReceiver<ConfirmTransactionMessage>>>,
    cancellation_token: CancellationToken,
}

impl Confirmer {
    /// Creates a new [`Confirmer`] instance.
    pub fn new(
        rpc_client: Arc<RpcClient>,
        block_message_rx: watch::Receiver<BlockMessage>,
        transaction_sender_tx: mpsc::UnboundedSender<SendTransactionMessage>,
        cancellation_token: CancellationToken,
    ) -> (Self, mpsc::UnboundedSender<ConfirmTransactionMessage>) {
        let (confirmer_tx, confirmer_rx) = mpsc::unbounded_channel();
        (
            Self {
                rpc_client,
                block_message_rx,
                sender_tx: transaction_sender_tx,
                confirmer_tx: confirmer_tx.clone(),
                confirmer_rx: Arc::new(RwLock::new(confirmer_rx)),
                cancellation_token,
            },
            confirmer_tx,
        )
    }

    async fn reconfirm_message(&self, msg: ConfirmTransactionMessage) -> ConfirmResult {
        self.confirmer_tx.send(msg).map_err(|e| {
            warn!("failed to re-queue transaction for confirmation: {e}");
            ConfirmError::ConfirmChannelClosed
        })
    }

    async fn resend_message(&self, msg: SendTransactionMessage) -> ConfirmResult {
        self.sender_tx.send(msg).map_err(|e| {
            warn!("failed to queue transactions for re-sending: {e}");
            ConfirmError::SenderChannelClosed
        })
    }

    async fn read_batch(&mut self) -> ConfirmResult<Vec<ConfirmTransactionMessage>> {
        let mut batch = Vec::new();

        let mut confirmer_rx = self.confirmer_rx.write().await;

        let received_messages = tokio::select! {
            _ = self.cancellation_token.cancelled() => {
                return Err(ConfirmError::ConfirmChannelClosed);
            }
            received = confirmer_rx.recv_many(&mut batch, 256) => {
                received
            }
        };

        if received_messages == 0 {
            // If this is ever zero, that means the channel was closed.
            // No more transactions will ever be received, so the task should exit.
            Err(ConfirmError::ConfirmChannelClosed)
        } else {
            Ok(batch)
        }
    }

    async fn get_transaction_statuses(
        &self,
        batch: Vec<ConfirmTransactionMessage>,
    ) -> ConfirmResult<
        Option<impl Iterator<Item = (Option<TransactionStatus>, ConfirmTransactionMessage)>>,
    > {
        let signatures: Vec<_> = batch
            .iter()
            .map(|msg| *msg.transaction.get_signature())
            .collect();

        let Ok(response) = self
            .rpc_client
            .get_signature_statuses(&signatures[..])
            .await
            .inspect_err(|e| {
                warn!("failed to get signatures: {e:?}");
            })
        else {
            for msg in &batch {
                self.reconfirm_message(msg.clone()).await?;
            }
            // The transactions were re-queued, keep the loop going.
            return Ok(None);
        };

        trace!(
            "got status for {} signatures",
            response.value.iter().flatten().count()
        );

        let mut all_logs = Vec::with_capacity(response.value.len());

        for (status, signature) in response.value.iter().zip(signatures.into_iter()) {
            let Some(status) = status else {
                // The RPC server didn't recognize the transaction, so it will be re-queued.
                all_logs.push(Vec::new());
                continue;
            };

            if status.err.is_none() {
                // The transaction was recognized and processed, so it will be reported.
                all_logs.push(Vec::new());
                continue;
            }

            let tx = match self
                .rpc_client
                .get_transaction(&signature, UiTransactionEncoding::Json)
                .await
            {
                Ok(tx) => tx,
                Err(e) => {
                    warn!("failed to get failed transaction: {e:?}");
                    all_logs.push(Vec::new());
                    continue;
                }
            };

            let Some(logs) = tx.transaction.meta.map(|meta| meta.log_messages) else {
                // The transaction was recognized but not processed, so it will be re-queued.
                all_logs.push(Vec::new());
                continue;
            };

            all_logs.push(logs.unwrap_or(Vec::new()));
        }

        let responses = response
            .value
            .into_iter()
            .zip(all_logs.into_iter())
            .map(|(status, logs)| {
                status.map(|status| TransactionStatus::from_solana_status(status, logs))
            })
            .zip(batch.into_iter());

        Ok(Some(responses))
    }

    #[instrument(skip(self), name = "[confirmer]")]
    async fn confirm_loop(&mut self) -> ConfirmResult {
        // Wait for a new blockhash to be available.
        self.block_message_rx.changed().await?;
        let blockdata = *self.block_message_rx.borrow_and_update();

        let batch = self.read_batch().await?;

        let Some(responses) = self.get_transaction_statuses(batch).await? else {
            // If transaction status retrieval fails, don't stop the loop, just keep going
            // and try again later.
            return Ok(());
        };

        let TransactionResponseCategories {
            status_updates,
            resend,
            mut reconfirm,
        } = TransactionResponseCategories::categorize(responses, blockdata.last_valid_block_height);

        for (status, msg) in status_updates {
            let slot = status.slot();

            trace!(
                "[{}] transaction {} status: {status:?} at slot {slot}",
                msg.index,
                msg.transaction.get_signature(),
            );

            // If the transaction wasn't committed or failed, it has to be checked again.
            if status.should_be_reconfirmed(self.rpc_client.commitment()) {
                reconfirm.push(msg.clone());
            }

            msg.send_response(StatusMessage {
                index: msg.index,
                landed_as: Some((slot, *msg.transaction.get_signature())),
                status,
            });
        }

        for msg in resend {
            self.resend_message(msg).await?;
        }

        for msg in reconfirm {
            self.reconfirm_message(msg).await?;
        }

        Ok(())
    }

    async fn confirmer_loop(mut self) {
        let cancellation_token = self.cancellation_token.clone();

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("received shutdown signal, exiting transaction confirmer");
                    break;
                }
                res = self.confirm_loop() => {
                    if let Err(e) = res {
                        warn!("transaction confirmer error: {e}");
                        break;
                    }
                }
            }
        }

        warn!("shutting down transaction confirmer");
    }

    /// Spawns the transaction confirmer task.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move { self.confirmer_loop().await })
    }
}

#[derive(Default)]
struct TransactionResponseCategories {
    pub status_updates: Vec<(TransactionStatus, ConfirmTransactionMessage)>,
    pub resend: Vec<SendTransactionMessage>,
    pub reconfirm: Vec<ConfirmTransactionMessage>,
}

impl TransactionResponseCategories {
    /// Categorizes the transaction status responses from the RPC client into three different types of outcomes:
    /// - Status updates, regardless of good or bad.
    /// - Transactions that need to be re-sent due to timeouts or errors.
    /// - Transactions that need to be re-confirmed due to still being processed.
    ///
    /// The transaction categories are not mutually exclusive.
    fn categorize(
        responses: impl Iterator<Item = (Option<TransactionStatus>, ConfirmTransactionMessage)>,
        last_valid_block_height: u64,
    ) -> Self {
        let (resend, reconfirm, status_updates) = responses.fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut resend, mut reconfirm, mut status_updates), (status, msg)| {
                if msg.response_tx.is_closed() {
                    // The receiver has been dropped, ignore the transaction and move on to the next.
                    return (resend, reconfirm, status_updates);
                }

                let Some(status) = status else {
                    // If there is no status, the transaction was not recognized by the RPC server.
                    if msg.last_valid_block_height + 10 < last_valid_block_height {
                        // The request was not successful within 10 slots using RPC, try again.
                        trace!(
                            "[{}] transaction {} timed out after {} slots, re-sending",
                            msg.index,
                            msg.transaction.get_signature(),
                            last_valid_block_height - msg.last_valid_block_height
                        );
                        resend.push(msg.into());
                    } else {
                        // No status reported, check again later.
                        reconfirm.push(msg);
                    }
                    return (resend, reconfirm, status_updates);
                };

                if status.should_be_resent() {
                    // Some instructions are expected to fail, for example inserting too far ahead
                    // or closing the Blober when it's not yet full.
                    // Other errors are *not* expected and will be logged, but will not otherwise be
                    // handled in any special way.
                    warn!(
                        "unexpected transaction error for [{}] (batch index: {}, slot: {}): {:?}",
                        msg.transaction.get_signature(),
                        msg.index,
                        status.slot(),
                        status.error(),
                    );
                    // Regardless of the error type, it will be reported, re-signed and re-sent.
                    resend.push(SendTransactionMessage {
                        span: msg.span.clone(),
                        index: msg.index,
                        transaction: msg.transaction.clone(),
                        // Force re-sign. Since the transaction itself failed, this is safe.
                        last_valid_block_height: 0,
                        response_tx: msg.response_tx.clone(),
                    });
                }

                status_updates.push((status, msg));
                (resend, reconfirm, status_updates)
            },
        );

        Self {
            status_updates,
            resend,
            reconfirm,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{mem, sync::Mutex};

    use async_trait::async_trait;
    use solana_client::{
        client_error::ClientError,
        rpc_client::RpcClientConfig,
        rpc_request::RpcRequest,
        rpc_response::{Response, RpcResponseContext},
        rpc_sender::{RpcSender, RpcTransportStats},
    };
    use solana_commitment_config::CommitmentConfig;
    use solana_keypair::Keypair;
    use solana_program::{hash::Hash, instruction::InstructionError};
    use solana_pubkey::Pubkey;
    use solana_rpc_client::mock_sender::MockSender;
    use solana_rpc_client_api::client_error::Result as SolanaResult;
    use solana_signature::Signature;
    use solana_signer::Signer;
    use solana_transaction::Transaction;
    use solana_transaction_error::TransactionError;
    use solana_transaction_status::{
        TransactionConfirmationStatus, TransactionStatus as SolanaTransactionStatus,
    };
    use tracing::{Level, Span};

    use super::*;
    use crate::transaction::TransactionStatus;

    fn categorize_helper(
        signature_status: Option<Result<(), TransactionError>>,
        signature_err: Option<TransactionError>,
        confirmation_status: Option<TransactionConfirmationStatus>,
        last_valid_block_height: u64,
    ) -> TransactionResponseCategories {
        let (response_tx, response_rx) = mpsc::unbounded_channel();
        // We don't need the receiver, just make sure it doesn't get dropped.
        std::mem::forget(response_rx);
        let status = signature_status.map(|status| {
            TransactionStatus::from_solana_status(
                SolanaTransactionStatus {
                    slot: 0,
                    confirmations: None,
                    status,
                    err: signature_err,
                    confirmation_status,
                },
                Vec::new(),
            )
        });
        let msg = ConfirmTransactionMessage {
            span: Span::current(),
            index: 0,
            transaction: Transaction {
                signatures: vec![Signature::default()],
                ..Default::default()
            },
            last_valid_block_height: 0,
            response_tx: response_tx.clone(),
        };
        TransactionResponseCategories::categorize(
            [(status, msg)].into_iter(),
            last_valid_block_height,
        )
    }

    #[tokio::test]
    async fn test_categorize_transaction_response() {
        // Status is present as processed, no error.
        let categories = categorize_helper(
            Some(Ok(())),
            None,
            Some(TransactionConfirmationStatus::Processed),
            0,
        );
        // Should result in a status update.
        assert_eq!(categories.status_updates.len(), 1);
        assert_eq!(categories.resend.len(), 0);
        assert_eq!(categories.reconfirm.len(), 0);

        // Status is present as confirmed, no error.
        let categories = categorize_helper(
            Some(Ok(())),
            None,
            Some(TransactionConfirmationStatus::Confirmed),
            0,
        );
        // Should result in a status update.
        assert_eq!(categories.status_updates.len(), 1);
        assert_eq!(categories.resend.len(), 0);
        assert_eq!(categories.reconfirm.len(), 0);

        // Status is absent, last valid block height is equal to the transaction's last valid block height.
        let categories = categorize_helper(
            None,
            None,
            Some(TransactionConfirmationStatus::Confirmed),
            0,
        );
        // Should result in a reconfirm.
        assert_eq!(categories.status_updates.len(), 0);
        assert_eq!(categories.resend.len(), 0);
        assert_eq!(categories.reconfirm.len(), 1);

        // Status is absent, last valid block height is >100 above the transaction's last valid block height,
        // and the TPU client is enabled.
        let categories = categorize_helper(
            None,
            None,
            Some(TransactionConfirmationStatus::Confirmed),
            101,
        );
        // Should result in a resend.
        assert_eq!(categories.status_updates.len(), 0);
        assert_eq!(categories.resend.len(), 1);
        assert_eq!(categories.reconfirm.len(), 0);

        // Status is absent, last valid block height is >10 above the transaction's last valid block height,
        // and the TPU client is disabled.
        let categories = categorize_helper(
            None,
            None,
            Some(TransactionConfirmationStatus::Confirmed),
            11,
        );
        // Should result in a resend.
        assert_eq!(categories.status_updates.len(), 0);
        assert_eq!(categories.resend.len(), 1);
        assert_eq!(categories.reconfirm.len(), 0);

        // Status is present as an AlreadyProcessed error.
        let categories = categorize_helper(
            Some(Ok(())),
            Some(TransactionError::AlreadyProcessed),
            Some(TransactionConfirmationStatus::Confirmed),
            0,
        );
        // Should result in a status update.
        assert_eq!(categories.status_updates.len(), 1);
        assert_eq!(categories.resend.len(), 0);
        assert_eq!(categories.reconfirm.len(), 0);

        // Status is present as some other error.
        let categories = categorize_helper(
            Some(Ok(())),
            Some(TransactionError::AccountInUse),
            Some(TransactionConfirmationStatus::Confirmed),
            0,
        );
        // Should result in a status update and a re-send.
        assert_eq!(categories.status_updates.len(), 1);
        assert_eq!(categories.resend.len(), 1);
        assert_eq!(categories.reconfirm.len(), 0);
    }

    #[tokio::test]
    async fn test_get_transaction_statuses_success() {
        let payer = Arc::new(Keypair::new());
        let status = SolanaTransactionStatus {
            status: Ok(()),
            slot: 0,
            confirmations: None,
            err: None,
            confirmation_status: Some(TransactionConfirmationStatus::Confirmed),
        };

        let rpc_client = Arc::new(RpcClient::new_sender(
            MockSender::new_with_mocks(
                "succeeds",
                [(
                    RpcRequest::GetSignatureStatuses,
                    serde_json::to_value(Response {
                        context: RpcResponseContext {
                            slot: 1,
                            api_version: None,
                        },
                        // One status value, one None.
                        value: vec![Some(status), None],
                    })
                    .unwrap(),
                )]
                .into_iter()
                .collect(),
            ),
            RpcClientConfig::with_commitment(CommitmentConfig::confirmed()),
        ));

        let (confirmer, _) = Confirmer::new(
            rpc_client.clone(),
            watch::channel(BlockMessage::default()).1,
            mpsc::unbounded_channel().0,
            CancellationToken::new(),
        );
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();

        let transaction = Transaction::new_signed_with_payer(
            &[solana_program::system_instruction::transfer(
                &payer.pubkey(),
                &solana_program::system_program::id(),
                1,
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let messages = confirmer
            .get_transaction_statuses(vec![
                ConfirmTransactionMessage {
                    span: Span::current(),
                    index: 0,
                    transaction: transaction.clone(),
                    last_valid_block_height: 0,
                    response_tx: response_tx.clone(),
                },
                ConfirmTransactionMessage {
                    span: Span::current(),
                    index: 1,
                    transaction,
                    last_valid_block_height: 0,
                    response_tx: response_tx.clone(),
                },
            ])
            .await
            .unwrap()
            .unwrap();

        // Both messages should be returned.
        let messages: Vec<_> = messages.collect();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].1.index, 0);
        assert_eq!(
            messages[0].0.as_ref().unwrap().confirmation_status(),
            Some(TransactionConfirmationStatus::Confirmed)
        );
        assert_eq!(messages[1].1.index, 1);
        assert_eq!(messages[1].0, None);

        // Nothing should have been queued for re-confirmation.
        confirmer.confirmer_rx.write().await.try_recv().unwrap_err();
        // And no status updates should have been sent.
        response_rx.try_recv().unwrap_err();
    }

    #[tokio::test]
    async fn test_get_transaction_statuses_rpc_fails() {
        let payer = Arc::new(Keypair::new());
        let rpc_client = Arc::new(RpcClient::new_mock("fails".to_string()));

        let (confirmer, _) = Confirmer::new(
            rpc_client.clone(),
            watch::channel(BlockMessage::default()).1,
            mpsc::unbounded_channel().0,
            CancellationToken::new(),
        );
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();

        let transaction = Transaction::new_signed_with_payer(
            &[solana_program::system_instruction::transfer(
                &payer.pubkey(),
                &solana_program::system_program::id(),
                1,
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        assert!(
            confirmer
                .get_transaction_statuses(vec![
                    ConfirmTransactionMessage {
                        span: Span::current(),
                        index: 0,
                        transaction: transaction.clone(),
                        last_valid_block_height: 0,
                        response_tx: response_tx.clone(),
                    },
                    ConfirmTransactionMessage {
                        span: Span::current(),
                        index: 1,
                        transaction,
                        last_valid_block_height: 0,
                        response_tx: response_tx.clone(),
                    },
                ],)
                .await
                .unwrap()
                .is_none()
        );

        // The messages should have been queued for re-confirmation.
        let msg_0 = confirmer.confirmer_rx.write().await.recv().await.unwrap();
        let msg_1 = confirmer.confirmer_rx.write().await.recv().await.unwrap();
        assert_eq!(msg_0.index, 0);
        assert_eq!(msg_1.index, 1);
        // But no status updates should have been sent.
        response_rx.try_recv().unwrap_err();
    }

    #[tokio::test]
    async fn test_get_next_batch_for_confirmation() {
        let cancellation_token = CancellationToken::new();
        let (mut confirmer, transaction_confirmer_tx) = Confirmer::new(
            Arc::new(RpcClient::new_mock("succeeds".to_string())),
            watch::channel(BlockMessage::default()).1,
            mpsc::unbounded_channel().0,
            cancellation_token.clone(),
        );
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();

        // Queue 5 transactions.
        for message in generate_confirm_messages(5, &response_tx) {
            transaction_confirmer_tx.send(message.clone()).unwrap();
        }
        let batch = confirmer.read_batch().await.unwrap();
        assert_eq!(batch.len(), 5);

        // Queue 300 transactions.
        for message in generate_confirm_messages(300, &response_tx) {
            transaction_confirmer_tx.send(message.clone()).unwrap();
        }
        // The first batch should contain the first 256 transactions.
        let batch = confirmer.read_batch().await.unwrap();
        assert_eq!(batch.len(), 256);
        // The next batch should contain the remaining 44 transactions.
        let batch = confirmer.read_batch().await.unwrap();
        assert_eq!(batch.len(), 44);

        // Drop the sender.
        drop(transaction_confirmer_tx);
        cancellation_token.cancel();

        // The next batch should fail out with a ControlFlow::Break.
        assert!(matches!(
            confirmer.read_batch().await,
            Err(ConfirmError::ConfirmChannelClosed)
        ));

        // The response channel shouldn't have been touched throughout all the above.
        response_rx.try_recv().unwrap_err();

        cancellation_token.cancelled().await;
    }

    fn generate_confirm_messages(
        amount: usize,
        response_tx: &mpsc::UnboundedSender<StatusMessage>,
    ) -> Vec<ConfirmTransactionMessage> {
        (0..amount)
            .map(|index| ConfirmTransactionMessage {
                span: Span::current(),
                index,
                transaction: Transaction::default(),
                last_valid_block_height: 0,
                response_tx: response_tx.clone(),
            })
            .collect()
    }

    /// This is essentially an integration test of the full lifecycle of the transaction confirmer.
    #[tokio::test(start_paused = true)]
    async fn test_transaction_confirmer() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .try_init();

        let mock_sender = TrackingMockSender::new(MockSender::new("succeeds".to_string()));
        let rpc_client = Arc::new(RpcClient::new_sender(
            mock_sender.clone(),
            RpcClientConfig::with_commitment(CommitmentConfig::confirmed()),
        ));
        let payer = Arc::new(Keypair::new());

        let initial_block = BlockMessage {
            blockhash: Hash::new_from_array(Pubkey::new_unique().to_bytes()),
            last_valid_block_height: 300,
            block_height: 150,
        };
        let (blockdata_tx, blockdata_rx) = watch::channel(initial_block);
        let (transaction_sender_tx, mut transaction_sender_rx) =
            mpsc::unbounded_channel::<SendTransactionMessage>();

        let cancellation_token = CancellationToken::new();
        let (confirmer, transaction_confirmer_tx) = Confirmer::new(
            rpc_client.clone(),
            blockdata_rx,
            transaction_sender_tx.clone(),
            cancellation_token.clone(),
        );
        confirmer.spawn();

        // No requests should be sent yet.
        let sent_requests = mock_sender.get_and_clear_sent_requests();
        assert_eq!(sent_requests, Vec::new());

        // Queue a transaction for confirmation.
        let transaction = Transaction::new_signed_with_payer(
            &[solana_program::system_instruction::transfer(
                &payer.pubkey(),
                &solana_program::system_program::id(),
                1,
            )],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();
        transaction_confirmer_tx
            .send(ConfirmTransactionMessage {
                span: Span::current(),
                index: 0,
                transaction: transaction.clone(),
                last_valid_block_height: initial_block.last_valid_block_height,
                response_tx: response_tx.clone(),
            })
            .unwrap();

        // Trigger a "new" blockhash to make the confirmation loop iterate.
        blockdata_tx.send_modify(|_| {});

        // Wait for the loop to finish. There should be exactly one response sent.
        let status = response_rx.recv().await.unwrap();
        response_rx.try_recv().unwrap_err();
        assert_eq!(status.index, 0);
        assert_eq!(status.landed_as, Some((0, transaction.signatures[0])));
        assert!(matches!(
            status.status,
            TransactionStatus::Processing(TransactionConfirmationStatus::Confirmed, _)
        ));
        // The signatures should have been checked with the RPC client.
        let sent_requests = mock_sender.get_and_clear_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        assert_eq!(sent_requests[0].request, RpcRequest::GetSignatureStatuses);
        assert_eq!(
            sent_requests[0].params,
            serde_json::json!([[transaction.signatures[0].to_string()]])
        );
        // But nothing should have been sent to the transaction sender for re-sending.
        transaction_sender_rx.try_recv().unwrap_err();

        // Prepare a nine-transaction batch.
        // Since the mock sender is set up to confirm three transactions at a time,
        // it's convenient to split the transactions into three batches.
        let transactions = (0..9)
            .map(|index| {
                let transaction = Transaction::new_signed_with_payer(
                    &[solana_program::system_instruction::transfer(
                        &payer.pubkey(),
                        &solana_program::system_program::id(),
                        1 + index as u64,
                    )],
                    Some(&payer.pubkey()),
                    &[&payer],
                    Hash::default(),
                );
                (
                    transaction.clone(),
                    ConfirmTransactionMessage {
                        span: Span::current(),
                        index,
                        transaction,
                        last_valid_block_height: initial_block.last_valid_block_height
                            + index as u64,
                        response_tx: response_tx.clone(),
                    },
                )
            })
            .collect::<Vec<_>>();
        let transaction_signatures = transactions
            .iter()
            .map(|(_, msg)| msg.transaction.signatures[0].to_string())
            .collect::<Vec<_>>();
        for (_tx, msg) in transactions.clone() {
            transaction_confirmer_tx.send(msg).unwrap();
        }

        // Trigger a "new" blockhash to make the confirmation loop iterate again.
        blockdata_tx.send_modify(|_| {});

        let mut responses = Vec::new();
        response_rx.recv_many(&mut responses, 100).await;
        assert_eq!(responses.len(), 5);
        // The first three in this batch should be committed, and the next two should be processing.
        // The other 5 shouldn't have been responded to yet.
        assert!(responses[..3].iter().all(|r| matches!(
            r.status,
            TransactionStatus::Processing(TransactionConfirmationStatus::Confirmed, _)
        )));
        assert!(responses[3..].iter().all(|r| matches!(
            r.status,
            TransactionStatus::Processing(TransactionConfirmationStatus::Processed, _)
        )));
        // It is the first five transactions that should have been responded to.
        assert_eq!(
            responses.iter().map(|r| r.index).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );

        // All 10 transactions should have been checked.
        let sent_requests: Vec<TrackedRequest> = mock_sender.get_and_clear_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        let sent_signatures: Vec<Vec<String>> =
            serde_json::from_value(sent_requests[0].params.clone()).unwrap();
        assert_eq!(sent_signatures, vec![transaction_signatures.clone()]);

        // Iterate again.
        blockdata_tx.send_modify(|_| {});
        let mut responses = Vec::new();
        response_rx.recv_many(&mut responses, 100).await;
        assert_eq!(responses.len(), 5);
        // Again, the first three in this batch should be committed, and the next two should be processing.
        assert!(responses[..3].iter().all(|r| matches!(
            r.status,
            TransactionStatus::Processing(TransactionConfirmationStatus::Confirmed, _)
        )));
        assert!(responses[3..].iter().all(|r| matches!(
            r.status,
            TransactionStatus::Processing(TransactionConfirmationStatus::Processed, _)
        )));
        // Even if transactions were queued for re-confirming, they should be checked *after* the
        // initial transactions.
        assert_eq!(
            responses.iter().map(|r| r.index).collect::<Vec<_>>(),
            // Note the 3 at the end, which was queued for re-confirming from the previous batch.
            [5, 6, 7, 8, 3]
        );

        // This time, transactions 5 6 7 8 (new) and 3 4 (re-confirmed) should have been checked.
        let sent_requests: Vec<TrackedRequest> = mock_sender.get_and_clear_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        let sent_signatures: Vec<Vec<String>> =
            serde_json::from_value(sent_requests[0].params.clone()).unwrap();
        assert_eq!(
            sent_signatures,
            vec![vec![
                transaction_signatures[5].clone(),
                transaction_signatures[6].clone(),
                transaction_signatures[7].clone(),
                transaction_signatures[8].clone(),
                transaction_signatures[3].clone(),
                transaction_signatures[4].clone()
            ]]
        );

        // One more round.
        blockdata_tx.send_modify(|_| {});
        let mut responses = Vec::new();
        response_rx.recv_many(&mut responses, 100).await;
        // This time there should only be three committed transactions, and nothing else.
        assert_eq!(responses.len(), 3);
        assert!(responses.iter().all(|r| matches!(
            r.status,
            TransactionStatus::Processing(TransactionConfirmationStatus::Confirmed, _)
        )));
        assert_eq!(
            responses.iter().map(|r| r.index).collect::<Vec<_>>(),
            // 4 from the first batch, and 8 and 3 (again!) from the second batch.
            [4, 8, 3]
        );

        // This time, transactions 4 8 3 (re-confirmed) should have been checked.
        let sent_requests: Vec<TrackedRequest> = mock_sender.get_and_clear_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        let sent_signatures: Vec<Vec<String>> =
            serde_json::from_value(sent_requests[0].params.clone()).unwrap();
        assert_eq!(
            sent_signatures,
            vec![vec![
                transaction_signatures[4].clone(),
                transaction_signatures[8].clone(),
                transaction_signatures[3].clone(),
            ]]
        );

        // Throughout all the above, *no* transactions should have been re-sent.
        transaction_sender_rx.try_recv().unwrap_err();

        // In TPU mode, trying to confirm a transaction that's more than 100 slots behind
        // should trigger a re-send.
        transaction_confirmer_tx
            .send(ConfirmTransactionMessage {
                span: Span::current(),
                index: 10,
                transaction: transaction.clone(),
                last_valid_block_height: 0,
                response_tx: response_tx.clone(),
            })
            .unwrap();
        // Set the sender to always reply with None for all statuses.
        // If the transaction was successful, it wouldn't trigger the TPU re-send.
        *mock_sender.mode.lock().unwrap() = TrackingMockSenderMode::AllNone;
        blockdata_tx.send_modify(|_| {});

        let message = transaction_sender_rx.recv().await.unwrap();
        assert_eq!(message.index, 10);
        let sent_requests: Vec<TrackedRequest> = mock_sender.get_and_clear_sent_requests();
        assert_eq!(sent_requests.len(), 1);

        // If transactions fail (and return an error), they should be re-sent.
        *mock_sender.mode.lock().unwrap() = TrackingMockSenderMode::AllInstructionError;
        transaction_confirmer_tx
            .send(ConfirmTransactionMessage {
                span: Span::current(),
                index: 11,
                transaction: transaction.clone(),
                last_valid_block_height: initial_block.last_valid_block_height,
                response_tx: response_tx.clone(),
            })
            .unwrap();
        blockdata_tx.send_modify(|_| {});

        let message = transaction_sender_rx.recv().await.unwrap();
        assert_eq!(message.index, 11);
        // Status should be sent as an error.
        let status = response_rx.recv().await.unwrap();
        response_rx.try_recv().unwrap_err();
        assert_eq!(status.index, 11);
        assert!(matches!(status.status, TransactionStatus::Failed(..)));
        let sent_requests: Vec<TrackedRequest> = mock_sender.get_and_clear_sent_requests();
        assert_eq!(sent_requests.len(), 2);

        // If getting signatures itself fails, the transactions should be re-confirmed.
        *mock_sender.mode.lock().unwrap() = TrackingMockSenderMode::RpcError;
        transaction_confirmer_tx
            .send(ConfirmTransactionMessage {
                span: Span::current(),
                index: 12,
                transaction: transaction.clone(),
                last_valid_block_height: initial_block.last_valid_block_height,
                response_tx: response_tx.clone(),
            })
            .unwrap();
        blockdata_tx.send_modify(|_| {});
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let sent_requests: Vec<TrackedRequest> = mock_sender.get_and_clear_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        // Reset the mode to have the confirmation go through this time.
        *mock_sender.mode.lock().unwrap() =
            TrackingMockSenderMode::ThreeConfirmedTwoProcessingRestNone;
        blockdata_tx.send_modify(|_| {});

        let status = response_rx.recv().await.unwrap();
        response_rx.try_recv().unwrap_err();
        assert_eq!(status.index, 12);
        assert!(matches!(
            status.status,
            TransactionStatus::Processing(TransactionConfirmationStatus::Confirmed, _)
        ));
        let sent_requests: Vec<TrackedRequest> = mock_sender.get_and_clear_sent_requests();
        assert_eq!(sent_requests.len(), 1);

        cancellation_token.cancel();
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TrackedRequest {
        request: RpcRequest,
        params: serde_json::Value,
        response: Option<serde_json::Value>,
    }

    #[derive(Clone)]
    struct TrackingMockSender {
        sender: Arc<MockSender>,
        log: Arc<Mutex<Vec<TrackedRequest>>>,
        mode: Arc<Mutex<TrackingMockSenderMode>>,
    }

    #[derive(Clone, Copy)]
    enum TrackingMockSenderMode {
        ThreeConfirmedTwoProcessingRestNone,
        AllNone,
        AllInstructionError,
        RpcError,
    }

    impl TrackingMockSender {
        fn new(sender: MockSender) -> Self {
            Self {
                sender: Arc::new(sender),
                log: Arc::new(Mutex::new(Vec::new())),
                mode: Arc::new(Mutex::new(
                    TrackingMockSenderMode::ThreeConfirmedTwoProcessingRestNone,
                )),
            }
        }

        fn get_and_clear_sent_requests(&self) -> Vec<TrackedRequest> {
            mem::take(&mut *self.log.lock().unwrap())
        }
    }

    #[async_trait]
    impl RpcSender for TrackingMockSender {
        async fn send(
            &self,
            request: RpcRequest,
            params: serde_json::Value,
        ) -> SolanaResult<serde_json::Value> {
            let response = match request {
                RpcRequest::GetSignatureStatuses => {
                    let request_signatures: Vec<Vec<String>> =
                        serde_json::from_value(params.clone()).unwrap();
                    let statuses = match *self.mode.lock().unwrap() {
                        TrackingMockSenderMode::ThreeConfirmedTwoProcessingRestNone => {
                            generate_three_two_rest_response(request_signatures)
                        }
                        TrackingMockSenderMode::AllNone => {
                            vec![None; request_signatures[0].len()]
                        }
                        TrackingMockSenderMode::AllInstructionError => {
                            let status = SolanaTransactionStatus {
                                slot: 0,
                                confirmations: None,
                                status: Ok(()),
                                err: Some(TransactionError::InstructionError(
                                    0,
                                    InstructionError::ProgramFailedToComplete,
                                )),
                                confirmation_status: Some(TransactionConfirmationStatus::Processed),
                            };
                            vec![Some(status); request_signatures[0].len()]
                        }
                        TrackingMockSenderMode::RpcError => {
                            self.log.lock().unwrap().push(TrackedRequest {
                                request,
                                params,
                                response: None,
                            });
                            return Err(ClientError {
                                request: Some(request),
                                kind: solana_client::client_error::ClientErrorKind::Custom(
                                    "fail".to_string(),
                                ),
                            });
                        }
                    };
                    Ok(serde_json::to_value(Response {
                        context: RpcResponseContext {
                            slot: 0,
                            api_version: None,
                        },
                        value: statuses,
                    })?)
                }
                _ => self.sender.send(request, params.clone()).await,
            };
            self.log.lock().unwrap().push(TrackedRequest {
                request,
                params,
                response: response.as_ref().ok().cloned(),
            });
            response
        }

        fn get_transport_stats(&self) -> RpcTransportStats {
            self.sender.get_transport_stats()
        }

        fn url(&self) -> String {
            self.sender.url()
        }
    }

    /// In order to simulate not all messages landing at once:
    /// Confirm 3, set 2 to processing, and the rest to None.
    fn generate_three_two_rest_response(
        request_signatures: Vec<Vec<String>>,
    ) -> Vec<Option<SolanaTransactionStatus>> {
        let mut it = request_signatures.concat().into_iter();
        let confirmed = it
            .by_ref()
            .take(3)
            .map(|_| {
                Some(SolanaTransactionStatus {
                    slot: 0,
                    confirmations: None,
                    status: Ok(()),
                    err: None,
                    confirmation_status: Some(TransactionConfirmationStatus::Confirmed),
                })
            })
            .collect::<Vec<_>>();
        let processing = it
            .by_ref()
            .take(2)
            .map(|_| {
                Some(SolanaTransactionStatus {
                    slot: 0,
                    confirmations: None,
                    status: Ok(()),
                    err: None,
                    confirmation_status: Some(TransactionConfirmationStatus::Processed),
                })
            })
            .collect::<Vec<_>>();
        let rest = it.map(|_| None).collect::<Vec<_>>();
        [confirmed, processing, rest].concat()
    }
}
