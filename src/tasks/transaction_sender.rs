use std::sync::Arc;

use solana_client::{
    client_error::{ClientError as Error, ClientErrorKind},
    rpc_client::SerializableTransaction,
    rpc_config::RpcSendTransactionConfig,
};
use solana_commitment_config::CommitmentLevel;
use solana_keypair::Keypair;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_transaction::Transaction;
use solana_transaction_error::TransactionError;
use tokio::{
    sync::{RwLock, mpsc, watch},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, trace, warn};

use crate::{
    client::SEND_TRANSACTION_INTERVAL,
    messages::{BlockMessage, ConfirmTransactionMessage, SendTransactionMessage, StatusMessage},
};

#[derive(Debug, thiserror::Error)]
pub enum SenderError {
    #[error("RPC error: {0}")]
    RpcError(#[from] Box<Error>),
    #[error("confirm channel closed")]
    ConfirmChannelClosed,
    #[error("send channel closed")]
    SendChannelClosed,
    #[error("response channel closed")]
    ResponseChannelClosed,
}

pub type SenderResult<T = ()> = Result<T, SenderError>;

pub fn is_transient_transaction_error(error: &TransactionError) -> bool {
    matches!(
        error,
        TransactionError::AccountInUse
            | TransactionError::BlockhashNotFound
            | TransactionError::ClusterMaintenance
            | TransactionError::CommitCancelled
            | TransactionError::InstructionError(..)
            | TransactionError::InsufficientFundsForRent { .. }
            | TransactionError::InvalidAccountForFee
            | TransactionError::InvalidLoadedAccountsDataSizeLimit
            | TransactionError::InvalidRentPayingAccount
            | TransactionError::MaxLoadedAccountsDataSizeExceeded
            | TransactionError::MissingSignatureForFee
            | TransactionError::ProgramCacheHitMaxLimit
            | TransactionError::ProgramExecutionTemporarilyRestricted { .. }
            | TransactionError::ResanitizationNeeded
            | TransactionError::SanitizeFailure
            | TransactionError::WouldExceedAccountDataBlockLimit
            | TransactionError::WouldExceedAccountDataTotalLimit
            | TransactionError::WouldExceedMaxAccountCostLimit
            | TransactionError::WouldExceedMaxBlockCostLimit
            | TransactionError::WouldExceedMaxVoteCostLimit
    )
}

impl SenderError {
    pub fn is_transient(&self) -> bool {
        match self {
            SenderError::RpcError(e) => match e.kind() {
                ClientErrorKind::TransactionError(transaction_error) => {
                    is_transient_transaction_error(transaction_error)
                }
                ClientErrorKind::SigningError(_) => true,
                _ => false,
            },
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct Sender {
    rpc_client: Arc<RpcClient>,
    signers: Vec<Arc<Keypair>>,
    blockdata_rx: watch::Receiver<BlockMessage>,
    confirmer_tx: mpsc::UnboundedSender<ConfirmTransactionMessage>,
    sender_tx: mpsc::UnboundedSender<SendTransactionMessage>,
    sender_rx: Arc<RwLock<mpsc::UnboundedReceiver<SendTransactionMessage>>>,
    cancellation_token: CancellationToken,
}

impl Sender {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        signers: Vec<Arc<Keypair>>,
        blockdata_rx: watch::Receiver<BlockMessage>,
        confirmer_tx: mpsc::UnboundedSender<ConfirmTransactionMessage>,
        sender_tx: mpsc::UnboundedSender<SendTransactionMessage>,
        sender_rx: mpsc::UnboundedReceiver<SendTransactionMessage>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            rpc_client,
            signers,
            blockdata_rx,
            confirmer_tx,
            sender_tx,
            sender_rx: Arc::new(RwLock::new(sender_rx)),
            cancellation_token,
        }
    }

    fn confirm_message(&self, msg: ConfirmTransactionMessage) -> SenderResult {
        self.confirmer_tx.send(msg).map_err(|e| {
            warn!("failed to queue transaction for confirmation: {e}");
            SenderError::ConfirmChannelClosed
        })
    }

    fn resend_message(&self, msg: SendTransactionMessage) -> SenderResult {
        self.sender_tx.send(msg).map_err(|e| {
            warn!("failed to re-queue transaction for sending: {e}");
            SenderError::SendChannelClosed
        })
    }

    #[instrument(skip(self, transaction), name = "send-transaction", fields(tx = %transaction.get_signature()))]
    async fn send_transaction(&self, transaction: &Transaction) -> SenderResult {
        self.rpc_client
            .send_transaction_with_config(
                transaction,
                RpcSendTransactionConfig {
                    max_retries: Some(0),
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentLevel::Processed),
                    min_context_slot: None,
                    encoding: None,
                },
            )
            .await
            .map_err(Box::new)?;

        Ok(())
    }

    async fn handle_transaction(&self, mut msg: SendTransactionMessage) -> SenderResult {
        let blockdata = *self.blockdata_rx.borrow();
        let last_valid_block_height = msg.sign(&blockdata, &self.signers);

        match self.send_transaction(&msg.transaction).await {
            Ok(_) => {
                trace!(
                    "[{}] successfully submitted tx {} to RPC",
                    msg.index,
                    msg.transaction.get_signature()
                );
                self.confirm_message(ConfirmTransactionMessage {
                    span: msg.span,
                    index: msg.index,
                    transaction: msg.transaction,
                    last_valid_block_height,
                    response_tx: msg.response_tx,
                })?;
            }
            Err(e) => {
                warn!(
                    "failed to send transaction [{}] (batch index: {}, target slot: {}, current block: {}): {e:?}",
                    msg.transaction.get_signature(),
                    msg.index,
                    last_valid_block_height,
                    blockdata.block_height
                );

                if !e.is_transient() {
                    warn!(
                        "not retrying transaction [{}] (batch index: {}), error is not transient",
                        msg.transaction.get_signature(),
                        msg.index
                    );
                    msg.update_status(StatusMessage::from_sender_error(msg.index, e))?;
                    return Ok(());
                }

                self.resend_message(SendTransactionMessage {
                    // Force re-sign. Since the transaction couldn't be sent, this should be safe.
                    last_valid_block_height: 0,
                    ..msg
                })?;
            }
        }

        Ok(())
    }

    async fn send_loop(&mut self) -> SenderResult {
        let mut last_send = Instant::now();

        loop {
            let mut sender_rx = self.sender_rx.write().await;
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    warn!("received shutdown signal, shutting down transaction sender");
                    break;
                }
                Some(msg) = sender_rx.recv() => {
                    // Space the transaction submissions out by a small delay to avoid rate limits.
                    tokio::time::sleep_until(last_send + SEND_TRANSACTION_INTERVAL).await;
                    last_send = Instant::now();
                    self.handle_transaction(msg).await?;
                }
            }
        }

        warn!("shutting down transaction sender");
        Ok(())
    }

    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.send_loop().await {
                warn!("transaction sender exited with error: {e}");
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use solana_program::hash::Hash;
    use solana_pubkey::Pubkey;
    use solana_signer::Signer;
    use tokio::time::{Duration, Instant, sleep_until};
    use tracing::{Level, Span};

    use super::*;

    /// This is essentially an integration test of the full lifecycle of the transaction sender.
    #[tokio::test(start_paused = true)]
    async fn test_transaction_sender() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .try_init();

        // Use the paused current time as a reference point so the rest of the test doesn't depend
        // on the current time.
        let initial_time = Instant::now();

        let rpc_client = Arc::new(RpcClient::new_mock("succeeds".to_string()));

        // This connection manager and its constituent parts are implemented below.
        let payer = Arc::new(Keypair::new());

        let initial_block = BlockMessage {
            blockhash: Hash::new_from_array(Pubkey::new_unique().to_bytes()),
            last_valid_block_height: 1150,
            block_height: 1000,
        };
        let (blockdata_tx, blockdata_rx) = watch::channel(initial_block);
        let (transaction_confirmer_tx, mut transaction_confirmer_rx) =
            mpsc::unbounded_channel::<ConfirmTransactionMessage>();
        let (transaction_sender_tx, transaction_sender_rx) =
            mpsc::unbounded_channel::<SendTransactionMessage>();

        let cancellation_token = CancellationToken::new();
        let sender = Sender::new(
            rpc_client.clone(),
            vec![payer.clone()],
            blockdata_rx,
            transaction_confirmer_tx,
            transaction_sender_tx.clone(),
            transaction_sender_rx,
            cancellation_token.clone(),
        );
        let handle = sender.spawn();

        // No transactions should be queued for confirmation yet.
        transaction_confirmer_rx.try_recv().unwrap_err();

        // Send a transaction.
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
        transaction_sender_tx
            .send(SendTransactionMessage {
                span: Span::current(),
                index: 0,
                transaction: transaction.clone(),
                last_valid_block_height: initial_block.last_valid_block_height,
                response_tx: response_tx.clone(),
            })
            .unwrap();
        sleep_until(initial_time + SEND_TRANSACTION_INTERVAL + Duration::from_millis(1)).await;

        // There should be one message in the confirmation queue.
        let confirmation = transaction_confirmer_rx.try_recv().unwrap();
        transaction_confirmer_rx.try_recv().unwrap_err();
        assert_eq!(confirmation.index, 0);
        assert_eq!(confirmation.transaction, transaction);
        assert_eq!(
            confirmation.last_valid_block_height,
            initial_block.last_valid_block_height
        );

        // Send the transaction again, but with a different last_valid_block_height.
        // This should cause the transaction to be re-signed.

        // Set a new blockhash to make the signature different.
        let new_block = BlockMessage {
            blockhash: Hash::new_from_array(Pubkey::new_unique().to_bytes()),
            last_valid_block_height: 1151,
            block_height: 1001,
        };
        blockdata_tx.send(new_block).unwrap();
        transaction_sender_tx
            .send(SendTransactionMessage {
                span: Span::current(),
                index: 1,
                transaction: transaction.clone(),
                last_valid_block_height: 0,
                response_tx: response_tx.clone(),
            })
            .unwrap();
        sleep_until(initial_time + 2 * SEND_TRANSACTION_INTERVAL + Duration::from_millis(1)).await;

        // There should be one message in the confirmation queue.
        let confirmation = transaction_confirmer_rx.try_recv().unwrap();
        transaction_confirmer_rx.try_recv().unwrap_err();
        assert_eq!(confirmation.index, 1);
        assert_eq!(
            confirmation.last_valid_block_height,
            new_block.last_valid_block_height
        );

        // No confirmations should have been sent to the response channel by this task.
        response_rx.try_recv().unwrap_err();

        // Drop the transaction sender and response receiver to trigger the watcher to exit.
        drop(transaction_sender_tx);
        drop(response_rx);
        cancellation_token.cancel();
        handle.await.unwrap();
    }
}
