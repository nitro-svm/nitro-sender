use std::sync::Arc;

use solana_client::rpc_client::SerializableTransaction;
use solana_keypair::Keypair;
use solana_program::{clock::Slot, hash::Hash};
use solana_signature::Signature;
use solana_transaction::Transaction;
use tokio::sync::mpsc;
use tracing::{Span, instrument, trace, warn};

use super::transaction::TransactionStatus;
use crate::tasks::transaction_sender::{SenderError, SenderResult};

/// Info about the current height of the blockchain.
#[derive(Clone, Debug, Copy, PartialEq, Default)]
pub struct BlockMessage {
    pub blockhash: Hash,
    pub last_valid_block_height: u64,
    pub block_height: u64,
}

/// A transaction that should be sent to the network.
#[derive(Clone, Debug)]
pub struct SendTransactionMessage {
    pub span: Span,
    pub index: usize,
    pub transaction: Transaction,
    pub last_valid_block_height: u64,
    pub response_tx: mpsc::UnboundedSender<StatusMessage>,
}

impl SendTransactionMessage {
    #[instrument(skip_all, fields(index = self.index, sig = %self.transaction.get_signature()))]
    pub fn sign(&mut self, block_message: &BlockMessage, signers: &[Arc<Keypair>]) -> u64 {
        let msg = self;
        let blockdata = block_message;
        if blockdata.block_height > msg.last_valid_block_height + 1 {
            let old_sig = *msg.transaction.get_signature();
            msg.transaction.sign(signers, blockdata.blockhash);
            if old_sig != Signature::default() {
                trace!(
                    "[{}] re-sending tx {} as {}",
                    msg.index,
                    old_sig,
                    msg.transaction.get_signature()
                );
            }
            blockdata.last_valid_block_height
        } else {
            trace!(
                "[{}] sending tx {}",
                msg.index,
                msg.transaction.get_signature()
            );
            msg.last_valid_block_height
        }
    }

    pub fn update_status(&self, status_message: StatusMessage) -> SenderResult {
        self.response_tx
            .send(status_message)
            .map_err(|_| SenderError::ResponseChannelClosed)
    }
}

/// A transaction that has been submitted to the network, and is awaiting confirmation.
#[derive(Clone, Debug)]
pub struct ConfirmTransactionMessage {
    pub span: Span,
    pub index: usize,
    pub transaction: Transaction,
    pub last_valid_block_height: u64,
    pub response_tx: mpsc::UnboundedSender<StatusMessage>,
}

impl ConfirmTransactionMessage {
    #[instrument(skip_all, fields(index = self.index, sig = %self.transaction.get_signature()))]
    pub fn send_response(&self, status: StatusMessage) {
        // If a response channel is dropped that's fine, that just means the future was dropped
        // (most likely due to timeout) and the transaction is no longer interesting.
        // Ignore the error and continue with other messages regardless.
        let _ = self.response_tx.send(status).inspect_err(|e| {
            warn!("failed to send response: {e}");
        });
    }
}

impl From<ConfirmTransactionMessage> for SendTransactionMessage {
    fn from(msg: ConfirmTransactionMessage) -> Self {
        Self {
            span: msg.span,
            index: msg.index,
            transaction: msg.transaction,
            last_valid_block_height: msg.last_valid_block_height,
            response_tx: msg.response_tx,
        }
    }
}

/// A status update for a transaction that has been submitted to the network, good or bad.
#[derive(Debug)]
pub struct StatusMessage {
    pub index: usize,
    pub landed_as: Option<(Slot, Signature)>,
    pub status: TransactionStatus,
}

impl StatusMessage {
    pub fn from_sender_error(index: usize, err: SenderError) -> Self {
        Self {
            index,
            landed_as: None,
            status: TransactionStatus::Failed(err.into(), Vec::new(), 0),
        }
    }
}
