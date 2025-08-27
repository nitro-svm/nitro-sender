use tokio::sync::{mpsc, watch};

use super::messages::{BlockMessage, ConfirmTransactionMessage, SendTransactionMessage};

/// Channels used by the [`super::BatchClient`].
pub struct Channels {
    pub blockdata_tx: watch::Sender<BlockMessage>,
    pub blockdata_rx: watch::Receiver<BlockMessage>,
    pub transaction_confirmer_tx: mpsc::UnboundedSender<ConfirmTransactionMessage>,
    pub transaction_confirmer_rx: mpsc::UnboundedReceiver<ConfirmTransactionMessage>,
    pub transaction_sender_tx: mpsc::UnboundedSender<SendTransactionMessage>,
    pub transaction_sender_rx: mpsc::UnboundedReceiver<SendTransactionMessage>,
}

impl Channels {
    /// Creates all the channels used by the [`super::BatchClient`].
    pub fn new() -> Self {
        let (blockdata_tx, mut blockdata_rx) = watch::channel(BlockMessage::default());
        // Mark unchanged to prevent the default value from leaking out.
        blockdata_rx.mark_unchanged();

        let (transaction_confirmer_tx, transaction_confirmer_rx) = mpsc::unbounded_channel();
        let (transaction_sender_tx, transaction_sender_rx) = mpsc::unbounded_channel();

        Self {
            blockdata_tx,
            blockdata_rx,
            transaction_confirmer_tx,
            transaction_confirmer_rx,
            transaction_sender_tx,
            transaction_sender_rx,
        }
    }
}
