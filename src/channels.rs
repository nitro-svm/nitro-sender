use tokio::sync::mpsc;

use super::messages::SendTransactionMessage;

/// Channels used by the [`super::BatchClient`].
pub struct Channels {
    pub transaction_sender_tx: mpsc::UnboundedSender<SendTransactionMessage>,
    pub transaction_sender_rx: mpsc::UnboundedReceiver<SendTransactionMessage>,
}

impl Channels {
    /// Creates all the channels used by the [`super::BatchClient`].
    pub fn new() -> Self {
        let (transaction_sender_tx, transaction_sender_rx) = mpsc::unbounded_channel();

        Self {
            transaction_sender_tx,
            transaction_sender_rx,
        }
    }
}
