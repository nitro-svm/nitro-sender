use solana_commitment_config::CommitmentConfig;
use solana_program::clock::Slot;
use solana_signature::Signature;
use solana_transaction_error::TransactionError;
use solana_transaction_status::{
    TransactionConfirmationStatus, TransactionStatus as SolanaTransactionStatus,
};

use crate::client::NitroSenderError;

/// The final outcome of a transaction after the [`BatchClient`] is done, either successfully
/// or due to reaching the timeout.
#[derive(Debug)]
pub enum TransactionOutcome<T> {
    /// The transaction was successfully confirmed by the network at the desired commitment level.
    Success(Box<SuccessfulTransaction<T>>),
    /// The transaction was not submitted to the network.
    Unsubmitted(UnsubmittedTransaction<T>),
    /// The transaction is currently being processed by the network.
    InFlight(InFlightTransaction<T>),
    /// The transaction latest status contained an error.
    Failure(Box<FailedTransaction<T>>),
}

/// A transaction that was successfully confirmed by the network at the desired commitment level.
#[derive(Debug)]
pub struct SuccessfulTransaction<T> {
    pub data: T,
    pub slot: Slot,
    pub signature: Signature,
}

/// A transaction that was not submitted to the network.
#[derive(Debug)]
pub struct UnsubmittedTransaction<T> {
    pub data: T,
    pub slot: Option<Slot>,
}

/// A transaction that is currently being processed by the network.
#[derive(Debug)]
pub struct InFlightTransaction<T> {
    pub data: T,
    pub slot: Slot,
    pub status: TransactionConfirmationStatus,
    pub signature: Signature,
}

impl<T> From<InFlightTransaction<T>> for SuccessfulTransaction<T> {
    fn from(in_flight: InFlightTransaction<T>) -> Self {
        Self {
            data: in_flight.data,
            slot: in_flight.slot,
            signature: in_flight.signature,
        }
    }
}

/// A transaction that resulted in an error.
#[derive(Debug)]
pub struct FailedTransaction<T> {
    pub data: T,
    pub slot: Slot,
    pub error: String,
    pub logs: Vec<String>,
}

impl<T> TransactionOutcome<T> {
    /// Returns `true` if the outcome was successful.
    pub fn successful(&self, commitment: CommitmentConfig) -> bool {
        match self {
            TransactionOutcome::Success(_) => true,
            TransactionOutcome::InFlight(in_flight) => {
                if commitment.is_finalized() {
                    TransactionConfirmationStatus::Finalized == in_flight.status
                } else if commitment.is_confirmed() {
                    TransactionConfirmationStatus::Processed != in_flight.status
                } else {
                    true
                }
            }
            _ => false,
        }
    }

    /// Returns [`Option::Some`] if the outcome was successful, or [`Option::None`] otherwise.
    pub fn into_successful(
        self,
        commitment: CommitmentConfig,
    ) -> Option<Box<SuccessfulTransaction<T>>> {
        match self {
            TransactionOutcome::Success(s) => Some(s),
            TransactionOutcome::InFlight(in_flight) => {
                if commitment.is_finalized() {
                    (in_flight.status == TransactionConfirmationStatus::Finalized)
                        .then_some(Box::new(in_flight.into()))
                } else if commitment.is_confirmed() {
                    (in_flight.status != TransactionConfirmationStatus::Processed)
                        .then_some(Box::new(in_flight.into()))
                } else {
                    Some(Box::new(in_flight.into()))
                }
            }
            _ => None,
        }
    }

    /// Returns a reference to the inner [`FailedTransaction`] if the outcome was a failure, or [`None`] otherwise.
    pub fn error(&self) -> Option<&FailedTransaction<T>> {
        match self {
            TransactionOutcome::Failure(f) => Some(f),
            _ => None,
        }
    }
}

/// Tracks the progress of a transaction, and holds on to its associated data.
pub struct TransactionProgress<T> {
    pub data: T,
    pub landed_as: Option<(Slot, Signature)>,
    pub status: TransactionStatus,
}

impl<T> TransactionProgress<T> {
    pub fn new(data: T) -> Self {
        Self {
            data,
            landed_as: None,
            status: TransactionStatus::Pending,
        }
    }
}

/// The current state of a transaction.
#[derive(Debug, PartialEq, Eq)]
pub enum TransactionStatus {
    Pending,
    Processing(TransactionConfirmationStatus, Slot),
    Committed(Slot),
    Failed(NitroSenderError, Vec<String>, Slot),
}

impl TransactionStatus {
    /// Translates from a [`SolanaTransactionStatus`] and a [commitment level](`CommitmentConfig`)
    /// to a [`TransactionStatus`].
    pub fn from_solana_status(status: SolanaTransactionStatus, logs: Vec<String>) -> Self {
        if let Some(TransactionError::AlreadyProcessed) = status.err {
            Self::Committed(status.slot)
        } else if let Some(err) = status.err {
            Self::Failed(err.into(), logs, status.slot)
        } else {
            Self::Processing(
                status
                    .confirmation_status
                    .unwrap_or(TransactionConfirmationStatus::Processed),
                status.slot,
            )
        }
    }

    /// Checks whether a transaction should be re-confirmed based on its status.
    ///
    /// These should be re-confirmed:
    /// - [`TransactionStatus::Pending`]
    /// - [`TransactionStatus::Processing`]
    ///
    /// These should *not* be re-confirmed:
    /// - [`TransactionStatus::Committed`]
    /// - [`TransactionStatus::Failed`]
    pub fn should_be_reconfirmed(&self, commitment: CommitmentConfig) -> bool {
        match self {
            TransactionStatus::Pending => true,
            TransactionStatus::Committed(_) | TransactionStatus::Failed(..) => false,
            TransactionStatus::Processing(status, _) => {
                if commitment.is_finalized() {
                    *status != TransactionConfirmationStatus::Finalized
                } else if commitment.is_confirmed() {
                    *status == TransactionConfirmationStatus::Processed
                } else {
                    false
                }
            }
        }
    }

    /// Checks whether the transactions should be resent based on its status.
    pub fn should_be_resent(&self) -> bool {
        self.error().map(|e| e.is_transient()).unwrap_or(false)
    }

    /// Returns the error if the transaction failed, or [`None`] otherwise.
    pub fn error(&self) -> Option<&NitroSenderError> {
        match self {
            TransactionStatus::Failed(err, _, _) => Some(err),
            _ => None,
        }
    }

    /// Returns the confirmation status if the transaction is being processed, or [`None`] otherwise.
    pub fn confirmation_status(&self) -> Option<TransactionConfirmationStatus> {
        match self {
            TransactionStatus::Processing(status, _) => Some(status.clone()),
            _ => None,
        }
    }

    /// Returns the slot associated with the transaction status.
    pub fn slot(&self) -> Slot {
        match self {
            TransactionStatus::Pending => 0,
            TransactionStatus::Processing(_, slot)
            | TransactionStatus::Committed(slot)
            | TransactionStatus::Failed(_, _, slot) => *slot,
        }
    }
}

impl<T> From<TransactionProgress<T>> for TransactionOutcome<T> {
    fn from(progress: TransactionProgress<T>) -> Self {
        match progress.status {
            TransactionStatus::Pending => TransactionOutcome::Unsubmitted(UnsubmittedTransaction {
                data: progress.data,
                slot: None,
            }),
            TransactionStatus::Processing(status, slot) => {
                let (_slot, signature) = progress.landed_as.expect(
                    "landed_as should be Some if status is Processing; this is a bug in BatchClient",
                );
                TransactionOutcome::InFlight(InFlightTransaction {
                    data: progress.data,
                    slot,
                    status,
                    signature,
                })
            }
            TransactionStatus::Failed(error, logs, slot) => {
                TransactionOutcome::Failure(Box::new(FailedTransaction {
                    data: progress.data,
                    error: error.to_string(),
                    slot,
                    logs,
                }))
            }
            TransactionStatus::Committed(_slot) => {
                let (slot, signature) = progress.landed_as.expect(
                    "landed_as should be Some if status is Committed; this is a bug in BatchClient",
                );
                TransactionOutcome::Success(Box::new(SuccessfulTransaction {
                    data: progress.data,
                    slot,
                    signature,
                }))
            }
        }
    }
}
