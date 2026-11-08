use thiserror::Error;

use crate::aggregate::OrderStatus;

/// Business invariant violations encountered during command processing or state validation.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum InvariantError {
    #[error("Courtesy credit of {attempted} cents exceeds maximum allowed limit of {limit} cents ($20.00)")]
    CourtesyCreditExceeded { attempted: i64, limit: i64 },

    #[error("Order is not modifiable in status {0:?}")]
    OrderNotModifiable(OrderStatus),

    #[error("Invariant violation: {0}")]
    Generic(String),
}

/// Strongly typed domain errors emitted across the CQRS-ES core.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum DomainError {
    #[error("Courtesy credit of {0} cents exceeds maximum allowed limit of {1} cents ($20.00)")]
    CourtesyCreditExceeded(i64, i64),

    #[error("Order is cancelled")]
    OrderCancelled,

    #[error("Order is fraud-locked")]
    OrderFraudLocked,

    #[error("Order has already been delivered")]
    OrderDelivered,

    #[error("Order '{0}' already exists")]
    OrderAlreadyExists(String),

    #[error("Order ID mismatch: expected '{expected}', actual '{actual}'")]
    OrderIdMismatch { expected: String, actual: String },

    #[error("Version mismatch: expected {expected}, actual {actual}")]
    VersionMismatch { expected: u64, actual: u64 },

    #[error("Invalid order status: {0:?}")]
    InvalidOrderStatus(OrderStatus),

    #[error("Invalid amount: {0} cents must be strictly positive")]
    InvalidAmount(i64),

    #[error("Invalid UUIDv7 identifier: {0}")]
    InvalidUuid(String),

    #[error("Business invariant violation: {0}")]
    Invariant(#[from] InvariantError),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Event application error: {0}")]
    EventApplicationError(String),

    #[error("Domain error: {0}")]
    Generic(String),
}

impl From<serde_json::Error> for DomainError {
    fn from(err: serde_json::Error) -> Self {
        DomainError::SerializationError(err.to_string())
    }
}

impl From<bincode::Error> for DomainError {
    fn from(err: bincode::Error) -> Self {
        DomainError::SerializationError(err.to_string())
    }
}
