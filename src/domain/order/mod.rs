//! # Order Domain Module
//!
//! Encapsulates all domain aggregates, commands, events, errors, and command handling
//! for the Order bounded context in FerrousCX.

pub mod aggregate;
pub mod commands;
pub mod error;
pub mod events;
pub mod handler;

pub use aggregate::{
    validate_courtesy_credit_amount, CreditSettlementStatus, OrderAggregate, OrderStatus,
    MAX_COURTESY_CREDIT_CENTS,
};
pub use commands::{
    CancelOrderCommand, CreateOrderCommand, FailCourtesyCreditSettlementCommand,
    IssueCourtesyCreditCommand, OrderCommand, SettleCourtesyCreditCommand,
};
pub use error::{DomainError, InvariantError};
pub use events::{
    CourtesyCreditSettledEvent, CourtesyCreditSettlementFailedEvent, CreditIssuedEvent, DomainEvent,
    OrderCancelledEvent, OrderCreatedEvent, OrderEvent, OrderStatusUpdatedEvent,
};
pub use handler::CommandHandler;
