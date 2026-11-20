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
    validate_courtesy_credit_amount, OrderAggregate, OrderStatus, MAX_COURTESY_CREDIT_CENTS,
};
pub use commands::{
    CancelOrderCommand, CreateOrderCommand, IssueCourtesyCreditCommand, OrderCommand,
};
pub use error::{DomainError, InvariantError};
pub use events::{
    CreditIssuedEvent, DomainEvent, OrderCancelledEvent, OrderCreatedEvent, OrderEvent,
    OrderStatusUpdatedEvent,
};
pub use handler::CommandHandler;
