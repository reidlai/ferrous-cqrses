//! # ferrous-cqrses
//!
//! Pure-Rust dual-role CQRS and event sourcing core crate providing shared domain contracts,
//! strongly typed command events, aggregate state models, serialization utilities, and
//! sequential command handler daemon services for the FerrousCX enterprise platform.

pub mod aggregate;
pub mod commands;
pub mod consumer;
pub mod error;
pub mod events;
pub mod handler;
pub mod id;
pub mod projection;
pub mod publisher;
pub mod serialization;

// Public re-exports for canonical top-level crate consumption (AD-15)
pub use aggregate::{
    validate_courtesy_credit_amount, OrderAggregate, OrderStatus, MAX_COURTESY_CREDIT_CENTS,
};
pub use commands::{
    CancelOrderCommand, CreateOrderCommand, IssueCourtesyCreditCommand, OrderCommand,
};
pub use consumer::{
    CommandConsumer, CommandOutcome, Config, IggyStreamConsumer, InMemoryStreamConsumer,
    ReceivedMessage, StreamConsumer,
};
pub use error::{DomainError, InvariantError};
pub use events::{
    CreditIssuedEvent, DomainEvent, OrderCancelledEvent, OrderCreatedEvent, OrderEvent,
    OrderStatusUpdatedEvent,
};
pub use handler::CommandHandler;
pub use id::{generate_uuidv7, is_valid_uuidv7, parse_uuidv7};
pub use projection::ConsolidatedOrderProjection;
pub use publisher::{
    IggyStreamPublisher, InMemoryStreamPublisher, PublishedMessage, StreamPublisher,
};
pub use serialization::{from_bincode, from_json, to_bincode, to_json, to_json_pretty};

/// Convenience module for glob-importing all core domain contracts.
pub mod prelude {
    pub use crate::aggregate::{
        validate_courtesy_credit_amount, OrderAggregate, OrderStatus, MAX_COURTESY_CREDIT_CENTS,
    };
    pub use crate::commands::{
        CancelOrderCommand, CreateOrderCommand, IssueCourtesyCreditCommand, OrderCommand,
    };
    pub use crate::consumer::{
        CommandConsumer, CommandOutcome, Config, IggyStreamConsumer, InMemoryStreamConsumer,
        ReceivedMessage, StreamConsumer,
    };
    pub use crate::error::{DomainError, InvariantError};
    pub use crate::events::{
        CreditIssuedEvent, DomainEvent, OrderCancelledEvent, OrderCreatedEvent, OrderEvent,
        OrderStatusUpdatedEvent,
    };
    pub use crate::handler::CommandHandler;
    pub use crate::id::{generate_uuidv7, is_valid_uuidv7, parse_uuidv7};
    pub use crate::projection::ConsolidatedOrderProjection;
    pub use crate::publisher::{
        IggyStreamPublisher, InMemoryStreamPublisher, PublishedMessage, StreamPublisher,
    };
    pub use crate::serialization::{from_bincode, from_json, to_bincode, to_json, to_json_pretty};
}
