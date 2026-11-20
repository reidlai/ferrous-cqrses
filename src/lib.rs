//! # ferrous-cqrses
//!
//! Pure-Rust dual-role CQRS and event sourcing core crate providing domain-oriented contracts,
//! strongly typed command events, aggregate state models, serialization utilities, and
//! sequential order handler daemon services for the FerrousCX enterprise platform.

pub mod domain;

// Backward-compatible module paths mapped to domain::order
pub mod aggregate {
    pub use crate::domain::order::aggregate::*;
}
pub mod commands {
    pub use crate::domain::order::commands::*;
}
pub mod error {
    pub use crate::domain::order::error::*;
}
pub mod events {
    pub use crate::domain::order::events::*;
}
pub mod handler {
    pub use crate::domain::order::handler::*;
}

// Infrastructure and transport modules
pub mod consumer;
pub mod id;
pub mod projection;
pub mod publisher;
pub mod serialization;
pub mod sierra;

// Public re-exports for canonical top-level crate consumption (AD-15)
pub use domain::order::{
    validate_courtesy_credit_amount, CancelOrderCommand, CommandHandler,
    CourtesyCreditSettledEvent, CourtesyCreditSettlementFailedEvent, CreateOrderCommand,
    CreditIssuedEvent, CreditSettlementStatus, DomainError, DomainEvent,
    FailCourtesyCreditSettlementCommand, InvariantError, IssueCourtesyCreditCommand, OrderAggregate,
    OrderCancelledEvent, OrderCommand, OrderCreatedEvent, OrderEvent, OrderStatus,
    OrderStatusUpdatedEvent, SettleCourtesyCreditCommand, MAX_COURTESY_CREDIT_CENTS,
};
pub use consumer::{
    CommandConsumer, CommandOutcome, Config, IggyStreamConsumer, InMemoryStreamConsumer,
    ProjectionConsumer, ProjectionOutcome, ReceivedMessage, StreamConsumer,
};
pub use id::{generate_uuidv7, is_valid_uuidv7, parse_uuidv7};
pub use projection::ConsolidatedOrderProjection;
pub use publisher::{
    IggyStreamPublisher, InMemoryStreamPublisher, PublishedMessage, StreamPublisher,
};
pub use serialization::{from_bincode, from_json, to_bincode, to_json, to_json_pretty};
pub use sierra::{
    deserialize_event, serialize_event, stream_id_for_order, EventStore, ExpectedVersion,
    InMemoryEventStore, RespValue, SierraClient, SierraDbEventStore, SierraEventStore,
};

/// Convenience module for glob-importing all core domain contracts and infrastructure interfaces.
pub mod prelude {
    pub use crate::domain::order::{
        validate_courtesy_credit_amount, CancelOrderCommand, CommandHandler,
        CourtesyCreditSettledEvent, CourtesyCreditSettlementFailedEvent, CreateOrderCommand,
        CreditIssuedEvent, CreditSettlementStatus, DomainError, DomainEvent,
        FailCourtesyCreditSettlementCommand, InvariantError, IssueCourtesyCreditCommand,
        OrderAggregate, OrderCancelledEvent, OrderCommand, OrderCreatedEvent, OrderEvent,
        OrderStatus, OrderStatusUpdatedEvent, SettleCourtesyCreditCommand,
        MAX_COURTESY_CREDIT_CENTS,
    };
    pub use crate::consumer::{
        CommandConsumer, CommandOutcome, Config, IggyStreamConsumer, InMemoryStreamConsumer,
        ProjectionConsumer, ProjectionOutcome, ReceivedMessage, StreamConsumer,
    };
    pub use crate::id::{generate_uuidv7, is_valid_uuidv7, parse_uuidv7};
    pub use crate::projection::ConsolidatedOrderProjection;
    pub use crate::publisher::{
        IggyStreamPublisher, InMemoryStreamPublisher, PublishedMessage, StreamPublisher,
    };
    pub use crate::serialization::{from_bincode, from_json, to_bincode, to_json, to_json_pretty};
    pub use crate::sierra::{
        deserialize_event, serialize_event, stream_id_for_order, EventStore, ExpectedVersion,
        InMemoryEventStore, RespValue, SierraClient, SierraDbEventStore, SierraEventStore,
    };
}
