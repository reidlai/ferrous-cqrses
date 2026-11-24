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
pub mod callback {
    pub use crate::domain::order::callback::*;
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
pub mod projection {
    pub use crate::domain::order::projection::*;
}
pub mod queries {
    pub use crate::domain::order::queries::*;
}
pub mod saga {
    pub use crate::domain::order::saga::*;
}

// Infrastructure and transport modules
pub mod consumer;
pub mod delta;
pub mod id;
pub mod publisher;
pub mod serialization;
pub mod sierra;

// Public re-exports for canonical top-level crate consumption (AD-15)
pub use consumer::{
    CommandConsumer, CommandOutcome, Config, IggyStreamConsumer, InMemoryStreamConsumer,
    ProjectionConsumer, ProjectionOutcome, ReceivedMessage, StreamConsumer,
};
pub use delta::{write_delta_table_snapshot, DeltaLogMetadata, ORDERS_CURRENT_STATE_SCHEMA_JSON};
pub use domain::order::{
    validate_courtesy_credit_amount, CallbackConsumer, CallbackConsumerOutcome, CallbackError,
    CallbackOutcome, CallbackPublisher, CallbackService, CancelOrderCommand, CommandHandler,
    ConsolidatedOrderProjection, CourtesyCreditSettledEvent, CourtesyCreditSettlementFailedEvent,
    CreateOrderCommand, CreditIssuedEvent, CreditSettlementStatus, DomainError, DomainEvent,
    EnterpriseDispatcher, EnterpriseResponse, EnterpriseSettlementRequest, EventListener,
    FailCourtesyCreditSettlementCommand, GetOrderQuery, HttpEnterpriseDispatcher,
    InMemoryEnterpriseDispatcher, InvariantError, IssueCourtesyCreditCommand, ListOrdersQuery,
    MockEnterpriseDispatcher, OlistOrderRecord, OrderAggregate, OrderCancelledEvent, OrderCommand,
    OrderCreatedEvent, OrderEvent, OrderSagaManager, OrderStatus, OrderStatusUpdatedEvent,
    SagaConfig, SagaError, SagaOutcome, SettleCourtesyCreditCommand, SettlementCallbackMessage,
    SettlementStatus, CALLBACKS_CONSUMER_GROUP, CALLBACKS_TOPIC, CALLBACK_TOPIC,
    DEFAULT_LIST_ORDERS_LIMIT, MAX_COURTESY_CREDIT_CENTS, MAX_LIST_ORDERS_LIMIT,
};
pub use id::{generate_uuidv7, is_valid_uuidv7, parse_uuidv7};
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
    pub use crate::consumer::{
        CommandConsumer, CommandOutcome, Config, IggyStreamConsumer, InMemoryStreamConsumer,
        ProjectionConsumer, ProjectionOutcome, ReceivedMessage, StreamConsumer,
    };
    pub use crate::delta::{
        write_delta_table_snapshot, DeltaLogMetadata, ORDERS_CURRENT_STATE_SCHEMA_JSON,
    };
    pub use crate::domain::order::{
        validate_courtesy_credit_amount, CallbackConsumer, CallbackConsumerOutcome, CallbackError,
        CallbackOutcome, CallbackPublisher, CallbackService, CancelOrderCommand, CommandHandler,
        ConsolidatedOrderProjection, CourtesyCreditSettledEvent,
        CourtesyCreditSettlementFailedEvent, CreateOrderCommand, CreditIssuedEvent,
        CreditSettlementStatus, DomainError, DomainEvent, EnterpriseDispatcher, EnterpriseResponse,
        EnterpriseSettlementRequest, EventListener, FailCourtesyCreditSettlementCommand,
        GetOrderQuery, HttpEnterpriseDispatcher, InMemoryEnterpriseDispatcher, InvariantError,
        IssueCourtesyCreditCommand, ListOrdersQuery, MockEnterpriseDispatcher, OlistOrderRecord,
        OrderAggregate, OrderCancelledEvent, OrderCommand, OrderCreatedEvent, OrderEvent,
        OrderSagaManager, OrderStatus, OrderStatusUpdatedEvent, SagaConfig, SagaError, SagaOutcome,
        SettleCourtesyCreditCommand, SettlementCallbackMessage, SettlementStatus,
        CALLBACKS_CONSUMER_GROUP, CALLBACKS_TOPIC, CALLBACK_TOPIC, DEFAULT_LIST_ORDERS_LIMIT,
        MAX_COURTESY_CREDIT_CENTS, MAX_LIST_ORDERS_LIMIT,
    };
    pub use crate::id::{generate_uuidv7, is_valid_uuidv7, parse_uuidv7};
    pub use crate::publisher::{
        IggyStreamPublisher, InMemoryStreamPublisher, PublishedMessage, StreamPublisher,
    };
    pub use crate::serialization::{from_bincode, from_json, to_bincode, to_json, to_json_pretty};
    pub use crate::sierra::{
        deserialize_event, serialize_event, stream_id_for_order, EventStore, ExpectedVersion,
        InMemoryEventStore, RespValue, SierraClient, SierraDbEventStore, SierraEventStore,
    };
}
