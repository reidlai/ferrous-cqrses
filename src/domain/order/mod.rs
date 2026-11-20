//! # Order Domain Module
//!
//! Encapsulates all domain aggregates, commands, events, errors, command handling,
//! projections, callbacks, and saga orchestration for the Order bounded context in FerrousCX.

pub mod aggregate;
pub mod callback;
pub mod commands;
pub mod error;
pub mod events;
pub mod handler;
pub mod projection;
pub mod saga;

pub use aggregate::{
    validate_courtesy_credit_amount, CreditSettlementStatus, OrderAggregate, OrderStatus,
    MAX_COURTESY_CREDIT_CENTS,
};
pub use callback::{
    CallbackError, CallbackOutcome, CallbackPublisher, CallbackService, EventListener,
    SettlementCallbackMessage, SettlementStatus, CALLBACKS_CONSUMER_GROUP, CALLBACKS_TOPIC,
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
pub use projection::ConsolidatedOrderProjection;
pub use saga::{
    EnterpriseDispatcher, EnterpriseResponse, EnterpriseSettlementRequest,
    HttpEnterpriseDispatcher, InMemoryEnterpriseDispatcher, MockEnterpriseDispatcher,
    OrderSagaManager, SagaConfig, SagaError, SagaOutcome,
};
