use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::aggregate::OrderStatus;
use crate::id::generate_uuidv7;

/// Event emitted when a courtesy credit concession is granted to an order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreditIssuedEvent {
    pub event_id: Uuid,
    pub order_id: String,
    pub amount_cents: i64,
    pub reason: String,
    pub issued_at: DateTime<Utc>,
    pub version: u64,
}

impl CreditIssuedEvent {
    pub fn new(
        order_id: impl Into<String>,
        amount_cents: i64,
        reason: impl Into<String>,
        version: u64,
    ) -> Self {
        Self {
            event_id: generate_uuidv7(),
            order_id: order_id.into(),
            amount_cents,
            reason: reason.into(),
            issued_at: Utc::now(),
            version,
        }
    }
}

/// Event emitted when an order transitions to a new operational lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderStatusUpdatedEvent {
    pub event_id: Uuid,
    pub order_id: String,
    pub previous_status: OrderStatus,
    pub new_status: OrderStatus,
    pub reason: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub version: u64,
}

impl OrderStatusUpdatedEvent {
    pub fn new(
        order_id: impl Into<String>,
        previous_status: OrderStatus,
        new_status: OrderStatus,
        reason: Option<String>,
        version: u64,
    ) -> Self {
        Self {
            event_id: generate_uuidv7(),
            order_id: order_id.into(),
            previous_status,
            new_status,
            reason,
            updated_at: Utc::now(),
            version,
        }
    }
}

/// Event emitted when a new order aggregate is instantiated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCreatedEvent {
    pub event_id: Uuid,
    pub order_id: String,
    pub customer_id: String,
    pub total_freight_cents: i64,
    pub delivery_sla: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub version: u64,
}

impl OrderCreatedEvent {
    pub fn new(
        order_id: impl Into<String>,
        customer_id: impl Into<String>,
        total_freight_cents: i64,
        delivery_sla: DateTime<Utc>,
    ) -> Self {
        Self {
            event_id: generate_uuidv7(),
            order_id: order_id.into(),
            customer_id: customer_id.into(),
            total_freight_cents,
            delivery_sla,
            created_at: Utc::now(),
            version: 1,
        }
    }
}

/// Event emitted when an order is cancelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCancelledEvent {
    pub event_id: Uuid,
    pub order_id: String,
    pub reason: String,
    pub cancelled_at: DateTime<Utc>,
    pub version: u64,
}

impl OrderCancelledEvent {
    pub fn new(order_id: impl Into<String>, reason: impl Into<String>, version: u64) -> Self {
        Self {
            event_id: generate_uuidv7(),
            order_id: order_id.into(),
            reason: reason.into(),
            cancelled_at: Utc::now(),
            version,
        }
    }
}

/// Sum type of all domain events originating from or mutating order aggregates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainEvent {
    OrderCreated(OrderCreatedEvent),
    CreditIssued(CreditIssuedEvent),
    OrderStatusUpdated(OrderStatusUpdatedEvent),
    OrderCancelled(OrderCancelledEvent),
}

/// Type alias aligning DomainEvent with order-centric event sourcing naming.
pub type OrderEvent = DomainEvent;

impl DomainEvent {
    pub fn event_id(&self) -> Uuid {
        match self {
            Self::OrderCreated(e) => e.event_id,
            Self::CreditIssued(e) => e.event_id,
            Self::OrderStatusUpdated(e) => e.event_id,
            Self::OrderCancelled(e) => e.event_id,
        }
    }

    pub fn order_id(&self) -> &str {
        match self {
            Self::OrderCreated(e) => &e.order_id,
            Self::CreditIssued(e) => &e.order_id,
            Self::OrderStatusUpdated(e) => &e.order_id,
            Self::OrderCancelled(e) => &e.order_id,
        }
    }

    pub fn version(&self) -> u64 {
        match self {
            Self::OrderCreated(e) => e.version,
            Self::CreditIssued(e) => e.version,
            Self::OrderStatusUpdated(e) => e.version,
            Self::OrderCancelled(e) => e.version,
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::OrderCreated(e) => e.created_at,
            Self::CreditIssued(e) => e.issued_at,
            Self::OrderStatusUpdated(e) => e.updated_at,
            Self::OrderCancelled(e) => e.cancelled_at,
        }
    }
}
