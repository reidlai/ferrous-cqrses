use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::id::generate_uuidv7;

/// Command requesting a courtesy credit concession on an order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueCourtesyCreditCommand {
    pub command_id: Uuid,
    pub order_id: String,
    pub amount_cents: i64,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl IssueCourtesyCreditCommand {
    pub fn new(order_id: impl Into<String>, amount_cents: i64, reason: impl Into<String>) -> Self {
        Self {
            command_id: generate_uuidv7(),
            order_id: order_id.into(),
            amount_cents,
            reason: reason.into(),
            created_at: Utc::now(),
        }
    }
}

/// Command requesting cancellation of an order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrderCommand {
    pub command_id: Uuid,
    pub order_id: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl CancelOrderCommand {
    pub fn new(order_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            command_id: generate_uuidv7(),
            order_id: order_id.into(),
            reason: reason.into(),
            created_at: Utc::now(),
        }
    }
}

/// Command requesting creation of a new order aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateOrderCommand {
    pub command_id: Uuid,
    pub order_id: String,
    pub customer_id: String,
    pub total_freight_cents: i64,
    pub delivery_sla: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl CreateOrderCommand {
    pub fn new(
        order_id: impl Into<String>,
        customer_id: impl Into<String>,
        total_freight_cents: i64,
        delivery_sla: DateTime<Utc>,
    ) -> Self {
        Self {
            command_id: generate_uuidv7(),
            order_id: order_id.into(),
            customer_id: customer_id.into(),
            total_freight_cents,
            delivery_sla,
            created_at: Utc::now(),
        }
    }
}

/// Command requesting settlement of an issued courtesy credit concession.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettleCourtesyCreditCommand {
    pub command_id: Uuid,
    pub order_id: String,
    pub amount_cents: i64,
    pub transaction_ref: String,
    pub created_at: DateTime<Utc>,
}

impl SettleCourtesyCreditCommand {
    pub fn new(
        order_id: impl Into<String>,
        amount_cents: i64,
        transaction_ref: impl Into<String>,
    ) -> Self {
        Self {
            command_id: generate_uuidv7(),
            order_id: order_id.into(),
            amount_cents,
            transaction_ref: transaction_ref.into(),
            created_at: Utc::now(),
        }
    }

    pub fn with_command_id(mut self, command_id: Uuid) -> Self {
        self.command_id = command_id;
        self
    }
}

/// Command recording failure to settle an issued courtesy credit concession.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailCourtesyCreditSettlementCommand {
    pub command_id: Uuid,
    pub order_id: String,
    pub amount_cents: i64,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl FailCourtesyCreditSettlementCommand {
    pub fn new(
        order_id: impl Into<String>,
        amount_cents: i64,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            command_id: generate_uuidv7(),
            order_id: order_id.into(),
            amount_cents,
            reason: reason.into(),
            created_at: Utc::now(),
        }
    }

    pub fn with_command_id(mut self, command_id: Uuid) -> Self {
        self.command_id = command_id;
        self
    }
}

/// Sum type of all commands dispatched to the CQRS intake pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderCommand {
    CreateOrder(CreateOrderCommand),
    IssueCourtesyCredit(IssueCourtesyCreditCommand),
    CancelOrder(CancelOrderCommand),
    SettleCourtesyCredit(SettleCourtesyCreditCommand),
    FailCourtesyCreditSettlement(FailCourtesyCreditSettlementCommand),
}

impl OrderCommand {
    pub fn command_id(&self) -> Uuid {
        match self {
            Self::CreateOrder(c) => c.command_id,
            Self::IssueCourtesyCredit(c) => c.command_id,
            Self::CancelOrder(c) => c.command_id,
            Self::SettleCourtesyCredit(c) => c.command_id,
            Self::FailCourtesyCreditSettlement(c) => c.command_id,
        }
    }

    pub fn order_id(&self) -> &str {
        match self {
            Self::CreateOrder(c) => &c.order_id,
            Self::IssueCourtesyCredit(c) => &c.order_id,
            Self::CancelOrder(c) => &c.order_id,
            Self::SettleCourtesyCredit(c) => &c.order_id,
            Self::FailCourtesyCreditSettlement(c) => &c.order_id,
        }
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        match self {
            Self::CreateOrder(c) => c.created_at,
            Self::IssueCourtesyCredit(c) => c.created_at,
            Self::CancelOrder(c) => c.created_at,
            Self::SettleCourtesyCredit(c) => c.created_at,
            Self::FailCourtesyCreditSettlement(c) => c.created_at,
        }
    }
}
