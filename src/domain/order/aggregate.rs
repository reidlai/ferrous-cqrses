use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::DomainError;
use crate::events::{CreditIssuedEvent, DomainEvent};

/// Maximum courtesy credit ceiling permitted per order: $20.00 (2,000 cents).
pub const MAX_COURTESY_CREDIT_CENTS: i64 = 2000;

/// Order lifecycle operational statuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    #[default]
    Created,
    Processing,
    InTransit,
    Shipped,
    Delivered,
    Cancelled,
    FraudLocked,
}

impl OrderStatus {
    /// Returns true if this order status permits further state mutations.
    pub fn is_modifiable(&self) -> bool {
        !matches!(self, Self::Cancelled | Self::FraudLocked)
    }

    /// Returns true if concession credits can be issued in this state.
    pub fn can_issue_credit(&self) -> bool {
        self.is_modifiable()
    }
}

/// Core domain aggregate representing an e-commerce order with event-sourced rehydration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderAggregate {
    pub order_id: String,
    pub customer_id: String,
    pub status: OrderStatus,
    pub total_freight_cents: i64,
    pub courtesy_credit_cents: i64,
    pub delivery_sla: DateTime<Utc>,
    pub actual_delivery: Option<DateTime<Utc>>,
    pub version: u64,
}

impl Default for OrderAggregate {
    fn default() -> Self {
        Self {
            order_id: String::new(),
            customer_id: String::new(),
            status: OrderStatus::Created,
            total_freight_cents: 0,
            courtesy_credit_cents: 0,
            delivery_sla: DateTime::UNIX_EPOCH,
            actual_delivery: None,
            version: 0,
        }
    }
}

impl OrderAggregate {
    /// Creates a newly initialized order aggregate instance.
    pub fn new(
        order_id: impl Into<String>,
        customer_id: impl Into<String>,
        total_freight_cents: i64,
        delivery_sla: DateTime<Utc>,
    ) -> Self {
        Self {
            order_id: order_id.into(),
            customer_id: customer_id.into(),
            status: OrderStatus::Created,
            total_freight_cents,
            courtesy_credit_cents: 0,
            delivery_sla,
            actual_delivery: None,
            version: 0,
        }
    }

    /// Validates whether a courtesy credit concession can be granted under business invariants:
    /// 1. Order must not be cancelled.
    /// 2. Order must not be fraud-locked.
    /// 3. Credit amount must be strictly positive.
    /// 4. Cumulative lifetime courtesy credit must not exceed $20.00 (2,000 cents).
    pub fn validate_courtesy_credit(&self, amount_cents: i64) -> Result<(), DomainError> {
        if self.status == OrderStatus::Cancelled {
            return Err(DomainError::OrderCancelled);
        }
        if self.status == OrderStatus::FraudLocked {
            return Err(DomainError::OrderFraudLocked);
        }
        if amount_cents <= 0 {
            return Err(DomainError::InvalidAmount(amount_cents));
        }

        let total_credit = self.courtesy_credit_cents.saturating_add(amount_cents);
        if total_credit > MAX_COURTESY_CREDIT_CENTS {
            return Err(DomainError::CourtesyCreditExceeded(
                total_credit,
                MAX_COURTESY_CREDIT_CENTS,
            ));
        }

        Ok(())
    }

    /// Applies a single domain event to this aggregate, advancing its internal version.
    pub fn apply(&mut self, event: &DomainEvent) -> Result<(), DomainError> {
        // Guard against mismatched aggregate ID
        if !self.order_id.is_empty() && event.order_id() != self.order_id {
            return Err(DomainError::OrderIdMismatch {
                expected: self.order_id.clone(),
                actual: event.order_id().to_string(),
            });
        }

        // Sequential version sequencing check
        if event.version() != self.version + 1 {
            return Err(DomainError::VersionMismatch {
                expected: self.version + 1,
                actual: event.version(),
            });
        }

        match event {
            DomainEvent::OrderCreated(e) => {
                if self.version > 0 {
                    return Err(DomainError::OrderAlreadyExists(self.order_id.clone()));
                }
                self.order_id = e.order_id.clone();
                self.customer_id = e.customer_id.clone();
                self.total_freight_cents = e.total_freight_cents;
                self.delivery_sla = e.delivery_sla;
                self.status = OrderStatus::Created;
                self.version = e.version;
                Ok(())
            }
            DomainEvent::CreditIssued(e) => {
                self.validate_courtesy_credit(e.amount_cents)?;
                self.courtesy_credit_cents =
                    self.courtesy_credit_cents.saturating_add(e.amount_cents);
                self.version = e.version;
                Ok(())
            }
            DomainEvent::OrderStatusUpdated(e) => {
                if !self.status.is_modifiable() {
                    return Err(DomainError::InvalidOrderStatus(self.status));
                }
                if self.status != e.previous_status {
                    return Err(DomainError::InvalidOrderStatus(self.status));
                }
                self.status = e.new_status;
                if e.new_status == OrderStatus::Delivered {
                    self.actual_delivery = Some(e.updated_at);
                }
                self.version = e.version;
                Ok(())
            }
            DomainEvent::OrderCancelled(_e) => {
                if self.status == OrderStatus::FraudLocked {
                    return Err(DomainError::OrderFraudLocked);
                }
                if self.status == OrderStatus::Cancelled {
                    return Err(DomainError::OrderCancelled);
                }
                if self.status == OrderStatus::Delivered {
                    return Err(DomainError::OrderDelivered);
                }
                self.status = OrderStatus::Cancelled;
                self.version = _e.version;
                Ok(())
            }
        }
    }

    /// Applies a typed CreditIssuedEvent directly, validating invariants and incrementing version.
    pub fn apply_credit_issued(&mut self, event: &CreditIssuedEvent) -> Result<(), DomainError> {
        self.apply(&DomainEvent::CreditIssued(event.clone()))
    }

    /// Rehydrates an order aggregate from a sequential stream of domain events.
    pub fn from_events<'a>(
        events: impl IntoIterator<Item = &'a DomainEvent>,
    ) -> Result<Self, DomainError> {
        let mut aggregate = Self::default();
        for event in events {
            aggregate.apply(event)?;
        }
        Ok(aggregate)
    }
}

/// Standalone invariant validation function for courtesy credit amounts.
pub fn validate_courtesy_credit_amount(amount_cents: i64) -> Result<(), DomainError> {
    if amount_cents <= 0 {
        return Err(DomainError::InvalidAmount(amount_cents));
    }
    if amount_cents > MAX_COURTESY_CREDIT_CENTS {
        return Err(DomainError::CourtesyCreditExceeded(
            amount_cents,
            MAX_COURTESY_CREDIT_CENTS,
        ));
    }
    Ok(())
}
