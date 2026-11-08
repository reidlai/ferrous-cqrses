//! Sequential command processing engine and in-memory aggregate repository.
//!
//! Provides the core [`CommandHandler`] responsible for validating domain business
//! invariants (including AD-17 courtesy credit cap and valid lifecycle state transitions),
//! enforcing command idempotency, rehydrating order aggregates from historical events,
//! and mutating aggregate states sequentially.

use std::collections::{HashMap, HashSet};
use tracing::{info, warn};
use uuid::Uuid;

use crate::aggregate::{OrderAggregate, OrderStatus, MAX_COURTESY_CREDIT_CENTS};
use crate::commands::{
    CancelOrderCommand, CreateOrderCommand, IssueCourtesyCreditCommand, OrderCommand,
};
use crate::error::DomainError;
use crate::events::{CreditIssuedEvent, DomainEvent, OrderCancelledEvent, OrderCreatedEvent};

/// Sequential command processor and aggregate state manager.
///
/// Maintains in-memory aggregate state cache, event history per order, and tracks
/// processed command IDs to enforce monotonic idempotency without race conditions.
#[derive(Debug, Default, Clone)]
pub struct CommandHandler {
    aggregates: HashMap<String, OrderAggregate>,
    event_store: HashMap<String, Vec<DomainEvent>>,
    processed_commands: HashSet<Uuid>,
}

impl CommandHandler {
    /// Creates a newly initialized, empty command handler instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Handles an incoming [`OrderCommand`] asynchronously, validating domain invariants
    /// and mutating aggregate state if permitted.
    ///
    /// Returns the resulting [`DomainEvent`] on success, or a [`DomainError`] if validation fails.
    pub async fn handle_command(&mut self, command: OrderCommand) -> Result<DomainEvent, DomainError> {
        let command_id = command.command_id();

        // 1. Monotonic idempotency verification
        if self.processed_commands.contains(&command_id) {
            warn!(
                command_id = %command_id,
                order_id = %command.order_id(),
                "Duplicate command rejected under idempotency contract"
            );
            return Err(DomainError::DuplicateCommand(command_id));
        }

        // 2. Route command based on variant
        match command {
            OrderCommand::CreateOrder(cmd) => self.handle_create_order(cmd).await,
            OrderCommand::IssueCourtesyCredit(cmd) => {
                self.handle_issue_courtesy_credit(cmd).await
            }
            OrderCommand::CancelOrder(cmd) => self.handle_cancel_order(cmd).await,
        }
    }

    /// Processes a [`CreateOrderCommand`].
    async fn handle_create_order(
        &mut self,
        cmd: CreateOrderCommand,
    ) -> Result<DomainEvent, DomainError> {
        let command_id = cmd.command_id;
        let order_id = cmd.order_id.clone();

        if cmd.order_id.trim().is_empty() || cmd.customer_id.trim().is_empty() {
            warn!(
                order_id = %cmd.order_id,
                customer_id = %cmd.customer_id,
                "CreateOrder rejected: order_id and customer_id cannot be empty"
            );
            return Err(DomainError::Generic(
                "order_id and customer_id cannot be empty".to_string(),
            ));
        }

        if cmd.total_freight_cents < 0 {
            warn!(
                order_id = %cmd.order_id,
                total_freight_cents = cmd.total_freight_cents,
                "CreateOrder rejected: freight amount cannot be negative"
            );
            return Err(DomainError::InvalidAmount(cmd.total_freight_cents));
        }

        if self.aggregates.contains_key(&order_id) {
            warn!(order_id = %order_id, "CreateOrder rejected: order already exists");
            return Err(DomainError::OrderAlreadyExists(order_id));
        }

        let mut aggregate = OrderAggregate::new(
            &cmd.order_id,
            &cmd.customer_id,
            cmd.total_freight_cents,
            cmd.delivery_sla,
        );

        let event = OrderCreatedEvent::new(
            &cmd.order_id,
            &cmd.customer_id,
            cmd.total_freight_cents,
            cmd.delivery_sla,
        );
        let domain_event = DomainEvent::OrderCreated(event);

        aggregate.apply(&domain_event)?;

        self.aggregates.insert(order_id.clone(), aggregate);
        self.event_store
            .entry(order_id)
            .or_default()
            .push(domain_event.clone());
        self.processed_commands.insert(command_id);

        info!(
            command_id = %command_id,
            order_id = %cmd.order_id,
            "Successfully created order aggregate"
        );

        Ok(domain_event)
    }

    /// Processes an [`IssueCourtesyCreditCommand`], strictly enforcing AD-17 and lifecycle checks.
    async fn handle_issue_courtesy_credit(
        &mut self,
        cmd: IssueCourtesyCreditCommand,
    ) -> Result<DomainEvent, DomainError> {
        let command_id = cmd.command_id;

        let aggregate = match self.aggregates.get_mut(&cmd.order_id) {
            Some(agg) => agg,
            None => {
                warn!(order_id = %cmd.order_id, "IssueCourtesyCredit rejected: order not found");
                return Err(DomainError::OrderNotFound(cmd.order_id));
            }
        };

        // Validate lifecycle state invariants
        if aggregate.status == OrderStatus::Cancelled {
            warn!(order_id = %cmd.order_id, "IssueCourtesyCredit rejected: order is cancelled");
            return Err(DomainError::OrderCancelled);
        }
        if aggregate.status == OrderStatus::FraudLocked {
            warn!(order_id = %cmd.order_id, "IssueCourtesyCredit rejected: order is fraud-locked");
            return Err(DomainError::OrderFraudLocked);
        }
        if cmd.amount_cents <= 0 {
            warn!(
                order_id = %cmd.order_id,
                amount_cents = cmd.amount_cents,
                "IssueCourtesyCredit rejected: amount must be strictly positive"
            );
            return Err(DomainError::InvalidAmount(cmd.amount_cents));
        }

        // Validate AD-17: Cumulative lifetime credit <= 2,000 cents ($20.00)
        let total_credit = aggregate.courtesy_credit_cents.saturating_add(cmd.amount_cents);
        if total_credit > MAX_COURTESY_CREDIT_CENTS {
            warn!(
                "Invariant violation AD-17: courtesy credit of {} cents exceeds limit for order {}",
                cmd.amount_cents, cmd.order_id
            );
            return Err(DomainError::CourtesyCreditExceeded(
                total_credit,
                MAX_COURTESY_CREDIT_CENTS,
            ));
        }

        let next_version = aggregate
            .version
            .checked_add(1)
            .ok_or_else(|| DomainError::Generic("Aggregate version overflow".into()))?;
        let event = CreditIssuedEvent::new(
            &cmd.order_id,
            cmd.amount_cents,
            &cmd.reason,
            next_version,
        );
        let domain_event = DomainEvent::CreditIssued(event);

        aggregate.apply(&domain_event)?;

        self.event_store
            .entry(cmd.order_id.clone())
            .or_default()
            .push(domain_event.clone());
        self.processed_commands.insert(command_id);

        info!(
            command_id = %command_id,
            order_id = %cmd.order_id,
            amount_cents = cmd.amount_cents,
            total_courtesy_cents = aggregate.courtesy_credit_cents,
            version = aggregate.version,
            "Successfully issued courtesy credit concession"
        );

        Ok(domain_event)
    }

    /// Processes a [`CancelOrderCommand`], checking lifecycle validity.
    async fn handle_cancel_order(
        &mut self,
        cmd: CancelOrderCommand,
    ) -> Result<DomainEvent, DomainError> {
        let command_id = cmd.command_id;

        let aggregate = match self.aggregates.get_mut(&cmd.order_id) {
            Some(agg) => agg,
            None => {
                warn!(order_id = %cmd.order_id, "CancelOrder rejected: order not found");
                return Err(DomainError::OrderNotFound(cmd.order_id));
            }
        };

        if aggregate.status == OrderStatus::FraudLocked {
            warn!(order_id = %cmd.order_id, "CancelOrder rejected: order is fraud-locked");
            return Err(DomainError::OrderFraudLocked);
        }
        if aggregate.status == OrderStatus::Cancelled {
            warn!(order_id = %cmd.order_id, "CancelOrder rejected: order is already cancelled");
            return Err(DomainError::OrderCancelled);
        }
        if aggregate.status == OrderStatus::Delivered {
            warn!(order_id = %cmd.order_id, "CancelOrder rejected: order has already been delivered");
            return Err(DomainError::OrderDelivered);
        }

        let next_version = aggregate
            .version
            .checked_add(1)
            .ok_or_else(|| DomainError::Generic("Aggregate version overflow".into()))?;
        let event = OrderCancelledEvent::new(&cmd.order_id, &cmd.reason, next_version);
        let domain_event = DomainEvent::OrderCancelled(event);

        aggregate.apply(&domain_event)?;

        self.event_store
            .entry(cmd.order_id.clone())
            .or_default()
            .push(domain_event.clone());
        self.processed_commands.insert(command_id);

        info!(
            command_id = %command_id,
            order_id = %cmd.order_id,
            version = aggregate.version,
            "Successfully cancelled order"
        );

        Ok(domain_event)
    }

    /// Rehydrates an order aggregate from historical domain events and updates internal cache.
    pub fn rehydrate(
        &mut self,
        order_id: &str,
        events: &[DomainEvent],
    ) -> Result<&OrderAggregate, DomainError> {
        if events.is_empty() {
            warn!(order_id = %order_id, "Rehydration failed: empty event history");
            return Err(DomainError::OrderNotFound(order_id.to_string()));
        }
        let aggregate = OrderAggregate::from_events(events)?;
        self.aggregates.insert(order_id.to_string(), aggregate);
        self.event_store
            .insert(order_id.to_string(), events.to_vec());
        Ok(self.aggregates.get(order_id).expect("Aggregate just inserted"))
    }

    /// Manually inserts or overrides an aggregate in the state cache (useful for testing states).
    pub fn insert_aggregate(&mut self, aggregate: OrderAggregate) {
        self.aggregates.insert(aggregate.order_id.clone(), aggregate);
    }

    /// Returns a reference to an aggregate by `order_id` if present in state cache.
    pub fn get_aggregate(&self, order_id: &str) -> Option<&OrderAggregate> {
        self.aggregates.get(order_id)
    }

    /// Returns a mutable reference to an aggregate by `order_id` if present in state cache.
    pub fn get_aggregate_mut(&mut self, order_id: &str) -> Option<&mut OrderAggregate> {
        self.aggregates.get_mut(order_id)
    }

    /// Returns historical domain events for an order.
    pub fn get_events(&self, order_id: &str) -> Option<&[DomainEvent]> {
        self.event_store.get(order_id).map(|v| v.as_slice())
    }

    /// Returns a reference to all cached aggregates.
    pub fn aggregates(&self) -> &HashMap<String, OrderAggregate> {
        &self.aggregates
    }

    /// Returns a reference to the event store.
    pub fn event_store(&self) -> &HashMap<String, Vec<DomainEvent>> {
        &self.event_store
    }

    /// Returns the set of processed command IDs.
    pub fn processed_commands(&self) -> &HashSet<Uuid> {
        &self.processed_commands
    }
}
