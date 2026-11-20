//! Sequential command processing engine and in-memory aggregate repository.
//!
//! Provides the core [`CommandHandler`] responsible for validating domain business
//! invariants (including AD-17 courtesy credit cap and valid lifecycle state transitions),
//! enforcing command idempotency, rehydrating order aggregates from historical events,
//! and mutating aggregate states sequentially backed by an [`EventStore`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::aggregate::{OrderAggregate, OrderStatus, MAX_COURTESY_CREDIT_CENTS};
use crate::commands::{
    CancelOrderCommand, CreateOrderCommand, FailCourtesyCreditSettlementCommand,
    IssueCourtesyCreditCommand, OrderCommand, SettleCourtesyCreditCommand,
};
use crate::error::DomainError;
use crate::events::{
    CourtesyCreditSettledEvent, CourtesyCreditSettlementFailedEvent, CreditIssuedEvent, DomainEvent,
    OrderCancelledEvent, OrderCreatedEvent,
};
use crate::sierra::{
    stream_id_for_order, EventStore, ExpectedVersion, InMemoryEventStore, SierraDbEventStore,
};

/// Sequential command processor and aggregate state manager.
///
/// Maintains an in-memory aggregate state cache, event history per order, and tracks
/// processed command IDs to enforce monotonic idempotency without race conditions,
/// delegating durable event persistence and concurrency checks to an [`EventStore`].
#[derive(Debug, Clone)]
pub struct CommandHandler {
    aggregates: HashMap<String, OrderAggregate>,
    event_store: Arc<dyn EventStore>,
    event_cache: HashMap<String, Vec<DomainEvent>>,
    processed_commands: HashSet<Uuid>,
}

impl Default for CommandHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandHandler {
    /// Creates a newly initialized command handler backed by an [`InMemoryEventStore`].
    pub fn new() -> Self {
        Self {
            aggregates: HashMap::new(),
            event_store: Arc::new(InMemoryEventStore::new()),
            event_cache: HashMap::new(),
            processed_commands: HashSet::new(),
        }
    }

    /// Creates a command handler with a custom [`EventStore`] implementation.
    pub fn new_with_store(store: Arc<dyn EventStore>) -> Self {
        Self {
            aggregates: HashMap::new(),
            event_store: store,
            event_cache: HashMap::new(),
            processed_commands: HashSet::new(),
        }
    }

    /// Alias for [`new_with_store`].
    pub fn with_store(store: Arc<dyn EventStore>) -> Self {
        Self::new_with_store(store)
    }

    /// Connects to a SierraDB instance at `addr` and returns a configured command handler.
    pub async fn connect_sierra(addr: &str) -> Result<Self, DomainError> {
        let store = SierraDbEventStore::connect(addr).await?;
        Ok(Self::new_with_store(Arc::new(store)))
    }

    /// Ensures the order aggregate is loaded into memory by scanning the event stream if un-cached.
    pub async fn ensure_aggregate_loaded(&mut self, order_id: &str) -> Result<(), DomainError> {
        if !self.aggregates.contains_key(order_id) {
            let stream_id = stream_id_for_order(order_id);
            let events = self.event_store.scan(&stream_id).await?;
            if !events.is_empty() {
                let aggregate = OrderAggregate::from_events(&events)?;
                self.aggregates.insert(order_id.to_string(), aggregate);
                self.event_cache.insert(order_id.to_string(), events);
            }
        }
        Ok(())
    }

    /// Handles an incoming [`OrderCommand`] asynchronously, validating domain invariants
    /// and mutating aggregate state if permitted.
    ///
    /// Returns the resulting [`DomainEvent`] on success, or a [`DomainError`] if validation fails.
    pub async fn handle_command(
        &mut self,
        command: OrderCommand,
    ) -> Result<DomainEvent, DomainError> {
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
            OrderCommand::IssueCourtesyCredit(cmd) => self.handle_issue_courtesy_credit(cmd).await,
            OrderCommand::CancelOrder(cmd) => self.handle_cancel_order(cmd).await,
            OrderCommand::SettleCourtesyCredit(cmd) => self.handle_settle_courtesy_credit(cmd).await,
            OrderCommand::FailCourtesyCreditSettlement(cmd) => {
                self.handle_fail_courtesy_credit_settlement(cmd).await
            }
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

        self.ensure_aggregate_loaded(&order_id).await?;

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

        let stream_id = stream_id_for_order(&order_id);
        self.event_store
            .append(&stream_id, &domain_event, ExpectedVersion::Empty)
            .await?;

        self.aggregates.insert(order_id.clone(), aggregate);
        self.event_cache
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

        self.ensure_aggregate_loaded(&cmd.order_id).await?;

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
        let total_credit = aggregate
            .courtesy_credit_cents
            .saturating_add(cmd.amount_cents);
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
        let event =
            CreditIssuedEvent::new(&cmd.order_id, cmd.amount_cents, &cmd.reason, next_version);
        let domain_event = DomainEvent::CreditIssued(event);

        // In SierraDB, expected stream version is 0-indexed (prior stream version = aggregate.version - 1)
        let expected_version = if aggregate.version == 0 {
            ExpectedVersion::Empty
        } else {
            ExpectedVersion::Exact(aggregate.version - 1)
        };

        let stream_id = stream_id_for_order(&cmd.order_id);
        if let Err(e) = self
            .event_store
            .append(&stream_id, &domain_event, expected_version)
            .await
        {
            if matches!(e, DomainError::VersionMismatch { .. }) {
                self.aggregates.remove(&cmd.order_id);
                self.event_cache.remove(&cmd.order_id);
            }
            return Err(e);
        }

        aggregate.apply(&domain_event)?;

        self.event_cache
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

        self.ensure_aggregate_loaded(&cmd.order_id).await?;

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

        let expected_version = if aggregate.version == 0 {
            ExpectedVersion::Empty
        } else {
            ExpectedVersion::Exact(aggregate.version - 1)
        };

        let stream_id = stream_id_for_order(&cmd.order_id);
        if let Err(e) = self
            .event_store
            .append(&stream_id, &domain_event, expected_version)
            .await
        {
            if matches!(e, DomainError::VersionMismatch { .. }) {
                self.aggregates.remove(&cmd.order_id);
                self.event_cache.remove(&cmd.order_id);
            }
            return Err(e);
        }

        aggregate.apply(&domain_event)?;

        self.event_cache
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

    /// Processes a [`SettleCourtesyCreditCommand`].
    async fn handle_settle_courtesy_credit(
        &mut self,
        cmd: SettleCourtesyCreditCommand,
    ) -> Result<DomainEvent, DomainError> {
        let command_id = cmd.command_id;

        self.ensure_aggregate_loaded(&cmd.order_id).await?;

        let aggregate = match self.aggregates.get_mut(&cmd.order_id) {
            Some(agg) => agg,
            None => {
                warn!(order_id = %cmd.order_id, "SettleCourtesyCredit rejected: order not found");
                return Err(DomainError::OrderNotFound(cmd.order_id));
            }
        };

        aggregate.validate_courtesy_credit_settlement(cmd.amount_cents)?;

        let next_version = aggregate
            .version
            .checked_add(1)
            .ok_or_else(|| DomainError::Generic("Aggregate version overflow".into()))?;
        let event = CourtesyCreditSettledEvent::new(
            &cmd.order_id,
            cmd.amount_cents,
            &cmd.transaction_ref,
            next_version,
        );
        let domain_event = DomainEvent::CourtesyCreditSettled(event);

        let expected_version = if aggregate.version == 0 {
            ExpectedVersion::Empty
        } else {
            ExpectedVersion::Exact(aggregate.version - 1)
        };

        let stream_id = stream_id_for_order(&cmd.order_id);
        if let Err(e) = self
            .event_store
            .append(&stream_id, &domain_event, expected_version)
            .await
        {
            if matches!(e, DomainError::VersionMismatch { .. }) {
                self.aggregates.remove(&cmd.order_id);
                self.event_cache.remove(&cmd.order_id);
            }
            return Err(e);
        }

        aggregate.apply(&domain_event)?;

        self.event_cache
            .entry(cmd.order_id.clone())
            .or_default()
            .push(domain_event.clone());
        self.processed_commands.insert(command_id);

        info!(
            command_id = %command_id,
            order_id = %cmd.order_id,
            amount_cents = cmd.amount_cents,
            settled_credit_cents = aggregate.settled_credit_cents,
            version = aggregate.version,
            "Successfully settled courtesy credit"
        );

        Ok(domain_event)
    }

    /// Processes a [`FailCourtesyCreditSettlementCommand`].
    async fn handle_fail_courtesy_credit_settlement(
        &mut self,
        cmd: FailCourtesyCreditSettlementCommand,
    ) -> Result<DomainEvent, DomainError> {
        let command_id = cmd.command_id;

        self.ensure_aggregate_loaded(&cmd.order_id).await?;

        let aggregate = match self.aggregates.get_mut(&cmd.order_id) {
            Some(agg) => agg,
            None => {
                warn!(order_id = %cmd.order_id, "FailCourtesyCreditSettlement rejected: order not found");
                return Err(DomainError::OrderNotFound(cmd.order_id));
            }
        };

        aggregate.validate_fail_courtesy_credit_settlement(cmd.amount_cents)?;

        let next_version = aggregate
            .version
            .checked_add(1)
            .ok_or_else(|| DomainError::Generic("Aggregate version overflow".into()))?;
        let event = CourtesyCreditSettlementFailedEvent::new(
            &cmd.order_id,
            cmd.amount_cents,
            &cmd.reason,
            next_version,
        );
        let domain_event = DomainEvent::CourtesyCreditSettlementFailed(event);

        let expected_version = if aggregate.version == 0 {
            ExpectedVersion::Empty
        } else {
            ExpectedVersion::Exact(aggregate.version - 1)
        };

        let stream_id = stream_id_for_order(&cmd.order_id);
        if let Err(e) = self
            .event_store
            .append(&stream_id, &domain_event, expected_version)
            .await
        {
            if matches!(e, DomainError::VersionMismatch { .. }) {
                self.aggregates.remove(&cmd.order_id);
                self.event_cache.remove(&cmd.order_id);
            }
            return Err(e);
        }

        aggregate.apply(&domain_event)?;

        self.event_cache
            .entry(cmd.order_id.clone())
            .or_default()
            .push(domain_event.clone());
        self.processed_commands.insert(command_id);

        info!(
            command_id = %command_id,
            order_id = %cmd.order_id,
            version = aggregate.version,
            "Successfully recorded courtesy credit settlement failure"
        );

        Ok(domain_event)
    }

    /// Rolls back the most recent domain event and processed command ID for an order
    /// in the event of an unrecoverable downstream publishing failure.
    pub fn rollback_last_event(&mut self, order_id: &str, command_id: &Uuid) {
        self.processed_commands.remove(command_id);
        if let Some(events) = self.event_cache.get_mut(order_id) {
            events.pop();
            if events.is_empty() {
                self.event_cache.remove(order_id);
                self.aggregates.remove(order_id);
            } else if let Ok(rehydrated) = OrderAggregate::from_events(events.as_slice()) {
                self.aggregates.insert(order_id.to_string(), rehydrated);
            }
        }
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
        self.event_cache
            .insert(order_id.to_string(), events.to_vec());
        Ok(self
            .aggregates
            .get(order_id)
            .expect("Aggregate just inserted"))
    }

    /// Rehydrates an aggregate directly from the configured [`EventStore`].
    pub async fn rehydrate_from_store(
        &mut self,
        order_id: &str,
    ) -> Result<&OrderAggregate, DomainError> {
        let stream_id = stream_id_for_order(order_id);
        let events = self.event_store.scan(&stream_id).await?;
        self.rehydrate(order_id, &events)
    }

    /// Manually inserts or overrides an aggregate in the state cache (useful for testing states).
    pub fn insert_aggregate(&mut self, aggregate: OrderAggregate) {
        self.aggregates
            .insert(aggregate.order_id.clone(), aggregate);
    }

    /// Returns a reference to an aggregate by `order_id` if present in state cache.
    pub fn get_aggregate(&self, order_id: &str) -> Option<&OrderAggregate> {
        self.aggregates.get(order_id)
    }

    /// Returns a mutable reference to an aggregate by `order_id` if present in state cache.
    pub fn get_aggregate_mut(&mut self, order_id: &str) -> Option<&mut OrderAggregate> {
        self.aggregates.get_mut(order_id)
    }

    /// Returns historical domain events for an order from local cache.
    pub fn get_events(&self, order_id: &str) -> Option<&[DomainEvent]> {
        self.event_cache.get(order_id).map(|v| v.as_slice())
    }

    /// Returns a reference to all cached aggregates.
    pub fn aggregates(&self) -> &HashMap<String, OrderAggregate> {
        &self.aggregates
    }

    /// Returns a reference to the local event cache for backward compatibility.
    pub fn event_store(&self) -> &HashMap<String, Vec<DomainEvent>> {
        &self.event_cache
    }

    /// Returns a reference to the configured [`EventStore`].
    pub fn store(&self) -> &Arc<dyn EventStore> {
        &self.event_store
    }

    /// Returns the set of processed command IDs.
    pub fn processed_commands(&self) -> &HashSet<Uuid> {
        &self.processed_commands
    }
}
