//! Asynchronous Callback Service and Stream Consumer for enterprise settlement workflows.
//!
//! Provides [`CallbackService`] and [`CallbackConsumer`] responsible for consuming settlement
//! messages from Apache Iggy topic `orders/callback` (consumer group `cg-order-callback`),
//! acting like [`CommandHandler`] to rehydrate `OrderAggregate` (`ESCAN`), evaluate business
//! invariants, atomically commit domain events (`CourtesyCreditSettled` or
//! `CourtesyCreditSettlementFailed`) via `EAPPEND` to SierraDB, and publish committed state
//! to Apache Iggy (`orders/consolidated`).

use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use uuid::Uuid;

use super::commands::{
    FailCourtesyCreditSettlementCommand, OrderCommand, SettleCourtesyCreditCommand,
};
use super::error::DomainError;
use super::events::DomainEvent;
use super::handler::CommandHandler;
use crate::consumer::{InMemoryStreamConsumer, ReceivedMessage, StreamConsumer};
use crate::publisher::StreamPublisher;

/// Canonical Apache Iggy topic name for incoming callback responses (`orders/callback`).
pub const CALLBACK_TOPIC: &str = "callback";

/// Backward-compatible alias for [`CALLBACK_TOPIC`].
pub const CALLBACKS_TOPIC: &str = CALLBACK_TOPIC;

/// Default Apache Iggy consumer group for the callback service.
pub const CALLBACKS_CONSUMER_GROUP: &str = "cg-order-callback";

/// Status reported by external enterprise settlement workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementStatus {
    Settled,
    Failed,
}

/// Incoming message payload from the callback queue (`orders/callback`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementCallbackMessage {
    pub order_id: String,
    pub amount_cents: i64,
    pub status: SettlementStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl SettlementCallbackMessage {
    /// Validates the structure and invariants of the callback payload.
    pub fn validate(&self) -> Result<(), CallbackError> {
        if self.order_id.trim().is_empty() {
            return Err(CallbackError::InvalidPayload(
                "order_id cannot be blank".to_string(),
            ));
        }
        if self.amount_cents <= 0 {
            return Err(CallbackError::InvalidPayload(
                "amount_cents must be strictly greater than 0".to_string(),
            ));
        }
        match self.status {
            SettlementStatus::Settled => match &self.transaction_ref {
                Some(tr) if !tr.trim().is_empty() => {}
                _ => {
                    return Err(CallbackError::InvalidPayload(
                        "transaction_ref is required for settled status and cannot be blank"
                            .to_string(),
                    ));
                }
            },
            SettlementStatus::Failed => match &self.reason {
                Some(r) if !r.trim().is_empty() => {}
                _ => {
                    return Err(CallbackError::InvalidPayload(
                        "reason is required for failed status and cannot be blank".to_string(),
                    ));
                }
            },
        }
        Ok(())
    }
}

/// Result of processing an incoming settlement callback message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallbackOutcome {
    Processed {
        command_id: Uuid,
        order_id: String,
        event_type: String,
    },
    DuplicateIgnored {
        command_id: Uuid,
        order_id: String,
    },
}

impl CallbackOutcome {
    pub fn command_id(&self) -> Uuid {
        match self {
            Self::Processed { command_id, .. } => *command_id,
            Self::DuplicateIgnored { command_id, .. } => *command_id,
        }
    }

    pub fn order_id(&self) -> &str {
        match self {
            Self::Processed { order_id, .. } => order_id.as_str(),
            Self::DuplicateIgnored { order_id, .. } => order_id.as_str(),
        }
    }

    pub fn is_processed(&self) -> bool {
        matches!(self, Self::Processed { .. })
    }

    pub fn is_duplicate(&self) -> bool {
        matches!(self, Self::DuplicateIgnored { .. })
    }
}

/// Errors that can occur during callback message processing.
#[derive(Debug, thiserror::Error)]
pub enum CallbackError {
    #[error("Invalid callback payload: {0}")]
    InvalidPayload(String),

    #[error("JSON deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),

    #[error("Domain invariant violation: {0}")]
    Domain(#[from] DomainError),

    #[error("Event publisher error: {0}")]
    Publisher(String),
}

/// Abstraction for publishing committed settlement domain events.
#[async_trait]
pub trait CallbackPublisher: Send + Sync {
    /// Publishes a domain event to the destination topic partitioned by partition key.
    async fn publish_domain_event(
        &self,
        topic: &str,
        partition_key: &str,
        event: &DomainEvent,
    ) -> Result<(), String>;
}

#[async_trait]
impl<P: StreamPublisher + ?Sized> CallbackPublisher for tokio::sync::Mutex<P> {
    async fn publish_domain_event(
        &self,
        topic: &str,
        partition_key: &str,
        event: &DomainEvent,
    ) -> Result<(), String> {
        let payload = serde_json::to_vec(event).map_err(|e| e.to_string())?;
        let mut publisher = self.lock().await;
        publisher
            .publish(topic, partition_key, &payload)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[async_trait]
impl<T: CallbackPublisher + ?Sized> CallbackPublisher for Arc<T> {
    async fn publish_domain_event(
        &self,
        topic: &str,
        partition_key: &str,
        event: &DomainEvent,
    ) -> Result<(), String> {
        self.as_ref()
            .publish_domain_event(topic, partition_key, event)
            .await
    }
}

/// Type alias for event listeners notified post-commit.
pub type EventListener = Arc<dyn Fn(&DomainEvent, &Uuid) + Send + Sync>;

/// Asynchronous callback processing service.
#[derive(Clone)]
pub struct CallbackService {
    command_handler: Arc<tokio::sync::Mutex<CommandHandler>>,
    publisher: Option<Arc<dyn CallbackPublisher>>,
    events_topic: String,
    event_listener: Option<EventListener>,
}

impl CallbackService {
    /// Creates a new `CallbackService` wired to the given `CommandHandler`.
    pub fn new(command_handler: Arc<tokio::sync::Mutex<CommandHandler>>) -> Self {
        Self {
            command_handler,
            publisher: None,
            events_topic: "consolidated".to_string(),
            event_listener: None,
        }
    }

    /// Explicit alias for [`new`].
    pub fn without_publisher(command_handler: Arc<tokio::sync::Mutex<CommandHandler>>) -> Self {
        Self::new(command_handler)
    }

    /// Sets the publisher for committed domain events.
    pub fn with_publisher(mut self, publisher: Arc<dyn CallbackPublisher>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    /// Convenience builder to wire a standard [`StreamPublisher`] behind a mutex.
    pub fn with_stream_publisher<P: StreamPublisher + 'static>(
        mut self,
        publisher: Arc<tokio::sync::Mutex<P>>,
    ) -> Self {
        self.publisher = Some(publisher);
        self
    }

    /// Sets the destination topic for committed domain events (defaults to `"consolidated"`).
    pub fn with_events_topic(mut self, topic: impl Into<String>) -> Self {
        self.events_topic = topic.into();
        self
    }

    /// Explicit alias for [`with_events_topic`] targeting `orders/consolidated`.
    pub fn with_consolidated_topic(self, topic: impl Into<String>) -> Self {
        self.with_events_topic(topic)
    }

    /// Sets an optional event listener callback (e.g. for real-time SSE broadcast).
    pub fn with_event_listener(
        mut self,
        listener: impl Fn(&DomainEvent, &Uuid) + Send + Sync + 'static,
    ) -> Self {
        self.event_listener = Some(Arc::new(listener));
        self
    }

    /// Returns a reference to the shared `CommandHandler`.
    pub fn command_handler(&self) -> &Arc<tokio::sync::Mutex<CommandHandler>> {
        &self.command_handler
    }

    /// Returns the target events topic.
    pub fn events_topic(&self) -> &str {
        &self.events_topic
    }

    /// Deserializes and processes raw JSON message bytes from the callback queue.
    pub async fn process_message(&self, payload_bytes: &[u8]) -> Result<CallbackOutcome, CallbackError> {
        let message: SettlementCallbackMessage = serde_json::from_slice(payload_bytes)?;
        self.process_callback(message).await
    }

    /// Validates and processes a typed settlement callback message.
    pub async fn process_callback(
        &self,
        message: SettlementCallbackMessage,
    ) -> Result<CallbackOutcome, CallbackError> {
        message.validate()?;

        let trimmed_order_id = message.order_id.trim();

        let (order_cmd, command_id) = match message.status {
            SettlementStatus::Settled => {
                let tx_ref = message.transaction_ref.as_deref().unwrap_or_default().trim();
                let cmd_id = Uuid::new_v5(
                    &Uuid::NAMESPACE_OID,
                    format!("{trimmed_order_id}:{tx_ref}").as_bytes(),
                );
                let cmd = SettleCourtesyCreditCommand::new(
                    trimmed_order_id,
                    message.amount_cents,
                    tx_ref,
                )
                .with_command_id(cmd_id);
                (OrderCommand::SettleCourtesyCredit(cmd), cmd_id)
            }
            SettlementStatus::Failed => {
                let reason = message.reason.as_deref().unwrap_or_default().trim();
                let cmd_id = Uuid::new_v5(
                    &Uuid::NAMESPACE_OID,
                    format!("{trimmed_order_id}:{reason}:{}", message.amount_cents).as_bytes(),
                );
                let cmd = FailCourtesyCreditSettlementCommand::new(
                    trimmed_order_id,
                    message.amount_cents,
                    reason,
                )
                .with_command_id(cmd_id);
                (OrderCommand::FailCourtesyCreditSettlement(cmd), cmd_id)
            }
        };

        // 1. Synchronously execute CommandHandler against SierraDB
        let domain_event = {
            let mut handler = self.command_handler.lock().await;
            match handler.handle_command(order_cmd).await {
                Ok(ev) => ev,
                Err(DomainError::DuplicateCommand(_)) => {
                    info!(
                        %command_id,
                        order_id = %trimmed_order_id,
                        "Callback queue consumer idempotently acknowledged duplicate command"
                    );
                    return Ok(CallbackOutcome::DuplicateIgnored {
                        command_id,
                        order_id: trimmed_order_id.to_string(),
                    });
                }
                Err(e) => return Err(CallbackError::Domain(e)),
            }
        };

        // 2. Post-commit publication to streaming broker (e.g. orders/consolidated)
        if let Some(publisher) = &self.publisher {
            if let Err(err_msg) = publisher
                .publish_domain_event(&self.events_topic, trimmed_order_id, &domain_event)
                .await
            {
                let mut handler = self.command_handler.lock().await;
                handler.rollback_last_event(trimmed_order_id, &command_id);
                return Err(CallbackError::Publisher(err_msg));
            }
        }

        // 3. Optional event listener notification (e.g. SSE broadcast)
        if let Some(listener) = &self.event_listener {
            listener(&domain_event, &command_id);
        }

        let event_type = match &domain_event {
            DomainEvent::CourtesyCreditSettled(_) => "CourtesyCreditSettled",
            DomainEvent::CourtesyCreditSettlementFailed(_) => "CourtesyCreditSettlementFailed",
            _ => "DomainEvent",
        };

        info!(
            %command_id,
            order_id = %trimmed_order_id,
            amount_cents = message.amount_cents,
            status = ?message.status,
            topic = %self.events_topic,
            "CallbackService successfully committed settlement domain event to SierraDB and published to consolidated topic"
        );

        Ok(CallbackOutcome::Processed {
            command_id,
            order_id: trimmed_order_id.to_string(),
            event_type: event_type.to_string(),
        })
    }
}

/// Outcome of consuming a raw message from Apache Iggy topic `orders/callback`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallbackConsumerOutcome {
    /// Callback validated against SierraDB aggregate, committed via `EAPPEND`, and offset acked.
    Processed {
        offset: u64,
        partition_id: u32,
        command_id: Uuid,
        order_id: String,
        event_type: String,
    },
    /// Duplicate callback idempotently ignored and offset acked.
    DuplicateIgnored {
        offset: u64,
        partition_id: u32,
        command_id: Uuid,
        order_id: String,
    },
    /// Callback rejected due to invalid payload or domain invariant violation (offset acked).
    Rejected {
        offset: u64,
        partition_id: u32,
        error: String,
    },
}

/// Stream consumer for Apache Iggy topic `orders/callback` (consumer group `cg-order-callback`).
///
/// Operates symmetrically to [`crate::consumer::CommandConsumer`]: polls inbound callback
/// messages, delegates to [`CallbackService`] (which rehydrates `OrderAggregate` via `ESCAN`,
/// evaluates domain invariants, and atomically commits via `EAPPEND` to SierraDB), publishes
/// committed state to `orders/consolidated`, and acknowledges stream offsets.
pub struct CallbackConsumer<S: StreamConsumer = InMemoryStreamConsumer> {
    service: CallbackService,
    stream: S,
    outcomes: Vec<CallbackConsumerOutcome>,
}

impl<S: StreamConsumer> CallbackConsumer<S> {
    /// Creates a new `CallbackConsumer` wrapping the given [`CallbackService`] and [`StreamConsumer`].
    pub fn new(service: CallbackService, stream: S) -> Self {
        Self {
            service,
            stream,
            outcomes: Vec::new(),
        }
    }

    pub fn service(&self) -> &CallbackService {
        &self.service
    }

    pub fn stream(&self) -> &S {
        &self.stream
    }

    pub fn stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    pub fn outcomes(&self) -> &[CallbackConsumerOutcome] {
        &self.outcomes
    }

    /// Processes a single received message from `orders/callback`.
    pub async fn process_message(
        &mut self,
        message: ReceivedMessage,
    ) -> Result<CallbackConsumerOutcome, CallbackError> {
        let offset = message.offset;
        let partition_id = message.partition_id;

        let outcome = match self.service.process_message(&message.payload).await {
            Ok(CallbackOutcome::Processed {
                command_id,
                order_id,
                event_type,
            }) => {
                self.stream
                    .acknowledge(partition_id, offset)
                    .await
                    .map_err(CallbackError::Domain)?;
                CallbackConsumerOutcome::Processed {
                    offset,
                    partition_id,
                    command_id,
                    order_id,
                    event_type,
                }
            }
            Ok(CallbackOutcome::DuplicateIgnored {
                command_id,
                order_id,
            }) => {
                self.stream
                    .acknowledge(partition_id, offset)
                    .await
                    .map_err(CallbackError::Domain)?;
                CallbackConsumerOutcome::DuplicateIgnored {
                    offset,
                    partition_id,
                    command_id,
                    order_id,
                }
            }
            Err(CallbackError::Publisher(err)) => {
                // Transient downstream publisher failure: do NOT acknowledge so broker redelivers
                error!(
                    partition_id = partition_id,
                    offset = offset,
                    error = %err,
                    "CallbackConsumer publisher failure; leaving offset unacknowledged for redelivery"
                );
                return Err(CallbackError::Publisher(err));
            }
            Err(err) => {
                let err_msg = err.to_string();
                warn!(
                    partition_id = partition_id,
                    offset = offset,
                    error = %err_msg,
                    "CallbackConsumer rejected invalid/invariant-violating message; acknowledging offset"
                );
                self.stream
                    .acknowledge(partition_id, offset)
                    .await
                    .map_err(CallbackError::Domain)?;
                CallbackConsumerOutcome::Rejected {
                    offset,
                    partition_id,
                    error: err_msg,
                }
            }
        };

        if self.outcomes.len() >= 1000 {
            self.outcomes.drain(0..100);
        }
        self.outcomes.push(outcome.clone());
        Ok(outcome)
    }

    /// Polls and processes the next available message from `orders/callback`.
    pub async fn process_next(&mut self) -> Result<Option<CallbackConsumerOutcome>, CallbackError> {
        match self
            .stream
            .poll_message()
            .await
            .map_err(CallbackError::Domain)?
        {
            Some(msg) => Ok(Some(self.process_message(msg).await?)),
            None => Ok(None),
        }
    }

    /// Runs the continuous callback consumption loop until `shutdown_rx` signals `true`.
    pub async fn run_loop(
        &mut self,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), CallbackError> {
        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            match self.process_next().await? {
                Some(_) => {}
                None => {
                    tokio::select! {
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                    }
                }
            }
        }
        Ok(())
    }
}
