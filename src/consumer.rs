//! Command consumer and streaming broker abstraction.
//!
//! Provides [`CommandConsumer`], configuration parsing ([`Config`]), message abstractions,
//! and standalone in-memory test harnesses ([`InMemoryStreamConsumer`], [`IggyStreamConsumer`])
//! allowing pure-Rust testing without live broker dependencies.

use std::collections::VecDeque;
use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::commands::OrderCommand;
use crate::error::DomainError;
use crate::events::DomainEvent;
use crate::handler::CommandHandler;
use crate::projection::ConsolidatedOrderProjection;
use crate::publisher::{InMemoryStreamPublisher, StreamPublisher};

/// Configuration options for the command consumer and streaming broker connection.
#[derive(Debug, Clone, PartialEq, Eq, Parser, Serialize, Deserialize)]
#[command(
    name = "ferrous-cqrses-daemon",
    author = "FerrousCX Contributors",
    version,
    about = "Sequential Command Handler Daemon for Apache Iggy stream processing"
)]
pub struct Config {
    /// Iggy streaming broker TCP address
    #[arg(long, env = "IGGY_SERVER_ADDRESS", default_value = "127.0.0.1:8090")]
    pub iggy_server_address: String,

    /// Iggy client username for authentication
    #[arg(long, env = "IGGY_USERNAME", default_value = "iggy")]
    pub iggy_username: String,

    /// Iggy client password for authentication
    #[arg(long, env = "IGGY_PASSWORD", default_value = "iggy")]
    pub iggy_password: String,

    /// Target stream name
    #[arg(long, env = "STREAM_NAME", default_value = "orders")]
    pub stream_name: String,

    /// Inbound command intake topic name
    #[arg(long, env = "TOPIC_ORDERS_INTAKE", default_value = "intake")]
    pub topic_orders_intake: String,

    /// Target consolidated order projections topic name
    #[arg(long, env = "TOPIC_ORDERS_CONSOLIDATED", default_value = "consolidated")]
    pub topic_orders_consolidated: String,

    /// Consumer group name for sequential partitioned consumption
    #[arg(long, env = "CONSUMER_GROUP", default_value = "cg-cmd-handler")]
    pub consumer_group: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            iggy_server_address: "127.0.0.1:8090".to_string(),
            iggy_username: "iggy".to_string(),
            iggy_password: "iggy".to_string(),
            stream_name: "orders".to_string(),
            topic_orders_intake: "intake".to_string(),
            topic_orders_consolidated: "consolidated".to_string(),
            consumer_group: "cg-cmd-handler".to_string(),
        }
    }
}

/// A raw message received from the streaming intake topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMessage {
    pub offset: u64,
    pub partition_id: u32,
    pub payload: Bytes,
}

impl ReceivedMessage {
    /// Creates a new received message.
    pub fn new(offset: u64, partition_id: u32, payload: impl Into<Bytes>) -> Self {
        Self {
            offset,
            partition_id,
            payload: payload.into(),
        }
    }

    /// Serializes an [`OrderCommand`] to JSON and constructs a [`ReceivedMessage`].
    pub fn from_command(
        offset: u64,
        partition_id: u32,
        command: &OrderCommand,
    ) -> Result<Self, DomainError> {
        let json_bytes = serde_json::to_vec(command)?;
        Ok(Self::new(offset, partition_id, json_bytes))
    }
}

/// Outcome of processing a received command message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOutcome {
    /// Command was successfully processed and aggregate state advanced.
    Success {
        offset: u64,
        partition_id: u32,
        command_id: Uuid,
        order_id: String,
        event: DomainEvent,
    },
    /// Command was already processed previously; acknowledged idempotently without state mutation.
    IdempotentDuplicate {
        offset: u64,
        partition_id: u32,
        command_id: Uuid,
        order_id: String,
    },
    /// Command was rejected due to domain invariant violation or invalid state.
    Rejected {
        offset: u64,
        partition_id: u32,
        command_id: Uuid,
        order_id: String,
        error: DomainError,
    },
    /// Message payload could not be deserialized into a valid [`OrderCommand`].
    DeserializationError {
        offset: u64,
        partition_id: u32,
        error: String,
    },
}

impl CommandOutcome {
    pub fn offset(&self) -> u64 {
        match self {
            Self::Success { offset, .. } => *offset,
            Self::IdempotentDuplicate { offset, .. } => *offset,
            Self::Rejected { offset, .. } => *offset,
            Self::DeserializationError { offset, .. } => *offset,
        }
    }

    pub fn partition_id(&self) -> u32 {
        match self {
            Self::Success { partition_id, .. } => *partition_id,
            Self::IdempotentDuplicate { partition_id, .. } => *partition_id,
            Self::Rejected { partition_id, .. } => *partition_id,
            Self::DeserializationError { partition_id, .. } => *partition_id,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. } | Self::IdempotentDuplicate { .. })
    }

    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected { .. })
    }
}

/// Abstraction representing a message stream consumer.
#[async_trait]
pub trait StreamConsumer: Send + Sync {
    /// Polls the next available message from the stream.
    async fn poll_message(&mut self) -> Result<Option<ReceivedMessage>, DomainError>;

    /// Acknowledges message offset for the specified partition to the broker.
    async fn acknowledge(&mut self, partition_id: u32, offset: u64) -> Result<(), DomainError>;
}

/// In-memory standalone message stream consumer harness for testing and local decoupled execution.
#[derive(Debug, Default)]
pub struct InMemoryStreamConsumer {
    queue: VecDeque<ReceivedMessage>,
    channel_rx: Option<tokio::sync::mpsc::Receiver<ReceivedMessage>>,
    acknowledged: Vec<(u32, u64)>,
}

impl InMemoryStreamConsumer {
    /// Creates an empty in-memory stream consumer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an in-memory stream consumer pre-populated with messages.
    pub fn from_messages(messages: impl IntoIterator<Item = ReceivedMessage>) -> Self {
        Self {
            queue: messages.into_iter().collect(),
            channel_rx: None,
            acknowledged: Vec::new(),
        }
    }

    /// Creates an in-memory stream consumer backed by an asynchronous mpsc channel.
    pub fn with_channel(receiver: tokio::sync::mpsc::Receiver<ReceivedMessage>) -> Self {
        Self {
            queue: VecDeque::new(),
            channel_rx: Some(receiver),
            acknowledged: Vec::new(),
        }
    }

    /// Enqueues a message into the stream buffer.
    pub fn enqueue(&mut self, message: ReceivedMessage) {
        self.queue.push_back(message);
    }

    /// Returns the list of acknowledged (partition_id, offset) pairs.
    pub fn acknowledged(&self) -> &[(u32, u64)] {
        &self.acknowledged
    }

    /// Returns the list of acknowledged message offsets across all partitions.
    pub fn acknowledged_offsets(&self) -> Vec<u64> {
        self.acknowledged.iter().map(|(_, offset)| *offset).collect()
    }
}

#[async_trait]
impl StreamConsumer for InMemoryStreamConsumer {
    async fn poll_message(&mut self) -> Result<Option<ReceivedMessage>, DomainError> {
        if let Some(msg) = self.queue.pop_front() {
            return Ok(Some(msg));
        }

        if let Some(rx) = &mut self.channel_rx {
            match rx.try_recv() {
                Ok(msg) => Ok(Some(msg)),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    async fn acknowledge(&mut self, partition_id: u32, offset: u64) -> Result<(), DomainError> {
        self.acknowledged.push((partition_id, offset));
        debug!(
            partition_id = partition_id,
            offset = offset,
            "Offset acknowledged in memory"
        );
        Ok(())
    }
}

/// Iggy stream consumer abstraction wrapping stream connection or mock fallback.
pub struct IggyStreamConsumer {
    pub config: Config,
    inner: Box<dyn StreamConsumer>,
}

impl IggyStreamConsumer {
    /// Creates a new Iggy stream consumer for the provided configuration.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            inner: Box::new(InMemoryStreamConsumer::new()),
        }
    }

    /// Creates an Iggy stream consumer with custom underlying stream implementation.
    pub fn with_stream(config: Config, stream: Box<dyn StreamConsumer>) -> Self {
        Self {
            config,
            inner: stream,
        }
    }
}

#[async_trait]
impl StreamConsumer for IggyStreamConsumer {
    async fn poll_message(&mut self) -> Result<Option<ReceivedMessage>, DomainError> {
        self.inner.poll_message().await
    }

    async fn acknowledge(&mut self, partition_id: u32, offset: u64) -> Result<(), DomainError> {
        self.inner.acknowledge(partition_id, offset).await
    }
}

/// Core sequential command consumer orchestrating message ingestion, JSON parsing,
/// command dispatch, consolidated projection snapshot publishing, and offset acknowledgement.
pub struct CommandConsumer<
    S: StreamConsumer = InMemoryStreamConsumer,
    P: StreamPublisher = InMemoryStreamPublisher,
> {
    config: Config,
    handler: CommandHandler,
    stream: S,
    publisher: P,
    outcomes: Vec<CommandOutcome>,
}

impl<S: StreamConsumer, P: StreamPublisher> CommandConsumer<S, P> {
    /// Creates a new command consumer with the given configuration, stream source, and publisher.
    pub fn new(config: Config, stream: S, publisher: P) -> Self {
        Self {
            config,
            handler: CommandHandler::new(),
            stream,
            publisher,
            outcomes: Vec::new(),
        }
    }

    /// Creates a command consumer with an existing pre-configured [`CommandHandler`].
    pub fn with_handler(config: Config, stream: S, publisher: P, handler: CommandHandler) -> Self {
        Self {
            config,
            handler,
            stream,
            publisher,
            outcomes: Vec::new(),
        }
    }

    /// Returns a reference to the active configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns an immutable reference to the internal [`CommandHandler`].
    pub fn handler(&self) -> &CommandHandler {
        &self.handler
    }

    /// Returns a mutable reference to the internal [`CommandHandler`].
    pub fn handler_mut(&mut self) -> &mut CommandHandler {
        &mut self.handler
    }

    /// Returns an immutable reference to the underlying stream source.
    pub fn stream(&self) -> &S {
        &self.stream
    }

    /// Returns a mutable reference to the underlying stream source.
    pub fn stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// Returns an immutable reference to the projection stream publisher.
    pub fn publisher(&self) -> &P {
        &self.publisher
    }

    /// Returns a mutable reference to the projection stream publisher.
    pub fn publisher_mut(&mut self) -> &mut P {
        &mut self.publisher
    }

    /// Returns all processed command outcomes.
    pub fn outcomes(&self) -> &[CommandOutcome] {
        &self.outcomes
    }

    /// Clears recorded command outcomes.
    pub fn clear_outcomes(&mut self) {
        self.outcomes.clear();
    }

    /// Processes a single received message: deserializes JSON payload, dispatches to handler,
    /// publishes consolidated state projection upon successful mutation, logs results, and
    /// acknowledges the message offset.
    pub async fn process_message(
        &mut self,
        message: ReceivedMessage,
    ) -> Result<CommandOutcome, DomainError> {
        let offset = message.offset;
        let partition_id = message.partition_id;

        // 1. Attempt deserialization of JSON OrderCommand payload
        let command: OrderCommand = match serde_json::from_slice(&message.payload) {
            Ok(cmd) => cmd,
            Err(err) => {
                let err_msg = err.to_string();
                error!(
                    partition_id = partition_id,
                    offset = offset,
                    error = %err_msg,
                    "Failed to deserialize JSON message payload into OrderCommand"
                );
                self.stream.acknowledge(partition_id, offset).await?;
                let outcome = CommandOutcome::DeserializationError {
                    offset,
                    partition_id,
                    error: err_msg,
                };
                self.outcomes.push(outcome.clone());
                return Ok(outcome);
            }
        };

        let command_id = command.command_id();
        let order_id = command.order_id().to_string();

        // 2. Dispatch command to sequential handler
        let outcome = match self.handler.handle_command(command).await {
            Ok(event) => {
                info!(
                    partition_id = partition_id,
                    offset = offset,
                    command_id = %command_id,
                    order_id = %order_id,
                    "Command executed successfully"
                );

                // Story 7.3: Extract credit_delta and publish verified projection to topic_orders_consolidated
                let credit_delta = match &event {
                    DomainEvent::CreditIssued(ref e) => e.amount_cents,
                    _ => 0,
                };

                let agg = self.handler.get_aggregate(&order_id).ok_or_else(|| {
                    tracing::error!(
                        order_id = %order_id,
                        "Aggregate missing from repository after successful command execution"
                    );
                    DomainError::OrderNotFound(order_id.clone())
                })?;

                let projection = ConsolidatedOrderProjection::from_aggregate(
                    agg,
                    credit_delta,
                    event.timestamp(),
                );
                let payload = projection.to_json_bytes()?;

                // Bounded retry with backoff for transient streaming publisher errors
                let mut attempts = 0;
                let published_offset = loop {
                    match self
                        .publisher
                        .publish(&self.config.topic_orders_consolidated, &order_id, &payload)
                        .await
                    {
                        Ok(off) => break off,
                        Err(err) if attempts < 3 => {
                            attempts += 1;
                            warn!(
                                order_id = %order_id,
                                attempt = attempts,
                                error = %err,
                                "Retrying transient publisher error"
                            );
                            tokio::time::sleep(Duration::from_millis(20 * attempts as u64)).await;
                        }
                        Err(err) => {
                            // Roll back aggregate and command idempotency marker so stream redelivery can cleanly re-execute
                            self.handler.rollback_last_event(&order_id, &command_id);
                            return Err(err);
                        }
                    }
                };

                info!(
                    partition_id = partition_id,
                    order_id = %order_id,
                    topic = %self.config.topic_orders_consolidated,
                    offset = published_offset,
                    credit_issued = agg.courtesy_credit_cents,
                    credit_delta = credit_delta,
                    "Published consolidated order projection snapshot"
                );

                CommandOutcome::Success {
                    offset,
                    partition_id,
                    command_id,
                    order_id,
                    event,
                }
            }
            Err(DomainError::DuplicateCommand(id)) => {
                info!(
                    partition_id = partition_id,
                    offset = offset,
                    command_id = %id,
                    order_id = %order_id,
                    "Duplicate command received; acknowledging idempotently"
                );
                CommandOutcome::IdempotentDuplicate {
                    offset,
                    partition_id,
                    command_id: id,
                    order_id,
                }
            }
            Err(err) => {
                warn!(
                    partition_id = partition_id,
                    offset = offset,
                    command_id = %command_id,
                    order_id = %order_id,
                    error = %err,
                    "Command rejected by domain validation"
                );
                CommandOutcome::Rejected {
                    offset,
                    partition_id,
                    command_id,
                    order_id,
                    error: err,
                }
            }
        };

        // 3. Acknowledge offset to maintain stream progress
        self.stream.acknowledge(partition_id, offset).await?;
        if self.outcomes.len() >= 1000 {
            self.outcomes.drain(0..100);
        }
        self.outcomes.push(outcome.clone());

        Ok(outcome)
    }

    /// Polls and processes the next message from the stream if available.
    pub async fn process_next(&mut self) -> Result<Option<CommandOutcome>, DomainError> {
        match self.stream.poll_message().await? {
            Some(msg) => {
                let outcome = self.process_message(msg).await?;
                Ok(Some(outcome))
            }
            None => Ok(None),
        }
    }

    /// Processes all currently available messages until the stream is empty.
    pub async fn process_all_available(&mut self) -> Result<usize, DomainError> {
        let mut count = 0;
        while let Some(_outcome) = self.process_next().await? {
            count += 1;
        }
        Ok(count)
    }

    /// Continuous consumer loop executing until a shutdown signal is received.
    pub async fn run_loop(
        &mut self,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), DomainError> {
        info!("Entering command consumer processing loop");

        while !*shutdown_rx.borrow() {
            match self.process_next().await {
                Ok(Some(_outcome)) => {
                    // Process next message immediately without sleeping
                    continue;
                }
                Ok(None) => {
                    // Stream empty; back off briefly before polling again
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                        res = shutdown_rx.changed() => {
                            if res.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    error!(error = %err, "Error in consumer loop");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

        info!("Graceful shutdown requested; exiting consumer loop");
        Ok(())
    }
}
