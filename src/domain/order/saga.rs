//! Asynchronous Saga Process Manager Daemon.
//!
//! Provides [`OrderSagaManager`], configuration parsing ([`SagaConfig`]), outbound enterprise
//! settlement dispatch abstractions ([`EnterpriseDispatcher`], [`HttpEnterpriseDispatcher`],
//! [`MockEnterpriseDispatcher`]), and correlation / idempotency tracking for post-order
//! courtesy credit settlements.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::aggregate::MAX_COURTESY_CREDIT_CENTS;
use super::error::DomainError;
use super::events::DomainEvent;
use crate::consumer::{Config, InMemoryStreamConsumer, ReceivedMessage, StreamConsumer};

/// Configuration options for the order saga process manager daemon.
#[derive(Clone, PartialEq, Eq, Parser, Serialize, Deserialize)]
#[command(
    name = "ferrous-cqrses-order-saga",
    author = "FerrousCX Contributors",
    version,
    about = "Asynchronous Saga Process Manager Daemon for enterprise settlement workflows"
)]
pub struct SagaConfig {
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

    /// Inbound committed domain events/snapshots topic name (`orders/consolidated`)
    #[arg(long, env = "TOPIC_ORDERS_EVENTS", default_value = "consolidated")]
    pub topic_orders_events: String,

    /// Consumer group name for sequential saga processing
    #[arg(long, env = "CONSUMER_GROUP", default_value = "cg-order-saga")]
    pub consumer_group: String,

    /// Enterprise settlement endpoint URL
    #[arg(
        long,
        env = "ENTERPRISE_SETTLEMENT_URL",
        default_value = "http://127.0.0.1:8080/api/v1/webhooks/orders/settlement-callback"
    )]
    pub enterprise_settlement_url: String,

    /// Optional bearer token for enterprise webhook authorization
    #[arg(long, env = "ENTERPRISE_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Maximum retry attempts for transient errors
    #[arg(long, env = "MAX_RETRIES", default_value_t = 3)]
    pub max_retries: u32,

    /// Initial retry backoff in milliseconds
    #[arg(long, env = "BACKOFF_INITIAL_MS", default_value_t = 100)]
    pub backoff_initial_ms: u64,

    /// Maximum retry backoff in milliseconds
    #[arg(long, env = "BACKOFF_MAX_MS", default_value_t = 2000)]
    pub backoff_max_ms: u64,

    /// Outbound HTTP request timeout in milliseconds
    #[arg(long, env = "REQUEST_TIMEOUT_MS", default_value_t = 5000)]
    pub request_timeout_ms: u64,

    /// Execute static healthcheck probe and exit immediately
    #[arg(long, default_value_t = false)]
    pub healthcheck: bool,
}

impl std::fmt::Debug for SagaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SagaConfig")
            .field("iggy_server_address", &self.iggy_server_address)
            .field("stream_name", &self.stream_name)
            .field("topic_orders_events", &self.topic_orders_events)
            .field("consumer_group", &self.consumer_group)
            .field("enterprise_settlement_url", &self.enterprise_settlement_url)
            .field("auth_token", &self.auth_token.as_ref().map(|_| "[REDACTED]"))
            .field("max_retries", &self.max_retries)
            .field("backoff_initial_ms", &self.backoff_initial_ms)
            .field("backoff_max_ms", &self.backoff_max_ms)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("healthcheck", &self.healthcheck)
            .finish()
    }
}

impl Default for SagaConfig {
    fn default() -> Self {
        Self {
            iggy_server_address: "127.0.0.1:8090".to_string(),
            iggy_username: "iggy".to_string(),
            iggy_password: "iggy".to_string(),
            stream_name: "orders".to_string(),
            topic_orders_events: "consolidated".to_string(),
            consumer_group: "cg-order-saga".to_string(),
            enterprise_settlement_url:
                "http://127.0.0.1:8080/api/v1/webhooks/orders/settlement-callback".to_string(),
            auth_token: None,
            max_retries: 3,
            backoff_initial_ms: 100,
            backoff_max_ms: 2000,
            request_timeout_ms: 5000,
            healthcheck: false,
        }
    }
}

impl From<&SagaConfig> for Config {
    fn from(saga: &SagaConfig) -> Self {
        Config {
            iggy_server_address: saga.iggy_server_address.clone(),
            iggy_username: saga.iggy_username.clone(),
            iggy_password: saga.iggy_password.clone(),
            stream_name: saga.stream_name.clone(),
            topic_orders_events: saga.topic_orders_events.clone(),
            consumer_group: saga.consumer_group.clone(),
            healthcheck: saga.healthcheck,
            ..Default::default()
        }
    }
}

/// Outbound request payload dispatched to the external enterprise settlement endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnterpriseSettlementRequest {
    pub order_id: String,
    pub command_id: String,
    pub idempotency_key: String,
    pub amount_cents: i64,
    pub status: String,
    pub timestamp: DateTime<Utc>,
}

/// Response returned from an enterprise settlement dispatch attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnterpriseResponse {
    pub status_code: u16,
    pub body: String,
}

/// Errors occurring during saga orchestration or external dispatch.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SagaError {
    #[error("Network error during enterprise dispatch: {0}")]
    NetworkError(String),

    #[error("Request timeout during enterprise dispatch: {0}")]
    Timeout(String),

    #[error("HTTP error response from enterprise endpoint (status {status}): {body}")]
    HttpError { status: u16, body: String },

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Stream error: {0}")]
    StreamError(String),

    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
}

impl SagaError {
    /// Returns true if the error represents a transient failure eligible for retry.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::NetworkError(_) | Self::Timeout(_) => true,
            Self::HttpError { status, .. } => *status >= 500 || *status == 429 || *status == 408,
            _ => false,
        }
    }
}

impl From<reqwest::Error> for SagaError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            SagaError::Timeout(err.to_string())
        } else {
            SagaError::NetworkError(err.to_string())
        }
    }
}

impl From<serde_json::Error> for SagaError {
    fn from(err: serde_json::Error) -> Self {
        SagaError::SerializationError(err.to_string())
    }
}

/// Asynchronous trait abstracting outbound enterprise settlement dispatch.
#[async_trait]
pub trait EnterpriseDispatcher: Send + Sync {
    async fn dispatch(
        &self,
        request: &EnterpriseSettlementRequest,
    ) -> Result<EnterpriseResponse, SagaError>;
}

/// Production HTTP enterprise dispatcher using `reqwest`.
#[derive(Clone)]
pub struct HttpEnterpriseDispatcher {
    client: reqwest::Client,
    endpoint_url: String,
    auth_token: Option<String>,
}

impl std::fmt::Debug for HttpEnterpriseDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpEnterpriseDispatcher")
            .field("endpoint_url", &self.endpoint_url)
            .field("auth_token", &self.auth_token.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

impl HttpEnterpriseDispatcher {
    pub fn new(
        endpoint_url: impl Into<String>,
        auth_token: Option<String>,
        timeout: Duration,
    ) -> Result<Self, SagaError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| SagaError::ConfigurationError(e.to_string()))?;

        Ok(Self {
            client,
            endpoint_url: endpoint_url.into(),
            auth_token,
        })
    }

    pub fn from_config(config: &SagaConfig) -> Result<Self, SagaError> {
        Self::new(
            &config.enterprise_settlement_url,
            config.auth_token.clone(),
            Duration::from_millis(config.request_timeout_ms),
        )
    }

    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    pub fn auth_token(&self) -> Option<&str> {
        self.auth_token.as_deref()
    }
}

#[async_trait]
impl EnterpriseDispatcher for HttpEnterpriseDispatcher {
    async fn dispatch(
        &self,
        request: &EnterpriseSettlementRequest,
    ) -> Result<EnterpriseResponse, SagaError> {
        let mut req_builder = self
            .client
            .post(&self.endpoint_url)
            .header("Idempotency-Key", &request.idempotency_key)
            .json(request);

        if let Some(ref token) = self.auth_token {
            req_builder = req_builder.bearer_auth(token);
        }

        let resp = req_builder.send().await.map_err(|e| {
            if e.is_timeout() {
                SagaError::Timeout(e.to_string())
            } else if e.is_builder() {
                SagaError::ConfigurationError(format!("Invalid request format/header: {e}"))
            } else {
                SagaError::NetworkError(e.to_string())
            }
        })?;

        let status_code = resp.status().as_u16();
        let body_bytes = resp.bytes().await.map_err(|e| {
            if e.is_timeout() {
                SagaError::Timeout(e.to_string())
            } else {
                SagaError::NetworkError(format!("Failed reading response body: {e}"))
            }
        })?;

        // Bound response body to 64KB to prevent OOM
        let capped_len = body_bytes.len().min(64 * 1024);
        let body = String::from_utf8_lossy(&body_bytes[..capped_len]).to_string();

        if (200..300).contains(&status_code) {
            Ok(EnterpriseResponse {
                status_code,
                body,
            })
        } else {
            Err(SagaError::HttpError {
                status: status_code,
                body,
            })
        }
    }
}

/// Mock / In-memory enterprise dispatcher for deterministic unit and integration testing.
#[derive(Debug, Clone)]
pub struct MockEnterpriseDispatcher {
    dispatched: Arc<std::sync::Mutex<Vec<EnterpriseSettlementRequest>>>,
    remaining_failures: Arc<std::sync::Mutex<usize>>,
    failure_error: Arc<std::sync::Mutex<Option<SagaError>>>,
    success_response: Arc<std::sync::Mutex<EnterpriseResponse>>,
}

/// Type alias for MockEnterpriseDispatcher.
pub type InMemoryEnterpriseDispatcher = MockEnterpriseDispatcher;

impl Default for MockEnterpriseDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl MockEnterpriseDispatcher {
    pub fn new() -> Self {
        Self {
            dispatched: Arc::new(std::sync::Mutex::new(Vec::new())),
            remaining_failures: Arc::new(std::sync::Mutex::new(0)),
            failure_error: Arc::new(std::sync::Mutex::new(None)),
            success_response: Arc::new(std::sync::Mutex::new(EnterpriseResponse {
                status_code: 200,
                body: r#"{"status":"settled"}"#.to_string(),
            })),
        }
    }

    pub fn with_transient_failures(count: usize) -> Self {
        let dispatcher = Self::new();
        *dispatcher.remaining_failures.lock().unwrap() = count;
        *dispatcher.failure_error.lock().unwrap() = Some(SagaError::NetworkError(
            "Transient network error in mock".to_string(),
        ));
        dispatcher
    }

    pub fn with_http_error(count: usize, status: u16, body: impl Into<String>) -> Self {
        let dispatcher = Self::new();
        *dispatcher.remaining_failures.lock().unwrap() = count;
        *dispatcher.failure_error.lock().unwrap() = Some(SagaError::HttpError {
            status,
            body: body.into(),
        });
        dispatcher
    }

    pub fn with_permanent_failure() -> Self {
        let dispatcher = Self::new();
        *dispatcher.remaining_failures.lock().unwrap() = usize::MAX;
        *dispatcher.failure_error.lock().unwrap() = Some(SagaError::HttpError {
            status: 500,
            body: "Internal Server Error".to_string(),
        });
        dispatcher
    }

    pub fn set_remaining_failures(&self, count: usize) {
        *self.remaining_failures.lock().unwrap() = count;
    }

    pub fn set_failure_error(&self, error: SagaError) {
        *self.failure_error.lock().unwrap() = Some(error);
    }

    pub fn set_success_response(&self, response: EnterpriseResponse) {
        *self.success_response.lock().unwrap() = response;
    }

    pub fn dispatched_requests(&self) -> Vec<EnterpriseSettlementRequest> {
        self.dispatched.lock().unwrap().clone()
    }

    pub fn dispatched_count(&self) -> usize {
        self.dispatched.lock().unwrap().len()
    }

    pub fn clear(&self) {
        self.dispatched.lock().unwrap().clear();
    }
}

#[async_trait]
impl EnterpriseDispatcher for MockEnterpriseDispatcher {
    async fn dispatch(
        &self,
        request: &EnterpriseSettlementRequest,
    ) -> Result<EnterpriseResponse, SagaError> {
        self.dispatched.lock().unwrap().push(request.clone());

        let mut rem = self.remaining_failures.lock().unwrap();
        if *rem > 0 {
            if *rem != usize::MAX {
                *rem -= 1;
            }
            let err = self
                .failure_error
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| {
                    SagaError::NetworkError("Transient connection error".to_string())
                });
            return Err(err);
        }

        Ok(self.success_response.lock().unwrap().clone())
    }
}

/// Outcome of processing a received domain event in the order saga manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaOutcome {
    /// Outbound enterprise settlement request was successfully dispatched.
    Dispatched {
        offset: u64,
        partition_id: u32,
        order_id: String,
        amount_cents: i64,
        idempotency_key: String,
        attempts: u32,
        response_status: u16,
    },
    /// Non-credit domain event was acknowledged and ignored.
    IgnoredNonCredit {
        offset: u64,
        partition_id: u32,
        order_id: String,
        event_type: String,
    },
    /// Zero or negative credit amount was detected; skipped dispatch and acknowledged.
    SkippedZeroOrNegativeCredit {
        offset: u64,
        partition_id: u32,
        order_id: String,
        amount_cents: i64,
    },
    /// Outbound dispatch exhausted all retries or failed permanently; acknowledged to preserve stream progress.
    ExhaustedRetries {
        offset: u64,
        partition_id: u32,
        order_id: String,
        amount_cents: i64,
        attempts: u32,
        error: String,
    },
    /// Message payload could not be deserialized into a valid [`DomainEvent`].
    DeserializationError {
        offset: u64,
        partition_id: u32,
        error: String,
    },
}

impl SagaOutcome {
    pub fn offset(&self) -> u64 {
        match self {
            Self::Dispatched { offset, .. } => *offset,
            Self::IgnoredNonCredit { offset, .. } => *offset,
            Self::SkippedZeroOrNegativeCredit { offset, .. } => *offset,
            Self::ExhaustedRetries { offset, .. } => *offset,
            Self::DeserializationError { offset, .. } => *offset,
        }
    }

    pub fn partition_id(&self) -> u32 {
        match self {
            Self::Dispatched { partition_id, .. } => *partition_id,
            Self::IgnoredNonCredit { partition_id, .. } => *partition_id,
            Self::SkippedZeroOrNegativeCredit { partition_id, .. } => *partition_id,
            Self::ExhaustedRetries { partition_id, .. } => *partition_id,
            Self::DeserializationError { partition_id, .. } => *partition_id,
        }
    }

    pub fn is_dispatched(&self) -> bool {
        matches!(self, Self::Dispatched { .. })
    }

    pub fn is_ignored(&self) -> bool {
        matches!(
            self,
            Self::IgnoredNonCredit { .. } | Self::SkippedZeroOrNegativeCredit { .. }
        )
    }

    pub fn is_exhausted(&self) -> bool {
        matches!(self, Self::ExhaustedRetries { .. })
    }
}

/// Asynchronous Saga Process Manager consuming committed `DomainEvent` records from `orders/consolidated`
/// and orchestrating outbound settlement calls with external enterprise systems.
pub struct OrderSagaManager<
    S: StreamConsumer = InMemoryStreamConsumer,
    D: EnterpriseDispatcher = HttpEnterpriseDispatcher,
> {
    config: SagaConfig,
    stream: S,
    dispatcher: D,
    outcomes: Vec<SagaOutcome>,
    heartbeat_tracker: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl<S: StreamConsumer, D: EnterpriseDispatcher> OrderSagaManager<S, D> {
    /// Creates a new order saga manager.
    pub fn new(config: SagaConfig, stream: S, dispatcher: D) -> Self {
        Self {
            config,
            stream,
            dispatcher,
            outcomes: Vec::new(),
            heartbeat_tracker: None,
        }
    }

    /// Attaches an atomic timestamp tracker to record liveness on each processing iteration.
    pub fn with_heartbeat_tracker(mut self, tracker: Arc<std::sync::atomic::AtomicU64>) -> Self {
        self.heartbeat_tracker = Some(tracker);
        self
    }

    pub fn config(&self) -> &SagaConfig {
        &self.config
    }

    pub fn stream(&self) -> &S {
        &self.stream
    }

    pub fn stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    pub fn dispatcher(&self) -> &D {
        &self.dispatcher
    }

    pub fn dispatcher_mut(&mut self) -> &mut D {
        &mut self.dispatcher
    }

    pub fn outcomes(&self) -> &[SagaOutcome] {
        &self.outcomes
    }

    pub fn clear_outcomes(&mut self) {
        self.outcomes.clear();
    }

    fn record_outcome(&mut self, outcome: SagaOutcome) {
        if self.outcomes.len() >= 1000 {
            self.outcomes.drain(0..100);
        }
        self.outcomes.push(outcome);
    }

    /// Processes a single received message from the event stream.
    pub async fn process_message(
        &mut self,
        msg: &ReceivedMessage,
    ) -> Result<SagaOutcome, SagaError> {
        let offset = msg.offset;
        let partition_id = msg.partition_id;

        // 1. Attempt deserialization: JSON first, then Bincode fallback
        let event: DomainEvent = match serde_json::from_slice(&msg.payload) {
            Ok(evt) => evt,
            Err(json_err) => match bincode::deserialize::<DomainEvent>(&msg.payload) {
                Ok(evt) => evt,
                Err(bin_err) => {
                    let err_msg =
                        format!("JSON error: {}, Bincode error: {}", json_err, bin_err);
                    error!(
                        partition_id = partition_id,
                        offset = offset,
                        error = %err_msg,
                        "Failed to deserialize message payload into DomainEvent"
                    );
                    self.stream
                        .acknowledge(partition_id, offset)
                        .await
                        .map_err(|e| SagaError::StreamError(e.to_string()))?;
                    let outcome = SagaOutcome::DeserializationError {
                        offset,
                        partition_id,
                        error: err_msg,
                    };
                    self.record_outcome(outcome.clone());
                    return Ok(outcome);
                }
            },
        };

        // 2. Filter domain events: only DomainEvent::CreditIssued triggers outbound settlement
        let credit_event = match event {
            DomainEvent::CreditIssued(ref e) => e,
            other => {
                let event_type = match other {
                    DomainEvent::OrderCreated(_) => "OrderCreated",
                    DomainEvent::OrderStatusUpdated(_) => "OrderStatusUpdated",
                    DomainEvent::OrderCancelled(_) => "OrderCancelled",
                    DomainEvent::CourtesyCreditSettled(_) => "CourtesyCreditSettled",
                    DomainEvent::CourtesyCreditSettlementFailed(_) => {
                        "CourtesyCreditSettlementFailed"
                    }
                    DomainEvent::CreditIssued(_) => unreachable!(),
                };
                debug!(
                    partition_id = partition_id,
                    offset = offset,
                    order_id = %other.order_id(),
                    event_type = %event_type,
                    "Filtering out non-credit domain event in saga manager"
                );
                self.stream
                    .acknowledge(partition_id, offset)
                    .await
                    .map_err(|e| SagaError::StreamError(e.to_string()))?;
                let outcome = SagaOutcome::IgnoredNonCredit {
                    offset,
                    partition_id,
                    order_id: other.order_id().to_string(),
                    event_type: event_type.to_string(),
                };
                self.record_outcome(outcome.clone());
                return Ok(outcome);
            }
        };

        // 3. Domain invariant check: non-positive or excessive credit amounts are discarded
        if credit_event.amount_cents <= 0 || credit_event.amount_cents > MAX_COURTESY_CREDIT_CENTS {
            warn!(
                partition_id = partition_id,
                offset = offset,
                order_id = %credit_event.order_id,
                amount_cents = credit_event.amount_cents,
                max_allowed = MAX_COURTESY_CREDIT_CENTS,
                "Ignoring CreditIssued event with credit amount outside valid range (1..={MAX_COURTESY_CREDIT_CENTS})"
            );
            self.stream
                .acknowledge(partition_id, offset)
                .await
                .map_err(|e| SagaError::StreamError(e.to_string()))?;
            let outcome = SagaOutcome::SkippedZeroOrNegativeCredit {
                offset,
                partition_id,
                order_id: credit_event.order_id.clone(),
                amount_cents: credit_event.amount_cents,
            };
            self.record_outcome(outcome.clone());
            return Ok(outcome);
        }

        // 4. Construct outbound EnterpriseSettlementRequest with correlation tokens
        let request = EnterpriseSettlementRequest {
            order_id: credit_event.order_id.clone(),
            command_id: credit_event.event_id.to_string(),
            idempotency_key: format!(
                "saga-settlement-{}-{}",
                credit_event.order_id, credit_event.event_id
            ),
            amount_cents: credit_event.amount_cents,
            status: "pending_settlement".to_string(),
            timestamp: credit_event.issued_at,
        };

        // 5. Outbound dispatch with exponential backoff retry loop
        let mut attempts: u32 = 0;
        let mut last_error_msg = String::new();
        let mut success_response = None;
        let max_backoff = Duration::from_millis(self.config.backoff_max_ms);
        let mut backoff = Duration::from_millis(self.config.backoff_initial_ms).min(max_backoff);

        loop {
            attempts += 1;
            match self.dispatcher.dispatch(&request).await {
                Ok(resp) => {
                    info!(
                        order_id = %request.order_id,
                        command_id = %request.command_id,
                        idempotency_key = %request.idempotency_key,
                        attempts = attempts,
                        status_code = resp.status_code,
                        "Outbound enterprise settlement dispatched successfully"
                    );
                    success_response = Some(resp);
                    break;
                }
                Err(err) => {
                    let is_transient = err.is_transient();
                    last_error_msg = err.to_string();
                    warn!(
                        order_id = %request.order_id,
                        command_id = %request.command_id,
                        attempt = attempts,
                        max_retries = self.config.max_retries,
                        error = %err,
                        "Outbound enterprise settlement dispatch attempt failed"
                    );

                    // Stop if non-transient or retries exhausted (attempts > max_retries)
                    if !is_transient || attempts > self.config.max_retries {
                        break;
                    }

                    tokio::time::sleep(backoff).await;
                    backoff = backoff.checked_mul(2).unwrap_or(max_backoff).min(max_backoff);
                }
            }
        }

        // 6. Acknowledge offset and record outcome
        self.stream
            .acknowledge(partition_id, offset)
            .await
            .map_err(|e| SagaError::StreamError(e.to_string()))?;

        let outcome = if let Some(resp) = success_response {
            SagaOutcome::Dispatched {
                offset,
                partition_id,
                order_id: request.order_id,
                amount_cents: request.amount_cents,
                idempotency_key: request.idempotency_key,
                attempts,
                response_status: resp.status_code,
            }
        } else {
            error!(
                order_id = %request.order_id,
                command_id = %request.command_id,
                idempotency_key = %request.idempotency_key,
                attempts = attempts,
                last_error = %last_error_msg,
                "Outbound enterprise settlement failed permanently after retries; acknowledged offset to avoid blocking stream"
            );
            SagaOutcome::ExhaustedRetries {
                offset,
                partition_id,
                order_id: request.order_id,
                amount_cents: request.amount_cents,
                attempts,
                error: last_error_msg,
            }
        };

        self.record_outcome(outcome.clone());
        Ok(outcome)
    }

    /// Polls and processes the next message from the stream if available.
    pub async fn process_next(&mut self) -> Result<Option<SagaOutcome>, SagaError> {
        if let Some(ref tracker) = self.heartbeat_tracker {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            tracker.store(now, std::sync::atomic::Ordering::Relaxed);
        }

        match self
            .stream
            .poll_message()
            .await
            .map_err(|e| SagaError::StreamError(e.to_string()))?
        {
            Some(msg) => {
                let outcome = self.process_message(&msg).await?;
                Ok(Some(outcome))
            }
            None => Ok(None),
        }
    }

    /// Processes all currently available messages until the stream is empty.
    pub async fn process_all_available(&mut self) -> Result<usize, SagaError> {
        let mut count = 0;
        while let Some(_outcome) = self.process_next().await? {
            count += 1;
        }
        Ok(count)
    }

    /// Continuous saga manager processing loop executing until a shutdown signal is received.
    pub async fn run_loop(
        &mut self,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), SagaError> {
        info!("Entering order saga consumer processing loop");

        while !*shutdown_rx.borrow() {
            match self.process_next().await {
                Ok(Some(_outcome)) => {
                    // Process next message immediately
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
                    error!(error = %err, "Error in saga consumer loop");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                        res = shutdown_rx.changed() => {
                            if res.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        info!("Graceful shutdown requested; exiting order saga consumer loop");
        Ok(())
    }
}
