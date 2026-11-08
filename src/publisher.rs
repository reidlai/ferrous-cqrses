//! Streaming broker publisher abstraction and implementations.
//!
//! Provides [`StreamPublisher`], [`PublishedMessage`], [`InMemoryStreamPublisher`] for tests,
//! and [`IggyStreamPublisher`] for Apache Iggy production/mock stream broker interactions.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::consumer::Config;
use crate::error::DomainError;

/// Abstraction representing a message stream publisher.
#[async_trait]
pub trait StreamPublisher: Send + Sync {
    /// Publishes a payload to the designated topic partitioned by partition key.
    /// Returns the assigned message offset within the partition.
    async fn publish(
        &mut self,
        topic: &str,
        partition_key: &str,
        payload: &[u8],
    ) -> Result<u64, DomainError>;
}

/// A published message recorded by a stream publisher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedMessage {
    pub topic: String,
    pub partition_key: String,
    pub payload: Bytes,
    pub offset: u64,
    pub published_at: DateTime<Utc>,
}

impl PublishedMessage {
    /// Creates a new published message instance.
    pub fn new(
        topic: impl Into<String>,
        partition_key: impl Into<String>,
        payload: impl Into<Bytes>,
        offset: u64,
        published_at: DateTime<Utc>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition_key: partition_key.into(),
            payload: payload.into(),
            offset,
            published_at,
        }
    }
}

/// In-memory stream publisher harness for unit/integration testing without live broker dependencies.
#[derive(Debug, Default, Clone)]
pub struct InMemoryStreamPublisher {
    published: Vec<PublishedMessage>,
}

impl InMemoryStreamPublisher {
    /// Creates a new empty in-memory stream publisher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a slice of all published messages.
    pub fn published(&self) -> &[PublishedMessage] {
        &self.published
    }

    /// Clears all recorded published messages.
    pub fn clear(&mut self) {
        self.published.clear();
    }

    /// Returns the most recently published message, if any.
    pub fn last_message(&self) -> Option<&PublishedMessage> {
        self.published.last()
    }
}

#[async_trait]
impl StreamPublisher for InMemoryStreamPublisher {
    async fn publish(
        &mut self,
        topic: &str,
        partition_key: &str,
        payload: &[u8],
    ) -> Result<u64, DomainError> {
        let offset = self.published.len() as u64;
        let message = PublishedMessage {
            topic: topic.to_string(),
            partition_key: partition_key.to_string(),
            payload: Bytes::copy_from_slice(payload),
            offset,
            published_at: Utc::now(),
        };
        self.published.push(message);
        Ok(offset)
    }
}

/// Iggy stream publisher abstraction wrapping stream connection or mock fallback.
pub struct IggyStreamPublisher {
    pub config: Config,
    inner: Box<dyn StreamPublisher>,
}

impl IggyStreamPublisher {
    /// Creates a new Iggy stream publisher with default in-memory fallback.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            inner: Box::new(InMemoryStreamPublisher::new()),
        }
    }

    /// Creates an Iggy stream publisher with custom underlying publisher implementation.
    pub fn with_publisher(config: Config, publisher: Box<dyn StreamPublisher>) -> Self {
        Self {
            config,
            inner: publisher,
        }
    }
}

impl std::fmt::Debug for IggyStreamPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IggyStreamPublisher")
            .field("config", &self.config)
            .finish()
    }
}

#[async_trait]
impl StreamPublisher for IggyStreamPublisher {
    async fn publish(
        &mut self,
        topic: &str,
        partition_key: &str,
        payload: &[u8],
    ) -> Result<u64, DomainError> {
        self.inner.publish(topic, partition_key, payload).await
    }
}
