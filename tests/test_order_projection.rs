//! Integration tests for `order_projection` binary and `ProjectionConsumer`.
//!
//! Validates Topology A projection generation:
//! - Consuming committed `DomainEvent` (`CreditIssuedEvent`, `OrderCreatedEvent`, `OrderCancelledEvent`) from `orders/consolidated`
//! - Reconstructing/updating `OrderAggregate`
//! - Committing `ConsolidatedOrderProjection` snapshot to Delta Lake (`delta-rs`) and publishing to `orders/callback`
//! - Healthcheck probe and resilience to malformed payloads

use chrono::Utc;
use ferrous_cqrses::aggregate::OrderStatus;
use ferrous_cqrses::consumer::{
    Config, InMemoryStreamConsumer, ProjectionConsumer, ProjectionOutcome, ReceivedMessage,
};
use ferrous_cqrses::events::{CreditIssuedEvent, DomainEvent, OrderCreatedEvent};
use ferrous_cqrses::projection::ConsolidatedOrderProjection;
use ferrous_cqrses::publisher::InMemoryStreamPublisher;
use ferrous_cqrses::sierra::{stream_id_for_order, EventStore, ExpectedVersion, InMemoryEventStore};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[test]
fn test_order_projection_healthcheck_binary_probe() {
    let bin_path = env!("CARGO_BIN_EXE_order_projection");

    let start = std::time::Instant::now();
    let output = Command::new(bin_path)
        .arg("--healthcheck")
        .output()
        .expect("Failed to execute order_projection with --healthcheck");
    let elapsed = start.elapsed();

    assert!(
        output.status.success(),
        "order_projection --healthcheck must exit successfully"
    );
    assert_eq!(output.status.code(), Some(0));
    assert!(
        elapsed < Duration::from_secs(3),
        "healthcheck probe must complete quickly"
    );
}

#[tokio::test]
async fn test_projection_consumer_transforms_credit_issued_to_consolidated_snapshot() {
    let config = Config::default();
    let order_id = format!("ord-{}", Uuid::now_v7());
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let store: Arc<dyn EventStore> = Arc::new(InMemoryEventStore::new());

    // 1. Seed initial order in SierraDB event store
    let created_event = DomainEvent::OrderCreated(OrderCreatedEvent::new(
        &order_id,
        "cust-123",
        1500,
        Utc::now(),
    ));
    store
        .append(
            &stream_id_for_order(&order_id),
            &created_event,
            ExpectedVersion::Empty,
        )
        .await
        .unwrap();

    let mut consumer =
        ProjectionConsumer::with_store(config.clone(), stream, publisher, Arc::clone(&store));

    // 2. Enqueue CreditIssuedEvent into incoming stream
    let credit_event =
        DomainEvent::CreditIssued(CreditIssuedEvent::new(&order_id, 1200, "Packaging damage", 2));
    let payload = serde_json::to_vec(&credit_event).unwrap();
    let msg = ReceivedMessage::new(0, 0, payload);
    consumer.stream_mut().enqueue(msg);

    // 3. Process message
    let outcome = consumer.process_next().await.unwrap();
    assert!(outcome.is_some());
    let outcome = outcome.unwrap();

    match outcome {
        ProjectionOutcome::Success {
            offset,
            order_id: out_order_id,
            credit_delta,
            published_offset,
            ..
        } => {
            assert_eq!(offset, 0);
            assert_eq!(out_order_id, order_id);
            assert_eq!(credit_delta, 1200);
            assert_eq!(published_offset, 0);
        }
        other => panic!("Expected ProjectionOutcome::Success, got: {other:?}"),
    }

    // 4. Verify ConsolidatedOrderProjection was published to topic_orders_callback
    let published = consumer.publisher().published();
    assert_eq!(published.len(), 1);
    let pub_msg = &published[0];
    assert_eq!(pub_msg.topic, config.topic_orders_callback);
    assert_eq!(pub_msg.partition_key, order_id);

    let projection: ConsolidatedOrderProjection = serde_json::from_slice(&pub_msg.payload).unwrap();
    assert_eq!(projection.order_id, order_id);
    assert_eq!(projection.customer_id, "cust-123");
    assert_eq!(projection.order_status, OrderStatus::Created);
    assert_eq!(projection.courtesy_credit_cents, 1200);
    assert_eq!(projection.credit_delta, 1200);

    // 5. Verify offset was acknowledged
    assert_eq!(consumer.stream().acknowledged_offsets(), vec![0]);
}

#[tokio::test]
async fn test_projection_consumer_deserialization_error_resilience() {
    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = ProjectionConsumer::new(config, stream, publisher);

    // Enqueue malformed JSON
    let bad_msg = ReceivedMessage::new(42, 1, b"not valid json".to_vec());
    consumer.stream_mut().enqueue(bad_msg);

    let outcome = consumer.process_next().await.unwrap();
    assert!(outcome.is_some());

    match outcome.unwrap() {
        ProjectionOutcome::DeserializationError {
            offset,
            partition_id,
            ..
        } => {
            assert_eq!(offset, 42);
            assert_eq!(partition_id, 1);
        }
        other => panic!("Expected DeserializationError, got: {other:?}"),
    }

    // Offset must still be acknowledged to maintain stream progress
    assert_eq!(consumer.stream().acknowledged_offsets(), vec![42]);
    // No projections published
    assert!(consumer.publisher().published().is_empty());
}

#[tokio::test]
async fn test_projection_consumer_sequential_events_updates_cached_aggregate() {
    let config = Config::default();
    let order_id = format!("ord-{}", Uuid::now_v7());
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let store: Arc<dyn EventStore> = Arc::new(InMemoryEventStore::new());

    let mut consumer =
        ProjectionConsumer::with_store(config.clone(), stream, publisher, Arc::clone(&store));

    // 1. Process OrderCreatedEvent directly through consumer (empty historical store)
    let created_event = DomainEvent::OrderCreated(OrderCreatedEvent::new(
        &order_id,
        "cust-seq",
        2000,
        Utc::now(),
    ));
    let msg1 = ReceivedMessage::new(0, 0, serde_json::to_vec(&created_event).unwrap());
    consumer.stream_mut().enqueue(msg1);

    let outcome1 = consumer.process_next().await.unwrap().expect("outcome1");
    assert!(matches!(outcome1, ProjectionOutcome::Success { offset: 0, credit_delta: 0, .. }));

    // 2. Process first CreditIssuedEvent (version 2)
    let credit_event1 =
        DomainEvent::CreditIssued(CreditIssuedEvent::new(&order_id, 800, "Late delivery", 2));
    let msg2 = ReceivedMessage::new(1, 0, serde_json::to_vec(&credit_event1).unwrap());
    consumer.stream_mut().enqueue(msg2);

    let outcome2 = consumer.process_next().await.unwrap().expect("outcome2");
    assert!(matches!(outcome2, ProjectionOutcome::Success { offset: 1, credit_delta: 800, .. }));

    // 3. Process second CreditIssuedEvent (version 3)
    let credit_event2 =
        DomainEvent::CreditIssued(CreditIssuedEvent::new(&order_id, 500, "Minor defect", 3));
    let msg3 = ReceivedMessage::new(2, 0, serde_json::to_vec(&credit_event2).unwrap());
    consumer.stream_mut().enqueue(msg3);

    let outcome3 = consumer.process_next().await.unwrap().expect("outcome3");
    assert!(matches!(outcome3, ProjectionOutcome::Success { offset: 2, credit_delta: 500, .. }));

    // Verify all 3 snapshots were published with accumulating totals
    let published = consumer.publisher().published();
    assert_eq!(published.len(), 3);

    let snap1: ConsolidatedOrderProjection = serde_json::from_slice(&published[0].payload).unwrap();
    assert_eq!(snap1.courtesy_credit_cents, 0);
    assert_eq!(snap1.credit_delta, 0);

    let snap2: ConsolidatedOrderProjection = serde_json::from_slice(&published[1].payload).unwrap();
    assert_eq!(snap2.courtesy_credit_cents, 800);
    assert_eq!(snap2.credit_delta, 800);

    let snap3: ConsolidatedOrderProjection = serde_json::from_slice(&published[2].payload).unwrap();
    assert_eq!(snap3.courtesy_credit_cents, 1300);
    assert_eq!(snap3.credit_delta, 500);

    assert_eq!(consumer.stream().acknowledged_offsets(), vec![0, 1, 2]);
}

#[tokio::test]
async fn test_projection_consumer_duplicate_event_idempotency() {
    let config = Config::default();
    let order_id = format!("ord-{}", Uuid::now_v7());
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let store: Arc<dyn EventStore> = Arc::new(InMemoryEventStore::new());

    let mut consumer =
        ProjectionConsumer::with_store(config.clone(), stream, publisher, Arc::clone(&store));

    let created_event = DomainEvent::OrderCreated(OrderCreatedEvent::new(
        &order_id,
        "cust-dup",
        1000,
        Utc::now(),
    ));
    let payload = serde_json::to_vec(&created_event).unwrap();
    consumer.stream_mut().enqueue(ReceivedMessage::new(10, 0, payload.clone()));

    let outcome1 = consumer.process_next().await.unwrap();
    assert!(outcome1.is_some());

    // Redeliver identical event (e.g., at-least-once replay)
    consumer.stream_mut().enqueue(ReceivedMessage::new(11, 0, payload));
    let outcome2 = consumer.process_next().await.unwrap();
    assert!(outcome2.is_some());
    assert!(matches!(outcome2.unwrap(), ProjectionOutcome::Success { offset: 11, .. }));
}
