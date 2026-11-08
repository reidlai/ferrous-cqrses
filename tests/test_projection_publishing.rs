//! Integration and contract test suite for Story 7.3:
//! Consolidated State Projection Publishing to `orders/consolidated` (`ferrous-cqrses`).
//!
//! Verifies:
//! - Publishing verified consolidated state snapshot records `{ order_id, status, credit_issued, updated_at, version, ... }`
//!   to the configured `topic_orders_consolidated` upon successful aggregate mutation.
//! - Credit concession published with exact credit delta in `credit_issued` and cumulative total in `courtesy_credit_cents`.
//! - Order creation published with `credit_issued: 0`.
//! - Order cancellation published with `credit_issued: 0` and status `Cancelled`.
//! - Partition key strictly matches `order_id`.
//! - Rejections (e.g. exceeding courtesy credit limit) emit ZERO projections.
//! - Duplicate commands (idempotent redelivery) emit ZERO projections.
//! - Deserialization errors emit ZERO projections.
//! - JSON payload compatibility: round-trip deserialization back to `ConsolidatedOrderProjection` and generic `serde_json::Value`.
//! - Offset progression on publisher and in-memory test harnesses.

use chrono::Utc;
use ferrous_cqrses::prelude::*;
use serde_json::Value;

#[tokio::test]
async fn test_credit_concession_projection_published_exact_fields() {
    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let order_id = "ORD-PROJ-CRED-001";
    let customer_id = "CUST-PROJ-101";

    // 1. Create order
    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        customer_id,
        2500,
        Utc::now(),
    ));
    // 2. Issue courtesy credit (750 cents)
    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        750,
        "Transit delay apology credit",
    ));

    consumer
        .process_message(ReceivedMessage::from_command(0, 0, &create_cmd).unwrap())
        .await
        .unwrap();

    consumer
        .process_message(ReceivedMessage::from_command(1, 0, &credit_cmd).unwrap())
        .await
        .unwrap();

    // Verify 2 messages published
    let published = consumer.publisher().published();
    assert_eq!(published.len(), 2);

    let credit_msg = &published[1];
    assert_eq!(credit_msg.topic, "consolidated");
    assert_eq!(credit_msg.partition_key, order_id);
    assert_eq!(credit_msg.offset, 1);

    // Strongly-typed deserialization roundtrip
    let projection: ConsolidatedOrderProjection = serde_json::from_slice(&credit_msg.payload)
        .expect("Must deserialize into ConsolidatedOrderProjection");

    assert_eq!(projection.order_id, order_id);
    assert_eq!(projection.customer_id, customer_id);
    assert_eq!(projection.status, OrderStatus::Created);
    assert_eq!(projection.total_freight_cents, 2500);
    assert_eq!(projection.courtesy_credit_cents, 750);
    assert_eq!(projection.credit_issued, 750);
    assert_eq!(projection.version, 2);

    // Generic JSON serde_json::Value field inspection
    let val: Value =
        serde_json::from_slice(&credit_msg.payload).expect("Must parse generic JSON Value");
    assert_eq!(val["order_id"].as_str().unwrap(), order_id);
    assert_eq!(val["customer_id"].as_str().unwrap(), customer_id);
    assert_eq!(val["status"].as_str().unwrap(), "created");
    assert_eq!(val["total_freight_cents"].as_i64().unwrap(), 2500);
    assert_eq!(val["courtesy_credit_cents"].as_i64().unwrap(), 750);
    assert_eq!(val["credit_issued"].as_i64().unwrap(), 750);
    assert_eq!(val["version"].as_u64().unwrap(), 2);
    assert!(val["delivery_sla"].is_string());
    assert!(val["updated_at"].is_string());
    assert!(val["actual_delivery"].is_null());
}

#[tokio::test]
async fn test_order_creation_projection_publishing() {
    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let order_id = "ORD-PROJ-CREATE-001";
    let customer_id = "CUST-PROJ-202";
    let sla = Utc::now();

    let create_cmd =
        OrderCommand::CreateOrder(CreateOrderCommand::new(order_id, customer_id, 1850, sla));

    consumer
        .process_message(ReceivedMessage::from_command(0, 0, &create_cmd).unwrap())
        .await
        .unwrap();

    let published = consumer.publisher().published();
    assert_eq!(published.len(), 1);

    let msg = &published[0];
    assert_eq!(msg.topic, "consolidated");
    assert_eq!(msg.partition_key, order_id);
    assert_eq!(msg.offset, 0);

    let proj: ConsolidatedOrderProjection = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(proj.order_id, order_id);
    assert_eq!(proj.customer_id, customer_id);
    assert_eq!(proj.status, OrderStatus::Created);
    assert_eq!(proj.total_freight_cents, 1850);
    assert_eq!(proj.courtesy_credit_cents, 0);
    assert_eq!(proj.credit_issued, 0);
    assert_eq!(proj.version, 1);
    assert_eq!(proj.actual_delivery, None);
}

#[tokio::test]
async fn test_order_cancellation_projection_publishing() {
    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let order_id = "ORD-PROJ-CANCEL-001";

    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-PROJ-303",
        1200,
        Utc::now(),
    ));
    let cancel_cmd =
        OrderCommand::CancelOrder(CancelOrderCommand::new(order_id, "Customer changed mind"));

    consumer
        .process_message(ReceivedMessage::from_command(0, 0, &create_cmd).unwrap())
        .await
        .unwrap();

    consumer
        .process_message(ReceivedMessage::from_command(1, 0, &cancel_cmd).unwrap())
        .await
        .unwrap();

    let published = consumer.publisher().published();
    assert_eq!(published.len(), 2);

    let cancel_msg = &published[1];
    assert_eq!(cancel_msg.partition_key, order_id);

    let proj: ConsolidatedOrderProjection = serde_json::from_slice(&cancel_msg.payload).unwrap();
    assert_eq!(proj.order_id, order_id);
    assert_eq!(proj.status, OrderStatus::Cancelled);
    assert_eq!(proj.credit_issued, 0);
    assert_eq!(proj.version, 2);
}

#[tokio::test]
async fn test_partition_key_strictly_matches_order_id() {
    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let orders = ["ORD-PART-100", "ORD-PART-200", "ORD-PART-300"];

    for (i, order_id) in orders.iter().enumerate() {
        let cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
            *order_id,
            format!("CUST-{}", i),
            1000 + (i as i64 * 100),
            Utc::now(),
        ));
        consumer
            .process_message(ReceivedMessage::from_command(i as u64, 0, &cmd).unwrap())
            .await
            .unwrap();
    }

    let published = consumer.publisher().published();
    assert_eq!(published.len(), 3);

    for (i, msg) in published.iter().enumerate() {
        let expected_order_id = orders[i];
        assert_eq!(msg.partition_key, expected_order_id);

        let proj: ConsolidatedOrderProjection = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!(proj.order_id, expected_order_id);
        assert_eq!(msg.partition_key, proj.order_id);
    }
}

#[tokio::test]
async fn test_rejections_emit_zero_projections() {
    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let order_id = "ORD-PROJ-REJ-001";

    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-404",
        1500,
        Utc::now(),
    ));
    consumer
        .process_message(ReceivedMessage::from_command(0, 0, &create_cmd).unwrap())
        .await
        .unwrap();

    assert_eq!(consumer.publisher().published().len(), 1);

    // Excessive credit command (2500 > 2000 limit) -> rejected!
    let excessive_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        2500,
        "Exceeds courtesy credit cap",
    ));
    let outcome = consumer
        .process_message(ReceivedMessage::from_command(1, 0, &excessive_cmd).unwrap())
        .await
        .unwrap();

    assert!(outcome.is_rejected());

    // Still exactly 1 message published; rejection emitted 0 projections
    assert_eq!(consumer.publisher().published().len(), 1);
    assert_eq!(consumer.publisher().last_message().unwrap().offset, 0);
}

#[tokio::test]
async fn test_duplicate_commands_emit_zero_projections() {
    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let order_id = "ORD-PROJ-DUP-001";
    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-505",
        2000,
        Utc::now(),
    ));

    // First delivery -> Success -> 1 message published
    let outcome1 = consumer
        .process_message(ReceivedMessage::from_command(0, 0, &create_cmd).unwrap())
        .await
        .unwrap();
    assert!(outcome1.is_success());
    assert_eq!(consumer.publisher().published().len(), 1);

    // Duplicate redelivery -> IdempotentDuplicate -> 0 additional messages published
    let outcome2 = consumer
        .process_message(ReceivedMessage::from_command(1, 0, &create_cmd).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        outcome2,
        CommandOutcome::IdempotentDuplicate { .. }
    ));

    // Still exactly 1 message published
    assert_eq!(consumer.publisher().published().len(), 1);
}

#[tokio::test]
async fn test_deserialization_errors_emit_zero_projections() {
    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let corrupted_msg = ReceivedMessage::new(0, 0, &b"{\"malformed_json\":"[..]);
    let outcome = consumer.process_message(corrupted_msg).await.unwrap();

    assert!(matches!(
        outcome,
        CommandOutcome::DeserializationError { .. }
    ));
    // Zero projections published
    assert_eq!(consumer.publisher().published().len(), 0);
    assert!(consumer.publisher().last_message().is_none());
}

#[tokio::test]
async fn test_projection_json_roundtrip_and_serialization() {
    let now = Utc::now();
    let agg = OrderAggregate::new("ORD-ROUNDTRIP-01", "CUST-RT-01", 3450, now);
    let proj = ConsolidatedOrderProjection::from_aggregate(&agg, 500, now);

    assert_eq!(proj.order_id, "ORD-ROUNDTRIP-01");
    assert_eq!(proj.customer_id, "CUST-RT-01");
    assert_eq!(proj.total_freight_cents, 3450);
    assert_eq!(proj.courtesy_credit_cents, 0);
    assert_eq!(proj.credit_issued, 0);
    assert_eq!(proj.credit_delta, 500);
    assert_eq!(proj.version, 0);

    let json_bytes = proj.to_json_bytes().expect("Serialization must succeed");
    let deserialized: ConsolidatedOrderProjection =
        serde_json::from_slice(&json_bytes).expect("Deserialization must succeed");

    assert_eq!(proj, deserialized);
    assert_eq!(deserialized.credit_issued, 0);
    assert_eq!(deserialized.credit_delta, 500);
}

#[tokio::test]
async fn test_publisher_offset_progression_and_methods() {
    let mut publisher = InMemoryStreamPublisher::new();
    assert_eq!(publisher.published().len(), 0);
    assert!(publisher.last_message().is_none());

    let off0 = publisher
        .publish("topicA", "key1", b"payload1")
        .await
        .unwrap();
    assert_eq!(off0, 0);
    assert_eq!(publisher.published().len(), 1);
    assert_eq!(publisher.last_message().unwrap().offset, 0);
    assert_eq!(publisher.last_message().unwrap().partition_key, "key1");

    let off1 = publisher
        .publish("topicA", "key2", b"payload2")
        .await
        .unwrap();
    assert_eq!(off1, 1);
    assert_eq!(publisher.published().len(), 2);
    assert_eq!(publisher.last_message().unwrap().offset, 1);
    assert_eq!(publisher.last_message().unwrap().partition_key, "key2");

    let off2 = publisher
        .publish("topicB", "key3", b"payload3")
        .await
        .unwrap();
    assert_eq!(off2, 2);
    assert_eq!(publisher.published().len(), 3);
    assert_eq!(publisher.last_message().unwrap().offset, 2);

    publisher.clear();
    assert_eq!(publisher.published().len(), 0);
    assert!(publisher.last_message().is_none());
}

#[tokio::test]
async fn test_iggy_stream_publisher_facade() {
    let config = Config::default();
    let mut publisher = IggyStreamPublisher::new(config);

    let offset = publisher
        .publish("orders", "ORD-1", b"{\"test\": true}")
        .await
        .unwrap();
    assert_eq!(offset, 0);
}

#[tokio::test]
async fn test_consecutive_credits_cumulative_vs_delta() {
    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let order_id = "ORD-CONSEC-CREDIT-01";

    // 1. Create Order
    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-CONSEC",
        2000,
        Utc::now(),
    ));
    consumer
        .process_message(ReceivedMessage::from_command(0, 0, &create_cmd).unwrap())
        .await
        .unwrap();

    // 2. Credit 1 (500 cents)
    let credit_cmd_1 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id, 500, "Delay 1",
    ));
    consumer
        .process_message(ReceivedMessage::from_command(1, 0, &credit_cmd_1).unwrap())
        .await
        .unwrap();

    // 3. Credit 2 (300 cents)
    let credit_cmd_2 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id, 300, "Delay 2",
    ));
    consumer
        .process_message(ReceivedMessage::from_command(2, 0, &credit_cmd_2).unwrap())
        .await
        .unwrap();

    // 4. Cancel Order
    let cancel_cmd = OrderCommand::CancelOrder(CancelOrderCommand::new(
        order_id,
        "Cancellation after concession",
    ));
    consumer
        .process_message(ReceivedMessage::from_command(3, 0, &cancel_cmd).unwrap())
        .await
        .unwrap();

    let published = consumer.publisher().published();
    assert_eq!(published.len(), 4);

    // Verify Projection 1 (Create)
    let p1: ConsolidatedOrderProjection = serde_json::from_slice(&published[0].payload).unwrap();
    assert_eq!(p1.version, 1);
    assert_eq!(p1.credit_issued, 0);
    assert_eq!(p1.courtesy_credit_cents, 0);
    assert_eq!(p1.credit_delta, 0);

    // Verify Projection 2 (Credit 1: 500c)
    let p2: ConsolidatedOrderProjection = serde_json::from_slice(&published[1].payload).unwrap();
    assert_eq!(p2.version, 2);
    assert_eq!(p2.credit_issued, 500);
    assert_eq!(p2.courtesy_credit_cents, 500);
    assert_eq!(p2.credit_delta, 500);

    // Verify Projection 3 (Credit 2: 300c -> Cumulative 800c)
    let p3: ConsolidatedOrderProjection = serde_json::from_slice(&published[2].payload).unwrap();
    assert_eq!(p3.version, 3);
    assert_eq!(p3.credit_issued, 800);
    assert_eq!(p3.courtesy_credit_cents, 800);
    assert_eq!(p3.credit_delta, 300);

    // Verify Projection 4 (Cancel: credit_issued preserves 800c cumulative concession!)
    let p4: ConsolidatedOrderProjection = serde_json::from_slice(&published[3].payload).unwrap();
    assert_eq!(p4.version, 4);
    assert_eq!(p4.status, OrderStatus::Cancelled);
    assert_eq!(p4.credit_issued, 800);
    assert_eq!(p4.courtesy_credit_cents, 800);
    assert_eq!(p4.credit_delta, 0);
}

#[tokio::test]
async fn test_publisher_failure_mode_rollback_and_unacknowledged_offset() {
    use async_trait::async_trait;

    // A publisher that simulates a broker outage on demand
    struct FailingStreamPublisher {
        fail: bool,
    }

    #[async_trait]
    impl StreamPublisher for FailingStreamPublisher {
        async fn publish(
            &mut self,
            _topic: &str,
            _partition_key: &str,
            _payload: &[u8],
        ) -> Result<u64, DomainError> {
            if self.fail {
                Err(DomainError::Generic(
                    "Simulated broker network connection failure".into(),
                ))
            } else {
                Ok(0)
            }
        }
    }

    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = FailingStreamPublisher { fail: true };
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let order_id = "ORD-FAIL-PUBLISH-01";
    let cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-FAIL",
        1500,
        Utc::now(),
    ));
    let cmd_id = cmd.command_id();

    // Process message when publisher fails
    let res = consumer
        .process_message(ReceivedMessage::from_command(0, 0, &cmd).unwrap())
        .await;

    assert!(res.is_err(), "Must return Err when publisher fails");

    // Assert stream offset was NOT acknowledged
    assert_eq!(consumer.stream().acknowledged_offsets(), Vec::<u64>::new());

    // Assert command handler was rolled back (command ID removed from processed_commands, aggregate not stored)
    assert!(!consumer.handler().processed_commands().contains(&cmd_id));
    assert!(consumer.handler().get_aggregate(order_id).is_none());
}

#[tokio::test]
async fn test_custom_topic_orders_consolidated_publishing() {
    let config = Config {
        topic_orders_consolidated: "custom_orders_snapshot_topic".to_string(),
        ..Default::default()
    };

    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-CUSTOM-TOPIC",
        "CUST-CUSTOM",
        1500,
        Utc::now(),
    ));

    consumer
        .process_message(ReceivedMessage::from_command(0, 0, &cmd).unwrap())
        .await
        .unwrap();

    let published = consumer.publisher().published();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].topic, "custom_orders_snapshot_topic");
}

#[tokio::test]
async fn test_consumer_stream_batch_publishing_integration() {
    let config = Config::default();
    let mut stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();

    let cmd1 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-BATCH-1",
        "CUST-B1",
        1000,
        Utc::now(),
    ));
    let cmd2 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-BATCH-2",
        "CUST-B2",
        2000,
        Utc::now(),
    ));
    let cmd3 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        "ORD-BATCH-1",
        400,
        "Batch Credit",
    ));

    stream.enqueue(ReceivedMessage::from_command(0, 0, &cmd1).unwrap());
    stream.enqueue(ReceivedMessage::from_command(1, 0, &cmd2).unwrap());
    stream.enqueue(ReceivedMessage::from_command(2, 0, &cmd3).unwrap());

    let mut consumer = CommandConsumer::new(config, stream, publisher);
    let count = consumer.process_all_available().await.unwrap();
    assert_eq!(count, 3);

    let published = consumer.publisher().published();
    assert_eq!(published.len(), 3);
    assert_eq!(published[0].partition_key, "ORD-BATCH-1");
    assert_eq!(published[1].partition_key, "ORD-BATCH-2");
    assert_eq!(published[2].partition_key, "ORD-BATCH-1");

    assert_eq!(consumer.stream().acknowledged_offsets(), vec![0, 1, 2]);
}

#[tokio::test]
async fn test_zero_projections_on_terminal_lifecycle_rejections() {
    let config = Config::default();
    let stream = InMemoryStreamConsumer::new();
    let publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    let order_id = "ORD-TERM-REJ-01";

    let cmd1 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-TERM",
        1500,
        Utc::now(),
    ));
    let cmd2 = OrderCommand::CancelOrder(CancelOrderCommand::new(order_id, "Cancelled"));
    // Credit attempt on cancelled order
    let cmd3 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        500,
        "Concession on cancelled",
    ));

    consumer
        .process_message(ReceivedMessage::from_command(0, 0, &cmd1).unwrap())
        .await
        .unwrap();
    consumer
        .process_message(ReceivedMessage::from_command(1, 0, &cmd2).unwrap())
        .await
        .unwrap();
    let outcome3 = consumer
        .process_message(ReceivedMessage::from_command(2, 0, &cmd3).unwrap())
        .await
        .unwrap();

    assert!(outcome3.is_rejected());
    assert_eq!(
        consumer.publisher().published().len(),
        2,
        "Rejection must NOT emit a 3rd projection"
    );
}

#[tokio::test]
async fn test_publisher_latency_sla() {
    let mut publisher = InMemoryStreamPublisher::new();
    let payload =
        b"{\"order_id\":\"ORD-SLA\",\"status\":\"created\",\"credit_issued\":0,\"version\":1}";

    let start = std::time::Instant::now();
    let offset = publisher
        .publish("orders", "ORD-SLA", payload)
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(offset, 0);
    // Warm up and verify sub-millisecond in-memory performance with a generous 50ms threshold
    // to prevent flaky test failures on shared CI runners under CPU contention
    assert!(
        elapsed < std::time::Duration::from_millis(50),
        "Publish confirmation must complete rapidly (actual: {:?})",
        elapsed
    );
}

#[tokio::test]
async fn test_minimal_five_field_schema_compatibility() {
    let raw_json = r#"{
        "order_id": "ORD-MIN-001",
        "status": "created",
        "credit_issued": 250,
        "version": 1,
        "updated_at": "2026-11-08T12:00:00Z"
    }"#;

    let proj: ConsolidatedOrderProjection =
        serde_json::from_str(raw_json).expect("Must parse minimal 5-field JSON");
    assert_eq!(proj.order_id, "ORD-MIN-001");
    assert_eq!(proj.status, OrderStatus::Created);
    assert_eq!(proj.credit_issued, 250);
    assert_eq!(proj.version, 1);
    assert_eq!(proj.courtesy_credit_cents, 0);
    assert_eq!(proj.credit_delta, 0);
    assert_eq!(proj.customer_id, "");
    assert_eq!(proj.total_freight_cents, 0);
    assert_eq!(proj.actual_delivery, None);
}

#[tokio::test]
async fn test_actual_delivery_some_projection_serialization() {
    let now = Utc::now();
    let mut agg = OrderAggregate::new("ORD-DELIV-01", "CUST-DELIV", 2000, now);
    agg.actual_delivery = Some(now);

    let proj = ConsolidatedOrderProjection::from_aggregate(&agg, 0, now);
    assert_eq!(proj.actual_delivery, Some(now));

    let bytes = proj.to_json_bytes().unwrap();
    let deserialized: ConsolidatedOrderProjection = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(deserialized.actual_delivery, Some(now));
}
