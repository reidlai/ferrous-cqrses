//! Comprehensive unit and integration test suite for `order_saga` daemon and `OrderSagaManager`.
//!
//! Validates Story 18.2:
//! - Outbound enterprise settlement dispatch triggered by `DomainEvent::CreditIssued`
//! - Correlation tokens (`order_id`, `command_id`) and deterministic idempotency keys
//! - Filtering and clean acknowledgement of non-credit domain events
//! - Discarding zero and negative credit amounts without dispatch
//! - Exponential backoff retry loop with transient network errors succeeding on retry 2
//! - Resilient offset acknowledgement upon retry exhaustion without crashing consumer loop
//! - Container `--healthcheck` probe execution

use chrono::Utc;
use ferrous_cqrses::aggregate::OrderStatus;
use ferrous_cqrses::consumer::{InMemoryStreamConsumer, ReceivedMessage};
use ferrous_cqrses::events::{
    CourtesyCreditSettledEvent, CourtesyCreditSettlementFailedEvent, CreditIssuedEvent, DomainEvent,
    OrderCancelledEvent, OrderCreatedEvent, OrderStatusUpdatedEvent,
};
use ferrous_cqrses::saga::{
    HttpEnterpriseDispatcher, MockEnterpriseDispatcher, OrderSagaManager, SagaConfig, SagaOutcome,
};
use std::process::Command;
use std::time::Duration;
use uuid::Uuid;

#[test]
fn test_order_saga_healthcheck_binary_probe() {
    let bin_path = env!("CARGO_BIN_EXE_order_saga");

    let start = std::time::Instant::now();
    let output = Command::new(bin_path)
        .arg("--healthcheck")
        .output()
        .expect("Failed to execute order_saga with --healthcheck");
    let elapsed = start.elapsed();

    assert!(
        output.status.success(),
        "order_saga --healthcheck must exit successfully"
    );
    assert_eq!(output.status.code(), Some(0));
    assert!(
        elapsed < Duration::from_secs(3),
        "healthcheck probe must complete quickly (elapsed: {elapsed:?})"
    );
}

#[test]
fn test_order_saga_help_displays_options() {
    let bin_path = env!("CARGO_BIN_EXE_order_saga");

    let output = Command::new(bin_path)
        .arg("--help")
        .output()
        .expect("Failed to execute order_saga --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("--healthcheck"),
        "Help text must document --healthcheck option"
    );
    assert!(
        stdout.contains("--enterprise-settlement-url"),
        "Help text must document --enterprise-settlement-url"
    );
    assert!(
        stdout.contains("--consumer-group"),
        "Help text must document --consumer-group"
    );
}

#[test]
fn test_saga_config_defaults() {
    let config = SagaConfig::default();
    assert_eq!(config.stream_name, "orders");
    assert_eq!(config.topic_orders_events, "events");
    assert_eq!(config.consumer_group, "cg-order-saga");
    assert_eq!(
        config.enterprise_settlement_url,
        "http://127.0.0.1:8080/api/v1/webhooks/orders/settlement-callback"
    );
    assert_eq!(config.max_retries, 3);
    assert_eq!(config.backoff_initial_ms, 100);
    assert_eq!(config.backoff_max_ms, 2000);
    assert_eq!(config.request_timeout_ms, 5000);
    assert!(!config.healthcheck);
}

#[tokio::test]
async fn test_credit_issued_event_triggers_outbound_dispatch_with_correlation() {
    let config = SagaConfig::default();
    let stream = InMemoryStreamConsumer::new();
    let dispatcher = MockEnterpriseDispatcher::new();
    let mut saga = OrderSagaManager::new(config, stream, dispatcher.clone());

    let order_id = format!("ord-{}", Uuid::now_v7());
    let credit_event =
        DomainEvent::CreditIssued(CreditIssuedEvent::new(&order_id, 1500, "Courier delay", 2));
    let expected_event_id = credit_event.event_id().to_string();

    let payload = serde_json::to_vec(&credit_event).unwrap();
    saga.stream_mut()
        .enqueue(ReceivedMessage::new(0, 0, payload));

    let outcome = saga
        .process_next()
        .await
        .expect("process_next should succeed")
        .expect("Outcome should be present");

    match outcome {
        SagaOutcome::Dispatched {
            offset,
            partition_id,
            order_id: out_order_id,
            amount_cents,
            idempotency_key,
            attempts,
            response_status,
        } => {
            assert_eq!(offset, 0);
            assert_eq!(partition_id, 0);
            assert_eq!(out_order_id, order_id);
            assert_eq!(amount_cents, 1500);
            assert!(
                idempotency_key.contains(&order_id),
                "Idempotency key must contain order_id"
            );
            assert!(
                idempotency_key.contains(&expected_event_id),
                "Idempotency key must contain event_id correlation token"
            );
            assert_eq!(attempts, 1);
            assert_eq!(response_status, 200);
        }
        other => panic!("Expected SagaOutcome::Dispatched, got: {other:?}"),
    }

    // Verify stream offset acknowledgement
    assert_eq!(saga.stream().acknowledged_offsets(), vec![0]);

    // Verify outbound request dispatched to external enterprise system
    let dispatched = dispatcher.dispatched_requests();
    assert_eq!(dispatched.len(), 1);
    let req = &dispatched[0];
    assert_eq!(req.order_id, order_id);
    assert_eq!(req.command_id, expected_event_id);
    assert_eq!(req.amount_cents, 1500);
    assert_eq!(req.status, "pending_settlement");
    assert!(req.idempotency_key.starts_with("saga-settlement-"));
}

#[tokio::test]
async fn test_non_credit_events_ignored_and_acknowledged() {
    let config = SagaConfig::default();
    let stream = InMemoryStreamConsumer::new();
    let dispatcher = MockEnterpriseDispatcher::new();
    let mut saga = OrderSagaManager::new(config, stream, dispatcher.clone());

    let order_id = "ord-non-credit-test";

    // Enqueue 5 non-credit domain events
    let events = vec![
        DomainEvent::OrderCreated(OrderCreatedEvent::new(order_id, "cust-1", 1000, Utc::now())),
        DomainEvent::OrderStatusUpdated(OrderStatusUpdatedEvent::new(
            order_id,
            OrderStatus::Created,
            OrderStatus::Processing,
            None,
            2,
        )),
        DomainEvent::OrderCancelled(OrderCancelledEvent::new(order_id, "Customer request", 3)),
        DomainEvent::CourtesyCreditSettled(CourtesyCreditSettledEvent::new(
            order_id, 500, "txn-ref-1", 4,
        )),
        DomainEvent::CourtesyCreditSettlementFailed(CourtesyCreditSettlementFailedEvent::new(
            order_id, 500, "Insufficient ledger", 5,
        )),
    ];

    for (idx, event) in events.into_iter().enumerate() {
        let payload = serde_json::to_vec(&event).unwrap();
        saga.stream_mut()
            .enqueue(ReceivedMessage::new(idx as u64, 0, payload));
    }

    let processed_count = saga
        .process_all_available()
        .await
        .expect("Processing all events should succeed");
    assert_eq!(processed_count, 5);

    // No outbound calls dispatched
    assert_eq!(dispatcher.dispatched_count(), 0);

    // All offsets acknowledged
    assert_eq!(
        saga.stream().acknowledged_offsets(),
        vec![0, 1, 2, 3, 4]
    );

    // Verify outcomes
    for outcome in saga.outcomes() {
        assert!(
            matches!(outcome, SagaOutcome::IgnoredNonCredit { .. }),
            "Expected IgnoredNonCredit outcome, got: {outcome:?}"
        );
        assert!(outcome.is_ignored());
    }
}

#[tokio::test]
async fn test_zero_and_negative_credit_amounts_skipped_without_dispatch() {
    let config = SagaConfig::default();
    let stream = InMemoryStreamConsumer::new();
    let dispatcher = MockEnterpriseDispatcher::new();
    let mut saga = OrderSagaManager::new(config, stream, dispatcher.clone());

    let zero_event =
        DomainEvent::CreditIssued(CreditIssuedEvent::new("ord-zero", 0, "Zero concession", 2));
    let neg_event = DomainEvent::CreditIssued(CreditIssuedEvent::new(
        "ord-neg",
        -500,
        "Negative concession",
        3,
    ));

    saga.stream_mut().enqueue(ReceivedMessage::new(
        0,
        0,
        serde_json::to_vec(&zero_event).unwrap(),
    ));
    saga.stream_mut().enqueue(ReceivedMessage::new(
        1,
        0,
        serde_json::to_vec(&neg_event).unwrap(),
    ));

    let processed = saga
        .process_all_available()
        .await
        .expect("Should process all available");
    assert_eq!(processed, 2);

    // No outbound requests dispatched
    assert_eq!(dispatcher.dispatched_count(), 0);

    // Both offsets acknowledged
    assert_eq!(saga.stream().acknowledged_offsets(), vec![0, 1]);

    // Outcomes match SkippedZeroOrNegativeCredit
    assert_eq!(saga.outcomes().len(), 2);
    assert_eq!(
        saga.outcomes()[0],
        SagaOutcome::SkippedZeroOrNegativeCredit {
            offset: 0,
            partition_id: 0,
            order_id: "ord-zero".to_string(),
            amount_cents: 0,
        }
    );
    assert_eq!(
        saga.outcomes()[1],
        SagaOutcome::SkippedZeroOrNegativeCredit {
            offset: 1,
            partition_id: 0,
            order_id: "ord-neg".to_string(),
            amount_cents: -500,
        }
    );
}

#[tokio::test]
async fn test_retry_mechanism_transient_error_succeeds_on_retry_2() {
    let config = SagaConfig {
        max_retries: 3,
        backoff_initial_ms: 1,
        backoff_max_ms: 5,
        ..Default::default()
    };

    let stream = InMemoryStreamConsumer::new();
    // Simulate 2 transient failures, then success on attempt 3 (retry 2)
    let dispatcher = MockEnterpriseDispatcher::with_transient_failures(2);
    let mut saga = OrderSagaManager::new(config, stream, dispatcher.clone());

    let credit_event =
        DomainEvent::CreditIssued(CreditIssuedEvent::new("ord-retry", 800, "Minor defect", 2));
    saga.stream_mut().enqueue(ReceivedMessage::new(
        0,
        0,
        serde_json::to_vec(&credit_event).unwrap(),
    ));

    let outcome = saga
        .process_next()
        .await
        .expect("process_next should succeed")
        .expect("Outcome should be present");

    match outcome {
        SagaOutcome::Dispatched {
            offset,
            order_id,
            amount_cents,
            attempts,
            response_status,
            ..
        } => {
            assert_eq!(offset, 0);
            assert_eq!(order_id, "ord-retry");
            assert_eq!(amount_cents, 800);
            assert_eq!(attempts, 3, "Must have succeeded on attempt 3 (retry 2)");
            assert_eq!(response_status, 200);
        }
        other => panic!("Expected Dispatched outcome, got: {other:?}"),
    }

    assert_eq!(dispatcher.dispatched_count(), 3);
    assert_eq!(saga.stream().acknowledged_offsets(), vec![0]);
}

#[tokio::test]
async fn test_exhausted_retries_log_error_and_acknowledge_offset_without_crashing() {
    let config = SagaConfig {
        max_retries: 2,
        backoff_initial_ms: 1,
        backoff_max_ms: 5,
        ..Default::default()
    };

    let stream = InMemoryStreamConsumer::new();
    // Simulate permanent failure (HTTP 500)
    let dispatcher = MockEnterpriseDispatcher::with_permanent_failure();
    let mut saga = OrderSagaManager::new(config, stream, dispatcher.clone());

    let credit_event =
        DomainEvent::CreditIssued(CreditIssuedEvent::new("ord-fail", 1200, "Lost package", 2));
    saga.stream_mut().enqueue(ReceivedMessage::new(
        0,
        0,
        serde_json::to_vec(&credit_event).unwrap(),
    ));

    let outcome = saga
        .process_next()
        .await
        .expect("process_next should succeed even on exhausted retries")
        .expect("Outcome should be present");

    match outcome {
        SagaOutcome::ExhaustedRetries {
            offset,
            partition_id,
            order_id,
            amount_cents,
            attempts,
            error,
        } => {
            assert_eq!(offset, 0);
            assert_eq!(partition_id, 0);
            assert_eq!(order_id, "ord-fail");
            assert_eq!(amount_cents, 1200);
            // 1 initial attempt + 2 retries = 3 attempts total
            assert_eq!(attempts, 3);
            assert!(error.contains("500") || error.contains("Internal Server Error"));
        }
        other => panic!("Expected ExhaustedRetries outcome, got: {other:?}"),
    }

    // Crucial requirement: offset MUST be acknowledged to prevent head-of-line blocking
    assert_eq!(saga.stream().acknowledged_offsets(), vec![0]);
    assert_eq!(dispatcher.dispatched_count(), 3);
}

#[tokio::test]
async fn test_deserialization_error_resilience() {
    let config = SagaConfig::default();
    let stream = InMemoryStreamConsumer::new();
    let dispatcher = MockEnterpriseDispatcher::new();
    let mut saga = OrderSagaManager::new(config, stream, dispatcher.clone());

    // Corrupted binary payload
    saga.stream_mut()
        .enqueue(ReceivedMessage::new(42, 0, &b"invalid garbage data"[..]));

    let outcome = saga
        .process_next()
        .await
        .expect("process_next should handle deserialization error cleanly")
        .expect("Outcome should be present");

    match outcome {
        SagaOutcome::DeserializationError {
            offset,
            partition_id,
            error,
        } => {
            assert_eq!(offset, 42);
            assert_eq!(partition_id, 0);
            assert!(!error.is_empty());
        }
        other => panic!("Expected DeserializationError outcome, got: {other:?}"),
    }

    assert_eq!(saga.stream().acknowledged_offsets(), vec![42]);
    assert_eq!(dispatcher.dispatched_count(), 0);
}

#[tokio::test]
async fn test_bincode_serialized_event_consumption() {
    let config = SagaConfig::default();
    let stream = InMemoryStreamConsumer::new();
    let dispatcher = MockEnterpriseDispatcher::new();
    let mut saga = OrderSagaManager::new(config, stream, dispatcher.clone());

    let credit_event = DomainEvent::CreditIssued(CreditIssuedEvent::new(
        "ord-bincode",
        1999,
        "VIP customer concession",
        2,
    ));

    // Serialize using Bincode
    let bincode_bytes = bincode::serialize(&credit_event).unwrap();
    saga.stream_mut()
        .enqueue(ReceivedMessage::new(10, 0, bincode_bytes));

    let outcome = saga
        .process_next()
        .await
        .expect("Should process bincode payload successfully")
        .expect("Outcome should be present");

    assert!(outcome.is_dispatched());
    assert_eq!(dispatcher.dispatched_count(), 1);
    assert_eq!(dispatcher.dispatched_requests()[0].amount_cents, 1999);
}

#[tokio::test]
async fn test_saga_manager_graceful_shutdown() {
    let config = SagaConfig::default();
    let stream = InMemoryStreamConsumer::new();
    let dispatcher = MockEnterpriseDispatcher::new();
    let mut saga = OrderSagaManager::new(config, stream, dispatcher);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let handle = tokio::spawn(async move { saga.run_loop(shutdown_rx).await });

    // Let the loop start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger shutdown
    shutdown_tx.send(true).expect("Should send shutdown signal");

    // Task must terminate cleanly
    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("Loop must terminate within timeout")
        .expect("Task must join without panic");

    assert!(result.is_ok(), "run_loop must return Ok(()) on shutdown");
}

#[test]
fn test_http_enterprise_dispatcher_configuration() {
    let config = SagaConfig {
        enterprise_settlement_url: "https://erp.internal/api/v1/settle".to_string(),
        auth_token: Some("secret-token-xyz".to_string()),
        request_timeout_ms: 3000,
        ..Default::default()
    };

    let dispatcher = HttpEnterpriseDispatcher::from_config(&config)
        .expect("Should build HttpEnterpriseDispatcher from config");

    assert_eq!(
        dispatcher.endpoint_url(),
        "https://erp.internal/api/v1/settle"
    );
    assert_eq!(dispatcher.auth_token(), Some("secret-token-xyz"));
}
