//! Tests for asynchronous Callback Service in ferrous-cqrses.
//!
//! Verifies:
//! - Callback message processing via `CallbackService`
//! - Invariant validation & direct CommandHandler execution against SierraDB
//! - Idempotent acknowledgment of duplicate callbacks via deterministic UUIDv5
//! - Stream publication to topic (e.g. `events`)
//! - Optional event listener invocation (SSE notification hook)
//! - Payload validation (blank fields, negative/zero amounts, missing refs/reasons)
//! - Rejection of invalid domain states (excessive amount, nonexistent order, cancelled, fraud-locked)
//! - Rollback on publisher failures
//! - Raw JSON message byte parsing via `process_message`

use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use ferrous_cqrses::{
    callback::{
        CallbackError, CallbackOutcome, CallbackPublisher, CallbackService,
        SettlementCallbackMessage, SettlementStatus,
    },
    commands::{
        CancelOrderCommand, CreateOrderCommand, IssueCourtesyCreditCommand, OrderCommand,
    },
    domain::order::{CreditSettlementStatus, DomainError, OrderStatus},
    events::DomainEvent,
    handler::CommandHandler,
    publisher::InMemoryStreamPublisher,
    sierra::InMemoryEventStore,
};
use serde_json::json;

/// Helper to initialize an in-memory CommandHandler with an initial created order
/// and an issued courtesy credit.
async fn setup_order_with_credit(
    order_id: &str,
    credit_cents: i64,
) -> (Arc<Mutex<CommandHandler>>, Arc<Mutex<InMemoryStreamPublisher>>) {
    let store = Arc::new(InMemoryEventStore::new());
    let mut handler = CommandHandler::new_with_store(store);

    // 1. Create order
    let create_cmd = CreateOrderCommand::new(order_id, "cust-1", 5000, chrono::Utc::now());
    handler
        .handle_command(OrderCommand::CreateOrder(create_cmd))
        .await
        .expect("order creation failed");

    // 2. Issue courtesy credit if positive
    if credit_cents > 0 {
        let issue_cmd = IssueCourtesyCreditCommand::new(order_id, credit_cents, "Courtesy concession");
        handler
            .handle_command(OrderCommand::IssueCourtesyCredit(issue_cmd))
            .await
            .expect("credit issuance failed");
    }

    let handler_arc = Arc::new(Mutex::new(handler));
    let publisher_arc = Arc::new(Mutex::new(InMemoryStreamPublisher::new()));

    (handler_arc, publisher_arc)
}

#[tokio::test]
async fn test_successful_settlement_callback() {
    let order_id = Uuid::now_v7().to_string();
    let (handler, publisher) = setup_order_with_credit(&order_id, 1500).await;

    let service = CallbackService::new(handler.clone())
        .with_stream_publisher(publisher.clone())
        .with_consolidated_topic("orders/consolidated");

    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-settle-101".to_string()),
        reason: None,
    };

    let outcome = service.process_callback(message).await.unwrap();
    match outcome {
        CallbackOutcome::Processed {
            command_id: _,
            order_id: ref oid,
            ref event_type,
        } => {
            assert_eq!(oid, &order_id);
            assert_eq!(event_type, "CourtesyCreditSettled");
        }
        other => panic!("Expected CallbackOutcome::Processed, got {other:?}"),
    }

    // Verify aggregate state in CommandHandler / SierraDB
    {
        let h = handler.lock().await;
        let agg = h.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.courtesy_credit_cents, 1500);
        assert_eq!(agg.settled_credit_cents, 1500);
        assert_eq!(agg.settlement_status, CreditSettlementStatus::Settled);
        assert_eq!(agg.version, 3);
    }

    // Verify event publication to stream publisher
    {
        let pub_guard = publisher.lock().await;
        let published = pub_guard.published();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].topic, "orders/consolidated");
        assert_eq!(published[0].partition_key, order_id);

        let event: DomainEvent = serde_json::from_slice(&published[0].payload).unwrap();
        match event {
            DomainEvent::CourtesyCreditSettled(e) => {
                assert_eq!(e.order_id, order_id);
                assert_eq!(e.amount_cents, 1500);
                assert_eq!(e.transaction_ref, "tx-settle-101");
                assert_eq!(e.version, 3);
            }
            other => panic!("Expected CourtesyCreditSettled event, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn test_successful_failed_settlement_callback() {
    let order_id = Uuid::now_v7().to_string();
    let (handler, publisher) = setup_order_with_credit(&order_id, 1500).await;

    let service = CallbackService::new(handler.clone())
        .with_stream_publisher(publisher.clone())
        .with_consolidated_topic("orders/consolidated");

    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1500,
        status: SettlementStatus::Failed,
        transaction_ref: None,
        reason: Some("Insufficient corporate funds".to_string()),
    };

    let outcome = service.process_callback(message).await.unwrap();
    match outcome {
        CallbackOutcome::Processed {
            command_id: _,
            order_id: ref oid,
            ref event_type,
        } => {
            assert_eq!(oid, &order_id);
            assert_eq!(event_type, "CourtesyCreditSettlementFailed");
        }
        other => panic!("Expected CallbackOutcome::Processed, got {other:?}"),
    }

    // Verify aggregate state
    {
        let h = handler.lock().await;
        let agg = h.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.courtesy_credit_cents, 1500);
        assert_eq!(agg.settled_credit_cents, 0);
        assert_eq!(agg.settlement_status, CreditSettlementStatus::SettlementFailed);
        assert_eq!(agg.version, 3);
    }

    // Verify event publication
    {
        let pub_guard = publisher.lock().await;
        let published = pub_guard.published();
        assert_eq!(published.len(), 1);
        let event: DomainEvent = serde_json::from_slice(&published[0].payload).unwrap();
        match event {
            DomainEvent::CourtesyCreditSettlementFailed(e) => {
                assert_eq!(e.order_id, order_id);
                assert_eq!(e.amount_cents, 1500);
                assert_eq!(e.reason, "Insufficient corporate funds");
                assert_eq!(e.version, 3);
            }
            other => panic!("Expected CourtesyCreditSettlementFailed event, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn test_idempotent_duplicate_callback() {
    let order_id = Uuid::now_v7().to_string();
    let (handler, publisher) = setup_order_with_credit(&order_id, 1500).await;

    let service = CallbackService::new(handler.clone())
        .with_stream_publisher(publisher.clone());

    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-idempotent-1".to_string()),
        reason: None,
    };

    // First delivery -> Processed
    let outcome1 = service.process_callback(message.clone()).await.unwrap();
    assert!(outcome1.is_processed());

    // Duplicate delivery -> DuplicateIgnored
    let outcome2 = service.process_callback(message).await.unwrap();
    match outcome2 {
        CallbackOutcome::DuplicateIgnored { command_id, order_id: ref oid } => {
            assert_eq!(command_id, outcome1.command_id());
            assert_eq!(oid, &order_id);
        }
        other => panic!("Expected DuplicateIgnored, got {other:?}"),
    }

    // Verify only 1 event was published
    {
        let pub_guard = publisher.lock().await;
        assert_eq!(pub_guard.published().len(), 1);
    }
}

#[tokio::test]
async fn test_event_listener_hook() {
    let order_id = Uuid::now_v7().to_string();
    let (handler, publisher) = setup_order_with_credit(&order_id, 1000).await;

    let listener_notified = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let listener_flag = listener_notified.clone();
    let expected_order_id = order_id.clone();

    let service = CallbackService::new(handler)
        .with_stream_publisher(publisher)
        .with_event_listener(move |event, _cmd_id| {
            if event.order_id() == expected_order_id {
                listener_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });

    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1000,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-listener-hook".to_string()),
        reason: None,
    };

    let outcome = service.process_callback(message).await.unwrap();
    assert!(outcome.is_processed());
    assert!(listener_notified.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn test_excessive_settlement_amount_rejected() {
    let order_id = Uuid::now_v7().to_string();
    // Issue only $10.00 (1000 cents)
    let (handler, publisher) = setup_order_with_credit(&order_id, 1000).await;

    let service = CallbackService::new(handler.clone())
        .with_stream_publisher(publisher.clone());

    // Try to settle $25.00 (2500 cents)
    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 2500,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-excess".to_string()),
        reason: None,
    };

    let result = service.process_callback(message).await;
    match result {
        Err(CallbackError::Domain(DomainError::InvalidAmount(amount))) => {
            assert_eq!(amount, 2500);
        }
        other => panic!("Expected Domain(InvalidAmount), got {other:?}"),
    }

    // Verify SierraDB was untouched: version remains 2, settled credit is 0
    {
        let h = handler.lock().await;
        let agg = h.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.settled_credit_cents, 0);
        assert_eq!(agg.version, 2);
    }

    // Verify publisher is empty
    {
        let pub_guard = publisher.lock().await;
        assert_eq!(pub_guard.published().len(), 0);
    }
}

#[tokio::test]
async fn test_nonexistent_order_rejected() {
    let (handler, publisher) = setup_order_with_credit("ORD-SEED", 1000).await;
    let service = CallbackService::new(handler).with_stream_publisher(publisher);

    let message = SettlementCallbackMessage {
        order_id: "ORD-DOES-NOT-EXIST".to_string(),
        amount_cents: 1000,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-none".to_string()),
        reason: None,
    };

    let result = service.process_callback(message).await;
    match result {
        Err(CallbackError::Domain(DomainError::OrderNotFound(oid))) => {
            assert_eq!(oid, "ORD-DOES-NOT-EXIST");
        }
        other => panic!("Expected Domain(OrderNotFound), got {other:?}"),
    }
}

#[tokio::test]
async fn test_cancelled_order_rejected() {
    let order_id = Uuid::now_v7().to_string();
    let (handler, publisher) = setup_order_with_credit(&order_id, 1000).await;

    // Cancel the order
    {
        let mut h = handler.lock().await;
        let cancel_cmd = CancelOrderCommand::new(&order_id, "Customer request");
        h.handle_command(OrderCommand::CancelOrder(cancel_cmd))
            .await
            .expect("order cancellation failed");
    }

    let service = CallbackService::new(handler).with_stream_publisher(publisher);
    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1000,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-cancelled".to_string()),
        reason: None,
    };

    let result = service.process_callback(message).await;
    match result {
        Err(CallbackError::Domain(DomainError::OrderCancelled)) => {}
        other => panic!("Expected Domain(OrderCancelled), got {other:?}"),
    }
}

#[tokio::test]
async fn test_fraud_locked_order_rejected() {
    let order_id = Uuid::now_v7().to_string();
    let (handler, publisher) = setup_order_with_credit(&order_id, 1000).await;

    // Manually mark aggregate fraud locked
    {
        let mut h = handler.lock().await;
        if let Some(agg) = h.get_aggregate_mut(&order_id) {
            agg.status = OrderStatus::FraudLocked;
        }
    }

    let service = CallbackService::new(handler).with_stream_publisher(publisher);
    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1000,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-fraud".to_string()),
        reason: None,
    };

    let result = service.process_callback(message).await;
    match result {
        Err(CallbackError::Domain(DomainError::OrderFraudLocked)) => {}
        other => panic!("Expected Domain(OrderFraudLocked), got {other:?}"),
    }
}

#[tokio::test]
async fn test_invalid_payload_validations() {
    let (handler, _) = setup_order_with_credit("ORD-VAL", 1000).await;
    let service = CallbackService::new(handler);

    // 1. Blank order ID
    let res = service
        .process_callback(SettlementCallbackMessage {
            order_id: "   ".to_string(),
            amount_cents: 1000,
            status: SettlementStatus::Settled,
            transaction_ref: Some("tx-1".to_string()),
            reason: None,
        })
        .await;
    assert!(matches!(res, Err(CallbackError::InvalidPayload(_))));

    // 2. Zero amount cents
    let res = service
        .process_callback(SettlementCallbackMessage {
            order_id: "ORD-VAL".to_string(),
            amount_cents: 0,
            status: SettlementStatus::Settled,
            transaction_ref: Some("tx-1".to_string()),
            reason: None,
        })
        .await;
    assert!(matches!(res, Err(CallbackError::InvalidPayload(_))));

    // 3. Negative amount cents
    let res = service
        .process_callback(SettlementCallbackMessage {
            order_id: "ORD-VAL".to_string(),
            amount_cents: -500,
            status: SettlementStatus::Settled,
            transaction_ref: Some("tx-1".to_string()),
            reason: None,
        })
        .await;
    assert!(matches!(res, Err(CallbackError::InvalidPayload(_))));

    // 4. Settled status without transaction ref
    let res = service
        .process_callback(SettlementCallbackMessage {
            order_id: "ORD-VAL".to_string(),
            amount_cents: 1000,
            status: SettlementStatus::Settled,
            transaction_ref: None,
            reason: None,
        })
        .await;
    assert!(matches!(res, Err(CallbackError::InvalidPayload(_))));

    // 5. Settled status with blank transaction ref
    let res = service
        .process_callback(SettlementCallbackMessage {
            order_id: "ORD-VAL".to_string(),
            amount_cents: 1000,
            status: SettlementStatus::Settled,
            transaction_ref: Some("  ".to_string()),
            reason: None,
        })
        .await;
    assert!(matches!(res, Err(CallbackError::InvalidPayload(_))));

    // 6. Failed status without reason
    let res = service
        .process_callback(SettlementCallbackMessage {
            order_id: "ORD-VAL".to_string(),
            amount_cents: 1000,
            status: SettlementStatus::Failed,
            transaction_ref: None,
            reason: None,
        })
        .await;
    assert!(matches!(res, Err(CallbackError::InvalidPayload(_))));

    // 7. Failed status with blank reason
    let res = service
        .process_callback(SettlementCallbackMessage {
            order_id: "ORD-VAL".to_string(),
            amount_cents: 1000,
            status: SettlementStatus::Failed,
            transaction_ref: None,
            reason: Some("  \t ".to_string()),
        })
        .await;
    assert!(matches!(res, Err(CallbackError::InvalidPayload(_))));
}

#[tokio::test]
async fn test_process_message_bytes() {
    let order_id = Uuid::now_v7().to_string();
    let (handler, publisher) = setup_order_with_credit(&order_id, 1200).await;
    let service = CallbackService::new(handler).with_stream_publisher(publisher);

    let valid_payload = json!({
        "order_id": order_id,
        "amount_cents": 1200,
        "status": "settled",
        "transaction_ref": "tx-bytes-1"
    });

    let outcome = service
        .process_message(valid_payload.to_string().as_bytes())
        .await
        .unwrap();
    assert!(outcome.is_processed());

    // Invalid json bytes
    let err = service
        .process_message(b"not valid json")
        .await
        .unwrap_err();
    assert!(matches!(err, CallbackError::Deserialization(_)));
}

/// Mock publisher that fails intentionally to verify rollback semantics.
struct FailingPublisher;

#[async_trait::async_trait]
impl CallbackPublisher for FailingPublisher {
    async fn publish_domain_event(
        &self,
        _topic: &str,
        _partition_key: &str,
        _event: &DomainEvent,
    ) -> Result<(), String> {
        Err("Broker socket connection refused".to_string())
    }
}

#[tokio::test]
async fn test_publisher_failure_rollback() {
    let order_id = Uuid::now_v7().to_string();
    let (handler, _) = setup_order_with_credit(&order_id, 1000).await;

    let service = CallbackService::new(handler.clone())
        .with_publisher(Arc::new(FailingPublisher));

    let message = SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1000,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-fail-rollback".to_string()),
        reason: None,
    };

    let result = service.process_callback(message).await;
    match result {
        Err(CallbackError::Publisher(msg)) => {
            assert!(msg.contains("Broker socket connection refused"));
        }
        other => panic!("Expected Publisher error, got {other:?}"),
    }

    // Verify aggregate in CommandHandler was rolled back to version 2 with settled_credit_cents = 0
    {
        let h = handler.lock().await;
        let agg = h.get_aggregate(&order_id).unwrap();
        assert_eq!(agg.settled_credit_cents, 0);
        assert_eq!(agg.version, 2);
    }
}

#[tokio::test]
async fn test_callback_consumer_rehydrates_validates_and_appends_to_event_store() {
    use ferrous_cqrses::{
        callback::{CallbackConsumer, CallbackConsumerOutcome, CALLBACK_TOPIC},
        consumer::{InMemoryStreamConsumer, ReceivedMessage},
        sierra::{stream_id_for_order, EventStore},
    };

    assert_eq!(CALLBACK_TOPIC, "callback");

    let order_id = Uuid::now_v7().to_string();
    let store = Arc::new(InMemoryEventStore::new());

    // 1. Seed initial OrderCreated + CreditIssued events into EventStore using a separate CommandHandler
    {
        let mut seed_handler = CommandHandler::new_with_store(store.clone());
        seed_handler
            .handle_command(OrderCommand::CreateOrder(CreateOrderCommand::new(
                &order_id,
                "cust-consumer-1",
                5000,
                chrono::Utc::now(),
            )))
            .await
            .unwrap();
        seed_handler
            .handle_command(OrderCommand::IssueCourtesyCredit(
                IssueCourtesyCreditCommand::new(&order_id, 1200, "Late delivery"),
            ))
            .await
            .unwrap();
    }

    // 2. Construct a cold CommandHandler backed by the same EventStore (forces ESCAN rehydration)
    let cold_handler = Arc::new(Mutex::new(CommandHandler::new_with_store(store.clone())));
    let publisher = Arc::new(Mutex::new(InMemoryStreamPublisher::new()));
    let service = CallbackService::new(cold_handler.clone())
        .with_stream_publisher(publisher.clone())
        .with_consolidated_topic("orders/consolidated");

    let callback_payload = serde_json::to_vec(&SettlementCallbackMessage {
        order_id: order_id.clone(),
        amount_cents: 1200,
        status: SettlementStatus::Settled,
        transaction_ref: Some("tx-consumer-999".to_string()),
        reason: None,
    })
    .unwrap();

    let stream = InMemoryStreamConsumer::from_messages(vec![ReceivedMessage::new(
        0,
        0,
        callback_payload,
    )]);

    let mut consumer = CallbackConsumer::new(service, stream);

    // 3. Consume from orders/callback -> ESCAN rehydrate -> invariant check -> EAPPEND -> publish orders/consolidated -> ack
    let outcome = consumer.process_next().await.unwrap().unwrap();
    match outcome {
        CallbackConsumerOutcome::Processed {
            offset,
            partition_id,
            order_id: ref oid,
            ref event_type,
            ..
        } => {
            assert_eq!(offset, 0);
            assert_eq!(partition_id, 0);
            assert_eq!(oid, &order_id);
            assert_eq!(event_type, "CourtesyCreditSettled");
        }
        other => panic!("Expected CallbackConsumerOutcome::Processed, got {other:?}"),
    }

    // 4. Verify EventStore received EAPPEND (3 events total: Created, CreditIssued, CourtesyCreditSettled)
    let stream_events = store.scan(&stream_id_for_order(&order_id)).await.unwrap();
    assert_eq!(stream_events.len(), 3);
    assert!(matches!(
        stream_events[2],
        DomainEvent::CourtesyCreditSettled(_)
    ));

    // 5. Verify offset was acknowledged and committed event was published to orders/consolidated
    assert_eq!(consumer.stream().acknowledged(), &[(0, 0)]);
    let pub_guard = publisher.lock().await;
    assert_eq!(pub_guard.published().len(), 1);
    assert_eq!(pub_guard.published()[0].topic, "orders/consolidated");
}
