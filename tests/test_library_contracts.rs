//! Comprehensive unit and integration test suite verifying ferrous-cqrses library contracts.
//! Covers JSON and Bincode serialization roundtrips, UUIDv7 monotonicity, business invariants ($20 credit cap),
//! and event-sourcing aggregate state folding.

use chrono::Utc;
use ferrous_cqrses::prelude::*;

#[test]
fn test_external_crate_import_prelude_contracts() {
    // Verifies AD-15: Public visibility and compile-time contract availability
    let cmd = IssueCourtesyCreditCommand::new("ORD-1001", 1500, "Delayed carrier delivery");
    assert_eq!(cmd.order_id, "ORD-1001");
    assert_eq!(cmd.amount_cents, 1500);

    let agg = OrderAggregate::new("ORD-1001", "CUST-55", 1200, Utc::now());
    assert_eq!(agg.courtesy_credit_cents, 0);
    assert_eq!(agg.version, 0);
    assert_eq!(agg.status, OrderStatus::Created);
}

#[test]
fn test_json_serialization_roundtrip_commands() {
    let credit_cmd = IssueCourtesyCreditCommand::new("ORD-9021", 1500, "Goodwill concession");
    let cmd_enum = OrderCommand::IssueCourtesyCredit(credit_cmd.clone());

    // Serialize to JSON envelope
    let json_str = to_json(&cmd_enum).expect("OrderCommand must serialize to JSON");
    assert!(json_str.contains("issue_courtesy_credit"));
    assert!(json_str.contains("ORD-9021"));
    assert!(json_str.contains("1500"));

    // Deserialize and assert strict equality
    let deserialized: OrderCommand =
        from_json(&json_str).expect("Valid JSON must deserialize cleanly");
    assert_eq!(cmd_enum, deserialized);

    // Cancel order command roundtrip
    let cancel_cmd = OrderCommand::CancelOrder(CancelOrderCommand::new(
        "ORD-9021",
        "Customer requested cancellation",
    ));
    let cancel_json = to_json(&cancel_cmd).expect("CancelOrderCommand must serialize to JSON");
    let cancel_deser: OrderCommand = from_json(&cancel_json).expect("Cancel JSON must deserialize");
    assert_eq!(cancel_cmd, cancel_deser);

    // Create order command roundtrip
    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-9021",
        "CUST-99",
        2500,
        Utc::now(),
    ));
    let create_json = to_json(&create_cmd).expect("CreateOrderCommand must serialize to JSON");
    let create_deser: OrderCommand = from_json(&create_json).expect("Create JSON must deserialize");
    assert_eq!(create_cmd, create_deser);
}

#[test]
fn test_bincode_serialization_roundtrip_commands() {
    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        "ORD-9021",
        1500,
        "Goodwill concession",
    ));
    let bin_credit =
        to_bincode(&credit_cmd).expect("IssueCourtesyCreditCommand must serialize to Bincode");
    let deser_credit: OrderCommand =
        from_bincode(&bin_credit).expect("Bincode credit must deserialize");
    assert_eq!(credit_cmd, deser_credit);

    let cancel_cmd = OrderCommand::CancelOrder(CancelOrderCommand::new(
        "ORD-9021",
        "Customer cancellation request",
    ));
    let bin_cancel = to_bincode(&cancel_cmd).expect("CancelOrderCommand must serialize to Bincode");
    let deser_cancel: OrderCommand =
        from_bincode(&bin_cancel).expect("Bincode cancel must deserialize");
    assert_eq!(cancel_cmd, deser_cancel);

    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-9021",
        "CUST-99",
        2500,
        Utc::now(),
    ));
    let bin_create = to_bincode(&create_cmd).expect("CreateOrderCommand must serialize to Bincode");
    let deser_create: OrderCommand =
        from_bincode(&bin_create).expect("Bincode create must deserialize");
    assert_eq!(create_cmd, deser_create);
}

#[test]
fn test_json_serialization_roundtrip_events() {
    let created = DomainEvent::OrderCreated(OrderCreatedEvent::new(
        "ORD-7744",
        "CUST-10",
        1500,
        Utc::now(),
    ));
    let json_created = to_json(&created).expect("OrderCreatedEvent must serialize to JSON");
    let deser_created: DomainEvent =
        from_json(&json_created).expect("JSON created must deserialize");
    assert_eq!(created, deser_created);

    let credit = DomainEvent::CreditIssued(CreditIssuedEvent::new(
        "ORD-7744",
        1500,
        "Goodwill credit",
        2,
    ));
    let json_credit = to_json(&credit).expect("CreditIssuedEvent must serialize to JSON");
    let deser_credit: DomainEvent = from_json(&json_credit).expect("JSON credit must deserialize");
    assert_eq!(credit, deser_credit);

    let status = DomainEvent::OrderStatusUpdated(OrderStatusUpdatedEvent::new(
        "ORD-7744",
        OrderStatus::Created,
        OrderStatus::Processing,
        Some("Payment confirmed".to_string()),
        3,
    ));
    let json_status = to_json(&status).expect("OrderStatusUpdatedEvent must serialize to JSON");
    let deser_status: DomainEvent = from_json(&json_status).expect("JSON status must deserialize");
    assert_eq!(status, deser_status);

    let cancel =
        DomainEvent::OrderCancelled(OrderCancelledEvent::new("ORD-7744", "Customer request", 4));
    let json_cancel = to_json(&cancel).expect("OrderCancelledEvent must serialize to JSON");
    let deser_cancel: DomainEvent = from_json(&json_cancel).expect("JSON cancel must deserialize");
    assert_eq!(cancel, deser_cancel);
}

#[test]
fn test_bincode_serialization_roundtrip_events() {
    let credit_event = CreditIssuedEvent::new("ORD-7744", 1500, "SLA compensation", 2);
    let domain_event = DomainEvent::CreditIssued(credit_event.clone());

    // Serialize to Bincode
    let bin_bytes = to_bincode(&domain_event).expect("DomainEvent must serialize to Bincode");
    assert!(!bin_bytes.is_empty());

    // Deserialize and assert strict equality
    let deserialized: DomainEvent =
        from_bincode(&bin_bytes).expect("Bincode bytes must deserialize cleanly");
    assert_eq!(domain_event, deserialized);

    // OrderStatusUpdatedEvent roundtrip
    let status_event = DomainEvent::OrderStatusUpdated(OrderStatusUpdatedEvent::new(
        "ORD-7744",
        OrderStatus::Created,
        OrderStatus::Processing,
        Some("Order payment settled".to_string()),
        1,
    ));
    let status_bin = to_bincode(&status_event).expect("Status event must serialize to Bincode");
    let status_deser: DomainEvent =
        from_bincode(&status_bin).expect("Status event must deserialize");
    assert_eq!(status_event, status_deser);

    // OrderCancelledEvent roundtrip
    let cancel_event = DomainEvent::OrderCancelled(OrderCancelledEvent::new(
        "ORD-7744",
        "Fraud alert triggered",
        3,
    ));
    let cancel_bin = to_bincode(&cancel_event).expect("Cancel event must serialize to Bincode");
    let cancel_deser: DomainEvent =
        from_bincode(&cancel_bin).expect("Cancel event must deserialize");
    assert_eq!(cancel_event, cancel_deser);
}

#[test]
fn test_serialization_error_handling() {
    // Malformed JSON should return DomainError::DeserializationError
    let bad_json = r#"{"invalid_json": true, missing_braces"#;
    let res: Result<OrderCommand, _> = from_json(bad_json);
    assert!(matches!(res, Err(DomainError::DeserializationError(_))));

    // Corrupted binary payload should return DomainError::DeserializationError
    let corrupted_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let bin_res: Result<DomainEvent, _> = from_bincode(&corrupted_bytes);
    assert!(matches!(bin_res, Err(DomainError::DeserializationError(_))));
}

#[test]
fn test_monotonic_uuidv7_generation_and_ordering() {
    let id_1 = generate_uuidv7();
    let id_2 = generate_uuidv7();
    let id_3 = generate_uuidv7();

    // Verify strict sequential ordering: id_1 < id_2 < id_3
    assert!(id_1 < id_2, "id_1 must be strictly smaller than id_2");
    assert!(id_2 < id_3, "id_2 must be strictly smaller than id_3");

    // Verify valid UUIDv7 format
    assert!(is_valid_uuidv7(&id_1));
    assert!(is_valid_uuidv7(&id_2));
    assert!(is_valid_uuidv7(&id_3));

    // Rapid generation stress test: 5,000 consecutive UUIDs
    let mut prev = generate_uuidv7();
    for _ in 0..5000 {
        let curr = generate_uuidv7();
        assert!(
            prev < curr,
            "Monotonic ordering invariant violated in sequential sequence"
        );
        prev = curr;
    }
}

#[test]
fn test_courtesy_credit_within_limit() {
    let mut aggregate = OrderAggregate::new("ORD-4001", "CUST-10", 1500, Utc::now());
    assert_eq!(aggregate.courtesy_credit_cents, 0);

    // Credit within limit ($15.00 = 1500 cents <= 2000 cents cap)
    let event = CreditIssuedEvent::new("ORD-4001", 1500, "First carrier delay concession", 1);
    let result = aggregate.apply_credit_issued(&event);

    assert!(result.is_ok(), "Credit of 1500 cents must be accepted");
    assert_eq!(aggregate.courtesy_credit_cents, 1500);
    assert_eq!(aggregate.version, 1);

    // Incremental credit up to exact $20.00 (2,000 cents) limit
    let second_event = CreditIssuedEvent::new("ORD-4001", 500, "Second concession", 2);
    let second_result = aggregate.apply_credit_issued(&second_event);

    assert!(
        second_result.is_ok(),
        "Additional credit bringing total to 2000 cents must be accepted"
    );
    assert_eq!(aggregate.courtesy_credit_cents, 2000);
    assert_eq!(aggregate.version, 2);
}

#[test]
fn test_courtesy_credit_exceeds_20_dollar_cap() {
    let mut aggregate = OrderAggregate::new("ORD-4002", "CUST-11", 1500, Utc::now());

    // Single credit exceeding $20.00 cap ($25.00 = 2500 cents)
    let excessive_event = CreditIssuedEvent::new("ORD-4002", 2500, "Excessive goodwill request", 1);
    let result = aggregate.apply_credit_issued(&excessive_event);

    assert_eq!(
        result,
        Err(DomainError::CourtesyCreditExceeded(2500, 2000)),
        "Credit exceeding 2000 cents must be rejected immediately with CourtesyCreditExceeded(2500, 2000)"
    );
    // Aggregate state must remain unchanged
    assert_eq!(aggregate.courtesy_credit_cents, 0);
    assert_eq!(aggregate.version, 0);

    // Cumulative credit exceeding cap: initial 1500 cents + attempted 600 cents = 2100 cents
    let valid_event = CreditIssuedEvent::new("ORD-4002", 1500, "Initial concession", 1);
    aggregate.apply_credit_issued(&valid_event).unwrap();
    assert_eq!(aggregate.courtesy_credit_cents, 1500);

    let second_excessive =
        CreditIssuedEvent::new("ORD-4002", 600, "Exceeding cumulative concession", 2);
    let second_res = aggregate.apply_credit_issued(&second_excessive);
    assert_eq!(
        second_res,
        Err(DomainError::CourtesyCreditExceeded(2100, 2000)),
        "Cumulative credit exceeding 2000 cents must be rejected"
    );
    assert_eq!(aggregate.courtesy_credit_cents, 1500);
    assert_eq!(aggregate.version, 1);
}

#[test]
fn test_standalone_courtesy_credit_invariant_validation() {
    assert!(validate_courtesy_credit_amount(1500).is_ok());
    assert!(validate_courtesy_credit_amount(2000).is_ok());

    assert_eq!(
        validate_courtesy_credit_amount(2500),
        Err(DomainError::CourtesyCreditExceeded(2500, 2000))
    );

    // Negative or zero credit amounts are strictly invalid
    assert_eq!(
        validate_courtesy_credit_amount(0),
        Err(DomainError::InvalidAmount(0))
    );
    assert_eq!(
        validate_courtesy_credit_amount(-500),
        Err(DomainError::InvalidAmount(-500))
    );
}

#[test]
fn test_courtesy_credit_rejected_on_cancelled_or_fraud_locked() {
    let mut cancelled_agg = OrderAggregate::new("ORD-5001", "CUST-20", 1000, Utc::now());
    cancelled_agg.status = OrderStatus::Cancelled;

    let credit_event = CreditIssuedEvent::new("ORD-5001", 1000, "Concession", 1);
    assert_eq!(
        cancelled_agg.apply_credit_issued(&credit_event),
        Err(DomainError::OrderCancelled),
        "Cancelled orders must reject courtesy credits"
    );

    let mut fraud_agg = OrderAggregate::new("ORD-5002", "CUST-21", 1000, Utc::now());
    fraud_agg.status = OrderStatus::FraudLocked;
    let fraud_credit_event = CreditIssuedEvent::new("ORD-5002", 1000, "Concession", 1);
    assert_eq!(
        fraud_agg.apply_credit_issued(&fraud_credit_event),
        Err(DomainError::OrderFraudLocked),
        "FraudLocked orders must reject courtesy credits"
    );
}

#[test]
fn test_event_folding_state_update_and_rehydration() {
    let created = DomainEvent::OrderCreated(OrderCreatedEvent::new(
        "ORD-8888",
        "CUST-77",
        1800,
        Utc::now(),
    ));

    let status_1 = DomainEvent::OrderStatusUpdated(OrderStatusUpdatedEvent::new(
        "ORD-8888",
        OrderStatus::Created,
        OrderStatus::Processing,
        None,
        2,
    ));

    let credit_1 = DomainEvent::CreditIssued(CreditIssuedEvent::new(
        "ORD-8888",
        1200,
        "Carrier delay concession",
        3,
    ));

    let status_2 = DomainEvent::OrderStatusUpdated(OrderStatusUpdatedEvent::new(
        "ORD-8888",
        OrderStatus::Processing,
        OrderStatus::InTransit,
        None,
        4,
    ));

    let events = vec![created, status_1, credit_1, status_2];

    // Rehydrate aggregate by folding event stream sequentially
    let aggregate = OrderAggregate::from_events(&events).expect("Aggregate folding must succeed");

    assert_eq!(aggregate.order_id, "ORD-8888");
    assert_eq!(aggregate.customer_id, "CUST-77");
    assert_eq!(aggregate.status, OrderStatus::InTransit);
    assert_eq!(aggregate.total_freight_cents, 1800);
    assert_eq!(aggregate.courtesy_credit_cents, 1200);
    assert_eq!(aggregate.version, 4);
}

#[test]
fn test_order_cancellation_and_terminal_lifecycle_invariants() {
    let mut aggregate = OrderAggregate::new("ORD-9901", "CUST-30", 2000, Utc::now());
    let cancel_event =
        DomainEvent::OrderCancelled(OrderCancelledEvent::new("ORD-9901", "Customer request", 1));

    // Normal cancellation transitions status to Cancelled and advances version
    let res = aggregate.apply(&cancel_event);
    assert!(res.is_ok());
    assert_eq!(aggregate.status, OrderStatus::Cancelled);
    assert_eq!(aggregate.version, 1);

    // Re-cancelling already cancelled order fails with OrderCancelled
    let second_cancel = DomainEvent::OrderCancelled(OrderCancelledEvent::new(
        "ORD-9901",
        "Duplicate cancellation",
        2,
    ));
    assert_eq!(
        aggregate.apply(&second_cancel),
        Err(DomainError::OrderCancelled)
    );

    // FraudLocked order cannot be cancelled
    let mut fraud_aggregate = OrderAggregate::new("ORD-9902", "CUST-31", 2000, Utc::now());
    fraud_aggregate.status = OrderStatus::FraudLocked;
    let cancel_fraud = DomainEvent::OrderCancelled(OrderCancelledEvent::new(
        "ORD-9902",
        "Customer cancellation attempt",
        1,
    ));
    assert_eq!(
        fraud_aggregate.apply(&cancel_fraud),
        Err(DomainError::OrderFraudLocked)
    );

    // Delivered order cannot be cancelled
    let mut delivered_aggregate = OrderAggregate::new("ORD-9903", "CUST-32", 2000, Utc::now());
    delivered_aggregate.status = OrderStatus::Delivered;
    let cancel_delivered = DomainEvent::OrderCancelled(OrderCancelledEvent::new(
        "ORD-9903",
        "Attempted cancel after delivery",
        1,
    ));
    assert_eq!(
        delivered_aggregate.apply(&cancel_delivered),
        Err(DomainError::OrderDelivered)
    );
}

#[test]
fn test_order_status_update_rejections_and_transitions() {
    // Attempting status update on Cancelled order returns InvalidOrderStatus
    let mut cancelled = OrderAggregate::new("ORD-9904", "CUST-33", 1000, Utc::now());
    cancelled.status = OrderStatus::Cancelled;
    let update_cancelled = DomainEvent::OrderStatusUpdated(OrderStatusUpdatedEvent::new(
        "ORD-9904",
        OrderStatus::Cancelled,
        OrderStatus::InTransit,
        None,
        1,
    ));
    assert_eq!(
        cancelled.apply(&update_cancelled),
        Err(DomainError::InvalidOrderStatus(OrderStatus::Cancelled))
    );

    // Attempting status update on FraudLocked order returns InvalidOrderStatus
    let mut fraud = OrderAggregate::new("ORD-9905", "CUST-34", 1000, Utc::now());
    fraud.status = OrderStatus::FraudLocked;
    let update_fraud = DomainEvent::OrderStatusUpdated(OrderStatusUpdatedEvent::new(
        "ORD-9905",
        OrderStatus::FraudLocked,
        OrderStatus::Delivered,
        None,
        1,
    ));
    assert_eq!(
        fraud.apply(&update_fraud),
        Err(DomainError::InvalidOrderStatus(OrderStatus::FraudLocked))
    );

    // Status update with previous_status mismatch fails
    let mut normal = OrderAggregate::new("ORD-9906", "CUST-35", 1000, Utc::now());
    let mismatch_update = DomainEvent::OrderStatusUpdated(OrderStatusUpdatedEvent::new(
        "ORD-9906",
        OrderStatus::Processing, // Actual status is Created
        OrderStatus::InTransit,
        None,
        1,
    ));
    assert_eq!(
        normal.apply(&mismatch_update),
        Err(DomainError::InvalidOrderStatus(OrderStatus::Created))
    );

    // Valid transition to Delivered sets actual_delivery timestamp
    let deliver_update = DomainEvent::OrderStatusUpdated(OrderStatusUpdatedEvent::new(
        "ORD-9906",
        OrderStatus::Created,
        OrderStatus::Delivered,
        Some("Delivered on time".to_string()),
        1,
    ));
    assert!(normal.apply(&deliver_update).is_ok());
    assert_eq!(normal.status, OrderStatus::Delivered);
    assert!(normal.actual_delivery.is_some());
    assert_eq!(normal.version, 1);
}

#[test]
fn test_aggregate_sequence_and_order_id_invariants() {
    let mut agg = OrderAggregate::new("ORD-9907", "CUST-36", 1200, Utc::now());

    // Mismatched aggregate ID must be rejected
    let wrong_id_event =
        DomainEvent::CreditIssued(CreditIssuedEvent::new("ORD-WRONG", 500, "Credit", 1));
    assert_eq!(
        agg.apply(&wrong_id_event),
        Err(DomainError::OrderIdMismatch {
            expected: "ORD-9907".to_string(),
            actual: "ORD-WRONG".to_string(),
        })
    );

    // Non-sequential version jump must be rejected
    let version_jump_event = DomainEvent::CreditIssued(CreditIssuedEvent::new(
        "ORD-9907", 500, "Credit", 5, // expected 1
    ));
    assert_eq!(
        agg.apply(&version_jump_event),
        Err(DomainError::VersionMismatch {
            expected: 1,
            actual: 5,
        })
    );

    // Duplicate OrderCreated must be rejected on an existing aggregate
    let duplicate_create = DomainEvent::OrderCreated(OrderCreatedEvent::new(
        "ORD-9907",
        "CUST-36",
        1200,
        Utc::now(),
    ));
    // First apply valid credit so version > 0
    let valid_credit =
        DomainEvent::CreditIssued(CreditIssuedEvent::new("ORD-9907", 500, "Credit", 1));
    agg.apply(&valid_credit).unwrap();
    assert_eq!(agg.version, 1);

    // Now duplicate create attempt
    assert_eq!(
        agg.apply(&duplicate_create),
        Err(DomainError::VersionMismatch {
            expected: 2,
            actual: 1,
        })
    );
}

#[test]
fn test_concurrent_monotonic_uuidv7_generation() {
    use std::thread;

    let thread_count = 8;
    let iterations_per_thread = 1000;
    let mut handles = Vec::new();

    for _ in 0..thread_count {
        let handle = thread::spawn(move || {
            let mut ids = Vec::with_capacity(iterations_per_thread);
            for _ in 0..iterations_per_thread {
                ids.push(generate_uuidv7());
            }
            ids
        });
        handles.push(handle);
    }

    let mut all_ids = Vec::with_capacity(thread_count * iterations_per_thread);
    for handle in handles {
        let thread_ids = handle.join().expect("Thread must not panic");
        // Verify monotonicity within the single thread's sequence
        for window in thread_ids.windows(2) {
            assert!(
                window[0] < window[1],
                "Per-thread sequence must be strictly monotonic"
            );
        }
        all_ids.extend(thread_ids);
    }

    // Verify global uniqueness across all generated IDs
    let total = all_ids.len();
    all_ids.sort();
    all_ids.dedup();
    assert_eq!(
        all_ids.len(),
        total,
        "All concurrently generated UUIDv7s must be globally distinct"
    );
}
