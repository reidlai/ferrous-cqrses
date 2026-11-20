//! Integration and contract test suite for Story 7.2:
//! Sequential Command Handler Daemon (`src/bin/command_handler.rs`) & Domain Invariant Validation.
//!
//! Verifies:
//! - Sequential command handling per order
//! - Valid courtesy credit issuance (<= 2,000 cents) advances aggregate state and version
//! - Excessive courtesy credit (> 2,000 cents) is rejected with `CourtesyCreditExceeded` and invariant violation logged without state mutation
//! - Commands on `Cancelled`, `FraudLocked`, and `Delivered` orders are rejected
//! - Idempotency and version sequencing
//! - Stream processing, offset acknowledgement, and error handling via `CommandConsumer`
//! - Graceful shutdown and configuration parsing

use chrono::Utc;
use clap::Parser;
use ferrous_cqrses::prelude::*;
use std::time::Duration;
use tokio::sync::watch;

#[tokio::test]
async fn test_sequential_command_handling_per_order() {
    let mut handler = CommandHandler::new();
    let order_id = "ORD-SEQ-001";

    // 1. CreateOrder
    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-100",
        1500,
        Utc::now(),
    ));
    let event1 = handler
        .handle_command(create_cmd)
        .await
        .expect("CreateOrder must succeed");

    assert_eq!(event1.order_id(), order_id);
    assert_eq!(event1.version(), 1);

    let agg = handler
        .get_aggregate(order_id)
        .expect("Aggregate must exist");
    assert_eq!(agg.status, OrderStatus::Created);
    assert_eq!(agg.version, 1);
    assert_eq!(agg.courtesy_credit_cents, 0);

    // 2. First Courtesy Credit (500 cents)
    let credit_cmd_1 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        500,
        "Carrier delay 30 mins",
    ));
    let event2 = handler
        .handle_command(credit_cmd_1)
        .await
        .expect("First credit must succeed");

    assert_eq!(event2.version(), 2);
    let agg = handler
        .get_aggregate(order_id)
        .expect("Aggregate must exist");
    assert_eq!(agg.version, 2);
    assert_eq!(agg.courtesy_credit_cents, 500);

    // 3. Second Courtesy Credit (800 cents) -> cumulative 1,300 cents
    let credit_cmd_2 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        800,
        "Damaged outer carton",
    ));
    let event3 = handler
        .handle_command(credit_cmd_2)
        .await
        .expect("Second credit must succeed");

    assert_eq!(event3.version(), 3);
    let agg = handler
        .get_aggregate(order_id)
        .expect("Aggregate must exist");
    assert_eq!(agg.version, 3);
    assert_eq!(agg.courtesy_credit_cents, 1300);

    // 4. Cancel Order
    let cancel_cmd = OrderCommand::CancelOrder(CancelOrderCommand::new(
        order_id,
        "Customer initiated cancellation",
    ));
    let event4 = handler
        .handle_command(cancel_cmd)
        .await
        .expect("CancelOrder must succeed");

    assert_eq!(event4.version(), 4);
    let agg = handler
        .get_aggregate(order_id)
        .expect("Aggregate must exist");
    assert_eq!(agg.version, 4);
    assert_eq!(agg.status, OrderStatus::Cancelled);

    // Verify event store captures all 4 events sequentially
    let events = handler.get_events(order_id).expect("Events must exist");
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].version(), 1);
    assert_eq!(events[1].version(), 2);
    assert_eq!(events[2].version(), 3);
    assert_eq!(events[3].version(), 4);
}

#[tokio::test]
async fn test_valid_courtesy_credit_issuance_at_limit() {
    let mut handler = CommandHandler::new();
    let order_id = "ORD-CAP-001";

    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-200",
        2500,
        Utc::now(),
    ));
    handler.handle_command(create_cmd).await.unwrap();

    // Exactly 2,000 cents ($20.00) credit issued at once
    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        2000,
        "Maximum allowable credit concession",
    ));
    let event = handler.handle_command(credit_cmd).await.unwrap();

    assert_eq!(event.version(), 2);
    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.courtesy_credit_cents, 2000);
    assert_eq!(agg.version, 2);
}

#[tokio::test]
async fn test_excessive_courtesy_credit_single_rejection() {
    let mut handler = CommandHandler::new();
    let order_id = "ORD-EXCESS-001";

    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-300",
        3000,
        Utc::now(),
    ));
    handler.handle_command(create_cmd).await.unwrap();

    // 2,500 cents > 2,000 cents limit -> invariant violation AD-17
    let excessive_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        2500,
        "Attempting credit above $20.00 cap",
    ));
    let res = handler.handle_command(excessive_cmd).await;

    assert_eq!(
        res,
        Err(DomainError::CourtesyCreditExceeded(2500, 2000)),
        "Single credit > 2000 must be rejected with CourtesyCreditExceeded(2500, 2000)"
    );

    // State and version must remain completely unmutated
    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.courtesy_credit_cents, 0);
    assert_eq!(agg.version, 1);
}

#[tokio::test]
async fn test_excessive_courtesy_credit_cumulative_rejection() {
    let mut handler = CommandHandler::new();
    let order_id = "ORD-EXCESS-002";

    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-301",
        3000,
        Utc::now(),
    ));
    handler.handle_command(create_cmd).await.unwrap();

    // Initial valid credit of 1,500 cents
    let credit_1 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        1500,
        "First concession",
    ));
    handler.handle_command(credit_1).await.unwrap();

    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.courtesy_credit_cents, 1500);
    assert_eq!(agg.version, 2);

    // Second credit of 600 cents (1500 + 600 = 2100 > 2000)
    let credit_2 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        600,
        "Second concession exceeding cap",
    ));
    let res = handler.handle_command(credit_2).await;

    assert_eq!(
        res,
        Err(DomainError::CourtesyCreditExceeded(2100, 2000)),
        "Cumulative credit exceeding 2000 must be rejected"
    );

    // Aggregate state must remain at 1,500 cents and version 2
    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.courtesy_credit_cents, 1500);
    assert_eq!(agg.version, 2);

    // A subsequent valid credit of 500 cents (1500 + 500 = 2000) must succeed
    let credit_3 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        500,
        "Final concession reaching exact cap",
    ));
    let res3 = handler.handle_command(credit_3).await;
    assert!(res3.is_ok());

    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.courtesy_credit_cents, 2000);
    assert_eq!(agg.version, 3);
}

#[tokio::test]
async fn test_negative_or_zero_credit_rejected() {
    let mut handler = CommandHandler::new();
    let order_id = "ORD-ZERO-001";

    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-400",
        1000,
        Utc::now(),
    ));
    handler.handle_command(create_cmd).await.unwrap();

    let zero_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        0,
        "Zero credit concession",
    ));
    assert_eq!(
        handler.handle_command(zero_cmd).await,
        Err(DomainError::InvalidAmount(0))
    );

    let negative_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        -500,
        "Negative credit concession",
    ));
    assert_eq!(
        handler.handle_command(negative_cmd).await,
        Err(DomainError::InvalidAmount(-500))
    );
}

#[tokio::test]
async fn test_commands_rejected_on_cancelled_orders() {
    let mut handler = CommandHandler::new();
    let order_id = "ORD-CANCEL-001";

    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-500",
        2000,
        Utc::now(),
    ));
    handler.handle_command(create_cmd).await.unwrap();

    let cancel_cmd =
        OrderCommand::CancelOrder(CancelOrderCommand::new(order_id, "Customer cancellation"));
    handler.handle_command(cancel_cmd).await.unwrap();

    // Courtesy credit on Cancelled order must be rejected
    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        500,
        "Post-cancel credit",
    ));
    assert_eq!(
        handler.handle_command(credit_cmd).await,
        Err(DomainError::OrderCancelled)
    );

    // Re-cancelling a Cancelled order must be rejected
    let re_cancel_cmd =
        OrderCommand::CancelOrder(CancelOrderCommand::new(order_id, "Duplicate cancel"));
    assert_eq!(
        handler.handle_command(re_cancel_cmd).await,
        Err(DomainError::OrderCancelled)
    );
}

#[tokio::test]
async fn test_commands_rejected_on_fraud_locked_orders() {
    let mut handler = CommandHandler::new();
    let order_id = "ORD-FRAUD-001";

    let mut fraud_aggregate = OrderAggregate::new(order_id, "CUST-600", 2500, Utc::now());
    fraud_aggregate.status = OrderStatus::FraudLocked;
    fraud_aggregate.version = 1;
    handler.insert_aggregate(fraud_aggregate);

    // Courtesy credit on FraudLocked order must be rejected
    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        1000,
        "Credit concession",
    ));
    assert_eq!(
        handler.handle_command(credit_cmd).await,
        Err(DomainError::OrderFraudLocked)
    );

    // Cancellation on FraudLocked order must be rejected
    let cancel_cmd =
        OrderCommand::CancelOrder(CancelOrderCommand::new(order_id, "Cancellation request"));
    assert_eq!(
        handler.handle_command(cancel_cmd).await,
        Err(DomainError::OrderFraudLocked)
    );
}

#[tokio::test]
async fn test_cancel_rejected_on_delivered_order() {
    let mut handler = CommandHandler::new();
    let order_id = "ORD-DELIV-001";

    let mut delivered_aggregate = OrderAggregate::new(order_id, "CUST-700", 2500, Utc::now());
    delivered_aggregate.status = OrderStatus::Delivered;
    delivered_aggregate.version = 2;
    handler.insert_aggregate(delivered_aggregate);

    let cancel_cmd = OrderCommand::CancelOrder(CancelOrderCommand::new(
        order_id,
        "Cancellation after delivery",
    ));
    assert_eq!(
        handler.handle_command(cancel_cmd).await,
        Err(DomainError::OrderDelivered)
    );
}

#[tokio::test]
async fn test_idempotency_and_duplicate_command_rejection() {
    let mut handler = CommandHandler::new();
    let order_id = "ORD-IDEM-001";

    let create_cmd = CreateOrderCommand::new(order_id, "CUST-800", 1200, Utc::now());
    let cmd_id = create_cmd.command_id;
    let order_cmd = OrderCommand::CreateOrder(create_cmd);

    // First execution succeeds
    let res1 = handler.handle_command(order_cmd.clone()).await;
    assert!(res1.is_ok());

    // Duplicate execution with exact same command_id must be rejected under idempotency
    let res2 = handler.handle_command(order_cmd).await;
    assert_eq!(res2, Err(DomainError::DuplicateCommand(cmd_id)));

    // Aggregate state was unaffected by duplicate
    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.version, 1);

    // Duplicate credit command rejection
    let credit_inner = IssueCourtesyCreditCommand::new(order_id, 400, "Concession 1");
    let credit_id = credit_inner.command_id;
    let credit_cmd = OrderCommand::IssueCourtesyCredit(credit_inner);

    let cred_res1 = handler.handle_command(credit_cmd.clone()).await;
    assert!(cred_res1.is_ok());

    let cred_res2 = handler.handle_command(credit_cmd).await;
    assert_eq!(cred_res2, Err(DomainError::DuplicateCommand(credit_id)));

    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.courtesy_credit_cents, 400);
    assert_eq!(agg.version, 2);
}

#[tokio::test]
async fn test_command_consumer_stream_processing() {
    let config = Config::default();
    let mut mock_stream = InMemoryStreamConsumer::new();

    let cmd1 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-STREAM-1",
        "CUST-A",
        1500,
        Utc::now(),
    ));
    let cmd2 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-STREAM-2",
        "CUST-B",
        2000,
        Utc::now(),
    ));
    let cmd3 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        "ORD-STREAM-1",
        500,
        "Credit 1",
    ));
    let cmd4 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        "ORD-STREAM-2",
        1000,
        "Credit 2",
    ));
    let cmd5 = OrderCommand::CancelOrder(CancelOrderCommand::new(
        "ORD-STREAM-1",
        "Cancel ORD-STREAM-1",
    ));

    mock_stream.enqueue(ReceivedMessage::from_command(0, 0, &cmd1).unwrap());
    mock_stream.enqueue(ReceivedMessage::from_command(1, 1, &cmd2).unwrap());
    mock_stream.enqueue(ReceivedMessage::from_command(2, 0, &cmd3).unwrap());
    mock_stream.enqueue(ReceivedMessage::from_command(3, 1, &cmd4).unwrap());
    mock_stream.enqueue(ReceivedMessage::from_command(4, 0, &cmd5).unwrap());

    let mut consumer = CommandConsumer::new(config, mock_stream, InMemoryStreamPublisher::new());
    let processed_count = consumer
        .process_all_available()
        .await
        .expect("Stream processing must succeed");

    assert_eq!(processed_count, 5);

    // All outcomes are successful
    assert_eq!(consumer.outcomes().len(), 5);
    for outcome in consumer.outcomes() {
        assert!(outcome.is_success());
    }

    // Verify all offsets acknowledged in stream
    assert_eq!(
        consumer.stream().acknowledged_offsets(),
        vec![0, 1, 2, 3, 4]
    );
    assert_eq!(
        consumer.stream().acknowledged(),
        &[(0, 0), (1, 1), (0, 2), (1, 3), (0, 4)]
    );

    // Verify aggregate states
    let agg1 = consumer.handler().get_aggregate("ORD-STREAM-1").unwrap();
    assert_eq!(agg1.status, OrderStatus::Cancelled);
    assert_eq!(agg1.courtesy_credit_cents, 500);
    assert_eq!(agg1.version, 3);

    let agg2 = consumer.handler().get_aggregate("ORD-STREAM-2").unwrap();
    assert_eq!(agg2.status, OrderStatus::Created);
    assert_eq!(agg2.courtesy_credit_cents, 1000);
    assert_eq!(agg2.version, 2);
}

#[tokio::test]
async fn test_command_consumer_deserialization_error_resilience() {
    let config = Config::default();
    let mut mock_stream = InMemoryStreamConsumer::new();

    let valid_cmd1 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-RESIL-1",
        "CUST-R",
        1000,
        Utc::now(),
    ));
    let valid_cmd2 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        "ORD-RESIL-1",
        300,
        "Valid concession",
    ));

    mock_stream.enqueue(ReceivedMessage::from_command(0, 0, &valid_cmd1).unwrap());
    // Corrupted message payload
    mock_stream.enqueue(ReceivedMessage::new(
        1,
        0,
        &b"{\"bad_json\": \"missing closing bracket"[..],
    ));
    mock_stream.enqueue(ReceivedMessage::from_command(2, 0, &valid_cmd2).unwrap());

    let mut consumer = CommandConsumer::new(config, mock_stream, InMemoryStreamPublisher::new());
    let count = consumer.process_all_available().await.unwrap();
    assert_eq!(count, 3);

    let outcomes = consumer.outcomes();
    assert_eq!(outcomes.len(), 3);
    assert!(matches!(outcomes[0], CommandOutcome::Success { .. }));
    assert!(matches!(
        outcomes[1],
        CommandOutcome::DeserializationError { offset: 1, .. }
    ));
    assert!(matches!(outcomes[2], CommandOutcome::Success { .. }));

    // All offsets acknowledged despite corrupted payload
    assert_eq!(consumer.stream().acknowledged_offsets(), vec![0, 1, 2]);
    assert_eq!(consumer.stream().acknowledged(), &[(0, 0), (0, 1), (0, 2)]);

    // Aggregate still processed the two valid commands cleanly
    let agg = consumer.handler().get_aggregate("ORD-RESIL-1").unwrap();
    assert_eq!(agg.courtesy_credit_cents, 300);
    assert_eq!(agg.version, 2);
}

#[tokio::test]
async fn test_command_consumer_graceful_shutdown() {
    let config = Config::default();
    let mock_stream = InMemoryStreamConsumer::new();
    let mock_publisher = InMemoryStreamPublisher::new();
    let mut consumer = CommandConsumer::new(config, mock_stream, mock_publisher);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let loop_handle = tokio::spawn(async move { consumer.run_loop(shutdown_rx).await });

    // Let the loop run briefly
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send shutdown signal
    shutdown_tx.send(true).expect("Shutdown signal must send");

    let result = tokio::time::timeout(Duration::from_secs(2), loop_handle)
        .await
        .expect("Consumer loop must shut down within timeout")
        .expect("Task must join cleanly");

    assert!(result.is_ok(), "Consumer loop must exit cleanly");
}

#[tokio::test]
async fn test_config_parsing_and_defaults() {
    let default_config = Config::default();
    assert_eq!(default_config.iggy_server_address, "127.0.0.1:8090");
    assert_eq!(default_config.iggy_username, "iggy");
    assert_eq!(default_config.iggy_password, "iggy");
    assert_eq!(default_config.stream_name, "orders");
    assert_eq!(default_config.topic_orders_intake, "intake");
    assert_eq!(default_config.topic_orders_consolidated, "consolidated");
    assert_eq!(default_config.consumer_group, "cg-cmd-handler");
    assert_eq!(default_config.sierradb_url, "127.0.0.1:9090");
    assert!(!default_config.healthcheck);

    // CLI argument parsing test
    let args = [
        "ferrous-cqrses-daemon",
        "--iggy-server-address",
        "10.0.0.5:8090",
        "--stream-name",
        "custom_stream",
        "--topic-orders-intake",
        "custom_intake",
        "--topic-orders-consolidated",
        "custom_consolidated",
        "--consumer-group",
        "custom_cg",
        "--sierradb-url",
        "10.0.0.99:9090",
    ];
    let parsed = Config::try_parse_from(args).expect("CLI parsing must succeed");
    assert_eq!(parsed.iggy_server_address, "10.0.0.5:8090");
    assert_eq!(parsed.stream_name, "custom_stream");
    assert_eq!(parsed.topic_orders_intake, "custom_intake");
    assert_eq!(parsed.topic_orders_consolidated, "custom_consolidated");
    assert_eq!(parsed.consumer_group, "custom_cg");
    assert_eq!(parsed.sierradb_url, "10.0.0.99:9090");
}

#[tokio::test]
async fn test_aggregate_rehydration_from_events() {
    let mut handler = CommandHandler::new();
    let order_id = "ORD-REHYD-001";

    let events = vec![
        DomainEvent::OrderCreated(OrderCreatedEvent::new(
            order_id,
            "CUST-999",
            2000,
            Utc::now(),
        )),
        DomainEvent::CreditIssued(CreditIssuedEvent::new(
            order_id,
            1200,
            "Rehydrated credit",
            2,
        )),
        DomainEvent::OrderStatusUpdated(OrderStatusUpdatedEvent::new(
            order_id,
            OrderStatus::Created,
            OrderStatus::Processing,
            None,
            3,
        )),
    ];

    let rehydrated = handler.rehydrate(order_id, &events).unwrap();
    assert_eq!(rehydrated.order_id, order_id);
    assert_eq!(rehydrated.status, OrderStatus::Processing);
    assert_eq!(rehydrated.courtesy_credit_cents, 1200);
    assert_eq!(rehydrated.version, 3);
}

#[tokio::test]
async fn test_command_consumer_domain_rejection_resilience() {
    let config = Config::default();
    let mut mock_stream = InMemoryStreamConsumer::new();

    let order_id = "ORD-REJECT-001";
    let cmd1 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-REJECT",
        1200,
        Utc::now(),
    ));
    // Excessive courtesy credit (> 2,000 cents)
    let cmd2 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        2500,
        "Concession over 2000 cents limit",
    ));
    // Valid subsequent courtesy credit (500 cents)
    let cmd3 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        500,
        "Valid Concession",
    ));

    mock_stream.enqueue(ReceivedMessage::from_command(0, 0, &cmd1).unwrap());
    mock_stream.enqueue(ReceivedMessage::from_command(1, 0, &cmd2).unwrap());
    mock_stream.enqueue(ReceivedMessage::from_command(2, 0, &cmd3).unwrap());

    let mut consumer = CommandConsumer::new(config, mock_stream, InMemoryStreamPublisher::new());
    let count = consumer.process_all_available().await.unwrap();
    assert_eq!(count, 3);

    let outcomes = consumer.outcomes();
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes[0].is_success());
    assert!(outcomes[1].is_rejected());
    assert!(outcomes[2].is_success());

    if let CommandOutcome::Rejected {
        error,
        order_id: rej_order,
        partition_id,
        offset,
        ..
    } = &outcomes[1]
    {
        assert_eq!(rej_order, order_id);
        assert_eq!(*partition_id, 0);
        assert_eq!(*offset, 1);
        assert_eq!(
            *error,
            DomainError::CourtesyCreditExceeded(2500, MAX_COURTESY_CREDIT_CENTS)
        );
    } else {
        panic!("Expected CommandOutcome::Rejected");
    }

    // Offsets all acknowledged even when rejection occurred
    assert_eq!(consumer.stream().acknowledged_offsets(), vec![0, 1, 2]);

    // Aggregate state only contains the valid credit (500 cents) and version 2
    let agg = consumer.handler().get_aggregate(order_id).unwrap();
    assert_eq!(agg.courtesy_credit_cents, 500);
    assert_eq!(agg.version, 2);
}

#[tokio::test]
async fn test_command_consumer_duplicate_command_idempotent_handling() {
    let config = Config::default();
    let mut mock_stream = InMemoryStreamConsumer::new();

    let order_id = "ORD-IDEMP-001";
    let cmd1 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-IDEMP",
        1500,
        Utc::now(),
    ));

    // Message 0 and 1 carry the identical command ID
    mock_stream.enqueue(ReceivedMessage::from_command(0, 2, &cmd1).unwrap());
    mock_stream.enqueue(ReceivedMessage::from_command(1, 2, &cmd1).unwrap());

    let mut consumer = CommandConsumer::new(config, mock_stream, InMemoryStreamPublisher::new());
    let count = consumer.process_all_available().await.unwrap();
    assert_eq!(count, 2);

    let outcomes = consumer.outcomes();
    assert_eq!(outcomes.len(), 2);
    assert!(matches!(outcomes[0], CommandOutcome::Success { .. }));
    assert!(matches!(
        outcomes[1],
        CommandOutcome::IdempotentDuplicate { .. }
    ));
    assert!(outcomes[1].is_success());

    // Both offsets acknowledged
    assert_eq!(consumer.stream().acknowledged_offsets(), vec![0, 1]);
    assert_eq!(consumer.stream().acknowledged(), &[(2, 0), (2, 1)]);

    // Version is still 1
    let agg = consumer.handler().get_aggregate(order_id).unwrap();
    assert_eq!(agg.version, 1);
}

#[tokio::test]
async fn test_command_handler_order_not_found_errors() {
    let mut handler = CommandHandler::new();

    // Issuing credit for non-existent order
    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        "ORD-NONEXISTENT",
        500,
        "Credit",
    ));
    let err = handler.handle_command(credit_cmd).await.unwrap_err();
    assert_eq!(
        err,
        DomainError::OrderNotFound("ORD-NONEXISTENT".to_string())
    );

    // Cancelling non-existent order
    let cancel_cmd =
        OrderCommand::CancelOrder(CancelOrderCommand::new("ORD-NONEXISTENT", "Cancel"));
    let err2 = handler.handle_command(cancel_cmd).await.unwrap_err();
    assert_eq!(
        err2,
        DomainError::OrderNotFound("ORD-NONEXISTENT".to_string())
    );
}

#[tokio::test]
async fn test_command_handler_duplicate_order_creation() {
    let mut handler = CommandHandler::new();

    let cmd1 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-DUP-001",
        "CUST-1",
        1000,
        Utc::now(),
    ));
    assert!(handler.handle_command(cmd1).await.is_ok());

    // Create another command with a different command_id but the same order_id
    let cmd2 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-DUP-001",
        "CUST-2",
        2000,
        Utc::now(),
    ));
    let err = handler.handle_command(cmd2).await.unwrap_err();
    assert_eq!(
        err,
        DomainError::OrderAlreadyExists("ORD-DUP-001".to_string())
    );
}

#[tokio::test]
async fn test_command_handler_empty_ids_and_negative_freight_rejected() {
    let mut handler = CommandHandler::new();

    // Empty order_id
    let empty_order_cmd =
        OrderCommand::CreateOrder(CreateOrderCommand::new("   ", "CUST-1", 1000, Utc::now()));
    let err1 = handler.handle_command(empty_order_cmd).await.unwrap_err();
    assert!(matches!(err1, DomainError::Generic(_)));

    // Empty customer_id
    let empty_cust_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-VALID",
        "   ",
        1000,
        Utc::now(),
    ));
    let err2 = handler.handle_command(empty_cust_cmd).await.unwrap_err();
    assert!(matches!(err2, DomainError::Generic(_)));

    // Negative freight
    let neg_freight_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-VALID",
        "CUST-1",
        -500,
        Utc::now(),
    ));
    let err3 = handler.handle_command(neg_freight_cmd).await.unwrap_err();
    assert_eq!(err3, DomainError::InvalidAmount(-500));
}

#[tokio::test]
async fn test_aggregate_rehydration_empty_events_rejected() {
    let mut handler = CommandHandler::new();
    let err = handler.rehydrate("ORD-EMPTY", &[]).unwrap_err();
    assert_eq!(err, DomainError::OrderNotFound("ORD-EMPTY".to_string()));
}

#[tokio::test]
async fn test_settle_courtesy_credit_success() {
    let mut handler = CommandHandler::new();

    // 1. Create order
    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-SETTLE-1",
        "CUST-1",
        1500,
        Utc::now(),
    ));
    handler.handle_command(create_cmd).await.unwrap();

    // 2. Issue courtesy credit ($15.00 / 1500 cents)
    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        "ORD-SETTLE-1",
        1500,
        "Carrier delay concession",
    ));
    handler.handle_command(credit_cmd).await.unwrap();

    let agg = handler.get_aggregate("ORD-SETTLE-1").unwrap();
    assert_eq!(agg.courtesy_credit_cents, 1500);
    assert_eq!(agg.settled_credit_cents, 0);
    assert_eq!(agg.settlement_status, CreditSettlementStatus::PendingSettlement);

    // 3. Settle courtesy credit
    let settle_cmd = OrderCommand::SettleCourtesyCredit(SettleCourtesyCreditCommand::new(
        "ORD-SETTLE-1",
        1500,
        "tx-settle-001",
    ));
    let event = handler.handle_command(settle_cmd).await.unwrap();
    assert!(matches!(event, DomainEvent::CourtesyCreditSettled(_)));

    let agg_settled = handler.get_aggregate("ORD-SETTLE-1").unwrap();
    assert_eq!(agg_settled.courtesy_credit_cents, 1500);
    assert_eq!(agg_settled.settled_credit_cents, 1500);
    assert_eq!(agg_settled.settlement_status, CreditSettlementStatus::Settled);
    assert_eq!(agg_settled.version, 3);
}

#[tokio::test]
async fn test_fail_courtesy_credit_settlement_success() {
    let mut handler = CommandHandler::new();

    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-SETTLE-FAIL",
        "CUST-1",
        1500,
        Utc::now(),
    ));
    handler.handle_command(create_cmd).await.unwrap();

    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        "ORD-SETTLE-FAIL",
        1500,
        "Concession",
    ));
    handler.handle_command(credit_cmd).await.unwrap();

    let fail_cmd = OrderCommand::FailCourtesyCreditSettlement(FailCourtesyCreditSettlementCommand::new(
        "ORD-SETTLE-FAIL",
        1500,
        "Insufficient account balance",
    ));
    let event = handler.handle_command(fail_cmd).await.unwrap();
    assert!(matches!(event, DomainEvent::CourtesyCreditSettlementFailed(_)));

    let agg = handler.get_aggregate("ORD-SETTLE-FAIL").unwrap();
    assert_eq!(agg.settled_credit_cents, 0);
    assert_eq!(agg.settlement_status, CreditSettlementStatus::SettlementFailed);
    assert_eq!(agg.version, 3);
}

#[tokio::test]
async fn test_settle_courtesy_credit_excessive_amount_rejected() {
    let mut handler = CommandHandler::new();

    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        "ORD-EXCESS-SETTLE",
        "CUST-1",
        1000,
        Utc::now(),
    ));
    handler.handle_command(create_cmd).await.unwrap();

    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        "ORD-EXCESS-SETTLE",
        1000, // $10 credit
        "Concession",
    ));
    handler.handle_command(credit_cmd).await.unwrap();

    // Attempt to settle 2500 cents (exceeds $10 granted)
    let settle_cmd = OrderCommand::SettleCourtesyCredit(SettleCourtesyCreditCommand::new(
        "ORD-EXCESS-SETTLE",
        2500,
        "tx-999",
    ));
    let err = handler.handle_command(settle_cmd).await.unwrap_err();
    assert_eq!(err, DomainError::InvalidAmount(2500));

    let agg = handler.get_aggregate("ORD-EXCESS-SETTLE").unwrap();
    assert_eq!(agg.settled_credit_cents, 0);
    assert_eq!(agg.version, 2);
}

#[tokio::test]
async fn test_settle_courtesy_credit_partial_and_multi_settlement() {
    let mut handler = CommandHandler::new();

    handler
        .handle_command(OrderCommand::CreateOrder(CreateOrderCommand::new(
            "ORD-MULTI-SETTLE",
            "CUST-1",
            2000,
            Utc::now(),
        )))
        .await
        .unwrap();

    handler
        .handle_command(OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
            "ORD-MULTI-SETTLE",
            1500,
            "Concession",
        )))
        .await
        .unwrap();

    // First partial settlement: 500
    handler
        .handle_command(OrderCommand::SettleCourtesyCredit(SettleCourtesyCreditCommand::new(
            "ORD-MULTI-SETTLE",
            500,
            "tx-p1",
        )))
        .await
        .unwrap();

    let agg1 = handler.get_aggregate("ORD-MULTI-SETTLE").unwrap();
    assert_eq!(agg1.settled_credit_cents, 500);
    assert_eq!(agg1.settlement_status, CreditSettlementStatus::PartiallySettled);

    // Second partial settlement: 1000 (total = 1500)
    handler
        .handle_command(OrderCommand::SettleCourtesyCredit(SettleCourtesyCreditCommand::new(
            "ORD-MULTI-SETTLE",
            1000,
            "tx-p2",
        )))
        .await
        .unwrap();

    let agg2 = handler.get_aggregate("ORD-MULTI-SETTLE").unwrap();
    assert_eq!(agg2.settled_credit_cents, 1500);
    assert_eq!(agg2.settlement_status, CreditSettlementStatus::Settled);

    // Third settlement attempt: 100 (exceeds 1500 granted credit)
    let err = handler
        .handle_command(OrderCommand::SettleCourtesyCredit(SettleCourtesyCreditCommand::new(
            "ORD-MULTI-SETTLE",
            100,
            "tx-p3",
        )))
        .await
        .unwrap_err();
    assert_eq!(err, DomainError::InvalidAmount(100));
}

#[tokio::test]
async fn test_settle_courtesy_credit_rejected_on_cancelled_or_fraud_locked() {
    let mut handler = CommandHandler::new();

    handler
        .handle_command(OrderCommand::CreateOrder(CreateOrderCommand::new(
            "ORD-CANCELLED-SETTLE",
            "CUST-1",
            1000,
            Utc::now(),
        )))
        .await
        .unwrap();

    handler
        .handle_command(OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
            "ORD-CANCELLED-SETTLE",
            1000,
            "Concession",
        )))
        .await
        .unwrap();

    handler
        .handle_command(OrderCommand::CancelOrder(CancelOrderCommand::new(
            "ORD-CANCELLED-SETTLE",
            "Cancelled by user",
        )))
        .await
        .unwrap();

    let settle_cmd = OrderCommand::SettleCourtesyCredit(SettleCourtesyCreditCommand::new(
        "ORD-CANCELLED-SETTLE",
        500,
        "tx-fail",
    ));
    let err = handler.handle_command(settle_cmd).await.unwrap_err();
    assert_eq!(err, DomainError::OrderCancelled);

    let fail_cmd = OrderCommand::FailCourtesyCreditSettlement(FailCourtesyCreditSettlementCommand::new(
        "ORD-CANCELLED-SETTLE",
        500,
        "Failed clearing",
    ));
    let err = handler.handle_command(fail_cmd).await.unwrap_err();
    assert_eq!(err, DomainError::OrderCancelled);
}

#[tokio::test]
async fn test_settle_and_fail_courtesy_credit_rejected_on_fraud_locked() {
    let mut handler = CommandHandler::new();

    let mut agg = OrderAggregate::new("ORD-FRAUD-SETTLE", "CUST-F", 1000, Utc::now());
    agg.status = OrderStatus::FraudLocked;
    agg.courtesy_credit_cents = 1000;
    agg.version = 2;
    handler.insert_aggregate(agg);

    let settle_cmd = OrderCommand::SettleCourtesyCredit(SettleCourtesyCreditCommand::new(
        "ORD-FRAUD-SETTLE",
        500,
        "tx-fraud",
    ));
    let err = handler.handle_command(settle_cmd).await.unwrap_err();
    assert_eq!(err, DomainError::OrderFraudLocked);

    let fail_cmd = OrderCommand::FailCourtesyCreditSettlement(FailCourtesyCreditSettlementCommand::new(
        "ORD-FRAUD-SETTLE",
        500,
        "Failed settlement",
    ));
    let err = handler.handle_command(fail_cmd).await.unwrap_err();
    assert_eq!(err, DomainError::OrderFraudLocked);
}

#[tokio::test]
async fn test_fail_courtesy_credit_settlement_rejected_on_zero_credit_or_fully_settled() {
    let mut handler = CommandHandler::new();

    // Order with $0 credit
    handler
        .handle_command(OrderCommand::CreateOrder(CreateOrderCommand::new(
            "ORD-ZERO-CREDIT-FAIL",
            "CUST-Z",
            1000,
            Utc::now(),
        )))
        .await
        .unwrap();

    let fail_cmd = OrderCommand::FailCourtesyCreditSettlement(FailCourtesyCreditSettlementCommand::new(
        "ORD-ZERO-CREDIT-FAIL",
        500,
        "Fail attempt",
    ));
    let err = handler.handle_command(fail_cmd).await.unwrap_err();
    assert_eq!(err, DomainError::InvalidAmount(500));

    // Order with 1000 credit, settled 1000
    handler
        .handle_command(OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
            "ORD-ZERO-CREDIT-FAIL",
            1000,
            "Concession",
        )))
        .await
        .unwrap();

    handler
        .handle_command(OrderCommand::SettleCourtesyCredit(SettleCourtesyCreditCommand::new(
            "ORD-ZERO-CREDIT-FAIL",
            1000,
            "tx-full",
        )))
        .await
        .unwrap();

    // Attempting to fail on already fully settled order
    let fail_cmd2 = OrderCommand::FailCourtesyCreditSettlement(FailCourtesyCreditSettlementCommand::new(
        "ORD-ZERO-CREDIT-FAIL",
        500,
        "Fail attempt post-settle",
    ));
    let err = handler.handle_command(fail_cmd2).await.unwrap_err();
    assert_eq!(err, DomainError::InvalidAmount(500));
}

#[tokio::test]
async fn test_settle_courtesy_credit_duplicate_command_id_rejected() {
    let mut handler = CommandHandler::new();

    handler
        .handle_command(OrderCommand::CreateOrder(CreateOrderCommand::new(
            "ORD-DUP-SETTLE",
            "CUST-D",
            1000,
            Utc::now(),
        )))
        .await
        .unwrap();

    handler
        .handle_command(OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
            "ORD-DUP-SETTLE",
            1000,
            "Concession",
        )))
        .await
        .unwrap();

    let settle_cmd = SettleCourtesyCreditCommand::new("ORD-DUP-SETTLE", 500, "tx-dup-1");
    let cmd_id = settle_cmd.command_id;
    let order_cmd = OrderCommand::SettleCourtesyCredit(settle_cmd);

    handler.handle_command(order_cmd.clone()).await.unwrap();

    // Dispatching identical command with same command_id
    let err = handler.handle_command(order_cmd).await.unwrap_err();
    assert_eq!(err, DomainError::DuplicateCommand(cmd_id));
}

#[tokio::test]
async fn test_aggregate_rehydration_with_settlement_events() {
    let mut handler = CommandHandler::new();

    let events = vec![
        DomainEvent::OrderCreated(OrderCreatedEvent::new(
            "ORD-REHYDRATE",
            "CUST-R",
            1000,
            Utc::now(),
        )),
        DomainEvent::CreditIssued(CreditIssuedEvent::new(
            "ORD-REHYDRATE",
            1500,
            "Concession",
            2,
        )),
        DomainEvent::CourtesyCreditSettled(CourtesyCreditSettledEvent::new(
            "ORD-REHYDRATE",
            1500,
            "tx-rehydrate",
            3,
        )),
    ];

    let rehydrated = handler.rehydrate("ORD-REHYDRATE", &events).unwrap();
    assert_eq!(rehydrated.order_id, "ORD-REHYDRATE");
    assert_eq!(rehydrated.courtesy_credit_cents, 1500);
    assert_eq!(rehydrated.settled_credit_cents, 1500);
    assert_eq!(rehydrated.settlement_status, CreditSettlementStatus::Settled);
    assert_eq!(rehydrated.version, 3);
}
