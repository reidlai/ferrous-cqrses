//! Comprehensive unit and integration test suite for SierraDB client & event sourcing primitives.
//!
//! Validates RESP3 protocol encoding/decoding, `SierraClient` operations (`PING`, `EAPPEND`, `ESCAN`, `ESVER`),
//! optimistic concurrency control (`EXPECTED_VERSION`), on-demand aggregate rehydration from stream,
//! domain business invariants ($20 courtesy credit cap), and error resilience.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use ferrous_cqrses::prelude::*;
use ferrous_cqrses::sierra::{
    encode_command, parse_resp_frame, stream_id_for_order, ExpectedVersion,
    InMemoryEventStore, RespValue, SierraClient, SierraDbEventStore,
};

// ============================================================================
// 1. RESP3 Framing Unit Tests
// ============================================================================

#[test]
fn test_resp3_framing_primitives() {
    // Simple String
    let (val, consumed) = parse_resp_frame(b"+PONG\r\n").unwrap().unwrap();
    assert_eq!(consumed, 7);
    assert_eq!(val, RespValue::SimpleString("PONG".to_string()));

    // Simple Error
    let (val, consumed) = parse_resp_frame(b"-ERR stream not found\r\n").unwrap().unwrap();
    assert_eq!(consumed, 23);
    assert_eq!(val, RespValue::Error("ERR stream not found".to_string()));

    // Integer
    let (val, consumed) = parse_resp_frame(b":12345\r\n").unwrap().unwrap();
    assert_eq!(consumed, 8);
    assert_eq!(val, RespValue::Integer(12345));

    // Bulk String
    let (val, consumed) = parse_resp_frame(b"$5\r\nhello\r\n").unwrap().unwrap();
    assert_eq!(consumed, 11);
    assert_eq!(val, RespValue::BulkString(b"hello".to_vec()));

    // Null Bulk String
    let (val, consumed) = parse_resp_frame(b"$-1\r\n").unwrap().unwrap();
    assert_eq!(consumed, 5);
    assert_eq!(val, RespValue::Null);

    // RESP3 Null
    let (val, consumed) = parse_resp_frame(b"_\r\n").unwrap().unwrap();
    assert_eq!(consumed, 3);
    assert_eq!(val, RespValue::Null);

    // Boolean
    let (val, consumed) = parse_resp_frame(b"#t\r\n").unwrap().unwrap();
    assert_eq!(consumed, 4);
    assert_eq!(val, RespValue::Boolean(true));

    let (val, consumed) = parse_resp_frame(b"#f\r\n").unwrap().unwrap();
    assert_eq!(consumed, 4);
    assert_eq!(val, RespValue::Boolean(false));

    // Double
    let (val, consumed) = parse_resp_frame(b",42.5\r\n").unwrap().unwrap();
    assert_eq!(consumed, 7);
    assert_eq!(val, RespValue::Double(42.5));

    // Blob Error
    let (val, consumed) = parse_resp_frame(b"!5\r\nfatal\r\n").unwrap().unwrap();
    assert_eq!(consumed, 11);
    assert_eq!(val, RespValue::BlobError(b"fatal".to_vec()));

    // Empty Simple String (must not panic on slice bounds)
    let (val, consumed) = parse_resp_frame(b"+\r\n").unwrap().unwrap();
    assert_eq!(consumed, 3);
    assert_eq!(val, RespValue::SimpleString("".to_string()));

    // Empty Simple Error (must not panic on slice bounds)
    let (val, consumed) = parse_resp_frame(b"-\r\n").unwrap().unwrap();
    assert_eq!(consumed, 3);
    assert_eq!(val, RespValue::Error("".to_string()));

    // Bulk String missing CRLF (must return malformed frame error)
    let bad_bulk = b"$5\r\nhelloww";
    let res = parse_resp_frame(bad_bulk);
    assert!(res.is_err(), "Bulk string with corrupted CRLF must return error");
}

#[test]
fn test_resp3_framing_composite_and_partial() {
    // Array
    let raw_array = b"*2\r\n$4\r\necho\r\n$5\r\nworld\r\n";
    let (val, consumed) = parse_resp_frame(raw_array).unwrap().unwrap();
    assert_eq!(consumed, raw_array.len());
    match val {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(b"echo".to_vec()));
            assert_eq!(items[1], RespValue::BulkString(b"world".to_vec()));
        }
        other => panic!("Expected Array, got {:?}", other),
    }

    // Map
    let raw_map = b"%1\r\n$6\r\nstatus\r\n+OK\r\n";
    let (val, consumed) = parse_resp_frame(raw_map).unwrap().unwrap();
    assert_eq!(consumed, raw_map.len());
    match val {
        RespValue::Map(entries) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, RespValue::BulkString(b"status".to_vec()));
            assert_eq!(entries[0].1, RespValue::SimpleString("OK".to_string()));
        }
        other => panic!("Expected Map, got {:?}", other),
    }

    // Incomplete frames must return Ok(None)
    assert_eq!(parse_resp_frame(b"").unwrap(), None);
    assert_eq!(parse_resp_frame(b"+PO").unwrap(), None);
    assert_eq!(parse_resp_frame(b"$5\r\nhel").unwrap(), None);
    assert_eq!(parse_resp_frame(b"*2\r\n$4\r\necho\r\n").unwrap(), None);
}

#[test]
fn test_command_encoding() {
    let ping = encode_command(&["PING"]);
    assert_eq!(ping, b"*1\r\n$4\r\nPING\r\n");

    let escan = encode_command(&["ESCAN", "order-101", "-", "+"]);
    assert_eq!(
        escan,
        b"*4\r\n$5\r\nESCAN\r\n$9\r\norder-101\r\n$1\r\n-\r\n$1\r\n+\r\n"
    );

    let eappend = encode_command(&[
        "EAPPEND",
        "order-101",
        "OrderCreated",
        "EXPECTED_VERSION",
        "empty",
        "PAYLOAD",
        "{}",
    ]);
    assert_eq!(
        eappend,
        b"*7\r\n$7\r\nEAPPEND\r\n$9\r\norder-101\r\n$12\r\nOrderCreated\r\n$16\r\nEXPECTED_VERSION\r\n$5\r\nempty\r\n$7\r\nPAYLOAD\r\n$2\r\n{}\r\n"
    );
}

#[test]
fn test_event_serialization_and_deserialization_roundtrip() {
    let created = DomainEvent::OrderCreated(OrderCreatedEvent::new(
        "ORD-501",
        "CUST-1",
        1500,
        Utc::now(),
    ));
    let (name, payload) = serialize_event(&created).unwrap();
    assert_eq!(name, "OrderCreated");
    let deserialized = deserialize_event(name, &payload).unwrap();
    assert_eq!(created, deserialized);

    let credit = DomainEvent::CreditIssued(CreditIssuedEvent::new(
        "ORD-501",
        1200,
        "Carrier compensation",
        2,
    ));
    let (name, payload) = serialize_event(&credit).unwrap();
    assert_eq!(name, "CreditIssued");
    let deserialized = deserialize_event(name, &payload).unwrap();
    assert_eq!(credit, deserialized);

    let cancel = DomainEvent::OrderCancelled(OrderCancelledEvent::new("ORD-501", "User dispute", 3));
    let (name, payload) = serialize_event(&cancel).unwrap();
    assert_eq!(name, "OrderCancelled");
    let deserialized = deserialize_event(name, &payload).unwrap();
    assert_eq!(cancel, deserialized);

    let settled = DomainEvent::CourtesyCreditSettled(CourtesyCreditSettledEvent::new(
        "ORD-501",
        1200,
        "tx-sierra-01",
        4,
    ));
    let (name, payload) = serialize_event(&settled).unwrap();
    assert_eq!(name, "CourtesyCreditSettled");
    let deserialized = deserialize_event(name, &payload).unwrap();
    assert_eq!(settled, deserialized);

    let failed = DomainEvent::CourtesyCreditSettlementFailed(CourtesyCreditSettlementFailedEvent::new(
        "ORD-501",
        1200,
        "Clearing failure",
        5,
    ));
    let (name, payload) = serialize_event(&failed).unwrap();
    assert_eq!(name, "CourtesyCreditSettlementFailed");
    let deserialized = deserialize_event(name, &payload).unwrap();
    assert_eq!(failed, deserialized);
}

// ============================================================================
// 2. InMemoryEventStore Concurrency & Contract Tests
// ============================================================================

#[tokio::test]
async fn test_in_memory_event_store_concurrency_and_versioning() {
    let store = InMemoryEventStore::new();
    let stream_id = "order-test-101";

    // 1. Initial append with ExpectedVersion::Empty
    let event1 = DomainEvent::OrderCreated(OrderCreatedEvent::new(
        "test-101",
        "CUST-1",
        2000,
        Utc::now(),
    ));
    let ver0 = store
        .append(stream_id, &event1, ExpectedVersion::Empty)
        .await
        .expect("Append empty stream must succeed");
    assert_eq!(ver0, 0);

    // 2. Reject duplicate creation with ExpectedVersion::Empty
    let err_dup = store
        .append(stream_id, &event1, ExpectedVersion::Empty)
        .await
        .unwrap_err();
    assert!(matches!(err_dup, DomainError::OrderAlreadyExists(_)));

    // 3. Subsequent append with ExpectedVersion::Exact(0)
    let event2 = DomainEvent::CreditIssued(CreditIssuedEvent::new(
        "test-101",
        500,
        "SLA credit",
        2,
    ));
    let ver1 = store
        .append(stream_id, &event2, ExpectedVersion::Exact(0))
        .await
        .expect("Append with expected version 0 must succeed");
    assert_eq!(ver1, 1);

    // 4. Version mismatch rejection: expected 0 when stream is at 1
    let event3 = DomainEvent::CreditIssued(CreditIssuedEvent::new(
        "test-101",
        500,
        "Extra credit",
        3,
    ));
    let err_ver = store
        .append(stream_id, &event3, ExpectedVersion::Exact(0))
        .await
        .unwrap_err();
    assert!(matches!(err_ver, DomainError::VersionMismatch { .. }));

    // 5. Scan events and verify stream version
    let events = store.scan(stream_id).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], event1);
    assert_eq!(events[1], event2);

    let current_ver = store.stream_version(stream_id).await.unwrap();
    assert_eq!(current_ver, Some(1));
}

// ============================================================================
// 3. Mock SierraDB RESP3 TCP Server Implementation
// ============================================================================

struct StoredMockEvent {
    event_name: String,
    payload: String,
    stream_version: u64,
}

#[derive(Default)]
struct MockSierraDbState {
    streams: HashMap<String, Vec<StoredMockEvent>>,
}

/// Spawns an in-process TCP server speaking native SierraDB RESP3 protocol.
async fn spawn_mock_sierradb_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(RwLock::new(MockSierraDbState::default()));

    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let state_clone = Arc::clone(&state);
            tokio::spawn(async move {
                let mut buf = bytes::BytesMut::with_capacity(4096);
                loop {
                    if let Ok(Some((frame, consumed))) = parse_resp_frame(&buf) {
                        let _ = buf.split_to(consumed);
                        if let RespValue::Array(args) = frame {
                            let str_args: Vec<String> = args
                                .iter()
                                .filter_map(|a| match a {
                                    RespValue::BulkString(b) => {
                                        Some(String::from_utf8_lossy(b).to_string())
                                    }
                                    RespValue::SimpleString(s) => Some(s.clone()),
                                    _ => None,
                                })
                                .collect();

                            if str_args.is_empty() {
                                continue;
                            }

                            let cmd = str_args[0].to_uppercase();
                            match cmd.as_str() {
                                "PING" => {
                                    let _ = socket.write_all(b"+PONG\r\n").await;
                                }
                                "ESVER" => {
                                    if str_args.len() < 2 {
                                        let _ = socket.write_all(b"-ERR missing stream_id\r\n").await;
                                        continue;
                                    }
                                    let stream_id = &str_args[1];
                                    let read_guard = state_clone.read().await;
                                    if let Some(events) = read_guard.streams.get(stream_id) {
                                        if events.is_empty() {
                                            let _ = socket.write_all(b"$-1\r\n").await;
                                        } else {
                                            let ver = events.len() as i64 - 1;
                                            let resp = format!(":{}\r\n", ver);
                                            let _ = socket.write_all(resp.as_bytes()).await;
                                        }
                                    } else {
                                        let _ = socket.write_all(b"$-1\r\n").await;
                                    }
                                }
                                "EAPPEND" => {
                                    // EAPPEND <stream> <event_name> EXPECTED_VERSION <ver> PAYLOAD <json>
                                    if str_args.len() < 7 {
                                        let _ = socket.write_all(b"-ERR invalid EAPPEND args\r\n").await;
                                        continue;
                                    }
                                    let stream_id = &str_args[1];
                                    let event_name = &str_args[2];
                                    let expected_ver = &str_args[4];
                                    let payload = &str_args[6];

                                    let mut write_guard = state_clone.write().await;
                                    let events = write_guard.streams.entry(stream_id.clone()).or_default();
                                    let current_count = events.len() as u64;

                                    if expected_ver == "empty" {
                                        if current_count > 0 {
                                            let _ = socket.write_all(b"-ERR stream already exists\r\n").await;
                                            continue;
                                        }
                                    } else if let Ok(exp_num) = expected_ver.parse::<u64>() {
                                        let actual_ver = if current_count == 0 {
                                            u64::MAX
                                        } else {
                                            current_count - 1
                                        };
                                        if actual_ver != exp_num {
                                            let err_resp = format!(
                                                "-WRONG_EXPECTED_VERSION expected {} but got {}\r\n",
                                                exp_num, actual_ver
                                            );
                                            let _ = socket.write_all(err_resp.as_bytes()).await;
                                            continue;
                                        }
                                    }

                                    let new_ver = current_count;
                                    events.push(StoredMockEvent {
                                        event_name: event_name.clone(),
                                        payload: payload.clone(),
                                        stream_version: new_ver,
                                    });

                                    // Return new stream version as integer frame
                                    let resp = format!(":{}\r\n", new_ver);
                                    let _ = socket.write_all(resp.as_bytes()).await;
                                }
                                "ESCAN" => {
                                    // ESCAN <stream> <start> <end>
                                    if str_args.len() < 4 {
                                        let _ = socket.write_all(b"-ERR invalid ESCAN args\r\n").await;
                                        continue;
                                    }
                                    let stream_id = &str_args[1];
                                    let read_guard = state_clone.read().await;

                                    if let Some(events) = read_guard.streams.get(stream_id) {
                                        // Construct RESP3 map response: %1\r\n$6\r\nevents\r\n*<N>\r\n...
                                        let mut resp = Vec::new();
                                        resp.extend_from_slice(format!("%1\r\n$6\r\nevents\r\n*{}\r\n", events.len()).as_bytes());
                                        for evt in events {
                                            let evt_map = format!(
                                                "%3\r\n$10\r\nevent_name\r\n${}\r\n{}\r\n$7\r\npayload\r\n${}\r\n{}\r\n$14\r\nstream_version\r\n:{}\r\n",
                                                evt.event_name.len(), evt.event_name,
                                                evt.payload.len(), evt.payload,
                                                evt.stream_version
                                            );
                                            resp.extend_from_slice(evt_map.as_bytes());
                                        }
                                        let _ = socket.write_all(&resp).await;
                                    } else {
                                        let _ = socket.write_all(b"%1\r\n$6\r\nevents\r\n*0\r\n").await;
                                    }
                                }
                                other => {
                                    let err = format!("-ERR unknown command {}\r\n", other);
                                    let _ = socket.write_all(err.as_bytes()).await;
                                }
                            }
                        }
                        continue;
                    }

                    match socket.read_buf(&mut buf).await {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
            });
        }
    });

    (addr, handle)
}

// ============================================================================
// 4. SierraClient & SierraDbEventStore Integration Tests
// ============================================================================

#[tokio::test]
async fn test_sierra_client_mock_integration() {
    let (addr, _server) = spawn_mock_sierradb_server().await;
    let addr_str = addr.to_string();

    let mut client = SierraClient::connect(&addr_str)
        .await
        .expect("Must connect to mock SierraDB");

    // 1. PING test
    client.ping().await.expect("PING must succeed");

    // 2. Stream version on non-existent stream
    let ver_none = client.stream_version("order-9999").await.unwrap();
    assert_eq!(ver_none, None);

    // 3. EAPPEND new stream
    let (created_name, payload) = serialize_event(&DomainEvent::OrderCreated(OrderCreatedEvent::new(
        "9999",
        "CUST-1",
        1000,
        Utc::now(),
    )))
    .unwrap();
    let v0 = client
        .append("order-9999", created_name, ExpectedVersion::Empty, &payload)
        .await
        .expect("EAPPEND empty stream must succeed");
    assert_eq!(v0, 0);

    // 4. Stream version after creation
    let ver_some = client.stream_version("order-9999").await.unwrap();
    assert_eq!(ver_some, Some(0));

    // 5. Version mismatch rejection
    let (credit_name, credit_payload) = serialize_event(&DomainEvent::CreditIssued(
        CreditIssuedEvent::new("9999", 500, "Delay", 2),
    ))
    .unwrap();
    let mismatch_err = client
        .append(
            "order-9999",
            credit_name,
            ExpectedVersion::Exact(5),
            &credit_payload,
        )
        .await
        .unwrap_err();
    assert!(matches!(mismatch_err, DomainError::VersionMismatch { .. }));

    // 6. Valid append with expected version 0
    let v1 = client
        .append(
            "order-9999",
            credit_name,
            ExpectedVersion::Exact(0),
            &credit_payload,
        )
        .await
        .expect("EAPPEND with version 0 must succeed");
    assert_eq!(v1, 1);

    // 7. ESCAN stream events
    let events = client.scan("order-9999", "-", "+").await.unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], DomainEvent::OrderCreated(_)));
    assert!(matches!(events[1], DomainEvent::CreditIssued(_)));
}

#[tokio::test]
async fn test_sierradb_event_store_trait_integration() {
    let (addr, _server) = spawn_mock_sierradb_server().await;
    let store = SierraDbEventStore::connect(addr.to_string()).await.unwrap();
    assert_eq!(store.addr(), addr.to_string());

    let stream_id = "order-store-trait-1";
    let event1 = DomainEvent::OrderCreated(OrderCreatedEvent::new(
        "store-trait-1",
        "CUST-ST",
        1500,
        Utc::now(),
    ));

    let ver0 = store
        .append(stream_id, &event1, ExpectedVersion::Empty)
        .await
        .unwrap();
    assert_eq!(ver0, 0);

    let stream_ver = store.stream_version(stream_id).await.unwrap();
    assert_eq!(stream_ver, Some(0));

    let events = store.scan(stream_id).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], event1);
}

// ============================================================================
// 5. CommandHandler Integration with SierraDbEventStore
// ============================================================================

#[tokio::test]
async fn test_command_handler_with_sierradb_lifecycle() {
    let (addr, _server) = spawn_mock_sierradb_server().await;
    let addr_str = addr.to_string();

    let mut handler = CommandHandler::connect_sierra(&addr_str)
        .await
        .expect("CommandHandler must connect to SierraDB");

    let order_id = "ORD-SIERRA-001";

    // 1. Create Order
    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-88",
        1800,
        Utc::now(),
    ));
    let evt1 = handler
        .handle_command(create_cmd)
        .await
        .expect("CreateOrder must succeed");
    assert_eq!(evt1.version(), 1);

    // Verify aggregate in cache
    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.version, 1);
    assert_eq!(agg.status, OrderStatus::Created);

    // 2. Issue Courtesy Credit ($15.00)
    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        1500,
        "Carrier transit delay compensation",
    ));
    let evt2 = handler
        .handle_command(credit_cmd)
        .await
        .expect("IssueCourtesyCredit must succeed");
    assert_eq!(evt2.version(), 2);

    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.courtesy_credit_cents, 1500);
    assert_eq!(agg.version, 2);

    // 3. Cancel Order
    let cancel_cmd = OrderCommand::CancelOrder(CancelOrderCommand::new(
        order_id,
        "Customer cancellation request",
    ));
    let evt3 = handler
        .handle_command(cancel_cmd)
        .await
        .expect("CancelOrder must succeed");
    assert_eq!(evt3.version(), 3);

    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.status, OrderStatus::Cancelled);
    assert_eq!(agg.version, 3);

    // Verify events persisted in SierraDB directly via a separate client
    let mut verifier_client = SierraClient::connect(&addr_str).await.unwrap();
    let stream_id = stream_id_for_order(order_id);
    let events = verifier_client.scan(&stream_id, "-", "+").await.unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].version(), 1);
    assert_eq!(events[1].version(), 2);
    assert_eq!(events[2].version(), 3);
}

#[tokio::test]
async fn test_command_handler_uncached_aggregate_rehydration_from_sierradb() {
    let (addr, _server) = spawn_mock_sierradb_server().await;
    let addr_str = addr.to_string();

    let order_id = "ORD-REHYDRATE-77";

    // Create order using Handler 1
    {
        let mut handler1 = CommandHandler::connect_sierra(&addr_str).await.unwrap();
        let cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
            order_id,
            "CUST-42",
            2400,
            Utc::now(),
        ));
        handler1.handle_command(cmd).await.unwrap();
    }

    // Spin up completely fresh Handler 2 with empty in-memory cache
    let mut handler2 = CommandHandler::connect_sierra(&addr_str).await.unwrap();
    assert!(handler2.get_aggregate(order_id).is_none());

    // Handler 2 receives a command for the un-cached order
    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        1000,
        "Goodwill credit for delivery delay",
    ));
    let evt = handler2
        .handle_command(credit_cmd)
        .await
        .expect("Command on un-cached aggregate must rehydrate from SierraDB and succeed");

    assert_eq!(evt.version(), 2);

    let rehydrated_agg = handler2.get_aggregate(order_id).unwrap();
    assert_eq!(rehydrated_agg.order_id, order_id);
    assert_eq!(rehydrated_agg.customer_id, "CUST-42");
    assert_eq!(rehydrated_agg.courtesy_credit_cents, 1000);
    assert_eq!(rehydrated_agg.version, 2);
}

#[tokio::test]
async fn test_courtesy_credit_ceiling_invariant_zero_append_to_sierradb() {
    let (addr, _server) = spawn_mock_sierradb_server().await;
    let addr_str = addr.to_string();

    let mut handler = CommandHandler::connect_sierra(&addr_str).await.unwrap();
    let order_id = "ORD-CAP-CHECK";

    // 1. Create order
    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-10",
        1500,
        Utc::now(),
    ));
    handler.handle_command(create_cmd).await.unwrap();

    // 2. Attempt excessive single courtesy credit: $20.01 (2001 cents) > $20.00 ceiling
    let excessive_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        2001,
        "Excessive courtesy credit",
    ));
    let err = handler.handle_command(excessive_cmd).await.unwrap_err();
    assert_eq!(
        err,
        DomainError::CourtesyCreditExceeded(2001, MAX_COURTESY_CREDIT_CENTS)
    );

    // Verify zero events were appended: stream must still have only 1 event in SierraDB
    let mut verifier = SierraClient::connect(&addr_str).await.unwrap();
    let stream_id = stream_id_for_order(order_id);
    let events = verifier.scan(&stream_id, "-", "+").await.unwrap();
    assert_eq!(events.len(), 1);

    // 3. Issue valid credit of $10.00 (1000 cents)
    let valid_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        1000,
        "Valid partial credit",
    ));
    handler.handle_command(valid_cmd).await.unwrap();

    // 4. Attempt cumulative credit exceeding cap: $10.00 + $10.01 = $20.01
    let cumulative_excess = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        1001,
        "Exceeds cumulative cap",
    ));
    let err_cumulative = handler.handle_command(cumulative_excess).await.unwrap_err();
    assert_eq!(
        err_cumulative,
        DomainError::CourtesyCreditExceeded(2001, MAX_COURTESY_CREDIT_CENTS)
    );

    // Stream must still have only 2 events
    let events_final = verifier.scan(&stream_id, "-", "+").await.unwrap();
    assert_eq!(events_final.len(), 2);
}

#[tokio::test]
async fn test_concurrency_conflict_preserves_aggregate_state() {
    let (addr, _server) = spawn_mock_sierradb_server().await;
    let addr_str = addr.to_string();

    let mut handler = CommandHandler::connect_sierra(&addr_str).await.unwrap();
    let order_id = "ORD-CONFLICT-1";

    // 1. Create order
    let create_cmd = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-9",
        1200,
        Utc::now(),
    ));
    handler.handle_command(create_cmd).await.unwrap();

    // 2. Out-of-band external client appends an event to the same stream in SierraDB
    let mut external_client = SierraClient::connect(&addr_str).await.unwrap();
    let stream_id = stream_id_for_order(order_id);
    let rogue_payload = r#"{"event_id":"019343ee-5e5d-7a6c-9411-925f617f6bc6","order_id":"ORD-CONFLICT-1","amount_cents":500,"reason":"Concurrent append","issued_at":"2026-11-19T00:00:00Z","version":2}"#;
    external_client
        .append(&stream_id, "CreditIssued", ExpectedVersion::Exact(0), rogue_payload)
        .await
        .unwrap();

    // 3. Handler attempts to issue credit using its local knowledge (expecting stream version 0)
    let credit_cmd = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        500,
        "Local credit attempt",
    ));
    let conflict_err = handler.handle_command(credit_cmd).await.unwrap_err();
    assert!(matches!(conflict_err, DomainError::VersionMismatch { .. }));

    // Handler aggregate state should have invalidated the stale cached aggregate on VersionMismatch
    assert!(
        handler.get_aggregate(order_id).is_none(),
        "Stale cached aggregate must be invalidated on concurrency conflict"
    );

    // Rehydrating the aggregate from SierraDB stream scans the true latest state (including rogue event)
    handler.ensure_aggregate_loaded(order_id).await.unwrap();
    let agg = handler.get_aggregate(order_id).unwrap();
    assert_eq!(agg.courtesy_credit_cents, 500);
    assert_eq!(agg.version, 2);
}

#[tokio::test]
async fn test_command_consumer_with_sierradb_handler() {
    let (addr, _task) = spawn_mock_sierradb_server().await;
    let addr_str = addr.to_string();

    let handler = CommandHandler::connect_sierra(&addr_str).await.unwrap();
    let config = ferrous_cqrses::consumer::Config::default();
    let mut mock_stream = ferrous_cqrses::consumer::InMemoryStreamConsumer::new();
    let publisher = ferrous_cqrses::publisher::InMemoryStreamPublisher::new();

    let order_id = "ORD-CONSUMER-SIERRA-1";
    let cmd1 = OrderCommand::CreateOrder(CreateOrderCommand::new(
        order_id,
        "CUST-100",
        2500,
        Utc::now(),
    ));
    let cmd2 = OrderCommand::IssueCourtesyCredit(IssueCourtesyCreditCommand::new(
        order_id,
        500,
        "Carrier delay concession",
    ));

    mock_stream.enqueue(
        ferrous_cqrses::consumer::ReceivedMessage::from_command(0, 0, &cmd1).unwrap(),
    );
    mock_stream.enqueue(
        ferrous_cqrses::consumer::ReceivedMessage::from_command(1, 0, &cmd2).unwrap(),
    );

    let mut consumer = ferrous_cqrses::consumer::CommandConsumer::with_handler(
        config,
        mock_stream,
        publisher,
        handler,
    );
    let processed = consumer.process_all_available().await.unwrap();
    assert_eq!(processed, 2);

    assert_eq!(consumer.stream().acknowledged_offsets(), vec![0, 1]);
    let agg = consumer.handler().get_aggregate(order_id).unwrap();
    assert_eq!(agg.courtesy_credit_cents, 500);
    assert_eq!(agg.version, 2);

    // Verify events were actually persisted in SierraDB by scanning with a fresh client
    let mut client = SierraClient::connect(&addr_str).await.unwrap();
    let stream_id = stream_id_for_order(order_id);
    let events = client.scan(&stream_id, "-", "+").await.unwrap();
    assert_eq!(events.len(), 2);
}

#[tokio::test]
async fn test_sierradb_connect_strips_url_prefixes() {
    let (addr, _server) = spawn_mock_sierradb_server().await;
    let redis_addr = format!("redis://{addr}");
    let tcp_addr = format!("tcp://{addr}");

    // Connect with redis:// prefix
    let mut client_redis = SierraClient::connect(&redis_addr).await.expect("connect with redis://");
    let ping_result = client_redis.ping().await;
    assert!(ping_result.is_ok(), "PING over redis:// prefix should succeed");

    // Connect with tcp:// prefix
    let mut client_tcp = SierraClient::connect(&tcp_addr).await.expect("connect with tcp://");
    let ping_result_tcp = client_tcp.ping().await;
    assert!(ping_result_tcp.is_ok(), "PING over tcp:// prefix should succeed");
}
