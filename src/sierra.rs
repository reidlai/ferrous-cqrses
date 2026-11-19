//! SierraDB RESP3 client and event sourcing primitives.
//!
//! Provides the native RESP3 wire protocol client [`SierraClient`], the [`EventStore`]
//! abstraction, [`InMemoryEventStore`], and [`SierraDbEventStore`] for durable,
//! distributed event persistence with optimistic concurrency control (`EXPECTED_VERSION`).

use std::collections::HashMap;
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::DomainError;
use crate::events::{
    CreditIssuedEvent, DomainEvent, OrderCancelledEvent, OrderCreatedEvent, OrderStatusUpdatedEvent,
};

/// Strongly typed RESP3 frame representation.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Null,
    Boolean(bool),
    Double(f64),
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
    BlobError(Vec<u8>),
}

impl RespValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::BulkString(b) => std::str::from_utf8(b).ok(),
            Self::SimpleString(s) => Some(s.as_str()),
            Self::Error(e) => Some(e.as_str()),
            _ => None,
        }
    }
}

/// Helper searching for `\r\n` boundary in a byte buffer.
fn find_crlf(buf: &[u8]) -> Option<usize> {
    (0..buf.len().saturating_sub(1)).find(|&i| buf[i] == b'\r' && buf[i + 1] == b'\n')
}

/// Parses a single RESP3 frame from `buf`.
/// Returns `Ok(Some((value, bytes_consumed)))` if a complete frame is present,
/// `Ok(None)` if more bytes are required, or `Err` if the frame is malformed.
pub fn parse_resp_frame(buf: &[u8]) -> Result<Option<(RespValue, usize)>, DomainError> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        // Simple String: +<string>\r\n
        b'+' => {
            if let Some(pos) = find_crlf(&buf[1..]) {
                let s = String::from_utf8_lossy(&buf[1..1 + pos]).to_string();
                Ok(Some((RespValue::SimpleString(s), pos + 3)))
            } else {
                Ok(None)
            }
        }
        // Simple Error: -<error>\r\n
        b'-' => {
            if let Some(pos) = find_crlf(&buf[1..]) {
                let s = String::from_utf8_lossy(&buf[1..1 + pos]).to_string();
                Ok(Some((RespValue::Error(s), pos + 3)))
            } else {
                Ok(None)
            }
        }
        // Integer: :<num>\r\n
        b':' => {
            if let Some(pos) = find_crlf(&buf[1..]) {
                let s = std::str::from_utf8(&buf[1..1 + pos])
                    .map_err(|e| DomainError::Sierra(format!("Invalid integer frame: {e}")))?;
                let n = s.parse::<i64>()
                    .map_err(|e| DomainError::Sierra(format!("Integer parse error: {e}")))?;
                Ok(Some((RespValue::Integer(n), pos + 3)))
            } else {
                Ok(None)
            }
        }
        // Bulk String: $<len>\r\n<data>\r\n
        b'$' => {
            if let Some(pos) = find_crlf(&buf[1..]) {
                let len_str = std::str::from_utf8(&buf[1..1 + pos])
                    .map_err(|e| DomainError::Sierra(format!("Invalid bulk string length: {e}")))?;
                let len = len_str.parse::<isize>()
                    .map_err(|e| DomainError::Sierra(format!("Bulk length parse error: {e}")))?;

                if len < 0 {
                    // Null bulk string: $-1\r\n
                    return Ok(Some((RespValue::Null, pos + 3)));
                }

                let data_len = len as usize;
                let header_len = pos + 3;
                let total_needed = header_len + data_len + 2;

                if buf.len() < total_needed {
                    return Ok(None);
                }

                if &buf[header_len + data_len..total_needed] != b"\r\n" {
                    return Err(DomainError::Sierra("Missing CRLF terminating bulk string".into()));
                }

                let data = buf[header_len..header_len + data_len].to_vec();
                Ok(Some((RespValue::BulkString(data), total_needed)))
            } else {
                Ok(None)
            }
        }
        // Blob Error: !<len>\r\n<data>\r\n
        b'!' => {
            if let Some(pos) = find_crlf(&buf[1..]) {
                let len_str = std::str::from_utf8(&buf[1..1 + pos])
                    .map_err(|e| DomainError::Sierra(format!("Invalid blob error length: {e}")))?;
                let len = len_str.parse::<isize>()
                    .map_err(|e| DomainError::Sierra(format!("Blob error length parse error: {e}")))?;

                if len < 0 {
                    return Ok(Some((RespValue::Null, pos + 3)));
                }

                let data_len = len as usize;
                let header_len = pos + 3;
                let total_needed = header_len + data_len + 2;

                if buf.len() < total_needed {
                    return Ok(None);
                }

                if &buf[header_len + data_len..total_needed] != b"\r\n" {
                    return Err(DomainError::Sierra("Missing CRLF terminating blob error".into()));
                }

                let data = buf[header_len..header_len + data_len].to_vec();
                Ok(Some((RespValue::BlobError(data), total_needed)))
            } else {
                Ok(None)
            }
        }
        // Array: *<len>\r\n<element 1>...<element N>
        b'*' => {
            if let Some(pos) = find_crlf(&buf[1..]) {
                let count_str = std::str::from_utf8(&buf[1..1 + pos])
                    .map_err(|e| DomainError::Sierra(format!("Invalid array length: {e}")))?;
                let count = count_str.parse::<isize>()
                    .map_err(|e| DomainError::Sierra(format!("Array length parse error: {e}")))?;

                if count < 0 {
                    return Ok(Some((RespValue::Null, pos + 3)));
                }

                let mut offset = pos + 3;
                let mut items = Vec::with_capacity(count as usize);

                for _ in 0..count {
                    if offset >= buf.len() {
                        return Ok(None);
                    }
                    match parse_resp_frame(&buf[offset..])? {
                        Some((item, consumed)) => {
                            items.push(item);
                            offset += consumed;
                        }
                        None => return Ok(None),
                    }
                }

                Ok(Some((RespValue::Array(items), offset)))
            } else {
                Ok(None)
            }
        }
        // Map: %<len>\r\n<key1><val1>...<keyN><valN>
        b'%' => {
            if let Some(pos) = find_crlf(&buf[1..]) {
                let count_str = std::str::from_utf8(&buf[1..1 + pos])
                    .map_err(|e| DomainError::Sierra(format!("Invalid map length: {e}")))?;
                let count = count_str.parse::<isize>()
                    .map_err(|e| DomainError::Sierra(format!("Map length parse error: {e}")))?;

                if count < 0 {
                    return Ok(Some((RespValue::Null, pos + 3)));
                }

                let mut offset = pos + 3;
                let mut entries = Vec::with_capacity(count as usize);

                for _ in 0..count {
                    if offset >= buf.len() {
                        return Ok(None);
                    }
                    let key = match parse_resp_frame(&buf[offset..])? {
                        Some((k, consumed)) => {
                            offset += consumed;
                            k
                        }
                        None => return Ok(None),
                    };

                    if offset >= buf.len() {
                        return Ok(None);
                    }
                    let val = match parse_resp_frame(&buf[offset..])? {
                        Some((v, consumed)) => {
                            offset += consumed;
                            v
                        }
                        None => return Ok(None),
                    };

                    entries.push((key, val));
                }

                Ok(Some((RespValue::Map(entries), offset)))
            } else {
                Ok(None)
            }
        }
        // Set: ~<len>\r\n<element 1>...<element N>
        b'~' => {
            if let Some(pos) = find_crlf(&buf[1..]) {
                let count_str = std::str::from_utf8(&buf[1..1 + pos])
                    .map_err(|e| DomainError::Sierra(format!("Invalid set length: {e}")))?;
                let count = count_str.parse::<isize>()
                    .map_err(|e| DomainError::Sierra(format!("Set length parse error: {e}")))?;

                if count < 0 {
                    return Ok(Some((RespValue::Null, pos + 3)));
                }

                let mut offset = pos + 3;
                let mut items = Vec::with_capacity(count as usize);

                for _ in 0..count {
                    if offset >= buf.len() {
                        return Ok(None);
                    }
                    match parse_resp_frame(&buf[offset..])? {
                        Some((item, consumed)) => {
                            items.push(item);
                            offset += consumed;
                        }
                        None => return Ok(None),
                    }
                }

                Ok(Some((RespValue::Set(items), offset)))
            } else {
                Ok(None)
            }
        }
        // Null: _\r\n
        b'_' => {
            if buf.len() >= 3 && &buf[..3] == b"_\r\n" {
                Ok(Some((RespValue::Null, 3)))
            } else if buf.len() < 3 {
                Ok(None)
            } else {
                Err(DomainError::Sierra("Malformed RESP3 Null frame".to_string()))
            }
        }
        // Boolean: #t\r\n or #f\r\n
        b'#' => {
            if buf.len() >= 4 && &buf[2..4] == b"\r\n" {
                let b = match buf[1] {
                    b't' => true,
                    b'f' => false,
                    other => {
                        return Err(DomainError::Sierra(format!(
                            "Invalid boolean character in RESP3: {other}"
                        )))
                    }
                };
                Ok(Some((RespValue::Boolean(b), 4)))
            } else if buf.len() < 4 {
                Ok(None)
            } else {
                Err(DomainError::Sierra("Malformed RESP3 boolean frame".to_string()))
            }
        }
        // Double: ,<float>\r\n
        b',' => {
            if let Some(pos) = find_crlf(&buf[1..]) {
                let s = std::str::from_utf8(&buf[1..1 + pos])
                    .map_err(|e| DomainError::Sierra(format!("Invalid double frame: {e}")))?;
                let val = s.parse::<f64>()
                    .map_err(|e| DomainError::Sierra(format!("Double parse error: {e}")))?;
                Ok(Some((RespValue::Double(val), pos + 3)))
            } else {
                Ok(None)
            }
        }
        other => Err(DomainError::Sierra(format!(
            "Unrecognized RESP3 frame prefix byte: 0x{:02x} ('{}')",
            other, other as char
        ))),
    }
}

/// Encodes a command and its string arguments into standard RESP array-of-bulk-strings wire format.
pub fn encode_command(args: &[&str]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
    }
    buf
}

/// Optimistic concurrency expected stream version for SierraDB `EAPPEND`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedVersion {
    /// Stream must not exist yet; event will be stored at version 0.
    Empty,
    /// Stream must be at exact specified version before appending.
    Exact(u64),
    /// Append regardless of current stream version.
    Any,
}

impl ExpectedVersion {
    pub fn to_arg(&self) -> String {
        match self {
            Self::Empty => "empty".to_string(),
            Self::Exact(v) => v.to_string(),
            Self::Any => "any".to_string(),
        }
    }
}

/// Resolves standard SierraDB stream identifier for an order.
pub fn stream_id_for_order(order_id: &str) -> String {
    if order_id.starts_with("order-") {
        order_id.to_string()
    } else {
        format!("order-{}", order_id)
    }
}

/// Serializes a domain event into its SierraDB event name and JSON payload.
pub fn serialize_event(event: &DomainEvent) -> Result<(&'static str, String), DomainError> {
    match event {
        DomainEvent::OrderCreated(e) => {
            let json = serde_json::to_string(e)?;
            Ok(("OrderCreated", json))
        }
        DomainEvent::CreditIssued(e) => {
            let json = serde_json::to_string(e)?;
            Ok(("CreditIssued", json))
        }
        DomainEvent::OrderStatusUpdated(e) => {
            let json = serde_json::to_string(e)?;
            Ok(("OrderStatusUpdated", json))
        }
        DomainEvent::OrderCancelled(e) => {
            let json = serde_json::to_string(e)?;
            Ok(("OrderCancelled", json))
        }
    }
}

/// Deserializes a domain event from an event name and JSON payload.
pub fn deserialize_event(event_name: &str, payload: &str) -> Result<DomainEvent, DomainError> {
    // 1. Try parsing directly as full DomainEvent envelope
    if let Ok(event) = serde_json::from_str::<DomainEvent>(payload) {
        return Ok(event);
    }

    // 2. Parse inner event struct based on event_name
    match event_name {
        "OrderCreated" | "OrderCreatedEvent" | "order_created" => {
            let inner: OrderCreatedEvent = serde_json::from_str(payload)
                .map_err(|e| DomainError::DeserializationError(e.to_string()))?;
            Ok(DomainEvent::OrderCreated(inner))
        }
        "CreditIssued" | "CreditIssuedEvent" | "credit_issued" => {
            let inner: CreditIssuedEvent = serde_json::from_str(payload)
                .map_err(|e| DomainError::DeserializationError(e.to_string()))?;
            Ok(DomainEvent::CreditIssued(inner))
        }
        "OrderStatusUpdated" | "OrderStatusUpdatedEvent" | "order_status_updated" => {
            let inner: OrderStatusUpdatedEvent = serde_json::from_str(payload)
                .map_err(|e| DomainError::DeserializationError(e.to_string()))?;
            Ok(DomainEvent::OrderStatusUpdated(inner))
        }
        "OrderCancelled" | "OrderCancelledEvent" | "order_cancelled" => {
            let inner: OrderCancelledEvent = serde_json::from_str(payload)
                .map_err(|e| DomainError::DeserializationError(e.to_string()))?;
            Ok(DomainEvent::OrderCancelled(inner))
        }
        other => Err(DomainError::DeserializationError(format!(
            "Unknown domain event name: {other}"
        ))),
    }
}

/// Helper parsing a list of events returned by `ESCAN`.
fn parse_event_list(arr: &[RespValue]) -> Result<Vec<DomainEvent>, DomainError> {
    let mut events = Vec::with_capacity(arr.len());
    for item in arr {
        match item {
            RespValue::Map(map) => {
                let mut event_name = String::new();
                let mut payload = String::new();
                for (k, v) in map {
                    let k_str = match k {
                        RespValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                        RespValue::SimpleString(s) => s.clone(),
                        _ => continue,
                    };
                    match k_str.as_str() {
                        "event_name" | "type" => {
                            if let RespValue::BulkString(b) = v {
                                event_name = String::from_utf8_lossy(b).to_string();
                            } else if let RespValue::SimpleString(s) = v {
                                event_name = s.clone();
                            }
                        }
                        "payload" | "data" => {
                            if let RespValue::BulkString(b) = v {
                                payload = String::from_utf8_lossy(b).to_string();
                            } else if let RespValue::SimpleString(s) = v {
                                payload = s.clone();
                            }
                        }
                        _ => {}
                    }
                }
                if !payload.is_empty() {
                    let evt = deserialize_event(&event_name, &payload)?;
                    events.push(evt);
                }
            }
            RespValue::Array(sub_arr) => {
                if sub_arr.len() >= 2 {
                    let name = sub_arr[0].as_str().unwrap_or("");
                    let data = sub_arr[1].as_str().unwrap_or("");
                    let evt = deserialize_event(name, data)?;
                    events.push(evt);
                }
            }
            RespValue::BulkString(b) => {
                let s = String::from_utf8_lossy(b);
                let evt = deserialize_event("", &s)?;
                events.push(evt);
            }
            RespValue::SimpleString(s) => {
                let evt = deserialize_event("", s)?;
                events.push(evt);
            }
            _ => {}
        }
    }
    Ok(events)
}

/// Native pure-Rust RESP3 client for SierraDB over Tokio `TcpStream`.
#[derive(Debug)]
pub struct SierraClient {
    addr: String,
    stream: tokio::net::TcpStream,
    read_buf: bytes::BytesMut,
}

impl SierraClient {
    /// Connects to a SierraDB instance at the given address.
    pub async fn connect(addr: &str) -> Result<Self, DomainError> {
        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| DomainError::Sierra(format!("Failed to connect to SierraDB at {addr}: {e}")))?;
        Ok(Self {
            addr: addr.to_string(),
            stream,
            read_buf: bytes::BytesMut::with_capacity(8192),
        })
    }

    /// Returns the configured connection address.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Sends a RESP3 command with string arguments and awaits the response frame.
    pub async fn send_command(&mut self, args: &[&str]) -> Result<RespValue, DomainError> {
        let encoded = encode_command(args);

        if let Err(_err) = self.stream.write_all(&encoded).await {
            // Attempt one reconnection on connection error
            self.stream = tokio::net::TcpStream::connect(&self.addr).await.map_err(|e| {
                DomainError::Sierra(format!(
                    "Connection lost and reconnect failed to {}: {}",
                    self.addr, e
                ))
            })?;
            self.read_buf.clear();
            self.stream.write_all(&encoded).await.map_err(|e| {
                DomainError::Sierra(format!("Failed to write to SierraDB at {}: {}", self.addr, e))
            })?;
        }

        loop {
            if let Some((frame, consumed)) = parse_resp_frame(&self.read_buf)? {
                let _ = self.read_buf.split_to(consumed);
                return Ok(frame);
            }

            let n = self.stream.read_buf(&mut self.read_buf).await.map_err(|e| {
                DomainError::Sierra(format!("Failed to read from SierraDB at {}: {}", self.addr, e))
            })?;
            if n == 0 {
                return Err(DomainError::Sierra("SierraDB connection closed by peer".to_string()));
            }
        }
    }

    /// Verifies server responsiveness with a `PING` command.
    pub async fn ping(&mut self) -> Result<(), DomainError> {
        let resp = self.send_command(&["PING"]).await?;
        match resp {
            RespValue::SimpleString(s) if s.eq_ignore_ascii_case("PONG") => Ok(()),
            RespValue::BulkString(b) if b.eq_ignore_ascii_case(b"PONG") => Ok(()),
            RespValue::Error(e) => Err(DomainError::Sierra(format!("PING returned error: {e}"))),
            other => Err(DomainError::Sierra(format!("Unexpected PING response: {:?}", other))),
        }
    }

    /// Queries the current stream version via `ESVER <stream_id>`.
    pub async fn stream_version(&mut self, stream_id: &str) -> Result<Option<u64>, DomainError> {
        let resp = self.send_command(&["ESVER", stream_id]).await?;
        match resp {
            RespValue::Integer(v) => {
                if v < 0 {
                    Ok(None)
                } else {
                    Ok(Some(v as u64))
                }
            }
            RespValue::Null => Ok(None),
            RespValue::BulkString(b) => {
                let s = String::from_utf8_lossy(&b);
                if let Ok(v) = s.trim().parse::<u64>() {
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }
            RespValue::SimpleString(s) => {
                if let Ok(v) = s.trim().parse::<u64>() {
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }
            RespValue::Error(e) => {
                let lower = e.to_lowercase();
                if lower.contains("not_found")
                    || lower.contains("not found")
                    || lower.contains("does not exist")
                {
                    Ok(None)
                } else {
                    Err(DomainError::Sierra(e))
                }
            }
            other => Err(DomainError::Sierra(format!("Unexpected ESVER response: {:?}", other))),
        }
    }

    /// Appends an event to an aggregate stream via `EAPPEND`.
    pub async fn append(
        &mut self,
        stream_id: &str,
        event_name: &str,
        expected_version: ExpectedVersion,
        payload: &str,
    ) -> Result<u64, DomainError> {
        let expected_arg = expected_version.to_arg();
        let resp = self
            .send_command(&[
                "EAPPEND",
                stream_id,
                event_name,
                "EXPECTED_VERSION",
                &expected_arg,
                "PAYLOAD",
                payload,
            ])
            .await?;

        match resp {
            RespValue::Integer(v) => Ok(v as u64),
            RespValue::SimpleString(_) => Ok(0),
            RespValue::Map(entries) => {
                for (k, v) in entries {
                    if let RespValue::BulkString(kb) = &k {
                        if kb == b"stream_version" || kb == b"version" {
                            if let RespValue::Integer(ver) = v {
                                return Ok(ver as u64);
                            }
                        }
                    } else if let RespValue::SimpleString(ks) = &k {
                        if ks == "stream_version" || ks == "version" {
                            if let RespValue::Integer(ver) = v {
                                return Ok(ver as u64);
                            }
                        }
                    }
                }
                Ok(0)
            }
            RespValue::Array(items) => {
                if let Some(RespValue::Integer(v)) = items.first() {
                    Ok(*v as u64)
                } else {
                    Ok(0)
                }
            }
            RespValue::Error(err_msg) => {
                let lower = err_msg.to_lowercase();
                if lower.contains("wrong_expected_version")
                    || lower.contains("wrongversion")
                    || lower.contains("version mismatch")
                {
                    let expected = match expected_version {
                        ExpectedVersion::Exact(v) => v,
                        ExpectedVersion::Empty => 0,
                        ExpectedVersion::Any => 0,
                    };
                    Err(DomainError::VersionMismatch {
                        expected,
                        actual: u64::MAX,
                    })
                } else if lower.contains("already exists") {
                    Err(DomainError::OrderAlreadyExists(stream_id.to_string()))
                } else {
                    Err(DomainError::Sierra(err_msg))
                }
            }
            other => Err(DomainError::Sierra(format!("Unexpected EAPPEND response: {:?}", other))),
        }
    }

    /// Scans events from a stream within range via `ESCAN <stream_id> <start> <end>`.
    pub async fn scan(
        &mut self,
        stream_id: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<DomainEvent>, DomainError> {
        let resp = self.send_command(&["ESCAN", stream_id, start, end]).await?;
        match resp {
            RespValue::Null => Ok(vec![]),
            RespValue::Array(arr) => parse_event_list(&arr),
            RespValue::Map(entries) => {
                for (k, v) in entries {
                    let key_str = match &k {
                        RespValue::BulkString(b) => std::str::from_utf8(b).unwrap_or(""),
                        RespValue::SimpleString(s) => s.as_str(),
                        _ => "",
                    };
                    if key_str == "events" {
                        if let RespValue::Array(arr) = v {
                            return parse_event_list(&arr);
                        }
                    }
                }
                Ok(vec![])
            }
            RespValue::Error(e) => {
                let lower = e.to_lowercase();
                if lower.contains("not_found")
                    || lower.contains("not found")
                    || lower.contains("does not exist")
                {
                    Ok(vec![])
                } else {
                    Err(DomainError::Sierra(e))
                }
            }
            other => Err(DomainError::Sierra(format!("Unexpected ESCAN response: {:?}", other))),
        }
    }
}

/// Abstract event store interface for pure-Rust event sourcing.
#[async_trait]
pub trait EventStore: Send + Sync + std::fmt::Debug {
    /// Appends a domain event to an aggregate stream with optimistic concurrency control.
    async fn append(
        &self,
        stream_id: &str,
        event: &DomainEvent,
        expected_version: ExpectedVersion,
    ) -> Result<u64, DomainError>;

    /// Scans all events in an aggregate stream from start to end (`- +`).
    async fn scan(&self, stream_id: &str) -> Result<Vec<DomainEvent>, DomainError>;

    /// Queries the current stream version if the stream exists.
    async fn stream_version(&self, stream_id: &str) -> Result<Option<u64>, DomainError>;
}

/// In-memory implementation of [`EventStore`] for standalone unit testing and backward compatibility.
#[derive(Debug, Default)]
pub struct InMemoryEventStore {
    streams: tokio::sync::RwLock<HashMap<String, Vec<DomainEvent>>>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_streams(initial: HashMap<String, Vec<DomainEvent>>) -> Self {
        Self {
            streams: tokio::sync::RwLock::new(initial),
        }
    }

    pub async fn get_stream(&self, stream_id: &str) -> Vec<DomainEvent> {
        let streams = self.streams.read().await;
        streams.get(stream_id).cloned().unwrap_or_default()
    }
}

#[async_trait]
impl EventStore for InMemoryEventStore {
    async fn append(
        &self,
        stream_id: &str,
        event: &DomainEvent,
        expected_version: ExpectedVersion,
    ) -> Result<u64, DomainError> {
        let mut streams = self.streams.write().await;
        let events = streams.entry(stream_id.to_string()).or_default();
        let current_count = events.len() as u64;

        match expected_version {
            ExpectedVersion::Empty => {
                if current_count > 0 {
                    return Err(DomainError::OrderAlreadyExists(stream_id.to_string()));
                }
            }
            ExpectedVersion::Exact(expected) => {
                let actual = if current_count == 0 {
                    return Err(DomainError::VersionMismatch {
                        expected,
                        actual: u64::MAX,
                    });
                } else {
                    current_count - 1
                };

                if actual != expected {
                    return Err(DomainError::VersionMismatch {
                        expected,
                        actual,
                    });
                }
            }
            ExpectedVersion::Any => {}
        }

        events.push(event.clone());
        let new_ver = events.len() as u64 - 1;
        Ok(new_ver)
    }

    async fn scan(&self, stream_id: &str) -> Result<Vec<DomainEvent>, DomainError> {
        let streams = self.streams.read().await;
        Ok(streams.get(stream_id).cloned().unwrap_or_default())
    }

    async fn stream_version(&self, stream_id: &str) -> Result<Option<u64>, DomainError> {
        let streams = self.streams.read().await;
        Ok(streams.get(stream_id).and_then(|events| {
            if events.is_empty() {
                None
            } else {
                Some(events.len() as u64 - 1)
            }
        }))
    }
}

/// Durable, distributed event store backed by SierraDB over RESP3 TCP connection.
#[derive(Debug)]
pub struct SierraDbEventStore {
    addr: String,
    client: tokio::sync::Mutex<SierraClient>,
}

impl SierraDbEventStore {
    /// Connects to a SierraDB instance at `addr` (e.g. `127.0.0.1:9090`).
    pub async fn connect(addr: impl Into<String>) -> Result<Self, DomainError> {
        let addr_str = addr.into();
        let client = SierraClient::connect(&addr_str).await?;
        Ok(Self {
            addr: addr_str,
            client: tokio::sync::Mutex::new(client),
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }
}

#[async_trait]
impl EventStore for SierraDbEventStore {
    async fn append(
        &self,
        stream_id: &str,
        event: &DomainEvent,
        expected_version: ExpectedVersion,
    ) -> Result<u64, DomainError> {
        let (event_name, payload) = serialize_event(event)?;
        let mut client = self.client.lock().await;
        client.append(stream_id, event_name, expected_version, &payload).await
    }

    async fn scan(&self, stream_id: &str) -> Result<Vec<DomainEvent>, DomainError> {
        let mut client = self.client.lock().await;
        client.scan(stream_id, "-", "+").await
    }

    async fn stream_version(&self, stream_id: &str) -> Result<Option<u64>, DomainError> {
        let mut client = self.client.lock().await;
        client.stream_version(stream_id).await
    }
}

/// Backward compatibility alias for [`SierraDbEventStore`].
pub type SierraEventStore = SierraDbEventStore;
