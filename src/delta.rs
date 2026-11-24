//! Delta Lake projection and snapshot utilities for the write/projection tier.
//!
//! Provides utilities for generating and writing Delta Lake table snapshots
//! (`_delta_log/00000000000000000000.json` and associated data files) for
//! projection storage and verification.

use std::fs;
use std::path::Path;
use serde::{Deserialize, Serialize};

/// Parsed metadata from a `_delta_log/*.json` transaction log directory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaLogMetadata {
    /// Highest commit version discovered in `_delta_log`.
    pub version: u64,
    /// Table ID from `metaData` action, if present.
    pub table_id: Option<String>,
    /// Minimum reader version from `protocol` action.
    pub min_reader_version: u32,
    /// Minimum writer version from `protocol` action.
    pub min_writer_version: u32,
    /// Active data file paths referenced by `add` actions (excluding `remove` tombstones).
    pub active_files: Vec<String>,
    /// Total number of commit JSON files inspected.
    pub commit_files_count: usize,
}

/// Canonical AD-34 Delta Lake struct schema for `orders_current_state`
/// (8 `olist_orders` base columns + 6 CQRS/concession overlay columns).
pub const ORDERS_CURRENT_STATE_SCHEMA_JSON: &str = r#"{"type":"struct","fields":[{"name":"order_id","type":"string","nullable":false,"metadata":{}},{"name":"customer_id","type":"string","nullable":false,"metadata":{}},{"name":"order_status","type":"string","nullable":false,"metadata":{}},{"name":"order_purchase_timestamp","type":"timestamp","nullable":false,"metadata":{}},{"name":"order_approved_at","type":"timestamp","nullable":true,"metadata":{}},{"name":"order_delivered_carrier_date","type":"timestamp","nullable":true,"metadata":{}},{"name":"order_delivered_customer_date","type":"timestamp","nullable":true,"metadata":{}},{"name":"order_estimated_delivery_date","type":"timestamp","nullable":false,"metadata":{}},{"name":"total_amount_cents","type":"long","nullable":false,"metadata":{}},{"name":"total_freight_cents","type":"long","nullable":false,"metadata":{}},{"name":"courtesy_credit_cents","type":"long","nullable":false,"metadata":{}},{"name":"credit_delta","type":"long","nullable":false,"metadata":{}},{"name":"version","type":"long","nullable":false,"metadata":{}},{"name":"updated_at","type":"timestamp","nullable":false,"metadata":{}}]}"#;

/// Helper utility to write a valid Delta Lake directory (`_delta_log/00000000000000000000.json`
/// plus snapshot data file) for testing and local bootstrapping in the projection tier.
pub fn write_delta_table_snapshot<T: Serialize>(
    table_dir: impl AsRef<Path>,
    records: &[T],
) -> Result<DeltaLogMetadata, std::io::Error> {
    let dir = table_dir.as_ref();
    let delta_log_dir = dir.join("_delta_log");
    fs::create_dir_all(&delta_log_dir)?;

    let data_file_name = "part-00000-orders-snapshot.json";
    let data_file_path = dir.join(data_file_name);
    let data_json = serde_json::to_string(records)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    fs::write(&data_file_path, data_json.as_bytes())?;

    let escaped_schema = serde_json::to_string(ORDERS_CURRENT_STATE_SCHEMA_JSON)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let commit_path = delta_log_dir.join("00000000000000000000.json");
    let commit_lines = format!(
        "{{\"protocol\":{{\"minReaderVersion\":1,\"minWriterVersion\":2}}}}\n\
         {{\"metaData\":{{\"id\":\"delta-orders-current-state\",\"name\":\"orders_current_state\",\"schemaString\":{escaped_schema}}}}}\n\
         {{\"add\":{{\"path\":\"{data_file_name}\",\"size\":{},\"modificationTime\":1730000000000,\"dataChange\":true}}}}\n",
        data_json.len()
    );
    fs::write(&commit_path, commit_lines.as_bytes())?;

    Ok(DeltaLogMetadata {
        version: 0,
        table_id: Some("delta-orders-current-state".to_string()),
        min_reader_version: 1,
        min_writer_version: 2,
        active_files: vec![data_file_name.to_string()],
        commit_files_count: 1,
    })
}
