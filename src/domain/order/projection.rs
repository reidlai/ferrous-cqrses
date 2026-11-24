//! Canonical `olist_orders` base contract and consolidated order projection (AD-34).
//!
//! Emits snapshot representations of [`OrderAggregate`] state to `orders/consolidated`
//! and Delta Lake `orders_current_state` using the exact 8-column Kaggle `olist_orders`
//! base schema extended with the 6-column CQRS concession/versioning overlay.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::aggregate::{OrderAggregate, OrderStatus};
use super::error::DomainError;

fn default_timestamp() -> DateTime<Utc> {
    Utc::now()
}

/// Canonical 8-column Kaggle `olist_orders` base record (AD-34).
///
/// Matches `olist_orders_dataset.csv`, `ferrous-cx` `olist_orders` Delta table, and
/// `stream-replay-rs` JSON payloads emitted to `olist/orders_stream`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OlistOrderRecord {
    pub order_id: String,
    pub customer_id: String,
    pub order_status: OrderStatus,
    pub order_purchase_timestamp: DateTime<Utc>,
    #[serde(default)]
    pub order_approved_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub order_delivered_carrier_date: Option<DateTime<Utc>>,
    #[serde(default)]
    pub order_delivered_customer_date: Option<DateTime<Utc>>,
    pub order_estimated_delivery_date: DateTime<Utc>,
}

/// Consolidated order projection snapshot emitted to `orders/consolidated` and written to
/// Delta Lake `orders_current_state` (AD-34).
///
/// Carries the exact 8 canonical `olist_orders` base columns plus the 6-column CQRS
/// concession & versioning overlay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsolidatedOrderProjection {
    // --- Canonical 8-column `olist_orders` base (AD-34) ---
    pub order_id: String,
    #[serde(default)]
    pub customer_id: String,
    #[serde(alias = "status")]
    pub order_status: OrderStatus,
    #[serde(default = "default_timestamp")]
    pub order_purchase_timestamp: DateTime<Utc>,
    #[serde(default)]
    pub order_approved_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub order_delivered_carrier_date: Option<DateTime<Utc>>,
    #[serde(default, alias = "actual_delivery")]
    pub order_delivered_customer_date: Option<DateTime<Utc>>,
    #[serde(default = "default_timestamp", alias = "delivery_sla")]
    pub order_estimated_delivery_date: DateTime<Utc>,

    // --- 6-column CQRS / concession overlay (AD-34) ---
    #[serde(default)]
    pub total_amount_cents: i64,
    #[serde(default)]
    pub total_freight_cents: i64,
    #[serde(default, alias = "credit_issued", alias = "credit_issued_cents")]
    pub courtesy_credit_cents: i64,
    #[serde(default)]
    pub credit_delta: i64,
    #[serde(default)]
    pub version: u64,
    #[serde(default = "default_timestamp")]
    pub updated_at: DateTime<Utc>,
}

impl ConsolidatedOrderProjection {
    /// Constructs a canonical `AD-34` consolidated projection snapshot from an [`OrderAggregate`],
    /// capturing the credit concession delta (if any) and event timestamp.
    pub fn from_aggregate(
        agg: &OrderAggregate,
        credit_delta: i64,
        updated_at: DateTime<Utc>,
    ) -> Self {
        let purchase_ts = if agg.order_purchase_timestamp == DateTime::UNIX_EPOCH {
            updated_at
        } else {
            agg.order_purchase_timestamp
        };

        Self {
            order_id: agg.order_id.clone(),
            customer_id: agg.customer_id.clone(),
            order_status: agg.status,
            order_purchase_timestamp: purchase_ts,
            order_approved_at: agg.order_approved_at,
            order_delivered_carrier_date: agg.order_delivered_carrier_date,
            order_delivered_customer_date: agg.actual_delivery,
            order_estimated_delivery_date: agg.delivery_sla,
            total_amount_cents: agg.total_amount_cents,
            total_freight_cents: agg.total_freight_cents,
            courtesy_credit_cents: agg.courtesy_credit_cents,
            credit_delta,
            version: agg.version,
            updated_at,
        }
    }

    /// Promotes a raw 8-column [`OlistOrderRecord`] into a [`ConsolidatedOrderProjection`]
    /// with default zeroed CQRS overlay fields (`AD-34`).
    pub fn from_olist_order(order: OlistOrderRecord) -> Self {
        let updated_at = order.order_purchase_timestamp;
        Self {
            order_id: order.order_id,
            customer_id: order.customer_id,
            order_status: order.order_status,
            order_purchase_timestamp: order.order_purchase_timestamp,
            order_approved_at: order.order_approved_at,
            order_delivered_carrier_date: order.order_delivered_carrier_date,
            order_delivered_customer_date: order.order_delivered_customer_date,
            order_estimated_delivery_date: order.order_estimated_delivery_date,
            total_amount_cents: 0,
            total_freight_cents: 0,
            courtesy_credit_cents: 0,
            credit_delta: 0,
            version: 0,
            updated_at,
        }
    }

    /// Serializes this projection snapshot into standard JSON bytes.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, DomainError> {
        serde_json::to_vec(self).map_err(DomainError::from)
    }
}
