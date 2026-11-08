//! Consolidated order projection contracts and serialization for streaming publishing.
//!
//! Emits snapshot representations of [`OrderAggregate`] state to the `orders/consolidated`
//! topic on Apache Iggy for downstream consumption by Apache Arroyo and analytics pipelines.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::aggregate::{OrderAggregate, OrderStatus};
use crate::error::DomainError;

fn default_delivery_sla() -> DateTime<Utc> {
    Utc::now()
}

/// Consolidated order projection snapshot emitted to `orders/consolidated`
/// topic on Apache Iggy for downstream real-time streaming engines (e.g. Arroyo).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsolidatedOrderProjection {
    pub order_id: String,
    #[serde(default)]
    pub customer_id: String,
    pub status: OrderStatus,
    #[serde(default)]
    pub total_freight_cents: i64,
    #[serde(default)]
    pub courtesy_credit_cents: i64,
    pub credit_issued: i64,
    #[serde(default)]
    pub credit_delta: i64,
    #[serde(default = "default_delivery_sla")]
    pub delivery_sla: DateTime<Utc>,
    #[serde(default)]
    pub actual_delivery: Option<DateTime<Utc>>,
    pub version: u64,
    pub updated_at: DateTime<Utc>,
}

impl ConsolidatedOrderProjection {
    /// Constructs a consolidated projection snapshot from an [`OrderAggregate`],
    /// capturing the credit concession delta (if any) and event timestamp.
    ///
    /// Note: `credit_issued` represents the order's cumulative courtesy credit concessions
    /// in integer cents (matching the Delta Lake schema contract `orders_current_state.sql`),
    /// while `credit_delta` tracks the incremental concession granted in this specific event.
    pub fn from_aggregate(
        agg: &OrderAggregate,
        credit_delta: i64,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            order_id: agg.order_id.clone(),
            customer_id: agg.customer_id.clone(),
            status: agg.status,
            total_freight_cents: agg.total_freight_cents,
            courtesy_credit_cents: agg.courtesy_credit_cents,
            credit_issued: agg.courtesy_credit_cents,
            credit_delta,
            delivery_sla: agg.delivery_sla,
            actual_delivery: agg.actual_delivery,
            version: agg.version,
            updated_at,
        }
    }

    /// Serializes this projection snapshot into standard JSON bytes.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, DomainError> {
        serde_json::to_vec(self).map_err(DomainError::from)
    }
}
