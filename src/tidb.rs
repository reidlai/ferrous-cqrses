//! Transactional TiDB Projection Store for `order-projection` (Story 23.3, AD-36).
//!
//! Replaces file-based Delta Lake snapshot writes with ACID `INSERT ... ON DUPLICATE KEY UPDATE`
//! against TiDB `orders_current_state`, while keeping an [`InMemoryProjectionStore`] for fast
//! zero-I/O unit tests.

use async_trait::async_trait;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use crate::domain::order::error::DomainError;
use crate::domain::order::projection::ConsolidatedOrderProjection;

/// Storage trait for committing materialized [`ConsolidatedOrderProjection`] snapshots.
#[async_trait]
pub trait OrderProjectionStore: Send + Sync {
    async fn upsert_projection(
        &self,
        projection: &ConsolidatedOrderProjection,
    ) -> Result<(), DomainError>;
}

/// Zero-I/O in-memory [`OrderProjectionStore`] for unit and integration tests.
#[derive(Debug, Default)]
pub struct InMemoryProjectionStore {
    rows: RwLock<HashMap<String, ConsolidatedOrderProjection>>,
}

impl InMemoryProjectionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, order_id: &str) -> Option<ConsolidatedOrderProjection> {
        self.rows
            .read()
            .ok()
            .and_then(|g| g.get(order_id).cloned())
    }

    pub fn len(&self) -> usize {
        self.rows.read().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl OrderProjectionStore for InMemoryProjectionStore {
    async fn upsert_projection(
        &self,
        projection: &ConsolidatedOrderProjection,
    ) -> Result<(), DomainError> {
        let mut guard = self
            .rows
            .write()
            .map_err(|e| DomainError::Generic(format!("projection store lock poisoned: {e}")))?;
        guard.insert(projection.order_id.clone(), projection.clone());
        Ok(())
    }
}

/// Live TiDB transactional [`OrderProjectionStore`] backed by `sqlx::MySqlPool` (AD-36).
#[derive(Debug, Clone)]
pub struct TiDbProjectionStore {
    pool: MySqlPool,
}

impl TiDbProjectionStore {
    pub async fn connect(tidb_url: &str) -> Result<Self, DomainError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect(tidb_url)
            .await
            .map_err(|e| DomainError::Generic(format!("TiDB connect failed: {e}")))?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: MySqlPool) -> Self {
        Self { pool }
    }
}

fn fmt_mysql_dt(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

#[async_trait]
impl OrderProjectionStore for TiDbProjectionStore {
    async fn upsert_projection(
        &self,
        p: &ConsolidatedOrderProjection,
    ) -> Result<(), DomainError> {
        let items_json = if p.items.is_empty() {
            None
        } else {
            serde_json::to_string(&p.items).ok()
        };
        let payments_json = if p.payments.is_empty() {
            None
        } else {
            serde_json::to_string(&p.payments).ok()
        };

        sqlx::query(
            "INSERT INTO orders_current_state \
             (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, \
              order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date, \
              total_amount_cents, total_freight_cents, courtesy_credit_cents, credit_delta, \
              version, updated_at, items_json, payments_json) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
             customer_id = VALUES(customer_id), order_status = VALUES(order_status), \
             order_approved_at = COALESCE(VALUES(order_approved_at), order_approved_at), \
             order_delivered_carrier_date = COALESCE(VALUES(order_delivered_carrier_date), order_delivered_carrier_date), \
             order_delivered_customer_date = COALESCE(VALUES(order_delivered_customer_date), order_delivered_customer_date), \
             total_amount_cents = VALUES(total_amount_cents), \
             total_freight_cents = VALUES(total_freight_cents), \
             courtesy_credit_cents = VALUES(courtesy_credit_cents), \
             credit_delta = VALUES(credit_delta), \
             version = VALUES(version), updated_at = VALUES(updated_at), \
             items_json = COALESCE(VALUES(items_json), items_json), \
             payments_json = COALESCE(VALUES(payments_json), payments_json)",
        )
        .bind(&p.order_id)
        .bind(&p.customer_id)
        .bind(p.order_status.as_olist_str())
        .bind(fmt_mysql_dt(p.order_purchase_timestamp))
        .bind(p.order_approved_at.map(fmt_mysql_dt))
        .bind(p.order_delivered_carrier_date.map(fmt_mysql_dt))
        .bind(p.order_delivered_customer_date.map(fmt_mysql_dt))
        .bind(fmt_mysql_dt(p.order_estimated_delivery_date))
        .bind(p.total_amount_cents)
        .bind(p.total_freight_cents)
        .bind(p.courtesy_credit_cents)
        .bind(p.credit_delta)
        .bind(p.version)
        .bind(fmt_mysql_dt(p.updated_at))
        .bind(items_json)
        .bind(payments_json)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::Generic(format!("TiDB orders_current_state upsert failed: {e}")))?;

        Ok(())
    }
}
