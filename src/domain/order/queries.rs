//! Canonical CQRS Query contracts for the Order bounded context (AD-32, CAP-1).
//!
//! `ferrous-cqrses` owns both Write-side (`Command`, `Aggregate`, `DomainEvent`) and
//! Read-side (`GetOrderQuery`, `ListOrdersQuery`) domain contracts. `ferrous-api` maps
//! incoming HTTP requests into these strongly typed Query structs and renders View DTOs.

use serde::{Deserialize, Serialize};

use super::error::DomainError;

/// Default page size for [`ListOrdersQuery`].
pub const DEFAULT_LIST_ORDERS_LIMIT: u32 = 50;

/// Maximum permitted page size for [`ListOrdersQuery`].
pub const MAX_LIST_ORDERS_LIMIT: u32 = 500;

/// Read-only CQRS Query to retrieve a single order's materialized View by `order_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetOrderQuery {
    pub order_id: String,
}

impl GetOrderQuery {
    /// Creates a new `GetOrderQuery`, trimming surrounding whitespace.
    pub fn new(order_id: impl Into<String>) -> Self {
        Self {
            order_id: order_id.into().trim().to_string(),
        }
    }

    /// Validates that `order_id` is non-empty.
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.order_id.trim().is_empty() {
            return Err(DomainError::Generic(
                "order_id cannot be blank in GetOrderQuery".to_string(),
            ));
        }
        Ok(())
    }
}

/// Read-only CQRS Query to retrieve a paginated, optionally filtered list of order Views.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListOrdersQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub customer_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub limit: u32,
    pub offset: u32,
}

impl Default for ListOrdersQuery {
    fn default() -> Self {
        Self {
            customer_id: None,
            status: None,
            limit: DEFAULT_LIST_ORDERS_LIMIT,
            offset: 0,
        }
    }
}

impl ListOrdersQuery {
    /// Creates a new `ListOrdersQuery` with optional filters and bounded pagination.
    pub fn new(
        customer_id: Option<String>,
        status: Option<String>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Self {
        Self {
            customer_id: customer_id
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            status: status
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            limit: limit.unwrap_or(DEFAULT_LIST_ORDERS_LIMIT),
            offset: offset.unwrap_or(0),
        }
    }

    /// Validates pagination bounds (`1 <= limit <= MAX_LIST_ORDERS_LIMIT`).
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.limit == 0 || self.limit > MAX_LIST_ORDERS_LIMIT {
            return Err(DomainError::Generic(format!(
                "limit must be between 1 and {MAX_LIST_ORDERS_LIMIT}, got {}",
                self.limit
            )));
        }
        Ok(())
    }
}
