//! Unit tests for canonical CQRS Query contracts (`GetOrderQuery`, `ListOrdersQuery`)
//! owned by `ferrous-cqrses` (Story 19.1, CAP-1, AD-32).

use ferrous_cqrses::queries::{
    GetOrderQuery, ListOrdersQuery, DEFAULT_LIST_ORDERS_LIMIT, MAX_LIST_ORDERS_LIMIT,
};

#[test]
fn test_get_order_query_trims_and_validates() {
    let q = GetOrderQuery::new("  ord-9901  ");
    assert_eq!(q.order_id, "ord-9901");
    assert!(q.validate().is_ok());

    let blank = GetOrderQuery::new("   ");
    assert!(blank.validate().is_err());
}

#[test]
fn test_list_orders_query_defaults_and_bounds() {
    let def = ListOrdersQuery::default();
    assert_eq!(def.limit, DEFAULT_LIST_ORDERS_LIMIT);
    assert_eq!(def.offset, 0);
    assert!(def.customer_id.is_none());
    assert!(def.status.is_none());
    assert!(def.validate().is_ok());

    let filtered = ListOrdersQuery::new(
        Some("  cust-42 ".to_string()),
        Some("   ".to_string()),
        Some(25),
        Some(10),
    );
    assert_eq!(filtered.customer_id.as_deref(), Some("cust-42"));
    assert!(filtered.status.is_none());
    assert_eq!(filtered.limit, 25);
    assert_eq!(filtered.offset, 10);
    assert!(filtered.validate().is_ok());

    let zero_limit = ListOrdersQuery::new(None, None, Some(0), None);
    assert!(zero_limit.validate().is_err());

    let over_limit = ListOrdersQuery::new(None, None, Some(MAX_LIST_ORDERS_LIMIT + 1), None);
    assert!(over_limit.validate().is_err());
}

#[test]
fn test_query_contracts_serde_roundtrip() {
    let get_q = GetOrderQuery::new("ord-serde-1");
    let json = serde_json::to_string(&get_q).unwrap();
    let decoded: GetOrderQuery = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, get_q);

    let list_q = ListOrdersQuery::new(Some("cust-7".into()), Some("delivered".into()), Some(10), Some(5));
    let json = serde_json::to_string(&list_q).unwrap();
    let decoded: ListOrdersQuery = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, list_q);
}
