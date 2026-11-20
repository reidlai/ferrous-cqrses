//! # Bounded Context Domain Modules
//!
//! Organizes Domain-Driven Design (DDD) aggregate roots, commands, events, errors,
//! and command handlers per domain bounded context.

pub mod order;

pub use order::*;
