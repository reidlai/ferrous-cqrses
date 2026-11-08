use serde::{de::DeserializeOwned, Serialize};

use crate::error::DomainError;

/// Serializes any supported domain payload into a JSON string envelope.
pub fn to_json<T: Serialize>(value: &T) -> Result<String, DomainError> {
    serde_json::to_string(value).map_err(|e| DomainError::SerializationError(e.to_string()))
}

/// Serializes any supported domain payload into a pretty-printed JSON string.
pub fn to_json_pretty<T: Serialize>(value: &T) -> Result<String, DomainError> {
    serde_json::to_string_pretty(value).map_err(|e| DomainError::SerializationError(e.to_string()))
}

/// Deserializes a JSON string slice into a strongly typed domain payload.
pub fn from_json<T: DeserializeOwned>(json: &str) -> Result<T, DomainError> {
    serde_json::from_str(json).map_err(|e| DomainError::DeserializationError(e.to_string()))
}

/// Serializes any supported domain payload into compact Bincode bytes.
pub fn to_bincode<T: Serialize>(value: &T) -> Result<Vec<u8>, DomainError> {
    bincode::serialize(value).map_err(|e| DomainError::SerializationError(e.to_string()))
}

/// Deserializes compact Bincode binary bytes into a strongly typed domain payload.
pub fn from_bincode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DomainError> {
    bincode::deserialize(bytes).map_err(|e| DomainError::DeserializationError(e.to_string()))
}
