use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::error::DomainError;

static STATE: Mutex<(u64, u16)> = Mutex::new((0, 0));

/// Generates a strictly monotonic UUIDv7 identifier.
///
/// Guarantees that consecutive calls yield strictly increasing IDs (`id_1 < id_2 < id_3`),
/// even when invoked within the same millisecond, concurrently across threads, or under system clock skew.
pub fn generate_uuidv7() -> Uuid {
    // Current Unix timestamp in milliseconds
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Synchronized update to monotonic timestamp and counter to guarantee thread safety
    let (ts_ms, counter_val) = {
        let mut state = STATE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if now_ms > state.0 {
            state.0 = now_ms;
            state.1 = 0;
            (now_ms, 0)
        } else {
            // Same millisecond or backward clock skew: keep `state.0` and increment counter
            if state.1 < 0x0FFF {
                state.1 += 1;
                (state.0, state.1)
            } else {
                // Counter overflowed 12 bits: advance 1ms forward to preserve strict monotonicity
                state.0 = state.0.saturating_add(1);
                state.1 = 0;
                (state.0, 0)
            }
        }
    };

    // Construct 128-bit UUIDv7 representation:
    // - 48 bits: timestamp in milliseconds (big-endian)
    // - 4 bits: version 7 (0b0111)
    // - 12 bits: sequence counter
    // - 2 bits: variant (0b10)
    // - 62 bits: pseudo-random entropy
    let mut bytes = [0u8; 16];

    // 48-bit timestamp (bytes 0..6)
    let ts_bytes = ts_ms.to_be_bytes();
    bytes[0..6].copy_from_slice(&ts_bytes[2..8]);

    // 4-bit version 7 + 12-bit sequence counter (bytes 6..8)
    let ver_counter = 0x7000u16 | (counter_val & 0x0FFF);
    bytes[6..8].copy_from_slice(&ver_counter.to_be_bytes());

    // 2-bit RFC 4122 variant (0b10) + 62 bits entropy (bytes 8..16)
    // Use uuid's internal random entropy or pseudo-random mix
    let rand_u64 = Uuid::new_v4().as_u128() as u64;
    let var_entropy = 0x8000_0000_0000_0000u64 | (rand_u64 & 0x3FFF_FFFF_FFFF_FFFF);
    bytes[8..16].copy_from_slice(&var_entropy.to_be_bytes());

    Uuid::from_bytes(bytes)
}

/// Validates whether a given UUID adheres to the UUIDv7 specification.
pub fn is_valid_uuidv7(uuid: &Uuid) -> bool {
    let bytes = uuid.as_bytes();
    // Check version == 7 (upper 4 bits of byte 6)
    let version = (bytes[6] >> 4) == 7;
    // Check RFC 4122 variant (upper 2 bits of byte 8 == 0b10)
    let variant = (bytes[8] >> 6) == 2;
    version && variant
}

/// Parses a string slice into a validated UUIDv7.
pub fn parse_uuidv7(input: &str) -> Result<Uuid, DomainError> {
    let parsed = Uuid::parse_str(input)
        .map_err(|e| DomainError::InvalidUuid(format!("Failed to parse UUID: {e}")))?;
    if !is_valid_uuidv7(&parsed) {
        return Err(DomainError::InvalidUuid(format!(
            "UUID '{input}' is not a valid UUIDv7"
        )));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_uuidv7_ordering() {
        let id1 = generate_uuidv7();
        let id2 = generate_uuidv7();
        let id3 = generate_uuidv7();

        assert!(
            id1 < id2,
            "id1 ({id1}) must be strictly less than id2 ({id2})"
        );
        assert!(
            id2 < id3,
            "id2 ({id2}) must be strictly less than id3 ({id3})"
        );
        assert!(is_valid_uuidv7(&id1));
        assert!(is_valid_uuidv7(&id2));
        assert!(is_valid_uuidv7(&id3));
    }

    #[test]
    fn test_consecutive_sequence_monotonicity() {
        let mut prev = generate_uuidv7();
        for _ in 0..1000 {
            let next = generate_uuidv7();
            assert!(
                prev < next,
                "Generated UUIDv7 sequence must be strictly increasing"
            );
            prev = next;
        }
    }

    #[test]
    fn test_parse_and_validate_uuidv7() {
        let id = generate_uuidv7();
        let id_str = id.to_string();
        let parsed = parse_uuidv7(&id_str).expect("Valid UUIDv7 string must parse cleanly");
        assert_eq!(id, parsed);

        // V4 uuid should fail v7 validation
        let v4_id = Uuid::new_v4();
        assert!(!is_valid_uuidv7(&v4_id));
        assert!(parse_uuidv7(&v4_id.to_string()).is_err());
    }
}
