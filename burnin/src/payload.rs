//! Test payload generation with CRC32 integrity verification.

/// Generate a test payload of the specified size with an embedded sequence number.
///
/// The payload format is: 8-byte sequence (big-endian) + 8-byte timestamp (nanos) + padding.
pub fn generate(size: usize, sequence: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(size.max(16));
    payload.extend_from_slice(&sequence.to_be_bytes());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    payload.extend_from_slice(&now.to_be_bytes());
    if payload.len() < size {
        payload.resize(size, 0xAB);
    }
    payload
}

/// Generate a payload with CRC32 hash. Returns (body, crc_hex).
pub fn encode(_sdk: &str, _run_id: &str, _channel: &str, seq: u64, size: usize) -> (Vec<u8>, String) {
    let body = generate(size, seq);
    let crc = crc32fast::hash(&body);
    let crc_hex = format!("{:08x}", crc);
    (body, crc_hex)
}

/// Verify CRC32 integrity of a payload.
pub fn verify_crc(payload: &[u8], expected_crc_hex: &str) -> bool {
    let actual_crc = crc32fast::hash(payload);
    let actual_hex = format!("{:08x}", actual_crc);
    actual_hex == expected_crc_hex
}

/// Extract the sequence number from a test payload.
pub fn extract_sequence(payload: &[u8]) -> Option<u64> {
    if payload.len() < 8 {
        return None;
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&payload[..8]);
    Some(u64::from_be_bytes(bytes))
}

/// Extract the timestamp (nanoseconds) from a test payload.
pub fn extract_timestamp_nanos(payload: &[u8]) -> Option<u64> {
    if payload.len() < 16 {
        return None;
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&payload[8..16]);
    Some(u64::from_be_bytes(bytes))
}

/// Calculate latency in milliseconds from the embedded timestamp to now.
pub fn latency_ms(payload: &[u8]) -> Option<f64> {
    let sent_nanos = extract_timestamp_nanos(payload)?;
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    if now_nanos >= sent_nanos {
        Some((now_nanos - sent_nanos) as f64 / 1_000_000.0)
    } else {
        Some(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_and_extract() {
        let payload = generate(1024, 42);
        assert_eq!(payload.len(), 1024);
        assert_eq!(extract_sequence(&payload), Some(42));
        assert!(extract_timestamp_nanos(&payload).is_some());
    }

    #[test]
    fn test_small_payload() {
        let payload = generate(8, 0);
        assert_eq!(payload.len(), 16);
        assert_eq!(extract_sequence(&payload), Some(0));
    }

    #[test]
    fn test_latency() {
        let payload = generate(32, 0);
        let lat = latency_ms(&payload);
        assert!(lat.is_some());
        assert!(lat.unwrap() >= 0.0);
    }

    #[test]
    fn test_encode_and_verify_crc() {
        let (body, crc_hex) = encode("rust", "test", "ch1", 1, 256);
        assert_eq!(body.len(), 256);
        assert!(verify_crc(&body, &crc_hex));
    }

    #[test]
    fn test_verify_crc_corrupt() {
        let (mut body, crc_hex) = encode("rust", "test", "ch1", 1, 256);
        body[0] ^= 0xFF;
        assert!(!verify_crc(&body, &crc_hex));
    }
}
