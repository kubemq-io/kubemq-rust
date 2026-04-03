//! Environment variable support for `KUBEMQ_*` configuration fallbacks.

use std::time::Duration;

/// Get host from KUBEMQ_HOST or the host part of KUBEMQ_ADDRESS / KUBEMQ_BROKER_ADDRESS.
pub(crate) fn get_host() -> Option<String> {
    if let Ok(host) = std::env::var("KUBEMQ_HOST") {
        return Some(host);
    }
    if let Some((host, _)) = get_address() {
        return Some(host);
    }
    None
}

/// Get port from KUBEMQ_PORT or the port part of KUBEMQ_ADDRESS / KUBEMQ_BROKER_ADDRESS.
pub(crate) fn get_port() -> Option<u16> {
    if let Ok(port_str) = std::env::var("KUBEMQ_PORT") {
        if let Ok(port) = port_str.parse::<u16>() {
            return Some(port);
        }
    }
    if let Some((_, port)) = get_address() {
        return Some(port);
    }
    None
}

/// Get client ID from KUBEMQ_CLIENT_ID.
pub(crate) fn get_client_id() -> Option<String> {
    std::env::var("KUBEMQ_CLIENT_ID").ok()
}

/// Get auth token from KUBEMQ_AUTH_TOKEN.
pub(crate) fn get_auth_token() -> Option<String> {
    std::env::var("KUBEMQ_AUTH_TOKEN").ok()
}

/// Get TLS cert file from KUBEMQ_TLS_CERT_FILE.
pub(crate) fn get_tls_cert_file() -> Option<String> {
    std::env::var("KUBEMQ_TLS_CERT_FILE").ok()
}

/// Get TLS cert data from KUBEMQ_TLS_CERT_DATA (base64 encoded).
pub(crate) fn get_tls_cert_data() -> Option<String> {
    std::env::var("KUBEMQ_TLS_CERT_DATA").ok()
}

/// Get mTLS client cert file from KUBEMQ_TLS_CLIENT_CERT.
pub(crate) fn get_tls_client_cert() -> Option<String> {
    std::env::var("KUBEMQ_TLS_CLIENT_CERT").ok()
}

/// Get mTLS client key file from KUBEMQ_TLS_CLIENT_KEY.
pub(crate) fn get_tls_client_key() -> Option<String> {
    std::env::var("KUBEMQ_TLS_CLIENT_KEY").ok()
}

/// Get max receive message size from KUBEMQ_MAX_RECEIVE_SIZE.
pub(crate) fn get_max_receive_size() -> Option<usize> {
    std::env::var("KUBEMQ_MAX_RECEIVE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
}

/// Get connection timeout from KUBEMQ_CONNECTION_TIMEOUT.
pub(crate) fn get_connection_timeout() -> Option<Duration> {
    parse_duration_env("KUBEMQ_CONNECTION_TIMEOUT")
}

/// Get keepalive time from KUBEMQ_KEEPALIVE_TIME.
pub(crate) fn get_keepalive_time() -> Option<Duration> {
    parse_duration_env("KUBEMQ_KEEPALIVE_TIME")
}

/// Get keepalive timeout from KUBEMQ_KEEPALIVE_TIMEOUT.
pub(crate) fn get_keepalive_timeout() -> Option<Duration> {
    parse_duration_env("KUBEMQ_KEEPALIVE_TIMEOUT")
}

/// Parse KUBEMQ_ADDRESS or KUBEMQ_BROKER_ADDRESS into (host, port).
fn get_address() -> Option<(String, u16)> {
    let addr = std::env::var("KUBEMQ_ADDRESS")
        .or_else(|_| std::env::var("KUBEMQ_BROKER_ADDRESS"))
        .ok()?;
    parse_address(&addr)
}

/// Parse "host:port" into (host, port).
fn parse_address(addr: &str) -> Option<(String, u16)> {
    let (host, port_str) = addr.rsplit_once(':')?;
    let port = port_str.parse::<u16>().ok()?;
    Some((host.to_string(), port))
}

/// Parse a duration from an environment variable.
/// Supports formats: "10s", "100ms", "5m", or raw seconds as integer.
fn parse_duration_env(var: &str) -> Option<Duration> {
    let val = std::env::var(var).ok()?;
    parse_duration_str(&val)
}

/// Parse duration string (e.g., "10s", "100ms", "5m", or plain seconds).
fn parse_duration_str(s: &str) -> Option<Duration> {
    let s = s.trim();
    if let Some(secs) = s.strip_suffix('s') {
        if let Some(ms) = secs.strip_suffix('m') {
            // "100ms"
            ms.parse::<u64>().ok().map(Duration::from_millis)
        } else {
            // "10s"
            secs.parse::<u64>().ok().map(Duration::from_secs)
        }
    } else if let Some(mins) = s.strip_suffix('m') {
        mins.parse::<u64>()
            .ok()
            .map(|m| Duration::from_secs(m * 60))
    } else {
        // Plain seconds
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_address() {
        assert_eq!(
            parse_address("localhost:50000"),
            Some(("localhost".to_string(), 50000))
        );
        assert_eq!(
            parse_address("192.168.1.1:8080"),
            Some(("192.168.1.1".to_string(), 8080))
        );
        assert_eq!(parse_address("noport"), None);
        assert_eq!(parse_address("host:notaport"), None);
    }

    #[test]
    fn test_parse_duration_str() {
        assert_eq!(parse_duration_str("10s"), Some(Duration::from_secs(10)));
        assert_eq!(
            parse_duration_str("100ms"),
            Some(Duration::from_millis(100))
        );
        assert_eq!(parse_duration_str("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration_str("30"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration_str("invalid"), None);
    }
}
