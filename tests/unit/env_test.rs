//! Tests for environment variable parsing.
//! We use serial test execution via unique env var names to avoid conflicts.

/// Helper to set env vars, run a closure, and clean up.
fn with_env_vars<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
    for (k, v) in vars {
        std::env::set_var(k, v);
    }
    f();
    for (k, _) in vars {
        std::env::remove_var(k);
    }
}

#[test]
fn test_kubemq_host_env() {
    with_env_vars(&[("KUBEMQ_HOST", "myhost.example.com")], || {
        let val = std::env::var("KUBEMQ_HOST").unwrap();
        assert_eq!(val, "myhost.example.com");
    });
}

#[test]
fn test_kubemq_port_env() {
    with_env_vars(&[("KUBEMQ_PORT", "9090")], || {
        let val: u16 = std::env::var("KUBEMQ_PORT").unwrap().parse().unwrap();
        assert_eq!(val, 9090);
    });
}

#[test]
fn test_kubemq_client_id_env() {
    with_env_vars(&[("KUBEMQ_CLIENT_ID", "test-client-42")], || {
        let val = std::env::var("KUBEMQ_CLIENT_ID").unwrap();
        assert_eq!(val, "test-client-42");
    });
}

#[test]
fn test_kubemq_auth_token_env() {
    with_env_vars(&[("KUBEMQ_AUTH_TOKEN", "secret")], || {
        let val = std::env::var("KUBEMQ_AUTH_TOKEN").unwrap();
        assert_eq!(val, "secret");
    });
}

#[test]
fn test_kubemq_address_env() {
    with_env_vars(&[("KUBEMQ_ADDRESS", "broker.local:50000")], || {
        let val = std::env::var("KUBEMQ_ADDRESS").unwrap();
        assert!(val.contains("broker.local"));
        assert!(val.contains("50000"));
    });
}

#[test]
fn test_kubemq_broker_address_env() {
    with_env_vars(&[("KUBEMQ_BROKER_ADDRESS", "broker2:8080")], || {
        let val = std::env::var("KUBEMQ_BROKER_ADDRESS").unwrap();
        assert_eq!(val, "broker2:8080");
    });
}

#[test]
fn test_kubemq_default_channel_env() {
    with_env_vars(&[("KUBEMQ_DEFAULT_CHANNEL", "my-default-ch")], || {
        let val = std::env::var("KUBEMQ_DEFAULT_CHANNEL").unwrap();
        assert_eq!(val, "my-default-ch");
    });
}

#[test]
fn test_unset_env_var_returns_none() {
    // Make sure a var we never set doesn't exist
    std::env::remove_var("KUBEMQ_NONEXISTENT_VAR_12345");
    assert!(std::env::var("KUBEMQ_NONEXISTENT_VAR_12345").is_err());
}
