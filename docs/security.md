# Security

This guide covers TLS, mutual TLS (mTLS), and authentication options for connecting to a KubeMQ broker.

## Connection Modes

| Mode | Encryption | Client Auth | Config Required |
|------|-----------|-------------|-----------------|
| Plain TCP | No | No | None (default) |
| Server TLS | Yes | No | CA certificate |
| Mutual TLS (mTLS) | Yes | Yes | CA cert + client cert + client key |
| Auth Token | Depends | Token-based | Token string or `CredentialProvider` |

## Plain TCP (Default)

By default, the SDK connects without encryption:

```rust,no_run
use kubemq::prelude::*;

# async fn example() -> kubemq::Result<()> {
let client = KubemqClient::builder()
    .host("localhost")
    .port(50000)
    .build()
    .await?;
# Ok(())
# }
```

Suitable for development and trusted networks only.

## Server-Side TLS

Server TLS encrypts the connection and verifies the broker's identity using a CA certificate:

### File-Based Configuration

```rust,no_run
use kubemq::prelude::*;
use kubemq::TlsConfig;

# async fn example() -> kubemq::Result<()> {
let tls = TlsConfig {
    ca_cert_file: Some("/path/to/ca.pem".to_string()),
    ..Default::default()
};

let client = KubemqClient::builder()
    .host("broker.example.com")
    .port(50000)
    .tls_config(tls)
    .build()
    .await?;
# Ok(())
# }
```

### PEM-Based Configuration

Load certificates from memory instead of files:

```rust,no_run
use kubemq::prelude::*;
use kubemq::TlsConfig;

# async fn example() -> kubemq::Result<()> {
let ca_pem = std::fs::read("/path/to/ca.pem")?;

let tls = TlsConfig {
    ca_cert_pem: Some(ca_pem),
    ..Default::default()
};

let client = KubemqClient::builder()
    .host("broker.example.com")
    .port(50000)
    .tls_config(tls)
    .build()
    .await?;
# Ok(())
# }
```

### Server Name Override

Override the expected server name for TLS verification (useful for testing or IP-based connections):

```rust,no_run
use kubemq::TlsConfig;

let tls = TlsConfig {
    ca_cert_file: Some("/path/to/ca.pem".to_string()),
    server_name: Some("kubemq.internal".to_string()),
    ..Default::default()
};
```

## Mutual TLS (mTLS)

mTLS adds client-side authentication: the broker verifies the client's certificate in addition to the client verifying the broker.

Both `cert_file`/`cert_pem` and `key_file`/`key_pem` must be provided. Providing only one returns a `KubemqError::Validation` error.

### File-Based mTLS

```rust,no_run
use kubemq::prelude::*;
use kubemq::TlsConfig;

# async fn example() -> kubemq::Result<()> {
let tls = TlsConfig {
    ca_cert_file: Some("/path/to/ca.pem".to_string()),
    cert_file: Some("/path/to/client.pem".to_string()),
    key_file: Some("/path/to/client-key.pem".to_string()),
    ..Default::default()
};

let client = KubemqClient::builder()
    .host("broker.example.com")
    .port(50000)
    .tls_config(tls)
    .build()
    .await?;
# Ok(())
# }
```

### PEM-Based mTLS

```rust,no_run
use kubemq::prelude::*;
use kubemq::TlsConfig;

# async fn example() -> kubemq::Result<()> {
let tls = TlsConfig {
    ca_cert_pem: Some(std::fs::read("/path/to/ca.pem")?),
    cert_pem: Some(std::fs::read("/path/to/client.pem")?),
    key_pem: Some(std::fs::read("/path/to/client-key.pem")?),
    ..Default::default()
};

let client = KubemqClient::builder()
    .host("broker.example.com")
    .port(50000)
    .tls_config(tls)
    .build()
    .await?;
# Ok(())
# }
```

## Authentication Token

The SDK sends an auth token as gRPC `authorization` metadata on every request.

### Static Token

```rust,no_run
use kubemq::prelude::*;

# async fn example() -> kubemq::Result<()> {
let client = KubemqClient::builder()
    .host("localhost")
    .port(50000)
    .auth_token("your-secret-token")
    .build()
    .await?;
# Ok(())
# }
```

### Dynamic Credential Provider

For tokens that expire or rotate, implement the `CredentialProvider` trait:

```rust,no_run
use kubemq::prelude::*;
use kubemq::{CredentialProvider, StaticTokenProvider};
use std::time::SystemTime;

struct MyTokenProvider {
    // Your token source (vault, file, etc.)
}

#[async_trait::async_trait]
impl CredentialProvider for MyTokenProvider {
    async fn get_token(&self) -> kubemq::Result<(String, Option<SystemTime>)> {
        let token = "fetched-from-vault".to_string();
        let expires = SystemTime::now() + std::time::Duration::from_secs(3600);
        Ok((token, Some(expires)))
    }
}

# async fn example() -> kubemq::Result<()> {
let client = KubemqClient::builder()
    .host("localhost")
    .port(50000)
    .credential_provider(MyTokenProvider {})
    .build()
    .await?;
# Ok(())
# }
```

The SDK calls `get_token()` before each gRPC request. When `expires_at` is `Some`, the SDK proactively refreshes the token before expiration. When `None`, it refreshes only on `UNAUTHENTICATED` errors.

For simple static tokens, use the built-in `StaticTokenProvider`:

```rust,no_run
use kubemq::prelude::*;
use kubemq::StaticTokenProvider;

# async fn example() -> kubemq::Result<()> {
let client = KubemqClient::builder()
    .host("localhost")
    .port(50000)
    .credential_provider(StaticTokenProvider::new("my-token"))
    .build()
    .await?;
# Ok(())
# }
```

## Environment Variables

TLS and auth can also be configured via environment variables:

| Variable | Description |
|----------|-------------|
| `KUBEMQ_AUTH_TOKEN` | Authentication token |
| `KUBEMQ_TLS_CERT_FILE` | CA certificate file path |
| `KUBEMQ_TLS_CERT_DATA` | Base64-encoded CA certificate |
| `KUBEMQ_TLS_CLIENT_CERT` | Client certificate file path (mTLS) |
| `KUBEMQ_TLS_CLIENT_KEY` | Client key file path (mTLS) |

Builder methods take precedence over environment variables.

## Best Practices

1. **Always use TLS in production** — plain TCP connections expose data in transit
2. **Use mTLS for service-to-service** — ensures both sides authenticate
3. **Rotate tokens** — implement `CredentialProvider` with `expires_at` for automatic rotation
4. **Store certificates securely** — use secret managers, not filesystem paths in code
5. **Use PEM-based config in containers** — mount secrets as environment variables or volumes

## TlsConfig Fields Reference

| Field | Type | Description |
|-------|------|-------------|
| `ca_cert_file` | `Option<String>` | Path to CA certificate file |
| `ca_cert_pem` | `Option<Vec<u8>>` | PEM-encoded CA certificate bytes |
| `cert_file` | `Option<String>` | Path to client certificate file (mTLS) |
| `key_file` | `Option<String>` | Path to client private key file (mTLS) |
| `cert_pem` | `Option<Vec<u8>>` | PEM-encoded client certificate bytes (mTLS) |
| `key_pem` | `Option<Vec<u8>>` | PEM-encoded client private key bytes (mTLS) |
| `server_name` | `Option<String>` | Override server name for TLS verification |

---

**Related docs:**
- [TLS Setup How-To](howto-tls.md) — step-by-step setup instructions
- [Concepts](concepts.md) — architecture and connection lifecycle
- [Error Handling](errors.md) — `Authentication` and `Authorization` errors
