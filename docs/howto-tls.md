# How-To: TLS & Authentication Setup

Step-by-step guide for configuring secure connections to KubeMQ.

## Prerequisites

- A KubeMQ broker configured for TLS (see KubeMQ server documentation)
- Certificate files: CA cert, and optionally client cert + key for mTLS
- `kubemq` crate added to your `Cargo.toml`

## Step 1: Server-Side TLS

Server TLS encrypts the connection and verifies the broker's identity.

### Using Certificate Files

```rust,no_run
use kubemq::prelude::*;
use kubemq::TlsConfig;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let tls = TlsConfig {
        ca_cert_file: Some("/etc/kubemq/certs/ca.pem".to_string()),
        ..Default::default()
    };

    let client = KubemqClient::builder()
        .host("broker.example.com")
        .port(50000)
        .tls_config(tls)
        .build()
        .await?;

    let info = client.ping().await?;
    println!("Connected via TLS to {} v{}", info.host, info.version);
    client.close().await
}
```

### Using PEM Bytes

Load certificates from memory (useful in containers or when mounting secrets):

```rust,no_run
use kubemq::prelude::*;
use kubemq::TlsConfig;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let ca_pem = tokio::fs::read("/etc/kubemq/certs/ca.pem").await?;

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

    client.close().await
}
```

### Using Environment Variables

Set `KUBEMQ_TLS_CERT_FILE` and let the builder auto-configure:

```bash
export KUBEMQ_TLS_CERT_FILE=/etc/kubemq/certs/ca.pem
```

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    // TLS is auto-configured from KUBEMQ_TLS_CERT_FILE
    let client = KubemqClient::builder()
        .host("broker.example.com")
        .port(50000)
        .build()
        .await?;

    client.close().await
}
```

## Step 2: Mutual TLS (mTLS)

mTLS adds client authentication — the broker verifies the client's identity.

### File-Based mTLS

```rust,no_run
use kubemq::prelude::*;
use kubemq::TlsConfig;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let tls = TlsConfig {
        ca_cert_file: Some("/etc/kubemq/certs/ca.pem".to_string()),
        cert_file: Some("/etc/kubemq/certs/client.pem".to_string()),
        key_file: Some("/etc/kubemq/certs/client-key.pem".to_string()),
        ..Default::default()
    };

    let client = KubemqClient::builder()
        .host("broker.example.com")
        .port(50000)
        .tls_config(tls)
        .build()
        .await?;

    let info = client.ping().await?;
    println!("Connected via mTLS to {} v{}", info.host, info.version);
    client.close().await
}
```

### PEM-Based mTLS

```rust,no_run
use kubemq::prelude::*;
use kubemq::TlsConfig;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let tls = TlsConfig {
        ca_cert_pem: Some(tokio::fs::read("/certs/ca.pem").await?),
        cert_pem: Some(tokio::fs::read("/certs/client.pem").await?),
        key_pem: Some(tokio::fs::read("/certs/client-key.pem").await?),
        ..Default::default()
    };

    let client = KubemqClient::builder()
        .host("broker.example.com")
        .port(50000)
        .tls_config(tls)
        .build()
        .await?;

    client.close().await
}
```

### Common mTLS Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `KubemqError::Validation` "mTLS requires both client certificate and key" | Only cert or only key provided | Provide both `cert_file`/`cert_pem` and `key_file`/`key_pem` |
| `KubemqError::Fatal` "failed to read CA cert file" | File path doesn't exist or not readable | Check file path and permissions |
| `KubemqError::Fatal` "failed to read client cert file" | Client cert file path invalid | Verify the cert file path |
| `KubemqError::Fatal` "failed to read client key file" | Client key file path invalid | Verify the key file path |

## Step 3: Authentication Token

### Static Token

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .auth_token("your-jwt-token")
        .build()
        .await?;

    client.close().await
}
```

### Via Environment Variable

```bash
export KUBEMQ_AUTH_TOKEN=your-jwt-token
```

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    // Token auto-loaded from KUBEMQ_AUTH_TOKEN
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    client.close().await
}
```

### Dynamic Token Provider

For rotating tokens (JWT, OIDC), implement `CredentialProvider`:

```rust,no_run
use kubemq::prelude::*;
use kubemq::CredentialProvider;
use std::time::SystemTime;

struct VaultTokenProvider {
    vault_url: String,
}

#[async_trait::async_trait]
impl CredentialProvider for VaultTokenProvider {
    async fn get_token(&self) -> kubemq::Result<(String, Option<SystemTime>)> {
        // Fetch token from your vault/secret manager
        let token = "fetched-token".to_string();
        let expires = SystemTime::now() + std::time::Duration::from_secs(3600);
        Ok((token, Some(expires)))
    }
}

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .credential_provider(VaultTokenProvider {
            vault_url: "https://vault.example.com".to_string(),
        })
        .build()
        .await?;

    client.close().await
}
```

## Step 4: Combined TLS + Auth Token

```rust,no_run
use kubemq::prelude::*;
use kubemq::TlsConfig;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let tls = TlsConfig {
        ca_cert_file: Some("/etc/kubemq/certs/ca.pem".to_string()),
        cert_file: Some("/etc/kubemq/certs/client.pem".to_string()),
        key_file: Some("/etc/kubemq/certs/client-key.pem".to_string()),
        ..Default::default()
    };

    let client = KubemqClient::builder()
        .host("broker.example.com")
        .port(50000)
        .tls_config(tls)
        .auth_token("your-jwt-token")
        .build()
        .await?;

    client.close().await
}
```

## Server Name Override

When connecting to a broker via IP address or through a load balancer, override the expected server name:

```rust,no_run
use kubemq::TlsConfig;

let tls = TlsConfig {
    ca_cert_file: Some("/path/to/ca.pem".to_string()),
    server_name: Some("kubemq.internal".to_string()),
    ..Default::default()
};
```

## Troubleshooting

1. **"transport error"** — Verify the broker is configured for TLS on the port you're connecting to
2. **"authentication failed"** — Check token validity and broker auth configuration
3. **"authorization failed"** — The token is valid but the client lacks permission for the requested operation
4. **Certificate errors at connect time** — Ensure the CA cert matches the broker's server certificate issuer

---

**Related docs:**
- [Security Reference](security.md) — complete security options
- [Error Handling](errors.md) — `Authentication` and `Authorization` errors
- [Concepts](concepts.md) — connection lifecycle
