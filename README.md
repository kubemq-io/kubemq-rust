# KubeMQ Rust SDK

The official Rust client library for [KubeMQ](https://kubemq.io) message broker. Supports Events, Events Store, Commands, Queries, and Queues messaging patterns.

[![Crates.io](https://img.shields.io/crates/v/kubemq.svg)](https://crates.io/crates/kubemq)
[![docs.rs](https://docs.rs/kubemq/badge.svg)](https://docs.rs/kubemq)
[![CI](https://github.com/kubemq-io/kubemq-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/kubemq-io/kubemq-rust/actions)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## Installation

```bash
cargo add kubemq
```

Or add to your `Cargo.toml`:

```toml
[dependencies]
kubemq = "1.0"
```

## Quick Start

```rust
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    // Send a fire-and-forget event
    let event = Event::builder()
        .channel("my-channel")
        .body(b"Hello KubeMQ!".to_vec())
        .build();
    client.send_event(&event).await?;

    client.close().await?;
    Ok(())
}
```

## Features

- **Events (Pub/Sub)** -- Fire-and-forget messaging with wildcard subscriptions and consumer groups
- **Events Store (Persistent Pub/Sub)** -- Persistent events with 6 replay start positions
- **Queues (Stream + Simple)** -- Reliable message queuing with ack/nack, dead-letter, delay, and expiration
- **Commands (RPC)** -- Request/response with timeout
- **Queries (RPC with Cache)** -- Request/response with server-side caching
- **Channel Management** -- Create, delete, and list channels for all pattern types
- **Auto-Reconnection** -- Automatic reconnection with configurable backoff
- **TLS & mTLS** -- Secure connections via rustls
- **OpenTelemetry** -- Optional tracing and metrics instrumentation (feature-gated)

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `host` | `"localhost"` | Server hostname |
| `port` | `50000` | Server port |
| `client_id` | Auto UUID | Client identifier |
| `auth_token` | None | Authentication token |
| `tls_config` | None | TLS/mTLS configuration |
| `connection_timeout` | 10s | Connection timeout |
| `check_connection` | false | Verify connection on build |
| `wait_for_ready` | true | Wait for gRPC ready state |
| `keepalive_time` | 10s | Keepalive ping interval (min 5s) |
| `keepalive_timeout` | 5s | Keepalive response timeout |
| `max_receive_message_size` | 4 MB | Max inbound message size |
| `max_send_message_size` | 100 MB | Max outbound message size |
| `retry_policy` | 3 retries, 100ms-10s backoff | Operation retry policy |
| `reconnect_policy` | Unlimited, 1s-30s backoff | Reconnection policy |

### Environment Variables

All configuration can be set via environment variables with precedence: **Builder method > Environment variable > Default**.

| Variable | Description |
|----------|-------------|
| `KUBEMQ_ADDRESS` / `KUBEMQ_BROKER_ADDRESS` | Server address (`host:port`) |
| `KUBEMQ_HOST` | Server hostname |
| `KUBEMQ_PORT` | Server port |
| `KUBEMQ_CLIENT_ID` | Client identifier |
| `KUBEMQ_AUTH_TOKEN` | Authentication token |
| `KUBEMQ_TLS_CERT_FILE` | TLS CA cert file path |
| `KUBEMQ_DEFAULT_CHANNEL` | Default channel |

## Examples

See the [`examples/`](examples/) directory for 34 standalone examples covering all patterns:

- **Connection:** ping, TLS, mTLS, auth token
- **Events:** pub/sub, streaming, wildcards, consumer groups
- **Events Store:** pub/sub, all 6 start types, streaming, consumer groups
- **Commands:** send/handle, consumer groups
- **Queries:** send/handle, caching, consumer groups
- **Queues Simple:** send/receive, batch, peek, ack-all, delay, expiration, dead-letter
- **Queues Stream:** upstream, downstream, auto-ack, ack-range, nack, requeue, poll
- **Management:** create/list/delete channels
- **Error handling** and **reconnection**

Run any example with:

```bash
cargo run --example connection_ping
cargo run --example events_pubsub
cargo run --example queues_stream_downstream
```

## Error Handling

All operations return `kubemq::Result<T>` with structured error types:

```rust
match client.send_event(&event).await {
    Ok(()) => println!("Sent"),
    Err(e) => {
        println!("Error: {}", e);
        println!("Code: {:?}", e.code());
        println!("Retryable: {}", e.is_retryable());
        println!("Suggestion: {}", e.suggestion());
    }
}
```

Error codes: `Transient`, `Timeout`, `Throttling`, `Authentication`, `Authorization`, `Validation`, `NotFound`, `Fatal`, `Cancellation`, `Backpressure`.

## Reconnection

Auto-reconnect is enabled by default with unlimited attempts and exponential backoff (1s-30s). Events and queues are buffered during reconnection (up to 1000 messages). Commands and queries fail immediately during reconnection.

```rust
let client = KubemqClient::builder()
    .host("localhost")
    .port(50000)
    .on_disconnected(|| println!("Disconnected"))
    .on_reconnecting(|| println!("Reconnecting..."))
    .on_reconnected(|| println!("Reconnected!"))
    .build()
    .await?;
```

## TLS & Authentication

### Server-side TLS

```rust
let tls = TlsConfig {
    ca_cert_file: Some("/path/to/ca.pem".to_string()),
    ..Default::default()
};
let client = KubemqClient::builder()
    .tls_config(tls)
    .build()
    .await?;
```

### Mutual TLS (mTLS)

```rust
let tls = TlsConfig {
    ca_cert_file: Some("/path/to/ca.pem".to_string()),
    cert_file: Some("/path/to/client.pem".to_string()),
    key_file: Some("/path/to/client-key.pem".to_string()),
    ..Default::default()
};
```

### Auth Token

```rust
let client = KubemqClient::builder()
    .auth_token("your-token")
    .build()
    .await?;
```

## Minimum Supported Rust Version

This crate requires **Rust 1.75** or later.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, build/test commands, and PR process.

## License

Apache-2.0. See [LICENSE](LICENSE) for details.
