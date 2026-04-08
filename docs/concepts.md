# KubeMQ Concepts

This guide explains the core concepts of the KubeMQ message broker and how they map to the Rust SDK.

## Architecture Overview

KubeMQ is a broker-centric messaging system. Clients connect to a KubeMQ broker via gRPC and exchange messages through named **channels**. The broker handles routing, persistence, and delivery guarantees.

```text
┌──────────┐         ┌──────────────┐         ┌──────────┐
│ Publisher │──gRPC──▶│ KubeMQ Broker│──gRPC──▶│Subscriber│
└──────────┘         │  (channels)  │         └──────────┘
                     └──────────────┘
```

The Rust SDK provides a single entry point — [`KubemqClient`](https://docs.rs/kubemq/latest/kubemq/struct.KubemqClient.html) — for all messaging patterns: Events, Events Store, Commands, Queries, and Queues.

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("my-service")
        .build()
        .await?;

    let info = client.ping().await?;
    println!("Connected to {} v{}", info.host, info.version);
    client.close().await?;
    Ok(())
}
```

## Channels

Channels are named message conduits. Every send or subscribe operation targets a specific channel.

### Naming Rules

- Alphanumeric characters, dots (`.`), hyphens (`-`), and underscores (`_`)
- Use dot-separated hierarchical namespacing for organization:

```text
orders.us.created
orders.eu.processed
telemetry.sensors.temperature
```

### Channel Types

Each channel is associated with a messaging pattern:

| Type | Constant | Description |
|------|----------|-------------|
| Events | `"events"` | Fire-and-forget pub/sub |
| Events Store | `"events_store"` | Persistent pub/sub with replay |
| Queues | `"queues"` | Point-to-point message queuing |
| Commands | `"commands"` | Request-response RPC |
| Queries | `"queries"` | Request-response RPC with caching |

Channels are created implicitly on first use or explicitly via channel management:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
client.create_events_channel("notifications").await?;

let channels = client.list_events_channels("").await?;
for ch in &channels {
    println!("Channel: {}", ch.name);
}
# Ok(())
# }
```

## Consumer Groups

When multiple subscribers share a **group name** on the same channel, each message is delivered to exactly one subscriber in the group. This provides load balancing across consumers.

```text
                              ┌─ Subscriber A (group="workers") ── receives msg 1
Channel "tasks" ──message──▶  │
                              └─ Subscriber B (group="workers") ── receives msg 2
```

Without a group (empty string `""`), all subscribers receive all messages (fan-out):

```text
                              ┌─ Subscriber A (group="") ── receives msg 1
Channel "events" ──message──▶ │
                              └─ Subscriber B (group="") ── receives msg 1
```

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
// Load-balanced: only one subscriber in "workers" receives each event
let sub = client.subscribe_to_events(
    "tasks",
    "workers",  // group name
    |event| Box::pin(async move {
        println!("Received: {:?}", event.body);
    }),
    None,
).await?;
# Ok(())
# }
```

## Wildcard Subscriptions

Wildcard subscriptions allow subscribing to multiple channels with a single pattern. **Wildcards are supported only for Events subscriptions** (not Events Store, Queues, Commands, or Queries).

| Pattern | Matches | Example |
|---------|---------|---------|
| `*` | Exactly one token | `orders.*` matches `orders.us` but not `orders.us.east` |
| `>` | One or more tokens | `orders.>` matches `orders.us` and `orders.us.east` |

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
// Subscribe to all order events across all regions
let sub = client.subscribe_to_events(
    "orders.>",
    "",
    |event| Box::pin(async move {
        println!("Order event on {}: {:?}", event.channel, event.body);
    }),
    None,
).await?;
# Ok(())
# }
```

Wildcards can only be used in subscribe channel names. Sending to a wildcard channel returns a `KubemqError::Validation` error.

## Connection Lifecycle

The SDK manages the gRPC connection lifecycle automatically:

```text
Build → Idle → Ready → Closed
                ↑         │
                └─────────┘ (auto-reconnect on disconnect)
```

| State | Description |
|-------|-------------|
| `Idle` | Client initialized but not yet connected |
| `Ready` | Connected and operational |
| `Closed` | Shut down via `close()` — cannot be reused |

Check the current state with `client.state()`:

```rust,no_run
use kubemq::prelude::*;

# fn example(client: &KubemqClient) {
match client.state() {
    ConnectionState::Ready => println!("Connected"),
    ConnectionState::Idle => println!("Connecting..."),
    ConnectionState::Closed => println!("Shut down"),
}
# }
```

Register lifecycle callbacks during build:

```rust,no_run
use kubemq::prelude::*;

# async fn example() -> kubemq::Result<()> {
let client = KubemqClient::builder()
    .host("localhost")
    .port(50000)
    .on_connected(|| async { println!("[CONNECTED]") })
    .on_closed(|| async { println!("[CLOSED]") })
    .build()
    .await?;
# Ok(())
# }
```

## Reconnection

The SDK automatically reconnects with exponential backoff when the connection drops. Active subscriptions are re-established after reconnection.

Default reconnection policy:

| Setting | Default |
|---------|---------|
| Max retries | 3 |
| Initial backoff | 100ms |
| Max backoff | 10s |
| Multiplier | 2.0x |
| Jitter | Full (random `[0, backoff]`) |

Customize via `ClientConfigBuilder::retry_policy()`:

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example() -> kubemq::Result<()> {
let client = KubemqClient::builder()
    .host("localhost")
    .port(50000)
    .retry_policy(RetryPolicy {
        max_retries: 0,  // unlimited
        initial_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(30),
        multiplier: 2.0,
        jitter_mode: JitterMode::Full,
    })
    .build()
    .await?;
# Ok(())
# }
```

## Client Identity

Every client has a `client_id` that identifies it to the broker. Set it explicitly or let the SDK generate a random UUID:

```rust,no_run
use kubemq::prelude::*;

# async fn example() -> kubemq::Result<()> {
let client = KubemqClient::builder()
    .host("localhost")
    .port(50000)
    .client_id("order-service-01")  // explicit
    .build()
    .await?;
# Ok(())
# }
```

Individual messages can override the client-level ID via their builder's `.client_id()` method.

## Graceful Shutdown

Always call `close()` before dropping the client to ensure graceful cleanup:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: KubemqClient) -> kubemq::Result<()> {
// Active subscriptions are cancelled, streams are closed
client.close().await?;
# Ok(())
# }
```

Dropping without `close()` cancels background tasks but skips broker-side close handshakes.

---

**Related docs:**
- [Messaging Patterns](patterns.md) — choosing between Events, Queues, Commands, and Queries
- [Error Handling](errors.md) — understanding SDK error types
- [Security](security.md) — TLS, mTLS, and authentication
