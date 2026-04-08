# How-To: Events & Events Store

Task-oriented guide for pub/sub messaging with KubeMQ.

## Prerequisites

- A running KubeMQ broker (default: `localhost:50000`)
- `kubemq` crate added to your `Cargo.toml`
- `tokio` runtime with the `macros` and `rt-multi-thread` features

## Events (Fire-and-Forget)

### Send a Single Event

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let event = Event::builder()
        .channel("notifications")
        .body(b"Hello KubeMQ!".to_vec())
        .metadata("text/plain")
        .add_tag("source", "my-service")
        .build();

    client.send_event(event).await?;
    println!("Event sent");
    client.close().await
}
```

### Publish with Convenience Method

For simple cases without tags or custom IDs:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
client.publish_event(
    "notifications",
    b"quick message".to_vec(),
    Some("text/plain"),
    None,  // no tags
).await?;
# Ok(())
# }
```

### Subscribe to Events

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let subscription = client.subscribe_to_events(
        "notifications",
        "",  // empty group = fan-out to all subscribers
        |event| Box::pin(async move {
            println!(
                "Received on '{}': {}",
                event.channel,
                String::from_utf8_lossy(&event.body)
            );
        }),
        Some(Box::new(|err| Box::pin(async move {
            eprintln!("Subscription error: {}", err);
        }))),
    ).await?;

    // Keep running until interrupted
    subscription.done().await;
    client.close().await
}
```

### Subscribe with Consumer Group

Load-balance events across multiple subscribers:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let sub = client.subscribe_to_events(
    "tasks",
    "workers",  // group name — each event goes to one subscriber
    |event| Box::pin(async move {
        println!("Worker received: {:?}", event.body);
    }),
    None,
).await?;
# Ok(())
# }
```

### Subscribe with Wildcards

Subscribe to multiple channels using pattern matching (Events only):

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
// * matches one token: orders.us, orders.eu (not orders.us.east)
let sub1 = client.subscribe_to_events("orders.*", "", |event| {
    Box::pin(async move { println!("Order: {}", event.channel); })
}, None).await?;

// > matches one or more tokens: orders.us, orders.us.east, orders.eu.west
let sub2 = client.subscribe_to_events("orders.>", "", |event| {
    Box::pin(async move { println!("Order (deep): {}", event.channel); })
}, None).await?;
# Ok(())
# }
```

### Stream Events (High Throughput)

Open a bidirectional stream for high-throughput publishing:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let mut handle = client.send_event_stream().await?;

for i in 0..100 {
    let event = Event::builder()
        .channel("telemetry")
        .body(format!("reading-{}", i).into_bytes())
        .build();
    handle.send(event).await?;
}

// Check for async errors
while let Ok(err) = handle.errors().try_recv() {
    eprintln!("Stream error: {}", err);
}

handle.close();
# Ok(())
# }
```

### Cancel a Subscription

```rust,no_run
use kubemq::prelude::*;

# async fn example(sub: Subscription) {
// Graceful: sends close signal to broker
sub.unsubscribe().await;

// Or immediate cancel without broker notification
// sub.cancel();
# }
```

## Events Store (Persistent)

### Send a Persistent Event

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let event = EventStore::builder()
    .channel("orders.created")
    .body(b"order-123".to_vec())
    .add_tag("priority", "high")
    .build();

let result = client.send_event_store(event).await?;
println!("Stored: id={}, sent={}", result.id, result.sent);
# Ok(())
# }
```

### Subscribe with Replay (All 6 Start Positions)

#### New Events Only

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let sub = client.subscribe_to_events_store(
    "orders.created",
    "",
    EventsStoreSubscription::StartNewOnly,
    |event| Box::pin(async move {
        println!("New event seq={}: {:?}", event.sequence, event.body);
    }),
    None,
).await?;
# Ok(())
# }
```

#### From First Event

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let sub = client.subscribe_to_events_store(
    "orders.created",
    "",
    EventsStoreSubscription::StartFromFirst,
    |event| Box::pin(async move {
        println!("Replay seq={}", event.sequence);
    }),
    None,
).await?;
# Ok(())
# }
```

#### From Last Event

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let sub = client.subscribe_to_events_store(
    "orders.created",
    "",
    EventsStoreSubscription::StartFromLast,
    |event| Box::pin(async move {
        println!("Latest: seq={}", event.sequence);
    }),
    None,
).await?;
# Ok(())
# }
```

#### At Specific Sequence

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let sub = client.subscribe_to_events_store(
    "orders.created",
    "",
    EventsStoreSubscription::StartAtSequence(100),
    |event| Box::pin(async move {
        println!("From seq 100: seq={}", event.sequence);
    }),
    None,
).await?;
# Ok(())
# }
```

#### At Specific Time

```rust,no_run
use kubemq::prelude::*;
use std::time::{SystemTime, Duration};

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let one_hour_ago = SystemTime::now() - Duration::from_secs(3600);
let sub = client.subscribe_to_events_store(
    "orders.created",
    "",
    EventsStoreSubscription::StartAtTime(one_hour_ago),
    |event| Box::pin(async move {
        println!("Since 1h ago: seq={}", event.sequence);
    }),
    None,
).await?;
# Ok(())
# }
```

#### At Time Delta

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let sub = client.subscribe_to_events_store(
    "orders.created",
    "",
    EventsStoreSubscription::StartAtTimeDelta(Duration::from_secs(300)),
    |event| Box::pin(async move {
        println!("Last 5 min: seq={}", event.sequence);
    }),
    None,
).await?;
# Ok(())
# }
```

### Stream Event Store Messages

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let mut handle = client.send_event_store_stream().await?;

for i in 0..50 {
    let event = EventStore::builder()
        .channel("logs")
        .body(format!("log-entry-{}", i).into_bytes())
        .build();
    handle.send(event).await?;
}

// Check confirmations
while let Ok(result) = handle.results().try_recv() {
    if result.sent {
        println!("Confirmed: {}", result.event_id);
    } else {
        eprintln!("Failed: {} - {}", result.event_id, result.error);
    }
}

handle.close();
# Ok(())
# }
```

---

**Related docs:**
- [Concepts](concepts.md) — channels, consumer groups, wildcards
- [Patterns](patterns.md) — comparing Events vs Events Store
- [Error Handling](errors.md) — error types and recovery
