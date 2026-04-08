# Scenario: Real-Time Event Dashboard

This tutorial builds a real-time event dashboard using Events Store with live streaming and historical replay.

## Architecture

```text
┌───────────┐    ┌────────────────────┐    ┌──────────────┐
│ Publisher  │───▶│ metrics.cpu        │───▶│ Dashboard    │
│ (sensors) │    │ metrics.memory     │    │ (live view)  │
│           │    │ metrics.disk       │    └──────────────┘
└───────────┘    └────────────────────┘
                                            ┌──────────────┐
                                            │ Replay       │
                                            │ (historical) │
                                            └──────────────┘
```

- **Publisher** sends metrics as persistent events
- **Dashboard** subscribes from the latest event for live updates
- **Replay** subscribes from the first event for historical analysis

## Prerequisites

- A running KubeMQ broker on `localhost:50000`
- `kubemq` and `tokio` in your `Cargo.toml`

## Publisher (Sensor Simulator)

Publishes system metrics as Events Store messages:

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("sensor-publisher")
        .build()
        .await?;

    println!("Publisher started, sending metrics...");

    let mut counter = 0u64;
    loop {
        // Simulate CPU metrics
        let cpu_event = EventStore::builder()
            .channel("metrics.cpu")
            .body(format!(
                r#"{{"host": "server-01", "cpu_percent": {}, "timestamp": {}}}"#,
                45.0 + (counter as f64 * 0.5) % 50.0,
                chrono_timestamp()
            ).into_bytes())
            .metadata("application/json")
            .add_tag("host", "server-01")
            .add_tag("metric_type", "cpu")
            .build();

        let result = client.send_event_store(cpu_event).await?;
        println!(
            "[{}] CPU metric sent: id={}, seq={}",
            counter, result.id, counter
        );

        // Simulate memory metrics
        let mem_event = EventStore::builder()
            .channel("metrics.memory")
            .body(format!(
                r#"{{"host": "server-01", "used_gb": {:.1}, "total_gb": 16.0}}"#,
                8.0 + (counter as f64 * 0.1) % 6.0
            ).into_bytes())
            .metadata("application/json")
            .add_tag("host", "server-01")
            .add_tag("metric_type", "memory")
            .build();

        client.send_event_store(mem_event).await?;

        counter += 1;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn chrono_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
```

## Dashboard (Live View)

Subscribes from the latest event to display real-time metrics:

```rust,no_run
use kubemq::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("dashboard-live")
        .build()
        .await?;

    println!("=== Live Dashboard ===");
    println!("Subscribing to metrics from latest...\n");

    let event_count = Arc::new(AtomicU64::new(0));

    // Subscribe to CPU metrics — start from latest
    let cpu_count = event_count.clone();
    let cpu_sub = client.subscribe_to_events_store(
        "metrics.cpu",
        "",
        EventsStoreSubscription::StartFromLast,
        move |event| {
            let count = cpu_count.clone();
            Box::pin(async move {
                let body = String::from_utf8_lossy(&event.body);
                let n = count.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[LIVE] CPU  seq={}: {}",
                    event.sequence, body
                );
                if n > 0 && n % 10 == 0 {
                    println!("--- {} events received so far ---", n);
                }
            })
        },
        Some(Box::new(|err| Box::pin(async move {
            eprintln!("[LIVE] Error: {}", err);
        }))),
    ).await?;

    // Subscribe to memory metrics — start from latest
    let mem_count = event_count.clone();
    let mem_sub = client.subscribe_to_events_store(
        "metrics.memory",
        "",
        EventsStoreSubscription::StartFromLast,
        move |event| {
            let count = mem_count.clone();
            Box::pin(async move {
                let body = String::from_utf8_lossy(&event.body);
                count.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[LIVE] MEM  seq={}: {}",
                    event.sequence, body
                );
            })
        },
        None,
    ).await?;

    println!("Dashboard running. Press Ctrl+C to stop.\n");

    // Keep running
    tokio::signal::ctrl_c().await.ok();

    cpu_sub.unsubscribe().await;
    mem_sub.unsubscribe().await;
    println!("\nTotal events received: {}", event_count.load(Ordering::Relaxed));
    client.close().await
}
```

## Replay (Historical Analysis)

Replays all historical metrics from the beginning for analysis:

```rust,no_run
use kubemq::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("dashboard-replay")
        .build()
        .await?;

    println!("=== Historical Replay ===");
    println!("Replaying all CPU metrics from the beginning...\n");

    let total = Arc::new(AtomicU64::new(0));
    let total_clone = total.clone();

    let sub = client.subscribe_to_events_store(
        "metrics.cpu",
        "",
        EventsStoreSubscription::StartFromFirst,
        move |event| {
            let count = total_clone.clone();
            Box::pin(async move {
                let n = count.fetch_add(1, Ordering::Relaxed);
                let body = String::from_utf8_lossy(&event.body);
                println!(
                    "[REPLAY] #{} seq={} ts={}: {}",
                    n, event.sequence, event.timestamp, body
                );
            })
        },
        Some(Box::new(|err| Box::pin(async move {
            eprintln!("[REPLAY] Error: {}", err);
        }))),
    ).await?;

    // Let replay run for a while, then report
    tokio::time::sleep(Duration::from_secs(10)).await;

    let count = total.load(Ordering::Relaxed);
    println!("\n=== Replay Summary ===");
    println!("Total historical events replayed: {}", count);

    sub.unsubscribe().await;
    client.close().await
}
```

## Replay from a Specific Time

Replay events from the last hour:

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let sub = client.subscribe_to_events_store(
    "metrics.cpu",
    "",
    EventsStoreSubscription::StartAtTimeDelta(Duration::from_secs(3600)),
    |event| Box::pin(async move {
        println!("Last hour: seq={}", event.sequence);
    }),
    None,
).await?;
# Ok(())
# }
```

## Replay from a Specific Sequence

Resume from where you left off:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let last_processed_seq = 42;  // load from persistent storage

let sub = client.subscribe_to_events_store(
    "metrics.cpu",
    "",
    EventsStoreSubscription::StartAtSequence(last_processed_seq + 1),
    |event| Box::pin(async move {
        println!("Catching up: seq={}", event.sequence);
        // Save event.sequence to persistent storage for resume
    }),
    None,
).await?;
# Ok(())
# }
```

## Running the Dashboard

Start each component in a separate terminal:

```bash
# Terminal 1: Start the publisher (sensor simulator)
KUBEMQ_ADDRESS=localhost:50000 cargo run --example publisher

# Terminal 2: Start the live dashboard
KUBEMQ_ADDRESS=localhost:50000 cargo run --example dashboard

# Terminal 3: Run historical replay
KUBEMQ_ADDRESS=localhost:50000 cargo run --example replay
```

## Expected Behavior

1. Publisher sends CPU and memory metrics every 2 seconds
2. Live dashboard shows metrics in real-time as they arrive
3. Replay subscriber replays all historical metrics from the beginning
4. New dashboard instances start from the latest event (no duplicate processing)
5. Replay instances can start from any position (first, sequence, time, time delta)

## Key Concepts Demonstrated

| Concept | Implementation |
|---------|---------------|
| Persistent events | `EventStore` with `send_event_store()` |
| Live streaming | `StartFromLast` subscription |
| Full replay | `StartFromFirst` subscription |
| Time-based replay | `StartAtTimeDelta` / `StartAtTime` |
| Sequence-based resume | `StartAtSequence` for exactly-once processing |
| Multiple subscribers | Independent subscriptions to same channel |
| Structured metadata | JSON body + tags for filtering |

---

**Related docs:**
- [Events & Events Store How-To](howto-pubsub.md) — all subscription start positions
- [Patterns](patterns.md) — Events vs Events Store comparison
- [Concepts](concepts.md) — channels and consumer groups
