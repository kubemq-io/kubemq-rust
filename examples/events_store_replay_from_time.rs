//! # Events Store — Replay From Time
//!
//! Demonstrates `StartAtTime` subscription mode. The subscriber starts replaying
//! stored events from a specific point in time (captured before sending events).
//!
//! ## Expected Output
//!
//! ```text
//! Sent 3 events after time marker
//! Received: seq=<n>, body=msg-<n>
//! ...
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_store_replay_from_time
//! ```
use kubemq::prelude::*;
use kubemq::{EventStoreBuilder, EventsStoreSubscription};
use std::time::{Duration, SystemTime};

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "events_store.replay_time.example";

    // Capture current time as the replay start point
    let start_time = SystemTime::now();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..3 {
        let event = EventStoreBuilder::new()
            .channel(channel)
            .body(format!("msg-{}", i).into_bytes())
            .build();
        client.send_event_store(event).await?;
    }
    println!("Sent 3 events after time marker");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let sub = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartAtTime(start_time),
            |event| {
                Box::pin(async move {
                    println!(
                        "Received: seq={}, body={}",
                        event.sequence,
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_secs(2)).await;
    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
