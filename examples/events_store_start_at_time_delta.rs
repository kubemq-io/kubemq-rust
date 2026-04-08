//! # Events Store — Start At Time Delta
//!
//! Demonstrates `StartAtTimeDelta` subscription mode. The subscriber replays
//! events stored within a relative time window (e.g. the last 30 seconds),
//! then continues with new live events.
//!
//! ## Expected Output
//!
//! ```text
//! Sent 3 events to store
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
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_store_start_at_time_delta
//! ```
use kubemq::prelude::*;
use kubemq::{EventStoreBuilder, EventsStoreSubscription};
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "events_store.time_delta.example";

    for i in 0..3 {
        let event = EventStoreBuilder::new()
            .channel(channel)
            .body(format!("msg-{}", i).into_bytes())
            .build();
        client.send_event_store(event).await?;
    }
    println!("Sent 3 events to store");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe from 30 seconds ago
    let sub = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartAtTimeDelta(Duration::from_secs(30)),
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
