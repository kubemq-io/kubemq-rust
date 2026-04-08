//! # Events Store — Start New Only
//!
//! Demonstrates `StartNewOnly` subscription mode. Events published before the
//! subscription are not delivered; only events sent after subscribing are received.
//!
//! ## Expected Output
//!
//! ```text
//! Pre-subscribe event sent (will NOT be received)
//! Subscribed with StartNewOnly
//! Received: seq=2, body=after-subscribe
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_store_start_new_only
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

    let channel = "events_store.start_new.example";

    // Send an event before subscribing
    let event = EventStoreBuilder::new()
        .channel(channel)
        .body(b"before-subscribe".to_vec())
        .build();
    client.send_event_store(event).await?;
    println!("Pre-subscribe event sent (will NOT be received)");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe with StartNewOnly
    let sub = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartNewOnly,
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
    println!("Subscribed with StartNewOnly");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send an event after subscribing -- this one will be received
    let event = EventStoreBuilder::new()
        .channel(channel)
        .body(b"after-subscribe".to_vec())
        .build();
    client.send_event_store(event).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;
    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
