//! # Events Store — Start From First
//!
//! Demonstrates `StartFromFirst` subscription mode. The subscriber replays all
//! stored events from the beginning of the channel, then continues with live events.
//!
//! ## Expected Output
//!
//! ```text
//! Sent 3 events to store
//! Received: seq=1, body=msg-0
//! Received: seq=2, body=msg-1
//! Received: seq=3, body=msg-2
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_store_start_from_first
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

    let channel = "events_store.from_first.example";

    // Pre-seed the channel with events
    for i in 0..3 {
        let event = EventStoreBuilder::new()
            .channel(channel)
            .body(format!("msg-{}", i).into_bytes())
            .build();
        client.send_event_store(event).await?;
    }
    println!("Sent 3 events to store");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe from the very first stored event
    let sub = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartFromFirst,
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
