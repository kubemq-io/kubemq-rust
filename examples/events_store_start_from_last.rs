//! # Events Store — Start From Last
//!
//! Demonstrates `StartFromLast` subscription mode. The subscriber receives the
//! last stored event, then continues with any new events published after subscribing.
//!
//! ## Expected Output
//!
//! ```text
//! Sent 3 events to store
//! Received: seq=3, body=msg-2
//! Received: seq=4, body=live-event
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_store_start_from_last
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

    let channel = "events_store.from_last.example";

    for i in 0..3 {
        let event = EventStoreBuilder::new()
            .channel(channel)
            .body(format!("msg-{}", i).into_bytes())
            .build();
        client.send_event_store(event).await?;
    }
    println!("Sent 3 events to store");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let sub = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartFromLast,
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

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Send a live event after subscribing
    let event = EventStoreBuilder::new()
        .channel(channel)
        .body(b"live-event".to_vec())
        .build();
    client.send_event_store(event).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;
    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
