//! # Events Store — Replay From Sequence
//!
//! Demonstrates `StartAtSequence` subscription mode. The subscriber starts
//! replaying stored events from a specific sequence number.
//!
//! ## Expected Output
//!
//! ```text
//! Sent 5 events to store
//! Received: seq=3, body=msg-2
//! Received: seq=4, body=msg-3
//! Received: seq=5, body=msg-4
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_store_replay_from_sequence
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

    let channel = "events_store.replay_seq.example";

    for i in 0..5 {
        let event = EventStoreBuilder::new()
            .channel(channel)
            .body(format!("msg-{}", i).into_bytes())
            .build();
        client.send_event_store(event).await?;
    }
    println!("Sent 5 events to store");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe from sequence 3
    let sub = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartAtSequence(3),
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
