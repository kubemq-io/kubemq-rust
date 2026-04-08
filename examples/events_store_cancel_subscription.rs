//! # Events Store Cancel Subscription
//!
//! Demonstrates cancelling an active events store subscription. After
//! subscribing and receiving stored events, the subscription is cancelled
//! and subsequent events are not delivered.
//!
//! ## Expected Output
//!
//! ```text
//! Received store event: seq=1, body=before-cancel
//! Subscription cancelled
//! Event sent after cancel (should not be received)
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_store_cancel_subscription
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

    let channel = "events_store.cancel.example";

    let sub = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartFromFirst,
            |event| {
                Box::pin(async move {
                    println!(
                        "Received store event: seq={}, body={}",
                        event.sequence,
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let event = EventStoreBuilder::new()
        .channel(channel)
        .body(b"before-cancel".to_vec())
        .build();
    client.send_event_store(event).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    sub.unsubscribe().await;
    println!("Subscription cancelled");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let event = EventStoreBuilder::new()
        .channel(channel)
        .body(b"after-cancel".to_vec())
        .build();
    client.send_event_store(event).await?;
    println!("Event sent after cancel (should not be received)");

    tokio::time::sleep(Duration::from_secs(1)).await;
    client.close().await?;
    Ok(())
}
