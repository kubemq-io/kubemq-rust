//! # Events Cancel Subscription
//!
//! Demonstrates cancelling an active event subscription. After subscribing and
//! receiving events, the subscription is cancelled and subsequent events are
//! not delivered.
//!
//! ## Expected Output
//!
//! ```text
//! Received event: body=before-cancel
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
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_cancel_subscription
//! ```
use kubemq::prelude::*;
use kubemq::EventBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "events.cancel.example";

    let sub = client
        .subscribe_to_events(
            channel,
            "",
            |event| {
                Box::pin(async move {
                    println!(
                        "Received event: body={}",
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send an event that should be received
    let event = EventBuilder::new()
        .channel(channel)
        .body(b"before-cancel".to_vec())
        .build();
    client.send_event(event).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Cancel the subscription
    sub.unsubscribe().await;
    println!("Subscription cancelled");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send another event -- should NOT be received
    let event = EventBuilder::new()
        .channel(channel)
        .body(b"after-cancel".to_vec())
        .build();
    client.send_event(event).await?;
    println!("Event sent after cancel (should not be received)");

    tokio::time::sleep(Duration::from_secs(1)).await;
    client.close().await?;
    Ok(())
}
