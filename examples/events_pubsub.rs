//! # Events Publish/Subscribe
//!
//! Demonstrates the basic pub/sub pattern: subscribes to a channel, publishes
//! a single event, and receives it. Events are fire-and-forget — the publisher
//! does not wait for subscriber acknowledgment.
//!
//! ## Expected Output
//!
//! ```text
//! Received event: id=<uuid>, channel=events.example, body=13 bytes
//! Event sent to channel: events.example
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_pubsub
//! ```
use kubemq::prelude::*;
use kubemq::{EventBuilder, Subscription};
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "events.example";

    // Subscribe to events
    let sub: Subscription = client
        .subscribe_to_events(
            channel,
            "",
            |event| {
                Box::pin(async move {
                    println!(
                        "Received event: id={}, channel={}, body={} bytes",
                        event.id,
                        event.channel,
                        event.body.len()
                    );
                })
            },
            None,
        )
        .await?;

    // Give subscription time to establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send an event
    let event = EventBuilder::new()
        .channel(channel)
        .metadata("example-metadata")
        .body(b"Hello KubeMQ!".to_vec())
        .build();

    client.send_event(event).await?;
    println!("Event sent to channel: {}", channel);

    // Wait to receive
    tokio::time::sleep(Duration::from_secs(2)).await;

    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
