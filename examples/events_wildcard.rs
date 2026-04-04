//! # Events Wildcard Subscription
//!
//! Demonstrates wildcard channel subscriptions. A single subscriber listens on
//! `events.wildcard.*` and receives events published to any matching sub-channel
//! (e.g., `events.wildcard.orders`, `events.wildcard.users`).
//!
//! ## Expected Output
//!
//! ```text
//! Sent event to events.wildcard.orders
//! Sent event to events.wildcard.users
//! Sent event to events.wildcard.logs
//! Wildcard received: channel=events.wildcard.orders, body=message for orders
//! Wildcard received: channel=events.wildcard.users, body=message for users
//! Wildcard received: channel=events.wildcard.logs, body=message for logs
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_wildcard
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

    // Subscribe with wildcard -- receives events on any channel matching the pattern
    let sub = client
        .subscribe_to_events(
            "events.wildcard.*",
            "",
            |event| {
                Box::pin(async move {
                    println!(
                        "Wildcard received: channel={}, body={}",
                        event.channel,
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send events on different sub-channels
    for suffix in &["orders", "users", "logs"] {
        let channel = format!("events.wildcard.{}", suffix);
        let event = EventBuilder::new()
            .channel(&channel)
            .body(format!("message for {}", suffix).into_bytes())
            .build();
        client.send_event(event).await?;
        println!("Sent event to {}", channel);
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
