//! # Events Multiple Subscribers
//!
//! Demonstrates fan-out event delivery. Two subscribers on the same channel
//! without a consumer group both receive every published event.
//!
//! ## Expected Output
//!
//! ```text
//! Subscriber-1 received: body=broadcast-message
//! Subscriber-2 received: body=broadcast-message
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_multiple_subscribers
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

    let channel = "events.multi_sub.example";

    // Two subscribers without a group -- both receive each event
    let sub1 = client
        .subscribe_to_events(
            channel,
            "",
            |event| {
                Box::pin(async move {
                    println!(
                        "Subscriber-1 received: body={}",
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    let sub2 = client
        .subscribe_to_events(
            channel,
            "",
            |event| {
                Box::pin(async move {
                    println!(
                        "Subscriber-2 received: body={}",
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let event = EventBuilder::new()
        .channel(channel)
        .body(b"broadcast-message".to_vec())
        .build();
    client.send_event(event).await?;
    println!("Event published to channel: {}", channel);

    tokio::time::sleep(Duration::from_secs(2)).await;

    sub1.unsubscribe().await;
    sub2.unsubscribe().await;
    client.close().await?;
    Ok(())
}
