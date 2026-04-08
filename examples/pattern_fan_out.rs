//! # Pattern — Fan Out
//!
//! Demonstrates the fan-out messaging pattern. Three subscribers on the same
//! channel (without a consumer group) each receive every published event.
//!
//! ## Expected Output
//!
//! ```text
//! Service-A received: broadcast-update
//! Service-B received: broadcast-update
//! Service-C received: broadcast-update
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example pattern_fan_out
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

    let channel = "patterns.fan_out.example";

    // Three subscribers without a group -- all receive each event
    let sub_a = client
        .subscribe_to_events(channel, "", |event| {
            Box::pin(async move {
                println!(
                    "Service-A received: {}",
                    String::from_utf8_lossy(&event.body)
                );
            })
        }, None)
        .await?;

    let sub_b = client
        .subscribe_to_events(channel, "", |event| {
            Box::pin(async move {
                println!(
                    "Service-B received: {}",
                    String::from_utf8_lossy(&event.body)
                );
            })
        }, None)
        .await?;

    let sub_c = client
        .subscribe_to_events(channel, "", |event| {
            Box::pin(async move {
                println!(
                    "Service-C received: {}",
                    String::from_utf8_lossy(&event.body)
                );
            })
        }, None)
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let event = EventBuilder::new()
        .channel(channel)
        .body(b"broadcast-update".to_vec())
        .build();
    client.send_event(event).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    sub_a.unsubscribe().await;
    sub_b.unsubscribe().await;
    sub_c.unsubscribe().await;
    client.close().await?;
    Ok(())
}
