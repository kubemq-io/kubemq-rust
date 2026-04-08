//! # Graceful Shutdown
//!
//! Demonstrates gracefully shutting down KubeMQ subscriptions and the client.
//! Sets up an event subscription, sends a few events, then cleanly unsubscribes
//! and closes the client — simulating application shutdown.
//!
//! ## Expected Output
//!
//! ```text
//! Subscription active, sending events...
//! Received: body=shutdown-msg-0
//! Received: body=shutdown-msg-1
//! Received: body=shutdown-msg-2
//! Cancelling subscription...
//! Subscription cancelled
//! Client closed — shutdown complete
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example graceful_shutdown
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

    let channel = "graceful_shutdown.example";

    let sub = client
        .subscribe_to_events(
            channel,
            "",
            |event| {
                Box::pin(async move {
                    println!(
                        "Received: body={}",
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("Subscription active, sending events...");

    for i in 0..3 {
        let event = EventBuilder::new()
            .channel(channel)
            .body(format!("shutdown-msg-{}", i).into_bytes())
            .build();
        client.send_event(event).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Graceful shutdown sequence
    println!("Cancelling subscription...");
    sub.unsubscribe().await;
    println!("Subscription cancelled");

    client.close().await?;
    println!("Client closed — shutdown complete");

    Ok(())
}
