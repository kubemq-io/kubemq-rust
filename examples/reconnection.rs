//! # Reconnection and Retry Policy
//!
//! Demonstrates configuring a retry policy with exponential backoff and
//! connection state callbacks. The client is configured with `max_retries=3`,
//! `on_connected`, and `on_closed` callbacks. Events are sent in a loop to
//! observe retry behavior if the broker becomes temporarily unavailable.
//!
//! ## Expected Output
//!
//! ```text
//! [STATE] Connected
//! Client state: Connected
//! Sent heartbeat 0
//! Sent heartbeat 1
//! ...
//! [STATE] Closed
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example reconnection
//! ```
use kubemq::prelude::*;
use kubemq::{EventBuilder, RetryPolicy};
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .retry_policy(RetryPolicy {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            multiplier: 2.0,
            ..Default::default()
        })
        .on_connected(|| async {
            println!("[STATE] Connected");
        })
        .on_closed(|| async {
            println!("[STATE] Closed");
        })
        .build()
        .await?;

    println!("Client state: {:?}", client.state());

    // Send events periodically to observe retry behavior
    let channel = "reconnection.example";
    for i in 0..30 {
        let event = EventBuilder::new()
            .channel(channel)
            .body(format!("heartbeat-{}", i).into_bytes())
            .build();

        match client.send_event(event).await {
            Ok(()) => println!("Sent heartbeat {}", i),
            Err(e) => println!("Send failed (retrying): {}", e),
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    client.close().await?;
    Ok(())
}
