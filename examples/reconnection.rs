//! Example: Demonstrate retry policy and state callbacks.
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
