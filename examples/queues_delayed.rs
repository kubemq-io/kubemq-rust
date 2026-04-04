//! # Queues Delayed Message
//!
//! Demonstrates sending a queue message with a delivery delay. The message is
//! accepted by the broker but not visible to consumers until the delay period
//! (5 seconds) elapses. An immediate receive returns nothing; after the delay
//! the message becomes available.
//!
//! ## Expected Output
//!
//! ```text
//! Sent delayed message: id=<uuid>, delayed_to=<timestamp>
//! Immediate receive (before delay): 0 messages
//! Waiting 6 seconds for delayed message...
//! After delay: 1 messages received
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_delayed
//! ```
use kubemq::prelude::*;
use kubemq::QueueMessageBuilder;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.delayed.example";

    // Send a message delayed by 5 seconds
    let msg = QueueMessageBuilder::new()
        .channel(channel)
        .body(b"delayed-message".to_vec())
        .delay_seconds(5)
        .build();

    let result = client.send_queue_message(msg).await?;
    println!(
        "Sent delayed message: id={}, delayed_to={}",
        result.message_id, result.delayed_to
    );

    // Try to receive immediately -- should be empty
    let immediate = client.receive_queue_messages(channel, 10, 1, false).await?;
    println!(
        "Immediate receive (before delay): {} messages",
        immediate.len()
    );

    // Wait for the delay
    println!("Waiting 6 seconds for delayed message...");
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    let delayed = client.receive_queue_messages(channel, 10, 5, false).await?;
    println!("After delay: {} messages received", delayed.len());

    client.close().await?;
    Ok(())
}
