//! # Queues Message Expiration
//!
//! Demonstrates queue messages with a time-to-live (TTL). The message is set
//! to expire after 3 seconds. A peek immediately after send confirms the
//! message exists; after waiting 4 seconds the message has expired and is no
//! longer available.
//!
//! ## Expected Output
//!
//! ```text
//! Sent message: id=<uuid>, expiration_at=<timestamp>
//! Before expiration (peek): 1 messages
//! Waiting 4 seconds for message to expire...
//! After expiration: 0 messages
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_expiration
//! ```
use kubemq::prelude::*;
use kubemq::QueueMessageBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.expiration.example";

    // Send a message that expires in 3 seconds
    let msg = QueueMessageBuilder::new()
        .channel(channel)
        .body(b"expires-soon".to_vec())
        .expiration_seconds(3)
        .build();

    let result = client.send_queue_message(msg).await?;
    println!(
        "Sent message: id={}, expiration_at={}",
        result.message_id, result.expiration_at
    );

    // Receive immediately -- should get it
    let immediate = client.receive_queue_messages(channel, 10, 1, true).await?;
    println!("Before expiration (peek): {} messages", immediate.len());

    // Wait for expiration
    println!("Waiting 4 seconds for message to expire...");
    tokio::time::sleep(Duration::from_secs(4)).await;

    let after = client.receive_queue_messages(channel, 10, 1, false).await?;
    println!("After expiration: {} messages", after.len());

    client.close().await?;
    Ok(())
}
