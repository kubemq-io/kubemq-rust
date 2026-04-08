//! # Queue Stream Expiration Policy
//!
//! Demonstrates sending queue messages with an expiration (TTL) policy via the
//! upstream stream. Messages that are not consumed before the TTL elapses are
//! automatically removed from the queue.
//!
//! ## Expected Output
//!
//! ```text
//! Sent 3 messages with 3s expiration via upstream stream
//! Before expiration: 3 messages (peeked)
//! Waiting 4 seconds for messages to expire...
//! After expiration: 0 messages
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_stream_expiration_policy
//! ```
use kubemq::prelude::*;
use kubemq::{PollRequest, QueueMessageBuilder};
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.stream.expiration_policy.example";

    // Send messages with a 3-second expiration via upstream stream
    let mut upstream = client.queue_upstream().await?;
    let messages: Vec<QueueMessage> = (0..3)
        .map(|i| {
            QueueMessageBuilder::new()
                .channel(channel)
                .body(format!("expires-soon-{}", i).into_bytes())
                .expiration_seconds(3)
                .build()
        })
        .collect();

    upstream.send("expiration-batch", messages).await?;
    println!("Sent 3 messages with 3s expiration via upstream stream");
    let _ = upstream.results().recv().await;
    upstream.close();

    // Peek immediately -- messages should still exist
    let before = client.receive_queue_messages(channel, 10, 1, true).await?;
    println!("Before expiration: {} messages (peeked)", before.len());

    // Wait for expiration
    println!("Waiting 4 seconds for messages to expire...");
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Poll after expiration -- messages should be gone
    let mut receiver = client.new_queue_downstream_receiver().await?;
    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 10,
        wait_timeout_seconds: 2,
        auto_ack: true,
    };
    let response = receiver.poll(poll).await?;
    println!("After expiration: {} messages", response.messages.len());

    receiver.close().await?;
    client.close().await?;
    Ok(())
}
