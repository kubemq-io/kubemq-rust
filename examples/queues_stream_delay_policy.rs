//! # Queue Stream Delay Policy
//!
//! Demonstrates sending queue messages with a delay policy via the upstream
//! stream. Messages are not visible to consumers until the delay elapses. An
//! immediate poll returns nothing; after the delay the messages appear.
//!
//! ## Expected Output
//!
//! ```text
//! Sent 3 delayed messages via upstream stream
//! Immediate poll: 0 messages
//! Waiting 6 seconds for delay to expire...
//! After delay: 3 messages received
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_stream_delay_policy
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

    let channel = "queues.stream.delay_policy.example";

    // Send messages with a 5-second delay
    let mut upstream = client.queue_upstream().await?;
    let messages: Vec<QueueMessage> = (0..3)
        .map(|i| {
            QueueMessageBuilder::new()
                .channel(channel)
                .body(format!("delayed-{}", i).into_bytes())
                .delay_seconds(5)
                .build()
        })
        .collect();

    upstream.send("delay-batch", messages).await?;
    println!("Sent 3 delayed messages via upstream stream");
    let _ = upstream.results().recv().await;
    upstream.close();

    // Immediate poll -- nothing visible yet
    let mut receiver = client.new_queue_downstream_receiver().await?;
    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 10,
        wait_timeout_seconds: 1,
        auto_ack: true,
    };
    let response = receiver.poll(poll).await?;
    println!("Immediate poll: {} messages", response.messages.len());

    // Wait for delay to expire
    println!("Waiting 6 seconds for delay to expire...");
    tokio::time::sleep(Duration::from_secs(6)).await;

    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 10,
        wait_timeout_seconds: 5,
        auto_ack: true,
    };
    let response = receiver.poll(poll).await?;
    println!("After delay: {} messages received", response.messages.len());

    receiver.close().await?;
    client.close().await?;
    Ok(())
}
