//! # Queue Stream Dead-Letter Policy
//!
//! Demonstrates dead-letter queue routing with the stream API. Messages are
//! sent with `max_receive_count` and a DLQ channel. After repeated nack cycles,
//! messages are automatically moved to the dead-letter queue.
//!
//! ## Expected Output
//!
//! ```text
//! Sent 2 messages with DLQ policy via upstream
//! Nack cycle 1: rejected 2 messages
//! Nack cycle 2: rejected 2 messages
//! Nack cycle 3: rejected 2 messages
//! Dead-letter queue: 2 messages
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_stream_dead_letter_policy
//! ```
use kubemq::prelude::*;
use kubemq::{PollRequest, QueueMessageBuilder};

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.stream.dlq_policy.example";
    let dlq_channel = "queues.stream.dlq_policy.dead_letters";

    // Send messages with DLQ policy
    let mut upstream = client.queue_upstream().await?;
    let messages: Vec<QueueMessage> = (0..2)
        .map(|i| {
            QueueMessageBuilder::new()
                .channel(channel)
                .body(format!("may-fail-{}", i).into_bytes())
                .max_receive_count(3)
                .max_receive_queue(dlq_channel)
                .build()
        })
        .collect();

    upstream.send("dlq-batch", messages).await?;
    println!("Sent 2 messages with DLQ policy via upstream");
    let _ = upstream.results().recv().await;
    upstream.close();

    // Nack messages repeatedly to trigger DLQ
    let mut receiver = client.new_queue_downstream_receiver().await?;
    for cycle in 0..3 {
        let poll = PollRequest {
            channel: channel.to_string(),
            max_items: 10,
            wait_timeout_seconds: 3,
            auto_ack: false,
        };
        let response = receiver.poll(poll).await?;
        let count = response.messages.len();
        if count > 0 {
            response.nack_all().await?;
            println!("Nack cycle {}: rejected {} messages", cycle + 1, count);
        }
    }

    // Check the dead-letter queue
    let dlq_msgs = client
        .receive_queue_messages(dlq_channel, 10, 3, false)
        .await?;
    println!("Dead-letter queue: {} messages", dlq_msgs.len());

    receiver.close().await?;
    client.close().await?;
    Ok(())
}
