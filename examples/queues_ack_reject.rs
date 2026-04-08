//! # Queues Ack/Reject Per Message
//!
//! Demonstrates per-message acknowledgment and rejection in a queue. Messages
//! are polled from a channel and individually acknowledged or rejected based on
//! their content. Rejected messages return to the queue for redelivery.
//!
//! ## Expected Output
//!
//! ```text
//! Polled 4 messages
//! Acked message: accept-0
//! Rejected message: reject-1
//! Acked message: accept-2
//! Rejected message: reject-3
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_ack_reject
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

    let channel = "queues.ack_reject.example";

    // Send messages with alternating accept/reject labels
    for i in 0..4 {
        let label = if i % 2 == 0 { "accept" } else { "reject" };
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("{}-{}", label, i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }

    let mut receiver = client.new_queue_downstream_receiver().await?;

    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 10,
        wait_timeout_seconds: 5,
        auto_ack: false,
    };

    let response = receiver.poll(poll).await?;
    println!("Polled {} messages", response.messages.len());

    for msg in &response.messages {
        let body = String::from_utf8_lossy(&msg.message.body);
        if body.starts_with("accept") {
            msg.ack().await?;
            println!("Acked message: {}", body);
        } else {
            msg.nack().await?;
            println!("Rejected message: {}", body);
        }
    }

    receiver.close().await?;
    client.close().await?;
    Ok(())
}
