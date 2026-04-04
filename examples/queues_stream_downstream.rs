//! # Queue Stream Downstream (Manual Ack)
//!
//! Demonstrates the queue stream downstream receiver with manual acknowledgment.
//! Messages are polled via a persistent gRPC stream, processed, and then
//! explicitly acknowledged with `ack_all()`. The transaction ID tracks the
//! poll batch.
//!
//! ## Expected Output
//!
//! ```text
//! Polled: 5 messages, transaction=<transaction_id>
//! All messages acknowledged
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_stream_downstream
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

    let channel = "queues.stream.downstream.example";

    // Send some messages first
    for i in 0..5 {
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("downstream-msg-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }

    // Create a downstream receiver
    let mut receiver = client.new_queue_downstream_receiver().await?;

    // Poll for messages
    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 5,
        wait_timeout_seconds: 5,
        auto_ack: false,
    };

    let response = receiver.poll(poll).await?;
    println!(
        "Polled: {} messages, transaction={}",
        response.messages.len(),
        response.transaction_id
    );

    // Acknowledge all received messages
    response.ack_all().await?;
    println!("All messages acknowledged");

    receiver.close().await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    client.close().await?;
    Ok(())
}
