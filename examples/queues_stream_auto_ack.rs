//! # Queue Stream Auto-Ack
//!
//! Demonstrates queue stream polling with automatic acknowledgment. When
//! `auto_ack` is set to `true` in the `PollRequest`, messages are automatically
//! acknowledged by the broker upon delivery — no explicit ack call is needed.
//!
//! ## Expected Output
//!
//! ```text
//! Auto-ack polled: 5 messages
//!   id=<uuid>, body=auto-ack-msg-0
//!   id=<uuid>, body=auto-ack-msg-1
//!   id=<uuid>, body=auto-ack-msg-2
//!   id=<uuid>, body=auto-ack-msg-3
//!   id=<uuid>, body=auto-ack-msg-4
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_stream_auto_ack
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

    let channel = "queues.stream.autoack.example";

    // Send messages
    for i in 0..5 {
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("auto-ack-msg-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }

    let mut receiver = client.new_queue_downstream_receiver().await?;

    // Poll with auto_ack=true -- messages are automatically acknowledged
    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 10,
        wait_timeout_seconds: 5,
        auto_ack: true,
    };

    let response = receiver.poll(poll).await?;
    println!("Auto-ack polled: {} messages", response.messages.len());
    for msg in &response.messages {
        println!(
            "  id={}, body={}",
            msg.message.id,
            String::from_utf8_lossy(&msg.message.body)
        );
    }

    receiver.close().await?;
    client.close().await?;
    Ok(())
}
