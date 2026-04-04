//! # Queue Stream Poll
//!
//! Demonstrates polling for queue messages using the downstream stream API.
//! Messages are sent first, then a `PollRequest` retrieves them in a single
//! batch with a wait timeout. After processing, all messages are acknowledged
//! together with `ack_all()`.
//!
//! ## Expected Output
//!
//! ```text
//! Polled 5 messages:
//!   seq=1, body=poll-msg-0
//!   seq=2, body=poll-msg-1
//!   seq=3, body=poll-msg-2
//!   seq=4, body=poll-msg-3
//!   seq=5, body=poll-msg-4
//! All acknowledged
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_stream_poll
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

    let channel = "queues.stream.poll.example";

    // Send messages
    for i in 0..5 {
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("poll-msg-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }

    let mut receiver = client.new_queue_downstream_receiver().await?;

    // Poll with wait
    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 10,
        wait_timeout_seconds: 5,
        auto_ack: false,
    };

    let response = receiver.poll(poll).await?;

    if response.is_error {
        eprintln!("Poll error: {}", response.error);
    } else {
        println!("Polled {} messages:", response.messages.len());
        for msg in &response.messages {
            println!(
                "  seq={}, body={}",
                msg.sequence,
                String::from_utf8_lossy(&msg.message.body)
            );
        }
        // Ack all
        response.ack_all().await?;
        println!("All acknowledged");
    }

    receiver.close().await?;
    client.close().await?;
    Ok(())
}
