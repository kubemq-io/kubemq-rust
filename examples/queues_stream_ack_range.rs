//! Example: Queue stream downstream with selective ack by range.
use kubemq::prelude::*;
use kubemq::{PollRequest, QueueMessageBuilder};

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.stream.ackrange.example";

    // Send messages
    for i in 0..5 {
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("range-msg-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }

    let mut receiver = client.new_queue_downstream_receiver().await?;

    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 5,
        wait_timeout_seconds: 5,
        auto_ack: false,
    };

    let response = receiver.poll(poll).await?;
    println!("Received {} messages", response.messages.len());

    // Ack only the first 3 messages individually
    for (i, msg) in response.messages.iter().enumerate() {
        if i < 3 {
            msg.ack().await?;
            println!("Acked message at index {}", i);
        } else {
            println!("Skipping ack for message at index {}", i);
        }
    }

    receiver.close().await?;
    client.close().await?;
    Ok(())
}
