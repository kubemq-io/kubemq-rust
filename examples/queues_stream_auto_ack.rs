//! Example: Queue stream downstream with auto-ack mode.
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
