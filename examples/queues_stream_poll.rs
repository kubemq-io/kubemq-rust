//! Example: Queue stream downstream convenience poll.
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
