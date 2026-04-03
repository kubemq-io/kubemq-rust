//! Example: Queue stream downstream -- reject (nack) messages.
use kubemq::prelude::*;
use kubemq::{PollRequest, QueueMessageBuilder};

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.stream.nack.example";

    // Send messages
    for i in 0..3 {
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("nack-msg-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }

    let mut receiver = client.new_queue_downstream_receiver().await?;

    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 3,
        wait_timeout_seconds: 5,
        auto_ack: false,
    };

    let response = receiver.poll(poll).await?;
    println!("Received {} messages", response.messages.len());

    // Nack all -- messages return to queue for redelivery
    response.nack_all().await?;
    println!("All messages nacked (returned to queue)");

    receiver.close().await?;
    client.close().await?;
    Ok(())
}
