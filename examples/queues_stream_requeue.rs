//! Example: Queue stream downstream -- re-queue messages to a different channel.
use kubemq::prelude::*;
use kubemq::{PollRequest, QueueMessageBuilder};

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let source_channel = "queues.stream.requeue.source";
    let target_channel = "queues.stream.requeue.target";

    // Send messages to source
    for i in 0..3 {
        let msg = QueueMessageBuilder::new()
            .channel(source_channel)
            .body(format!("requeue-msg-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }

    let mut receiver = client.new_queue_downstream_receiver().await?;

    let poll = PollRequest {
        channel: source_channel.to_string(),
        max_items: 3,
        wait_timeout_seconds: 5,
        auto_ack: false,
    };

    let response = receiver.poll(poll).await?;
    println!("Received {} messages from source", response.messages.len());

    // Re-queue each message to the target channel
    for msg in &response.messages {
        msg.re_queue(target_channel).await?;
        println!("Re-queued message {} to {}", msg.message.id, target_channel);
    }

    // Verify messages are in target channel
    let target_msgs = client
        .receive_queue_messages(target_channel, 10, 5, false)
        .await?;
    println!("Target channel has {} messages", target_msgs.len());

    receiver.close().await?;
    client.close().await?;
    Ok(())
}
