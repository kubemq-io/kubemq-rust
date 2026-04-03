//! Example: Publish an event and subscribe to receive it.
use kubemq::prelude::*;
use kubemq::{EventBuilder, Subscription};
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "events.example";

    // Subscribe to events
    let sub: Subscription = client
        .subscribe_to_events(
            channel,
            "",
            |event| {
                Box::pin(async move {
                    println!(
                        "Received event: id={}, channel={}, body={} bytes",
                        event.id,
                        event.channel,
                        event.body.len()
                    );
                })
            },
            None,
        )
        .await?;

    // Give subscription time to establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send an event
    let event = EventBuilder::new()
        .channel(channel)
        .metadata("example-metadata")
        .body(b"Hello KubeMQ!".to_vec())
        .build();

    client.send_event(event).await?;
    println!("Event sent to channel: {}", channel);

    // Wait to receive
    tokio::time::sleep(Duration::from_secs(2)).await;

    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
