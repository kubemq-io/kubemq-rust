//! Example: Publish and subscribe to persistent events (Events Store).
use kubemq::prelude::*;
use kubemq::{EventStoreBuilder, EventsStoreSubscription};
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "events_store.example";

    // Subscribe from first message
    let sub = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartFromFirst,
            |event| {
                Box::pin(async move {
                    println!(
                        "Received store event: id={}, seq={}, body={}",
                        event.id,
                        event.sequence,
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send a persistent event
    let event = EventStoreBuilder::new()
        .channel(channel)
        .metadata("store-example")
        .body(b"Persistent message".to_vec())
        .build();

    let result = client.send_event_store(event).await?;
    println!("Event store sent: id={}, sent={}", result.id, result.sent);

    tokio::time::sleep(Duration::from_secs(2)).await;
    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
