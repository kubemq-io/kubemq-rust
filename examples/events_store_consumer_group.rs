//! Example: Events Store subscription with consumer group.
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

    let channel = "events_store.group.example";
    let group = "store-consumer-group";

    // Two consumers in the same group
    let sub1 = client
        .subscribe_to_events_store(
            channel,
            group,
            EventsStoreSubscription::StartFromFirst,
            |event| {
                Box::pin(async move {
                    println!(
                        "Consumer-1: seq={}, body={}",
                        event.sequence,
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    let sub2 = client
        .subscribe_to_events_store(
            channel,
            group,
            EventsStoreSubscription::StartFromFirst,
            |event| {
                Box::pin(async move {
                    println!(
                        "Consumer-2: seq={}, body={}",
                        event.sequence,
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send events
    for i in 0..10 {
        let event = EventStoreBuilder::new()
            .channel(channel)
            .body(format!("group-msg-{}", i).into_bytes())
            .build();
        client.send_event_store(event).await?;
    }

    println!("Sent 10 events to store group '{}'", group);
    tokio::time::sleep(Duration::from_secs(2)).await;

    sub1.unsubscribe().await;
    sub2.unsubscribe().await;
    client.close().await?;
    Ok(())
}
