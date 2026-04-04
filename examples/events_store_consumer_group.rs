//! # Events Store Consumer Group
//!
//! Demonstrates load-balanced consumption of persistent events using a consumer
//! group. Two subscribers in the same group share the workload — each stored
//! event is delivered to only one consumer in the group.
//!
//! ## Expected Output
//!
//! ```text
//! Consumer-1: seq=1, body=group-msg-0
//! Consumer-2: seq=2, body=group-msg-1
//! Consumer-1: seq=3, body=group-msg-2
//! ...
//! Sent 10 events to store group 'store-consumer-group'
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_store_consumer_group
//! ```
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
