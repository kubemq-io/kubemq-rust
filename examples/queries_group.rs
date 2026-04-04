//! # Queries with Consumer Group
//!
//! Demonstrates load-balanced query handling using a consumer group. Two
//! query responders join the same group — each incoming query is routed to
//! only one handler, and the caller receives the response from whichever
//! handler processed it.
//!
//! ## Expected Output
//!
//! ```text
//! Handler-1 received query: <uuid>
//! Query 0 response from: from-handler-1
//! Handler-2 received query: <uuid>
//! Query 1 response from: from-handler-2
//! ...
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queries_group
//! ```
use kubemq::prelude::*;
use kubemq::{QueryBuilder, QueryReplyBuilder};
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queries.group.example";
    let group = "query-handler-group";

    let rc1 = client.clone();
    let sub1 = client
        .subscribe_to_queries(
            channel,
            group,
            move |query| {
                let c = rc1.clone();
                Box::pin(async move {
                    println!("Handler-1 received query: {}", query.id);
                    let reply = QueryReplyBuilder::new()
                        .request_id(&query.id)
                        .response_to(&query.response_to)
                        .body(b"from-handler-1".to_vec())
                        .build();
                    tokio::spawn(async move {
                        let _ = c.send_query_response(reply).await;
                    });
                })
            },
            None,
        )
        .await?;

    let rc2 = client.clone();
    let sub2 = client
        .subscribe_to_queries(
            channel,
            group,
            move |query| {
                let c = rc2.clone();
                Box::pin(async move {
                    println!("Handler-2 received query: {}", query.id);
                    let reply = QueryReplyBuilder::new()
                        .request_id(&query.id)
                        .response_to(&query.response_to)
                        .body(b"from-handler-2".to_vec())
                        .build();
                    tokio::spawn(async move {
                        let _ = c.send_query_response(reply).await;
                    });
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    for i in 0..5 {
        let query = QueryBuilder::new()
            .channel(channel)
            .body(format!("query-{}", i).into_bytes())
            .timeout(Duration::from_secs(10))
            .build();
        let resp = client.send_query(query).await?;
        println!(
            "Query {} response from: {}",
            i,
            String::from_utf8_lossy(&resp.body)
        );
    }

    sub1.unsubscribe().await;
    sub2.unsubscribe().await;
    client.close().await?;
    Ok(())
}
