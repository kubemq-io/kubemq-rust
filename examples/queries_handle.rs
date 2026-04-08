//! # Queries — Handle Query
//!
//! Demonstrates a long-lived query handler that subscribes to a channel,
//! processes incoming queries, and returns structured response bodies.
//!
//! ## Expected Output
//!
//! ```text
//! Query handler ready on channel: queries.handle.example
//! Received query: id=<uuid>, body=lookup-item
//! Response: executed=true, body=item-found:lookup-item
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queries_handle
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

    let channel = "queries.handle.example";

    let rc = client.clone();
    let sub = client
        .subscribe_to_queries(
            channel,
            "",
            move |query| {
                let c = rc.clone();
                Box::pin(async move {
                    let body_str = String::from_utf8_lossy(&query.body).to_string();
                    println!("Received query: id={}, body={}", query.id, body_str);

                    let reply = QueryReplyBuilder::new()
                        .request_id(&query.id)
                        .response_to(&query.response_to)
                        .body(format!("item-found:{}", body_str).into_bytes())
                        .metadata("result-metadata")
                        .build();
                    tokio::spawn(async move {
                        let _ = c.send_query_response(reply).await;
                    });
                })
            },
            None,
        )
        .await?;

    println!("Query handler ready on channel: {}", channel);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let query = QueryBuilder::new()
        .channel(channel)
        .body(b"lookup-item".to_vec())
        .timeout(Duration::from_secs(10))
        .build();

    let response = client.send_query(query).await?;
    println!(
        "Response: executed={}, body={}",
        response.executed,
        String::from_utf8_lossy(&response.body)
    );

    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
