//! # Pattern — Request/Reply
//!
//! Demonstrates the request/reply RPC pattern using queries. A query handler
//! acts as a server returning data, while the client sends queries and reads
//! the response body — classic synchronous RPC over messaging.
//!
//! ## Expected Output
//!
//! ```text
//! Request: get-user-info
//! Reply: user-info:id=42,name=Alice
//! Request: get-user-info
//! Reply: user-info:id=42,name=Alice
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example pattern_request_reply
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

    let channel = "patterns.request_reply.example";

    // Server: subscribe and respond to queries
    let rc = client.clone();
    let sub = client
        .subscribe_to_queries(
            channel,
            "",
            move |query| {
                let c = rc.clone();
                Box::pin(async move {
                    let reply = QueryReplyBuilder::new()
                        .request_id(&query.id)
                        .response_to(&query.response_to)
                        .body(b"user-info:id=42,name=Alice".to_vec())
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

    // Client: send queries and read replies
    for _ in 0..2 {
        let query = QueryBuilder::new()
            .channel(channel)
            .body(b"get-user-info".to_vec())
            .timeout(Duration::from_secs(10))
            .build();

        println!("Request: get-user-info");
        let response = client.send_query(query).await?;
        println!("Reply: {}", String::from_utf8_lossy(&response.body));
    }

    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
