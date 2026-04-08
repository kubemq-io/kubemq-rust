//! # Queries — Send Query
//!
//! Demonstrates sending a query and receiving the response body. A query handler
//! is set up first, then the sender issues a query and prints the reply data.
//!
//! ## Expected Output
//!
//! ```text
//! Handler received query: id=<uuid>
//! Query response: body=response-data, cache_hit=false
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queries_send
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

    let channel = "queries.send.example";

    let rc = client.clone();
    let sub = client
        .subscribe_to_queries(
            channel,
            "",
            move |query| {
                let c = rc.clone();
                Box::pin(async move {
                    println!("Handler received query: id={}", query.id);
                    let reply = QueryReplyBuilder::new()
                        .request_id(&query.id)
                        .response_to(&query.response_to)
                        .body(b"response-data".to_vec())
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

    let query = QueryBuilder::new()
        .channel(channel)
        .body(b"get-data".to_vec())
        .metadata("query-metadata")
        .timeout(Duration::from_secs(10))
        .build();

    let response = client.send_query(query).await?;
    println!(
        "Query response: body={}, cache_hit={}",
        String::from_utf8_lossy(&response.body),
        response.cache_hit
    );

    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
