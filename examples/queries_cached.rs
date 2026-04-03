//! Example: Query with cache TTL -- verify cache hit on second query.
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

    let channel = "queries.cache.example";

    // Set up a query responder
    let responder_client = client.clone();
    let sub = client
        .subscribe_to_queries(
            channel,
            "",
            move |query| {
                let rc = responder_client.clone();
                Box::pin(async move {
                    println!("Responder handling query: {}", query.id);
                    let reply = QueryReplyBuilder::new()
                        .request_id(&query.id)
                        .response_to(&query.response_to)
                        .body(b"cached-response".to_vec())
                        .build();
                    tokio::spawn(async move {
                        let _ = rc.send_query_response(reply).await;
                    });
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // First query -- sets cache
    let query = QueryBuilder::new()
        .channel(channel)
        .body(b"get-data".to_vec())
        .timeout(Duration::from_secs(10))
        .cache_key("my-cache-key")
        .cache_ttl(Duration::from_secs(60))
        .build();

    let resp1 = client.send_query(query).await?;
    println!(
        "First query: cache_hit={}, body={}",
        resp1.cache_hit,
        String::from_utf8_lossy(&resp1.body)
    );

    // Second query -- should hit cache
    let query2 = QueryBuilder::new()
        .channel(channel)
        .body(b"get-data".to_vec())
        .timeout(Duration::from_secs(10))
        .cache_key("my-cache-key")
        .cache_ttl(Duration::from_secs(60))
        .build();

    let resp2 = client.send_query(query2).await?;
    println!(
        "Second query: cache_hit={}, body={}",
        resp2.cache_hit,
        String::from_utf8_lossy(&resp2.body)
    );

    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
