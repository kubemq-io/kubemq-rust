//! Example: Send a query, subscribe as a responder, and respond with data.
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

    let channel = "queries.example";

    // Set up a query responder
    let responder_client = client.clone();
    let sub = client
        .subscribe_to_queries(
            channel,
            "",
            move |query| {
                let rc = responder_client.clone();
                Box::pin(async move {
                    println!(
                        "Received query: id={}, body={}",
                        query.id,
                        String::from_utf8_lossy(&query.body)
                    );

                    let reply = QueryReplyBuilder::new()
                        .request_id(&query.id)
                        .response_to(&query.response_to)
                        .body(b"query-response-data".to_vec())
                        .metadata("response-metadata")
                        .build();

                    let rc = rc;
                    tokio::spawn(async move {
                        if let Err(e) = rc.send_query_response(reply).await {
                            eprintln!("Failed to send query response: {}", e);
                        }
                    });
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send a query and get the response
    let query = QueryBuilder::new()
        .channel(channel)
        .body(b"what-is-the-answer".to_vec())
        .timeout(Duration::from_secs(10))
        .build();

    let response = client.send_query(query).await?;
    println!(
        "Query response: executed={}, body={}, cache_hit={}",
        response.executed,
        String::from_utf8_lossy(&response.body),
        response.cache_hit
    );

    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
