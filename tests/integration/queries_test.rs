//! Integration tests for Queries pattern (requires live KubeMQ broker).

use kubemq::{KubemqClient, Query, QueryReply};
use std::time::Duration;

async fn live_client(id: &str) -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id(id)
        .check_connection(false)
        .build()
        .await
        .expect("live broker required")
}

#[tokio::test]
#[ignore]
async fn test_subscribe_and_send_query() {
    let responder = live_client("rust-query-responder").await;
    let sender = live_client("rust-query-sender").await;

    let responder_clone = responder.clone();
    let sub = responder
        .subscribe_to_queries(
            "integration.queries.test",
            "",
            move |qry| {
                let client = responder_clone.clone();
                Box::pin(async move {
                    let reply = QueryReply::builder()
                        .request_id(&qry.id)
                        .response_to(&qry.response_to)
                        .body(b"query-response".to_vec())
                        .build();
                    tokio::spawn(async move {
                        let _ = client.send_query_response(reply).await;
                    });
                })
            },
            None,
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let q = Query::builder()
        .channel("integration.queries.test")
        .metadata("test")
        .body(b"query-payload".to_vec())
        .timeout(Duration::from_secs(5))
        .build();

    let response = sender.send_query(q).await.unwrap();
    assert!(response.executed);
    assert_eq!(response.body, b"query-response");

    sub.cancel();
    sender.close().await.unwrap();
    responder.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_query_timeout() {
    let sender = live_client("rust-query-timeout").await;

    let q = Query::builder()
        .channel("integration.queries.timeout")
        .metadata("test")
        .timeout(Duration::from_secs(1))
        .build();

    let result = sender.send_query(q).await;
    if let Ok(resp) = result {
        assert!(!resp.error.is_empty() || !resp.executed);
    }

    sender.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_send_query_simple() {
    let client = live_client("rust-query-simple").await;
    let _ = client
        .send_query_simple(
            "integration.queries.simple",
            b"data",
            Duration::from_secs(1),
        )
        .await;
    client.close().await.unwrap();
}
