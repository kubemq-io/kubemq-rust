//! Integration tests for channel management (requires live KubeMQ broker).

use kubemq::{channel_type, KubemqClient};

async fn live_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-integration-channels")
        .check_connection(false)
        .build()
        .await
        .expect("live broker required")
}

#[test]
#[ignore]
fn test_channel_type_constants() {
    assert_eq!(channel_type::EVENTS, "events");
    assert_eq!(channel_type::EVENTS_STORE, "events_store");
    assert_eq!(channel_type::COMMANDS, "commands");
    assert_eq!(channel_type::QUERIES, "queries");
    assert_eq!(channel_type::QUEUES, "queues");
}

#[tokio::test]
#[ignore]
async fn test_ping_returns_server_info() {
    let client = live_client().await;
    let info = client.ping().await.unwrap();
    assert!(!info.host.is_empty());
    assert!(!info.version.is_empty());
    assert!(info.server_start_time > 0);
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_client_close_idempotent() {
    let client = live_client().await;
    client.close().await.unwrap();
    client.close().await.unwrap(); // second close should not error
}

#[tokio::test]
#[ignore]
async fn test_create_and_delete_events_channel() {
    let client = live_client().await;
    let name = format!("test-events-ch-{}", uuid::Uuid::new_v4());
    client.create_events_channel(&name).await.unwrap();
    // List should contain the channel
    let channels = client.list_events_channels("").await.unwrap();
    assert!(channels.iter().any(|c| c.name == name));
    // Delete
    client.delete_events_channel(&name).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_create_and_delete_events_store_channel() {
    let client = live_client().await;
    let name = format!("test-es-ch-{}", uuid::Uuid::new_v4());
    client.create_events_store_channel(&name).await.unwrap();
    let channels = client.list_events_store_channels("").await.unwrap();
    assert!(channels.iter().any(|c| c.name == name));
    client.delete_events_store_channel(&name).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_create_and_delete_commands_channel() {
    let client = live_client().await;
    let name = format!("test-cmd-ch-{}", uuid::Uuid::new_v4());
    client.create_commands_channel(&name).await.unwrap();
    let channels = client.list_commands_channels("").await.unwrap();
    assert!(channels.iter().any(|c| c.name == name));
    client.delete_commands_channel(&name).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_create_and_delete_queries_channel() {
    let client = live_client().await;
    let name = format!("test-qry-ch-{}", uuid::Uuid::new_v4());
    client.create_queries_channel(&name).await.unwrap();
    let channels = client.list_queries_channels("").await.unwrap();
    assert!(channels.iter().any(|c| c.name == name));
    client.delete_queries_channel(&name).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_create_and_delete_queues_channel() {
    let client = live_client().await;
    let name = format!("test-q-ch-{}", uuid::Uuid::new_v4());
    client.create_queues_channel(&name).await.unwrap();
    let channels = client.list_queues_channels("").await.unwrap();
    assert!(channels.iter().any(|c| c.name == name));
    client.delete_queues_channel(&name).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_create_channel_empty_name_rejected() {
    let client = live_client().await;
    let result = client.create_channel("", channel_type::EVENTS).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), kubemq::ErrorCode::Validation);
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_create_channel_invalid_type_rejected() {
    let client = live_client().await;
    let result = client.create_channel("test", "invalid").await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), kubemq::ErrorCode::Validation);
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_delete_channel_empty_name_rejected() {
    let client = live_client().await;
    let result = client.delete_channel("", channel_type::EVENTS).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), kubemq::ErrorCode::Validation);
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_list_channels_invalid_type_rejected() {
    let client = live_client().await;
    let result = client.list_channels("bad_type", "").await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), kubemq::ErrorCode::Validation);
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_list_channels_with_search() {
    let client = live_client().await;
    let prefix = format!(
        "test-search-{}",
        uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
    );
    let name = format!("{}-events", prefix);
    client.create_events_channel(&name).await.unwrap();

    let filtered = client.list_events_channels(&prefix).await.unwrap();
    assert!(filtered.iter().any(|c| c.name == name));

    // Cleanup
    client.delete_events_channel(&name).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_closed_client_rejects_channel_operations() {
    let client = live_client().await;
    client.close().await.unwrap();

    let result = client.create_channel("test", channel_type::EVENTS).await;
    assert!(result.is_err());
    let result = client.delete_channel("test", channel_type::EVENTS).await;
    assert!(result.is_err());
    let result = client.list_channels(channel_type::EVENTS, "").await;
    assert!(result.is_err());
}
