//! Integration tests for Commands pattern (requires live KubeMQ broker).

use kubemq::{Command, CommandReply, KubemqClient};
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
async fn test_subscribe_and_send_command() {
    let responder = live_client("rust-cmd-responder").await;
    let sender = live_client("rust-cmd-sender").await;

    let responder_clone = responder.clone();
    let sub = responder
        .subscribe_to_commands(
            "integration.commands.test",
            "",
            move |cmd| {
                let client = responder_clone.clone();
                Box::pin(async move {
                    let reply = CommandReply::builder()
                        .request_id(&cmd.id)
                        .response_to(&cmd.response_to)
                        .build();
                    tokio::spawn(async move {
                        let _ = client.send_command_response(reply).await;
                    });
                })
            },
            None,
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let cmd = Command::builder()
        .channel("integration.commands.test")
        .metadata("test")
        .body(b"command-payload".to_vec())
        .timeout(Duration::from_secs(5))
        .build();

    let response = sender.send_command(cmd).await.unwrap();
    assert!(response.executed);

    sub.cancel();
    sender.close().await.unwrap();
    responder.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_command_timeout() {
    let sender = live_client("rust-cmd-timeout").await;

    // No responder listening, so this should time out
    let cmd = Command::builder()
        .channel("integration.commands.timeout")
        .metadata("test")
        .timeout(Duration::from_secs(1))
        .build();

    let result = sender.send_command(cmd).await;
    // Should either get a timeout error or a response with executed=false
    if let Ok(resp) = result {
        // Server may return a response with error
        assert!(!resp.error.is_empty() || !resp.executed);
    }

    sender.close().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_send_command_simple() {
    let client = live_client("rust-cmd-simple").await;
    // Will likely timeout since no responder, but should not panic
    let _ = client
        .send_command_simple(
            "integration.commands.simple",
            b"data",
            Duration::from_secs(1),
        )
        .await;
    client.close().await.unwrap();
}
