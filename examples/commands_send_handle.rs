//! Example: Send a command, subscribe as a responder, and respond.
use kubemq::prelude::*;
use kubemq::{CommandBuilder, CommandReplyBuilder};
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "commands.example";

    // Set up a command responder
    let responder_client = client.clone();
    let sub = client
        .subscribe_to_commands(
            channel,
            "",
            move |cmd| {
                let rc = responder_client.clone();
                Box::pin(async move {
                    println!(
                        "Received command: id={}, body={}",
                        cmd.id,
                        String::from_utf8_lossy(&cmd.body)
                    );

                    // Build and send a response
                    let reply = CommandReplyBuilder::new()
                        .request_id(&cmd.id)
                        .response_to(&cmd.response_to)
                        .executed_at(chrono_timestamp_millis())
                        .build();
                    tokio::spawn(async move {
                        if let Err(e) = rc.send_command_response(reply).await {
                            eprintln!("Failed to send command response: {}", e);
                        }
                    });
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send a command and wait for response
    let command = CommandBuilder::new()
        .channel(channel)
        .body(b"do-something".to_vec())
        .timeout(Duration::from_secs(10))
        .build();

    let response = client.send_command(command).await?;
    println!(
        "Command response: executed={}, error='{}'",
        response.executed, response.error
    );

    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}

fn chrono_timestamp_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
