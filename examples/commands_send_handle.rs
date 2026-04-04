//! # Commands Send and Handle
//!
//! Demonstrates the command (RPC) pattern: a responder subscribes to a channel
//! and handles incoming commands by sending back a response. The sender waits
//! synchronously for the response within a timeout.
//!
//! ## Expected Output
//!
//! ```text
//! Received command: id=<uuid>, body=do-something
//! Command response: executed=true, error=''
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example commands_send_handle
//! ```
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
