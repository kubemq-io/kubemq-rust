//! # Commands — Send Command
//!
//! Demonstrates sending a command and waiting for the handler's response.
//! A command handler is set up first, then the sender issues a command and
//! prints the execution result.
//!
//! ## Expected Output
//!
//! ```text
//! Handler received command: id=<uuid>
//! Command response: executed=true, error=''
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example commands_send
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

    let channel = "commands.send.example";

    let rc = client.clone();
    let sub = client
        .subscribe_to_commands(
            channel,
            "",
            move |cmd| {
                let c = rc.clone();
                Box::pin(async move {
                    println!("Handler received command: id={}", cmd.id);
                    let reply = CommandReplyBuilder::new()
                        .request_id(&cmd.id)
                        .response_to(&cmd.response_to)
                        .build();
                    tokio::spawn(async move {
                        let _ = c.send_command_response(reply).await;
                    });
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let command = CommandBuilder::new()
        .channel(channel)
        .body(b"do-something".to_vec())
        .metadata("command-metadata")
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
