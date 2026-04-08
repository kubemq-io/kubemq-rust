//! # Commands — Handle Command
//!
//! Demonstrates a long-lived command handler that subscribes to a channel and
//! processes incoming commands. Each command is logged and replied to with a
//! success response.
//!
//! ## Expected Output
//!
//! ```text
//! Handler ready on channel: commands.handle.example
//! Received command: id=<uuid>, body=task-0
//! Received command: id=<uuid>, body=task-1
//! Handled 2 commands
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example commands_handle
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

    let channel = "commands.handle.example";

    let rc = client.clone();
    let sub = client
        .subscribe_to_commands(
            channel,
            "",
            move |cmd| {
                let c = rc.clone();
                Box::pin(async move {
                    println!(
                        "Received command: id={}, body={}",
                        cmd.id,
                        String::from_utf8_lossy(&cmd.body)
                    );
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

    println!("Handler ready on channel: {}", channel);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send a few test commands
    for i in 0..2 {
        let command = CommandBuilder::new()
            .channel(channel)
            .body(format!("task-{}", i).into_bytes())
            .timeout(Duration::from_secs(10))
            .build();
        client.send_command(command).await?;
    }

    println!("Handled 2 commands");

    sub.unsubscribe().await;
    client.close().await?;
    Ok(())
}
