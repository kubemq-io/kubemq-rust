//! Example: Command handler with consumer group.
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

    let channel = "commands.group.example";
    let group = "cmd-handler-group";

    // Two responders in the same group -- only one handles each command
    let rc1 = client.clone();
    let sub1 = client
        .subscribe_to_commands(
            channel,
            group,
            move |cmd| {
                let c = rc1.clone();
                Box::pin(async move {
                    println!("Handler-1 received command: {}", cmd.id);
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

    let rc2 = client.clone();
    let sub2 = client
        .subscribe_to_commands(
            channel,
            group,
            move |cmd| {
                let c = rc2.clone();
                Box::pin(async move {
                    println!("Handler-2 received command: {}", cmd.id);
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

    // Send several commands
    for i in 0..5 {
        let command = CommandBuilder::new()
            .channel(channel)
            .body(format!("command-{}", i).into_bytes())
            .timeout(Duration::from_secs(10))
            .build();
        let resp = client.send_command(command).await?;
        println!("Command {} response: executed={}", i, resp.executed);
    }

    sub1.unsubscribe().await;
    sub2.unsubscribe().await;
    client.close().await?;
    Ok(())
}
