# Scenario: Task Processing Pipeline

This tutorial builds a complete task processing pipeline using KubeMQ Queues with producer, worker, and dead-letter consumer components.

## Architecture

```text
┌──────────┐    ┌─────────┐    ┌────────┐
│ Producer │───▶│  tasks  │───▶│ Worker │
└──────────┘    └─────────┘    └───┬────┘
                                   │ nack (after 3 attempts)
                               ┌───▼────────────┐
                               │ tasks.dead-letter│
                               └───┬────────────┘
                                   │
                            ┌──────▼──────┐
                            │ DLQ Consumer │
                            └─────────────┘
```

- **Producer** sends tasks with expiration and dead-letter policies
- **Worker** polls tasks, processes them, and acks or nacks
- **DLQ Consumer** monitors failed tasks in the dead-letter queue

## Prerequisites

- A running KubeMQ broker on `localhost:50000`
- `kubemq` and `tokio` in your `Cargo.toml`

## Producer

The producer sends tasks with delivery policies:

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("task-producer")
        .build()
        .await?;

    for i in 0..10 {
        let msg = QueueMessage::builder()
            .channel("tasks")
            .body(format!(r#"{{"task_id": {}, "action": "process_order"}}"#, i).into_bytes())
            .metadata("application/json")
            .add_tag("priority", if i % 3 == 0 { "high" } else { "normal" })
            .expiration_seconds(300)          // expire after 5 minutes
            .max_receive_count(3)             // allow 3 delivery attempts
            .max_receive_queue("tasks.dead-letter")  // route to DLQ after failures
            .build();

        let result = client.send_queue_message(msg).await?;
        println!("Task {} sent: id={}", i, result.message_id);
    }

    println!("All tasks submitted");
    client.close().await
}
```

## Worker

The worker polls for tasks, processes them, and acknowledges or rejects:

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("task-worker")
        .build()
        .await?;

    println!("Worker started, polling for tasks...");

    loop {
        let request = PollRequest {
            channel: "tasks".to_string(),
            max_items: 5,
            wait_timeout_seconds: 10,
            auto_ack: false,
        };

        let mut response = client.poll_queue(request).await?;

        if response.is_error {
            eprintln!("Poll error: {}", response.error);
            continue;
        }

        if response.messages.is_empty() {
            println!("No tasks available, waiting...");
            continue;
        }

        println!("Received {} tasks", response.messages.len());

        let mut all_ok = true;
        for msg in &response.messages {
            let body = String::from_utf8_lossy(&msg.body);
            let receive_count = msg.attributes
                .as_ref()
                .map(|a| a.receive_count)
                .unwrap_or(0);

            println!(
                "Processing task: {} (attempt {})",
                body, receive_count
            );

            // Simulate processing — some tasks fail
            let success = process_task(&body);

            if !success {
                println!("Task failed, will nack");
                all_ok = false;
            }
        }

        if all_ok {
            response.ack_all().await?;
            println!("All tasks acknowledged");
        } else {
            response.nack_all().await?;
            println!("Tasks nacked — will be redelivered");
        }
    }
}

fn process_task(body: &str) -> bool {
    // Simulate: tasks with id divisible by 5 fail
    !body.contains("\"task_id\": 5") && !body.contains("\"task_id\": 0")
}
```

## Dead-Letter Consumer

Monitor the dead-letter queue for permanently failed tasks:

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("dlq-consumer")
        .build()
        .await?;

    println!("Dead-letter consumer started...");

    loop {
        let request = PollRequest {
            channel: "tasks.dead-letter".to_string(),
            max_items: 10,
            wait_timeout_seconds: 15,
            auto_ack: false,
        };

        let mut response = client.poll_queue(request).await?;

        if response.is_error || response.messages.is_empty() {
            continue;
        }

        for msg in &response.messages {
            let body = String::from_utf8_lossy(&msg.body);
            let attrs = msg.attributes.as_ref();

            println!(
                "[DLQ] Failed task: {}",
                body,
            );

            if let Some(attrs) = attrs {
                println!(
                    "  Receive count: {}, Re-routed: {}, From: {}",
                    attrs.receive_count,
                    attrs.re_routed,
                    attrs.re_routed_from_queue
                );
            }

            // Log, alert, or store for manual review
        }

        response.ack_all().await?;
        println!("[DLQ] Acknowledged {} dead-letter messages", response.messages.len());
    }
}
```

## Running the Pipeline

Start each component in a separate terminal:

```bash
# Terminal 1: Start the worker
KUBEMQ_ADDRESS=localhost:50000 cargo run --example worker

# Terminal 2: Start the DLQ consumer
KUBEMQ_ADDRESS=localhost:50000 cargo run --example dlq_consumer

# Terminal 3: Send tasks
KUBEMQ_ADDRESS=localhost:50000 cargo run --example producer
```

## Expected Behavior

1. Producer sends 10 tasks to `tasks` queue
2. Worker polls and processes tasks:
   - Successful tasks are acked and removed
   - Failed tasks are nacked and returned to queue
3. After 3 delivery attempts (nacks), failed tasks are routed to `tasks.dead-letter`
4. DLQ consumer picks up permanently failed tasks for logging/alerting
5. Tasks that are not processed within 5 minutes expire automatically

## Key Concepts Demonstrated

| Concept | Implementation |
|---------|---------------|
| Guaranteed delivery | Queue with ack/nack |
| Dead-letter routing | `max_receive_count` + `max_receive_queue` |
| Message expiration | `expiration_seconds` |
| Transactional polling | `PollRequest` with `auto_ack: false` |
| Error visibility | `QueueMessageAttributes.receive_count` tracking |

---

**Related docs:**
- [Queues How-To](howto-queues.md) — complete queue operations reference
- [Patterns](patterns.md) — when to use queues vs other patterns
- [Error Handling](errors.md) — handling queue errors
