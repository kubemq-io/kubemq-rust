# How-To: Queues

Task-oriented guide for queue messaging with KubeMQ.

## Prerequisites

- A running KubeMQ broker (default: `localhost:50000`)
- `kubemq` crate added to your `Cargo.toml`
- `tokio` runtime with the `macros` and `rt-multi-thread` features

## Simple API

### Send a Queue Message

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let msg = QueueMessage::builder()
        .channel("tasks")
        .body(b"process-order-42".to_vec())
        .metadata("application/json")
        .add_tag("priority", "high")
        .build();

    let result = client.send_queue_message(msg).await?;
    println!("Sent: id={}, sent_at={}", result.message_id, result.sent_at);
    client.close().await
}
```

### Send with Convenience Method

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let result = client.send_queue_message_simple(
    "tasks",
    b"quick task".to_vec(),
    None,  // no metadata
    None,  // no tags
).await?;
# Ok(())
# }
```

### Send a Batch of Messages

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let messages: Vec<QueueMessage> = (0..10).map(|i| {
    QueueMessage::builder()
        .channel("tasks")
        .body(format!("task-{}", i).into_bytes())
        .build()
}).collect();

let results = client.send_queue_messages(messages).await?;
for result in &results {
    println!("Message {}: sent_at={}", result.message_id, result.sent_at);
}
# Ok(())
# }
```

### Receive Messages

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let messages = client.receive_queue_messages(
    "tasks",
    5,      // max messages
    10,     // wait timeout seconds
    false,  // is_peek (false = consume, true = peek without consuming)
).await?;

for msg in &messages {
    println!(
        "Received: channel={}, body={}",
        msg.channel,
        String::from_utf8_lossy(&msg.body)
    );
}
# Ok(())
# }
```

### Peek Messages (Without Consuming)

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let messages = client.receive_queue_messages(
    "tasks",
    5,
    10,
    true,  // peek = true: messages remain in queue
).await?;
println!("Peeked {} messages (not consumed)", messages.len());
# Ok(())
# }
```

### Acknowledge All Messages

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let response = client.ack_all_queue_messages("tasks", 5).await?;
println!("Acknowledged {} messages", response.affected_messages);
# Ok(())
# }
```

## Delivery Policies

### Delayed Messages

Send a message that becomes available after a delay:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let msg = QueueMessage::builder()
    .channel("scheduled-tasks")
    .body(b"run at midnight".to_vec())
    .delay_seconds(3600)  // available after 1 hour
    .build();

client.send_queue_message(msg).await?;
# Ok(())
# }
```

### Message Expiration

Set a time-to-live on messages:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let msg = QueueMessage::builder()
    .channel("time-sensitive")
    .body(b"flash sale".to_vec())
    .expiration_seconds(300)  // expires after 5 minutes
    .build();

client.send_queue_message(msg).await?;
# Ok(())
# }
```

### Dead-Letter Queue

Route messages to a dead-letter queue after exceeding retry attempts:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let msg = QueueMessage::builder()
    .channel("tasks")
    .body(b"might-fail".to_vec())
    .max_receive_count(3)                    // max 3 delivery attempts
    .max_receive_queue("tasks.dead-letter")  // route to DLQ after 3 failures
    .build();

client.send_queue_message(msg).await?;
# Ok(())
# }
```

## Stream API

### Poll Queue Messages

Poll for messages with transactional acknowledgment:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let request = PollRequest {
    channel: "tasks".to_string(),
    max_items: 10,
    wait_timeout_seconds: 5,
    auto_ack: false,
};

let mut response = client.poll_queue(request).await?;

if response.is_error {
    eprintln!("Poll error: {}", response.error);
    return Ok(());
}

for msg in &response.messages {
    println!("Processing: {}", String::from_utf8_lossy(&msg.body));
}

// Acknowledge all messages in this transaction
response.ack_all().await?;
# Ok(())
# }
```

### Poll with Auto-Ack

Automatically acknowledge messages on receipt:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let request = PollRequest {
    channel: "fire-and-forget-tasks".to_string(),
    max_items: 10,
    wait_timeout_seconds: 5,
    auto_ack: true,  // auto-ack on receipt
};

let response = client.poll_queue(request).await?;
for msg in &response.messages {
    println!("Auto-acked: {}", String::from_utf8_lossy(&msg.body));
}
# Ok(())
# }
```

### Nack Messages (Reject)

Reject messages so they return to the queue:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let request = PollRequest {
    channel: "tasks".to_string(),
    max_items: 5,
    wait_timeout_seconds: 5,
    auto_ack: false,
};

let mut response = client.poll_queue(request).await?;

// Reject all — messages return to queue for redelivery
response.nack_all().await?;
# Ok(())
# }
```

### Requeue Messages

Move messages to a different queue:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let request = PollRequest {
    channel: "incoming".to_string(),
    max_items: 5,
    wait_timeout_seconds: 5,
    auto_ack: false,
};

let mut response = client.poll_queue(request).await?;

// Move all messages to a different queue
response.requeue_all("outgoing").await?;
# Ok(())
# }
```

### Upstream Stream (High-Throughput Send)

Open a persistent stream for sending messages:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let mut handle = client.queue_upstream().await?;

let batch: Vec<QueueMessage> = (0..100).map(|i| {
    QueueMessage::builder()
        .channel("high-throughput")
        .body(format!("msg-{}", i).into_bytes())
        .build()
}).collect();

handle.send("batch-001", batch).await?;

// Check results
while let Ok(result) = handle.results().try_recv() {
    for r in &result.results {
        if r.is_error {
            eprintln!("Send error: {}", r.error);
        }
    }
}

handle.close();
# Ok(())
# }
```

### Downstream Receiver

Create a downstream receiver for continuous queue consumption:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let mut receiver = client.new_queue_downstream_receiver().await?;

// Poll and process in a loop
let request = PollRequest {
    channel: "tasks".to_string(),
    max_items: 10,
    wait_timeout_seconds: 5,
    auto_ack: false,
};

let mut response = receiver.poll(request).await?;

for msg in &response.messages {
    println!("Processing: {}", String::from_utf8_lossy(&msg.body));
}

// Acknowledge successful processing
response.ack_all().await?;

receiver.close();
# Ok(())
# }
```

## QueuePolicy Fields

| Field | Type | Description |
|-------|------|-------------|
| `expiration_seconds` | `u32` | Message TTL in seconds (0 = no expiration) |
| `delay_seconds` | `u32` | Delay before message becomes available (0 = immediate) |
| `max_receive_count` | `u32` | Max delivery attempts before dead-letter (0 = unlimited) |
| `max_receive_queue` | `String` | Dead-letter queue channel name |

## QueueMessageAttributes (Received Messages)

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | `i64` | Server timestamp when message was enqueued |
| `sequence` | `u64` | Message sequence number in the queue |
| `md5_of_body` | `String` | MD5 hash of the message body |
| `receive_count` | `u32` | Number of times this message has been delivered |
| `re_routed` | `bool` | Whether this message was re-routed from another queue |
| `re_routed_from_queue` | `String` | Source queue if re-routed |
| `expiration_at` | `i64` | Expiration timestamp (0 = no expiration) |
| `delayed_to` | `i64` | Timestamp when message becomes available |

---

**Related docs:**
- [Patterns](patterns.md) — comparing Queues with other patterns
- [Task Pipeline Scenario](scenario-task-pipeline.md) — end-to-end queue example
- [Error Handling](errors.md) — handling queue errors
