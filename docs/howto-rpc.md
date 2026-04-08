# How-To: Commands & Queries

Task-oriented guide for RPC-style messaging with KubeMQ.

## Prerequisites

- A running KubeMQ broker (default: `localhost:50000`)
- `kubemq` crate added to your `Cargo.toml`
- `tokio` runtime with the `macros` and `rt-multi-thread` features

## Commands

Commands are request-response with timeout. The sender blocks until a subscriber executes the command and returns a success/failure response.

### Send a Command

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let cmd = Command::builder()
    .channel("device.reboot")
    .body(b"device-42".to_vec())
    .timeout(Duration::from_secs(10))
    .metadata("reboot-reason: maintenance")
    .build();

let response = client.send_command(cmd).await?;
println!("Executed: {}", response.executed);
# Ok(())
# }
```

### Send with Convenience Method

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let response = client.send_command_simple(
    "device.reboot",
    b"device-42".to_vec(),
    Duration::from_secs(10),
    None,  // no metadata
    None,  // no tags
).await?;
println!("Executed: {}", response.executed);
# Ok(())
# }
```

### Handle Commands (Subscribe)

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let client_for_handler = client.clone();
    let subscription = client.subscribe_to_commands(
        "device.reboot",
        "",  // no consumer group
        move |cmd| {
            let client = client_for_handler.clone();
            Box::pin(async move {
                println!(
                    "Received command on '{}': {:?}",
                    cmd.channel,
                    String::from_utf8_lossy(&cmd.body)
                );

                // Process the command and send response
                let reply = CommandReply::builder()
                    .reply_channel(&cmd.reply_channel)
                    .request_id(&cmd.id)
                    .client_id(&cmd.client_id)
                    .executed(true)
                    .build();

                if let Err(e) = client.send_command_response(reply).await {
                    eprintln!("Failed to send response: {}", e);
                }
            })
        },
        Some(Box::new(|err| Box::pin(async move {
            eprintln!("Command subscription error: {}", err);
        }))),
    ).await?;

    subscription.done().await;
    client.close().await
}
```

### Handle Commands with Consumer Group

Load-balance commands across multiple handlers:

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let client_clone = client.clone();
let sub = client.subscribe_to_commands(
    "device.reboot",
    "handlers",  // group name — each command goes to one handler
    move |cmd| {
        let c = client_clone.clone();
        Box::pin(async move {
            let reply = CommandReply::builder()
                .reply_channel(&cmd.reply_channel)
                .request_id(&cmd.id)
                .client_id(&cmd.client_id)
                .executed(true)
                .build();
            let _ = c.send_command_response(reply).await;
        })
    },
    None,
).await?;
# Ok(())
# }
```

### Handle Timeout

Commands always have a timeout. If no subscriber responds in time, the sender receives a `KubemqError::Timeout`:

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let cmd = Command::builder()
    .channel("slow.operation")
    .body(b"data".to_vec())
    .timeout(Duration::from_secs(2))
    .build();

match client.send_command(cmd).await {
    Ok(response) => println!("Executed: {}", response.executed),
    Err(e) if e.is_retryable() => {
        eprintln!("Timed out or transient error, retry: {}", e);
    }
    Err(e) => eprintln!("Permanent error: {}", e),
}
# Ok(())
# }
```

## Queries

Queries are request-response with optional server-side caching. The response can carry data.

### Send a Query

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let query = Query::builder()
    .channel("inventory.lookup")
    .body(b"sku-12345".to_vec())
    .timeout(Duration::from_secs(5))
    .build();

let response = client.send_query(query).await?;
println!(
    "Executed: {}, Body: {}",
    response.executed,
    String::from_utf8_lossy(&response.body)
);
# Ok(())
# }
```

### Send with Convenience Method

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let response = client.send_query_simple(
    "inventory.lookup",
    b"sku-12345".to_vec(),
    Duration::from_secs(5),
    None,  // no metadata
    None,  // no tags
).await?;
# Ok(())
# }
```

### Send a Cached Query

Cache the response on the server to avoid repeated computation:

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let query = Query::builder()
    .channel("inventory.lookup")
    .body(b"sku-12345".to_vec())
    .timeout(Duration::from_secs(5))
    .cache_key("sku-12345")        // enable caching with this key
    .cache_ttl(Duration::from_secs(60))  // cache for 60 seconds
    .build();

let response = client.send_query(query).await?;
println!(
    "Executed: {}, Cache hit: {}, Body: {}",
    response.executed,
    response.cache_hit,
    String::from_utf8_lossy(&response.body)
);

// Second call with the same cache_key may return a cached response
let query2 = Query::builder()
    .channel("inventory.lookup")
    .body(b"sku-12345".to_vec())
    .timeout(Duration::from_secs(5))
    .cache_key("sku-12345")
    .cache_ttl(Duration::from_secs(60))
    .build();

let response2 = client.send_query(query2).await?;
println!("Cache hit: {}", response2.cache_hit);  // likely true
# Ok(())
# }
```

### Handle Queries (Subscribe)

```rust,no_run
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let client_for_handler = client.clone();
    let subscription = client.subscribe_to_queries(
        "inventory.lookup",
        "",
        move |query| {
            let client = client_for_handler.clone();
            Box::pin(async move {
                println!(
                    "Query on '{}': {}",
                    query.channel,
                    String::from_utf8_lossy(&query.body)
                );

                // Look up the data and respond
                let inventory_data = b"in_stock: 42".to_vec();

                let reply = QueryReply::builder()
                    .reply_channel(&query.reply_channel)
                    .request_id(&query.id)
                    .client_id(&query.client_id)
                    .executed(true)
                    .body(inventory_data)
                    .metadata("application/json")
                    .build();

                if let Err(e) = client.send_query_response(reply).await {
                    eprintln!("Failed to send response: {}", e);
                }
            })
        },
        Some(Box::new(|err| Box::pin(async move {
            eprintln!("Query subscription error: {}", err);
        }))),
    ).await?;

    subscription.done().await;
    client.close().await
}
```

### Handle Queries with Consumer Group

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let client_clone = client.clone();
let sub = client.subscribe_to_queries(
    "inventory.lookup",
    "responders",  // group name
    move |query| {
        let c = client_clone.clone();
        Box::pin(async move {
            let reply = QueryReply::builder()
                .reply_channel(&query.reply_channel)
                .request_id(&query.id)
                .client_id(&query.client_id)
                .executed(true)
                .body(b"result".to_vec())
                .build();
            let _ = c.send_query_response(reply).await;
        })
    },
    None,
).await?;
# Ok(())
# }
```

## Key Differences: Commands vs Queries

| Feature | Commands | Queries |
|---------|----------|---------|
| Response data | `executed` (bool) only | `executed` + `body` + `metadata` |
| Caching | No | Yes (`cache_key`, `cache_ttl`) |
| Use case | "Do this" | "Get me this" |
| Response type | `CommandResponse` | `QueryResponse` |

---

**Related docs:**
- [Patterns](patterns.md) — comparing all messaging patterns
- [Error Handling](errors.md) — timeout and RPC errors
- [Concepts](concepts.md) — consumer groups and channels
