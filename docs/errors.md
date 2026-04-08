# Error Reference

Complete reference for error types in the KubeMQ Rust SDK.

## Overview

All SDK operations return `kubemq::Result<T>`, which is an alias for `Result<T, KubemqError>`. The `KubemqError` enum provides structured error information including:

- **Machine-readable code** via `code()` → `ErrorCode`
- **Retryability** via `is_retryable()` → `bool`
- **Recovery guidance** via `suggestion()` → `&str`
- **Human-readable message** via `Display` / `to_string()`

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
match client.ping().await {
    Ok(info) => println!("OK: {}", info.host),
    Err(e) => {
        eprintln!("Error: {}", e);
        eprintln!("Code: {}", e.code());
        eprintln!("Retryable: {}", e.is_retryable());
        let hint = e.suggestion();
        if !hint.is_empty() {
            eprintln!("Suggestion: {}", hint);
        }
    }
}
# Ok(())
# }
```

## gRPC-to-KubemqError Mapping

The SDK maps gRPC status codes to `KubemqError` variants:

| gRPC Code | KubemqError Variant | ErrorCode | Retryable | Recovery |
|-----------|-------------------|-----------|-----------|----------|
| `CANCELLED` | `Cancellation` | `Cancellation` | No | Check if operation was intentionally cancelled |
| `UNKNOWN` | `Transient` | `Transient` | Yes | Retry with backoff |
| `INVALID_ARGUMENT` | `Validation` | `Validation` | No | Fix request parameters |
| `DEADLINE_EXCEEDED` | `Timeout` | `Timeout` | Yes | Increase timeout or retry |
| `NOT_FOUND` | `NotFound` | `NotFound` | No | Create the channel first |
| `ALREADY_EXISTS` | `Validation` | `Validation` | No | Resource already exists |
| `PERMISSION_DENIED` | `Authorization` | `Authorization` | No | Check client permissions |
| `RESOURCE_EXHAUSTED` | `Throttling` | `Throttling` | Yes | Reduce request rate |
| `FAILED_PRECONDITION` | `Validation` | `Validation` | No | Fix precondition |
| `ABORTED` | `Transient` | `Transient` | Yes | Retry with backoff |
| `OUT_OF_RANGE` | `Validation` | `Validation` | No | Fix parameter range |
| `UNIMPLEMENTED` | `Fatal` | `Fatal` | No | Feature not supported by this server version |
| `INTERNAL` | `Fatal` | `Fatal` | No | Server bug — report to KubeMQ support |
| `UNAVAILABLE` | `Transient` | `Transient` | Yes | Check broker is running and reachable |
| `DATA_LOSS` | `Fatal` | `Fatal` | No | Data corruption — check server logs |
| `UNAUTHENTICATED` | `Authentication` | `Authentication` | No | Check auth token validity |

## KubemqError Variants

### `Transient`

A transient failure that may succeed on retry. Maps to gRPC `UNKNOWN`, `ABORTED`, or `UNAVAILABLE`.

| Field | Type | Description |
|-------|------|-------------|
| `code` | `ErrorCode` | Always `ErrorCode::Transient` |
| `message` | `String` | Human-readable error description |
| `operation` | `String` | The operation that failed (e.g., `"send_event"`) |
| `channel` | `String` | Target channel name |
| `is_retryable` | `bool` | Always `true` |
| `source` | `Option<Box<dyn Error>>` | Underlying error |
| `request_id` | `String` | Request ID for correlation |
| `suggestion` | `&'static str` | Recovery guidance |

### `Timeout`

The operation timed out. Maps to gRPC `DEADLINE_EXCEEDED`. Retryable.

| Field | Type | Description |
|-------|------|-------------|
| `code` | `ErrorCode` | Always `ErrorCode::Timeout` |
| `message` | `String` | Human-readable description |
| `operation` | `String` | The operation that timed out |
| `channel` | `String` | Target channel |
| `is_retryable` | `bool` | Always `true` |
| `source` | `Option<Box<dyn Error>>` | Underlying error |
| `request_id` | `String` | Request ID |
| `suggestion` | `&'static str` | Recovery guidance |

### `Authentication`

Auth token is invalid or expired. Maps to gRPC `UNAUTHENTICATED`. Not retryable.

| Field | Type | Description |
|-------|------|-------------|
| `code` | `ErrorCode` | Always `ErrorCode::Authentication` |
| `message` | `String` | Human-readable description |
| `operation` | `String` | The operation that failed |
| `source` | `Option<Box<dyn Error>>` | Underlying error |
| `suggestion` | `&'static str` | Recovery guidance |

### `Authorization`

Client lacks permission. Maps to gRPC `PERMISSION_DENIED`. Not retryable.

| Field | Type | Description |
|-------|------|-------------|
| `code` | `ErrorCode` | Always `ErrorCode::Authorization` |
| `message` | `String` | Human-readable description |
| `operation` | `String` | The operation that failed |
| `channel` | `String` | Target channel |
| `source` | `Option<Box<dyn Error>>` | Underlying error |
| `suggestion` | `&'static str` | Recovery guidance |

### `Validation`

Invalid arguments or violated precondition. Maps to gRPC `INVALID_ARGUMENT`, `ALREADY_EXISTS`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`. Not retryable.

| Field | Type | Description |
|-------|------|-------------|
| `code` | `ErrorCode` | Always `ErrorCode::Validation` |
| `message` | `String` | Human-readable description |
| `operation` | `String` | The operation that failed |
| `channel` | `String` | Target channel |
| `suggestion` | `&'static str` | Recovery guidance |

### `NotFound`

Requested resource not found. Maps to gRPC `NOT_FOUND`. Not retryable.

| Field | Type | Description |
|-------|------|-------------|
| `code` | `ErrorCode` | Always `ErrorCode::NotFound` |
| `message` | `String` | Human-readable description |
| `operation` | `String` | The operation that failed |
| `channel` | `String` | Target channel |
| `suggestion` | `&'static str` | Recovery guidance |

### `Throttling`

Rate limited. Maps to gRPC `RESOURCE_EXHAUSTED`. Retryable with backoff.

| Field | Type | Description |
|-------|------|-------------|
| `code` | `ErrorCode` | Always `ErrorCode::Throttling` |
| `message` | `String` | Human-readable description |
| `operation` | `String` | The operation that was throttled |
| `channel` | `String` | Target channel |
| `is_retryable` | `bool` | Always `true` |
| `suggestion` | `&'static str` | Recovery guidance |

### `Fatal`

Unrecoverable server error. Maps to gRPC `UNIMPLEMENTED`, `INTERNAL`, `DATA_LOSS`. Not retryable.

| Field | Type | Description |
|-------|------|-------------|
| `code` | `ErrorCode` | Always `ErrorCode::Fatal` |
| `message` | `String` | Human-readable description |
| `operation` | `String` | The operation that failed |
| `source` | `Option<Box<dyn Error>>` | Underlying error |
| `suggestion` | `&'static str` | Recovery guidance |

### `Cancellation`

Operation cancelled by the caller. Maps to gRPC `CANCELLED`. Not retryable.

| Field | Type | Description |
|-------|------|-------------|
| `code` | `ErrorCode` | Always `ErrorCode::Cancellation` |
| `message` | `String` | Human-readable description |
| `operation` | `String` | The cancelled operation |

### Non-gRPC Errors

These errors are generated by the SDK itself, not mapped from gRPC:

#### `BufferFull`

The internal send buffer is full. Reduce send rate or increase buffer capacity.

| Field | Type | Description |
|-------|------|-------------|
| `code` | `ErrorCode` | Always `ErrorCode::Backpressure` |
| `buffer_size` | `usize` | Maximum buffer capacity |
| `queued_count` | `usize` | Messages currently queued |
| `suggestion` | `&'static str` | Recovery guidance |

#### `StreamBroken`

A stream was broken with unacknowledged messages.

| Field | Type | Description |
|-------|------|-------------|
| `unacknowledged_ids` | `Vec<String>` | IDs of unacknowledged messages |
| `unacknowledged_count` | `usize` | Number of unacknowledged messages |

#### `ClientClosed`

Operation attempted after `client.close()` was called. No fields — unit variant.

#### `Transport`

Low-level transport failure (gRPC connection error).

| Field | Type | Description |
|-------|------|-------------|
| `operation` | `String` | The operation that encountered the error |
| `source` | `Box<dyn Error>` | Underlying transport error |

#### `Handler`

User-provided callback returned an error.

| Field | Type | Description |
|-------|------|-------------|
| `handler` | `String` | Name of the handler |
| `source` | `Box<dyn Error>` | Error from the handler |

## ErrorCode Reference

| Code | Retryable | Description |
|------|-----------|-------------|
| `Transient` | Yes | Transient failure — retry with backoff |
| `Timeout` | Yes | Operation timed out |
| `Throttling` | Yes | Rate limited |
| `Authentication` | No | Invalid or expired token |
| `Authorization` | No | Insufficient permissions |
| `Validation` | No | Invalid request parameters |
| `NotFound` | No | Resource not found |
| `Fatal` | No | Unrecoverable server error |
| `Cancellation` | No | Operation cancelled |
| `Backpressure` | No | Send buffer full |

## Error Handling Patterns

### Match on Error Code

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let event = Event::builder()
    .channel("test")
    .body(b"data".to_vec())
    .build();

match client.send_event(event).await {
    Ok(()) => println!("Sent"),
    Err(e) => match e.code() {
        ErrorCode::Authentication => {
            eprintln!("Auth failed — check your token");
        }
        ErrorCode::Validation => {
            eprintln!("Bad request: {}", e);
        }
        code if e.is_retryable() => {
            eprintln!("Retryable error ({:?}): {}", code, e);
        }
        _ => {
            eprintln!("Fatal: {}", e);
        }
    },
}
# Ok(())
# }
```

### Retry on Transient Errors

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

async fn send_with_retry(
    client: &KubemqClient,
    event: Event,
    max_attempts: u32,
) -> kubemq::Result<()> {
    let mut attempt = 0;
    loop {
        let ev = event.clone();
        match client.send_event(ev).await {
            Ok(()) => return Ok(()),
            Err(e) if e.is_retryable() && attempt < max_attempts => {
                let delay = Duration::from_millis(100 * 2u64.pow(attempt));
                eprintln!("Retry {}/{}: {}", attempt + 1, max_attempts, e);
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Match on Specific Variants

```rust,no_run
use kubemq::prelude::*;

fn handle_error(err: KubemqError) {
    match err {
        KubemqError::ClientClosed => {
            eprintln!("Client was closed — create a new one");
        }
        KubemqError::BufferFull { buffer_size, queued_count, .. } => {
            eprintln!("Buffer full: {}/{}", queued_count, buffer_size);
        }
        KubemqError::StreamBroken { unacknowledged_count, .. } => {
            eprintln!("{} messages need resending", unacknowledged_count);
        }
        other => {
            eprintln!("Error ({}): {}", other.code(), other);
        }
    }
}
```

---

**Related docs:**
- [Concepts](concepts.md) — connection lifecycle and reconnection
- [Patterns](patterns.md) — pattern-specific error behavior
- [README](../README.md) — error handling overview
- [TROUBLESHOOTING](../TROUBLESHOOTING.md) — common error resolution
