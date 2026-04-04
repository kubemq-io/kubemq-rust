# Troubleshooting

## Connection Issues

### Cannot connect to KubeMQ broker

**Symptom:** `Transport` error or connection timeout on `build()`.

**Solutions:**
1. Verify the broker is running: `kubemqctl status`
2. Check host and port are correct
3. If using Docker: ensure the port is exposed (`-p 50000:50000`)
4. Check firewall rules
5. Try `check_connection(true)` in the builder for early failure

### TLS connection fails

**Symptom:** TLS handshake error.

**Solutions:**
1. Verify the CA certificate is correct and matches the server
2. For mTLS, ensure both client cert and key are provided
3. Check `server_name` override if the hostname does not match the certificate
4. Ensure the server is configured for TLS

### Authentication failure

**Symptom:** `Authentication` error on operations.

**Solutions:**
1. Verify the auth token is correct
2. Check token expiration
3. Use `KUBEMQ_AUTH_TOKEN` environment variable as fallback

## Subscription Issues

### Subscription drops unexpectedly

**Symptom:** No more messages received, `is_done()` returns true.

**Solutions:**
1. Check the error callback for details
2. Verify the channel name is correct
3. For Events Store, ensure the start position is valid
4. Check if the broker restarted (subscriptions do not auto-reconnect individually)

### Messages not received in consumer group

**Symptom:** Only one consumer receives messages.

**Solutions:**
- This is expected behavior. In a consumer group, each message goes to exactly one consumer.

## Queue Issues

### Messages stuck in queue

**Symptom:** Messages sent but not received.

**Solutions:**
1. Ensure you are polling the correct channel
2. Check if messages have expired (`expiration_seconds`)
3. Check if messages are delayed (`delay_seconds`)
4. Verify the queue depth has not exceeded server limits

### Ack/Nack not working

**Symptom:** Messages reappear after ack, or do not reappear after nack.

**Solutions:**
1. Ensure `auto_ack` is `false` in the poll request
2. Verify the `transaction_id` is from the current poll response
3. Call `ack()` or `nack()` before the server-side visibility timeout expires

## Performance Issues

### Low throughput

**Solutions:**
1. Use stream API (`send_event_stream()`) instead of individual sends for events
2. Use queue upstream for batch queue sends
3. Increase `receive_buffer_size` for subscriptions
4. Check network latency to the broker
5. Run benchmarks to establish baseline: `cargo bench`

### High memory usage

**Solutions:**
1. Reduce `reconnect_policy.buffer_size` if buffering during reconnection
2. Ensure subscriptions are properly unsubscribed when no longer needed
3. Close clients that are no longer in use

## Build Issues

### Compilation fails with MSRV error

**Solution:** Upgrade to Rust 1.75 or later: `rustup update stable`

### Proto-related build errors

**Solution:** The generated proto code is committed. You should not need `protoc`. If you see proto errors, ensure the `src/proto/kubemq.rs` file exists and is not corrupted.

## gRPC Status Code Mapping

The SDK maps gRPC status codes to `KubemqError` variants via `from_grpc_status()`. This table shows the complete mapping:

| gRPC Status Code | `KubemqError` Variant | `ErrorCode` | Retryable | Suggestion |
|---|---|---|---|---|
| `OK` | `Fatal` | `Fatal` | No | This is a bug in the SDK. Please report it. |
| `CANCELLED` | `Cancellation` | `Cancellation` | No | Operation was cancelled. Check if the client is closing. |
| `UNKNOWN` | `Transient` | `Transient` | Yes | Retry the operation. If the problem persists, check server health. |
| `INVALID_ARGUMENT` | `Validation` | `Validation` | No | Check the request parameters (channel, body, metadata). |
| `DEADLINE_EXCEEDED` | `Timeout` | `Timeout` | Yes | Increase the timeout or check server connectivity and firewall rules. |
| `NOT_FOUND` | `NotFound` | `NotFound` | No | The channel or queue does not exist. Create it first. |
| `ALREADY_EXISTS` | `Validation` | `Validation` | No | Check the request parameters (channel, body, metadata). |
| `PERMISSION_DENIED` | `Authorization` | `Authorization` | No | Verify the client has permissions for this channel. |
| `RESOURCE_EXHAUSTED` | `Throttling` | `Throttling` | Yes | Reduce request rate or increase server capacity. |
| `FAILED_PRECONDITION` | `Validation` | `Validation` | No | Check the request parameters (channel, body, metadata). |
| `ABORTED` | `Transient` | `Transient` | Yes | Retry the operation. If the problem persists, check server health. |
| `OUT_OF_RANGE` | `Validation` | `Validation` | No | Check the request parameters (channel, body, metadata). |
| `UNIMPLEMENTED` | `Fatal` | `Fatal` | No | This is an unrecoverable server error. Contact support. |
| `INTERNAL` | `Fatal` | `Fatal` | No | This is an unrecoverable server error. Contact support. |
| `UNAVAILABLE` | `Transient` | `Transient` | Yes | Retry the operation. If the problem persists, check server health. |
| `DATA_LOSS` | `Fatal` | `Fatal` | No | Data loss detected. Check server logs and data integrity. |
| `UNAUTHENTICATED` | `Authentication` | `Authentication` | No | Check your auth token. It may have expired. |

Use `error.is_retryable()` to check whether an operation can be retried, and `error.suggestion()` for recovery guidance.

## Debug Logging

The SDK uses the [`tracing`](https://docs.rs/tracing) crate for structured diagnostic logging. To see SDK log output, add a `tracing-subscriber` to your application:

```toml
[dependencies]
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

```rust
fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // ... your application code ...
}
```

Control log verbosity with the `RUST_LOG` environment variable:

| Level | `RUST_LOG` value | What is logged |
|---|---|---|
| Error | `RUST_LOG=kubemq=error` | Unrecoverable failures, panicked callbacks |
| Warn | `RUST_LOG=kubemq=warn` | Client dropped without `close()`, retryable failures |
| Info | `RUST_LOG=kubemq=info` | Connection state changes, subscription lifecycle |
| Debug | `RUST_LOG=kubemq=debug` | gRPC call details, retry attempts, reconnection events |
| Trace | `RUST_LOG=kubemq=trace` | Full message payloads, proto serialization details |

**Examples:**

```bash
# Show only errors from the SDK
RUST_LOG=kubemq=error cargo run --example events_pubsub

# Show debug-level output for the SDK, info for everything else
RUST_LOG=info,kubemq=debug cargo run --example events_pubsub

# Show all trace output (very verbose)
RUST_LOG=kubemq=trace cargo run --example events_pubsub
```

## Getting Help

- Open an issue on [GitHub](https://github.com/kubemq-io/kubemq-rust/issues)
- Check the [examples](examples/) for working code
- Refer to [docs.rs/kubemq](https://docs.rs/kubemq) for API documentation
