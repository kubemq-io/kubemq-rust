# Messaging Patterns

KubeMQ supports five messaging patterns. This guide explains each pattern, when to use it, and provides a quick comparison to help you choose.

## Pattern Comparison

| Feature | Events | Events Store | Queues | Commands | Queries |
|---------|--------|-------------|--------|----------|---------|
| **Direction** | 1-to-many | 1-to-many | 1-to-1 | 1-to-1 | 1-to-1 |
| **Persistence** | No | Yes | Yes | No | No |
| **Delivery guarantee** | At-most-once | At-least-once | Exactly-once | At-most-once | At-most-once |
| **Replay** | No | Yes (6 modes) | No | No | No |
| **Response** | No | No | Ack/Nack | Yes (executed) | Yes (data + cache) |
| **Consumer groups** | Yes | Yes | Implicit (1-to-1) | Yes | Yes |
| **Wildcards** | Yes | No | No | No | No |
| **Dead-letter** | No | No | Yes | No | No |
| **Delayed delivery** | No | No | Yes | No | No |
| **Caching** | No | No | No | No | Yes |
| **Timeout** | No | No | No | Yes | Yes |

## Events (Pub/Sub)

Fire-and-forget messaging with fan-out delivery. Events are delivered to all active subscribers and then discarded — no persistence.

**Key types:** `Event`, `EventBuilder`, `EventReceive`, `EventStreamHandle`

**When to use:**
- Real-time notifications (e.g., user activity, system alerts)
- Telemetry and metrics streaming
- Loosely coupled inter-service communication where dropped messages are acceptable

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let event = Event::builder()
    .channel("notifications")
    .body(b"user logged in".to_vec())
    .build();
client.send_event(event).await?;
# Ok(())
# }
```

**How-to guide:** [Events & Events Store](howto-pubsub.md)

## Events Store (Persistent Pub/Sub)

Persistent events with replay capability. Messages are stored on the broker and can be replayed from any position when new subscribers connect.

**Key types:** `EventStore`, `EventStoreBuilder`, `EventStoreReceive`, `EventsStoreSubscription`

**Six start positions for replay:**

| Start Position | Description |
|---------------|-------------|
| `StartNewOnly` | Only new events after subscription |
| `StartFromFirst` | Replay from the first stored event |
| `StartFromLast` | Start from the most recent event |
| `StartAtSequence(n)` | Start at sequence number `n` |
| `StartAtTime(t)` | Start from a specific timestamp |
| `StartAtTimeDelta(d)` | Start from `now - duration` |

**When to use:**
- Event sourcing and audit logs
- Message replay for late-joining consumers
- Guaranteed delivery for critical events

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let event = EventStore::builder()
    .channel("orders.created")
    .body(b"order-123".to_vec())
    .build();
client.send_event_store(event).await?;
# Ok(())
# }
```

**How-to guide:** [Events & Events Store](howto-pubsub.md)

## Queues

Transactional point-to-point messaging. Each message is delivered to exactly one consumer and must be explicitly acknowledged. Supports dead-letter routing, delayed delivery, and message expiration.

**Key types:** `QueueMessage`, `QueueMessageBuilder`, `QueuePolicy`, `QueueSendResult`, `PollRequest`, `PollResponse`

**Two APIs:**
- **Simple API** — `send_queue_message()` / `receive_queue_messages()` for straightforward operations
- **Stream API** — `queue_upstream()` / `poll_queue()` for high-throughput and transactional processing

**When to use:**
- Task processing pipelines (producer → worker)
- Order processing with guaranteed delivery
- Work distribution with load balancing
- Scenarios requiring dead-letter handling and retry limits

```rust,no_run
use kubemq::prelude::*;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let msg = QueueMessage::builder()
    .channel("tasks")
    .body(b"process-order".to_vec())
    .max_receive_count(3)
    .max_receive_queue("tasks.dead-letter")
    .build();
client.send_queue_message(msg).await?;
# Ok(())
# }
```

**How-to guide:** [Queues](howto-queues.md)

## Commands (RPC)

Synchronous request-response with timeout. The sender blocks until exactly one subscriber executes the command and returns a success/failure response.

**Key types:** `Command`, `CommandBuilder`, `CommandReceive`, `CommandResponse`, `CommandReply`

**When to use:**
- Device control (reboot, configure)
- Administrative operations
- Any operation requiring a success/failure acknowledgment

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let cmd = Command::builder()
    .channel("device.reboot")
    .body(b"device-42".to_vec())
    .timeout(Duration::from_secs(10))
    .build();
let response = client.send_command(cmd).await?;
println!("Executed: {}", response.executed);
# Ok(())
# }
```

**How-to guide:** [Commands & Queries](howto-rpc.md)

## Queries (RPC with Cache)

Synchronous request-response with optional server-side caching. Like Commands, but the response can carry data and be cached to avoid repeated computation.

**Key types:** `Query`, `QueryBuilder`, `QueryReceive`, `QueryResponse`, `QueryReply`

**When to use:**
- Data lookups (inventory check, user profile)
- Cacheable computations
- Any request-response needing a data payload in the response

```rust,no_run
use kubemq::prelude::*;
use std::time::Duration;

# async fn example(client: &KubemqClient) -> kubemq::Result<()> {
let query = Query::builder()
    .channel("inventory.lookup")
    .body(b"sku-12345".to_vec())
    .timeout(Duration::from_secs(5))
    .cache_key("sku-12345")
    .cache_ttl(Duration::from_secs(60))
    .build();
let response = client.send_query(query).await?;
println!("Cache hit: {}, Body: {:?}", response.cache_hit, response.body);
# Ok(())
# }
```

**How-to guide:** [Commands & Queries](howto-rpc.md)

## Decision Matrix

Use this matrix to choose the right pattern:

| Scenario | Recommended Pattern |
|----------|-------------------|
| Broadcast notifications to all listeners | **Events** |
| Audit log with replay for new consumers | **Events Store** |
| Task queue with retry and dead-letter | **Queues** |
| "Execute this action" with success/failure | **Commands** |
| "Get me this data" with optional caching | **Queries** |
| High-throughput fire-and-forget telemetry | **Events** (with stream API) |
| Guaranteed message processing | **Queues** (with ack/nack) |
| Event sourcing / CQRS | **Events Store** |

---

**Related docs:**
- [Concepts](concepts.md) — architecture, channels, consumer groups
- [Events & Events Store how-to](howto-pubsub.md)
- [Queues how-to](howto-queues.md)
- [Commands & Queries how-to](howto-rpc.md)
