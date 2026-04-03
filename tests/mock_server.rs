//! Mock gRPC server implementing the Kubemq trait for testing.
//!
//! Configurable response fields let each test scenario set up expected
//! return values or inject errors.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status};

use kubemq::proto::kubemq::{
    self as pb,
    kubemq_server::{Kubemq, KubemqServer},
};

// ---------------------------------------------------------------------------
// Configurable mock state
// ---------------------------------------------------------------------------

/// Holds configurable response payloads for each RPC.
#[derive(Default)]
pub struct MockState {
    // Ping
    pub ping_result: Option<pb::PingResult>,
    pub ping_error: Option<Status>,

    // SendEvent
    pub send_event_result: Option<pb::Result>,
    pub send_event_error: Option<Status>,

    // SendRequest (Commands / Queries)
    pub send_request_response: Option<pb::Response>,
    pub send_request_error: Option<Status>,

    // SendResponse
    pub send_response_error: Option<Status>,

    // SendQueueMessage
    pub send_queue_message_result: Option<pb::SendQueueMessageResult>,
    pub send_queue_message_error: Option<Status>,

    // SendQueueMessagesBatch
    pub send_queue_batch_response: Option<pb::QueueMessagesBatchResponse>,
    pub send_queue_batch_error: Option<Status>,

    // ReceiveQueueMessages
    pub receive_queue_response: Option<pb::ReceiveQueueMessagesResponse>,
    pub receive_queue_error: Option<Status>,

    // AckAllQueueMessages
    pub ack_all_response: Option<pb::AckAllQueueMessagesResponse>,
    pub ack_all_error: Option<Status>,

    // SubscribeToEvents (push events into this vec and they'll be streamed)
    pub subscribe_events: Vec<pb::EventReceive>,
    pub subscribe_events_error: Option<Status>,

    // SubscribeToRequests
    pub subscribe_requests: Vec<pb::Request>,
    pub subscribe_requests_error: Option<Status>,

    // Captured requests (for assertion)
    pub last_event: Option<pb::Event>,
    pub last_request: Option<pb::Request>,
    pub last_response: Option<pb::Response>,
    pub last_queue_message: Option<pb::QueueMessage>,
}

impl MockState {
    pub fn new() -> Self {
        Self::default()
    }
}

pub type SharedMockState = Arc<Mutex<MockState>>;

// ---------------------------------------------------------------------------
// Mock service implementation
// ---------------------------------------------------------------------------

pub struct MockKubemqService {
    pub state: SharedMockState,
}

impl MockKubemqService {
    pub fn new(state: SharedMockState) -> Self {
        Self { state }
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl Kubemq for MockKubemqService {
    // -- SendEvent -----------------------------------------------------------
    async fn send_event(
        &self,
        request: Request<pb::Event>,
    ) -> std::result::Result<Response<pb::Result>, Status> {
        let mut state = self.state.lock().unwrap();
        state.last_event = Some(request.into_inner());
        if let Some(ref err) = state.send_event_error {
            return Err(err.clone());
        }
        let result = state.send_event_result.clone().unwrap_or(pb::Result {
            event_id: String::new(),
            sent: true,
            error: String::new(),
        });
        Ok(Response::new(result))
    }

    // -- SendEventsStream ----------------------------------------------------
    type SendEventsStreamStream = BoxStream<pb::Result>;

    async fn send_events_stream(
        &self,
        request: Request<tonic::Streaming<pb::Event>>,
    ) -> std::result::Result<Response<Self::SendEventsStreamStream>, Status> {
        let state = self.state.clone();
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            while let Some(Ok(event)) = in_stream.next().await {
                let result = {
                    let mut s = state.lock().unwrap();
                    s.last_event = Some(event.clone());
                    s.send_event_result.clone().unwrap_or(pb::Result {
                        event_id: event.event_id,
                        sent: true,
                        error: String::new(),
                    })
                };
                if tx.send(Ok(result)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // -- SubscribeToEvents ---------------------------------------------------
    type SubscribeToEventsStream = BoxStream<pb::EventReceive>;

    async fn subscribe_to_events(
        &self,
        _request: Request<pb::Subscribe>,
    ) -> std::result::Result<Response<Self::SubscribeToEventsStream>, Status> {
        let state = self.state.lock().unwrap();
        if let Some(ref err) = state.subscribe_events_error {
            return Err(err.clone());
        }
        let events = state.subscribe_events.clone();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for e in events {
                if tx.send(Ok(e)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // -- SubscribeToRequests -------------------------------------------------
    type SubscribeToRequestsStream = BoxStream<pb::Request>;

    async fn subscribe_to_requests(
        &self,
        _request: Request<pb::Subscribe>,
    ) -> std::result::Result<Response<Self::SubscribeToRequestsStream>, Status> {
        let state = self.state.lock().unwrap();
        if let Some(ref err) = state.subscribe_requests_error {
            return Err(err.clone());
        }
        let requests = state.subscribe_requests.clone();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for r in requests {
                if tx.send(Ok(r)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // -- SendRequest ---------------------------------------------------------
    async fn send_request(
        &self,
        request: Request<pb::Request>,
    ) -> std::result::Result<Response<pb::Response>, Status> {
        let mut state = self.state.lock().unwrap();
        state.last_request = Some(request.into_inner());
        if let Some(ref err) = state.send_request_error {
            return Err(err.clone());
        }
        let response = state.send_request_response.clone().unwrap_or(pb::Response {
            client_id: String::new(),
            request_id: String::new(),
            reply_channel: String::new(),
            metadata: String::new(),
            body: Vec::new(),
            cache_hit: false,
            timestamp: 0,
            executed: true,
            error: String::new(),
            span: Vec::new(),
            tags: HashMap::new(),
        });
        Ok(Response::new(response))
    }

    // -- SendResponse --------------------------------------------------------
    async fn send_response(
        &self,
        request: Request<pb::Response>,
    ) -> std::result::Result<Response<pb::Empty>, Status> {
        let mut state = self.state.lock().unwrap();
        state.last_response = Some(request.into_inner());
        if let Some(ref err) = state.send_response_error {
            return Err(err.clone());
        }
        Ok(Response::new(pb::Empty {}))
    }

    // -- SendQueueMessage ----------------------------------------------------
    async fn send_queue_message(
        &self,
        request: Request<pb::QueueMessage>,
    ) -> std::result::Result<Response<pb::SendQueueMessageResult>, Status> {
        let mut state = self.state.lock().unwrap();
        state.last_queue_message = Some(request.into_inner());
        if let Some(ref err) = state.send_queue_message_error {
            return Err(err.clone());
        }
        let result =
            state
                .send_queue_message_result
                .clone()
                .unwrap_or(pb::SendQueueMessageResult {
                    message_id: String::new(),
                    sent_at: 0,
                    expiration_at: 0,
                    delayed_to: 0,
                    is_error: false,
                    error: String::new(),
                });
        Ok(Response::new(result))
    }

    // -- SendQueueMessagesBatch ----------------------------------------------
    async fn send_queue_messages_batch(
        &self,
        _request: Request<pb::QueueMessagesBatchRequest>,
    ) -> std::result::Result<Response<pb::QueueMessagesBatchResponse>, Status> {
        let state = self.state.lock().unwrap();
        if let Some(ref err) = state.send_queue_batch_error {
            return Err(err.clone());
        }
        let response =
            state
                .send_queue_batch_response
                .clone()
                .unwrap_or(pb::QueueMessagesBatchResponse {
                    batch_id: String::new(),
                    results: Vec::new(),
                    have_errors: false,
                });
        Ok(Response::new(response))
    }

    // -- ReceiveQueueMessages ------------------------------------------------
    async fn receive_queue_messages(
        &self,
        _request: Request<pb::ReceiveQueueMessagesRequest>,
    ) -> std::result::Result<Response<pb::ReceiveQueueMessagesResponse>, Status> {
        let state = self.state.lock().unwrap();
        if let Some(ref err) = state.receive_queue_error {
            return Err(err.clone());
        }
        let response =
            state
                .receive_queue_response
                .clone()
                .unwrap_or(pb::ReceiveQueueMessagesResponse {
                    request_id: String::new(),
                    messages: Vec::new(),
                    messages_received: 0,
                    messages_expired: 0,
                    is_peak: false,
                    is_error: false,
                    error: String::new(),
                });
        Ok(Response::new(response))
    }

    // -- StreamQueueMessage --------------------------------------------------
    type StreamQueueMessageStream = BoxStream<pb::StreamQueueMessagesResponse>;

    async fn stream_queue_message(
        &self,
        _request: Request<tonic::Streaming<pb::StreamQueueMessagesRequest>>,
    ) -> std::result::Result<Response<Self::StreamQueueMessageStream>, Status> {
        // Stub: return empty stream
        let (_tx, rx) = mpsc::channel(1);
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // -- AckAllQueueMessages -------------------------------------------------
    async fn ack_all_queue_messages(
        &self,
        _request: Request<pb::AckAllQueueMessagesRequest>,
    ) -> std::result::Result<Response<pb::AckAllQueueMessagesResponse>, Status> {
        let state = self.state.lock().unwrap();
        if let Some(ref err) = state.ack_all_error {
            return Err(err.clone());
        }
        let response = state
            .ack_all_response
            .clone()
            .unwrap_or(pb::AckAllQueueMessagesResponse {
                request_id: String::new(),
                affected_messages: 0,
                is_error: false,
                error: String::new(),
            });
        Ok(Response::new(response))
    }

    // -- Ping ----------------------------------------------------------------
    async fn ping(
        &self,
        _request: Request<pb::Empty>,
    ) -> std::result::Result<Response<pb::PingResult>, Status> {
        let state = self.state.lock().unwrap();
        if let Some(ref err) = state.ping_error {
            return Err(err.clone());
        }
        let result = state.ping_result.clone().unwrap_or(pb::PingResult {
            host: "mock-server".to_string(),
            version: "1.0.0-test".to_string(),
            server_start_time: 1000000,
            server_up_time_seconds: 3600,
        });
        Ok(Response::new(result))
    }

    // -- QueuesDownstream ----------------------------------------------------
    type QueuesDownstreamStream = BoxStream<pb::QueuesDownstreamResponse>;

    async fn queues_downstream(
        &self,
        _request: Request<tonic::Streaming<pb::QueuesDownstreamRequest>>,
    ) -> std::result::Result<Response<Self::QueuesDownstreamStream>, Status> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // -- QueuesUpstream ------------------------------------------------------
    type QueuesUpstreamStream = BoxStream<pb::QueuesUpstreamResponse>;

    async fn queues_upstream(
        &self,
        _request: Request<tonic::Streaming<pb::QueuesUpstreamRequest>>,
    ) -> std::result::Result<Response<Self::QueuesUpstreamStream>, Status> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

// ---------------------------------------------------------------------------
// Helper: start mock server on random port
// ---------------------------------------------------------------------------

/// Start the mock server and return (address, shared state).
/// The server runs in a background tokio task and is shut down when the
/// returned `tokio::sync::oneshot::Sender` is dropped.
pub async fn start_mock_server() -> (
    SocketAddr,
    SharedMockState,
    tokio::sync::oneshot::Sender<()>,
) {
    let state = Arc::new(Mutex::new(MockState::new()));
    let service = MockKubemqService::new(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KubemqServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .unwrap();
    });

    // Give the server a moment to bind
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (addr, state, shutdown_tx)
}

/// Helper: build a KubemqClient connected to the mock server.
pub async fn build_test_client(addr: SocketAddr) -> kubemq::KubemqClient {
    kubemq::KubemqClient::builder()
        .host("127.0.0.1")
        .port(addr.port())
        .client_id("test-client")
        .check_connection(false)
        .build()
        .await
        .expect("failed to connect test client to mock server")
}
