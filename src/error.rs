/// Machine-readable error codes for classifying [`KubemqError`] variants.
///
/// Each variant maps to one or more gRPC status codes and indicates whether
/// the error is retryable. Use [`KubemqError::code()`] to extract the code
/// from an error, and [`KubemqError::is_retryable()`] to check if the
/// operation can be retried.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// A transient failure occurred. The operation can be retried with backoff.
    /// Maps to gRPC `UNKNOWN`, `ABORTED`, `UNAVAILABLE`.
    Transient,
    /// The operation timed out. Retryable with a longer timeout or backoff.
    /// Maps to gRPC `DEADLINE_EXCEEDED`.
    Timeout,
    /// The request was throttled due to rate limiting. Retryable with backoff.
    /// Maps to gRPC `RESOURCE_EXHAUSTED`.
    Throttling,
    /// Authentication failed — the auth token is invalid or expired.
    /// Maps to gRPC `UNAUTHENTICATED`. Not retryable — fix credentials.
    Authentication,
    /// The client lacks permission for this operation.
    /// Maps to gRPC `PERMISSION_DENIED`. Not retryable — check ACL configuration.
    Authorization,
    /// The request contained invalid arguments or violated a precondition.
    /// Maps to gRPC `INVALID_ARGUMENT`, `ALREADY_EXISTS`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`.
    /// Not retryable — fix the request before retrying.
    Validation,
    /// The requested resource (e.g., channel) was not found.
    /// Maps to gRPC `NOT_FOUND`. Not retryable — create the resource first.
    NotFound,
    /// An unrecoverable server-side error. Do not retry.
    /// Maps to gRPC `UNIMPLEMENTED`, `INTERNAL`, `DATA_LOSS`.
    Fatal,
    /// The operation was cancelled, typically by the caller.
    /// Maps to gRPC `CANCELLED`. Not retryable.
    Cancellation,
    /// The internal send buffer is full — the operation cannot be queued.
    /// Not a network error. Consider reducing send rate or increasing buffer capacity.
    Backpressure,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient => write!(f, "Transient"),
            Self::Timeout => write!(f, "Timeout"),
            Self::Throttling => write!(f, "Throttling"),
            Self::Authentication => write!(f, "Authentication"),
            Self::Authorization => write!(f, "Authorization"),
            Self::Validation => write!(f, "Validation"),
            Self::NotFound => write!(f, "NotFound"),
            Self::Fatal => write!(f, "Fatal"),
            Self::Cancellation => write!(f, "Cancellation"),
            Self::Backpressure => write!(f, "Backpressure"),
        }
    }
}

impl From<std::io::Error> for KubemqError {
    fn from(e: std::io::Error) -> Self {
        Self::Fatal {
            code: ErrorCode::Fatal,
            message: e.to_string(),
            operation: "io".to_string(),
            source: Some(Box::new(e)),
            suggestion: "Check file system permissions and paths.",
        }
    }
}

/// The primary error type returned by all SDK operations.
///
/// Each variant carries context about the failure including a machine-readable
/// [`ErrorCode`], a human-readable message, an optional recovery suggestion,
/// and the originating source error.
///
/// Use [`is_retryable()`](Self::is_retryable) to check if an operation can be retried,
/// [`code()`](Self::code) for the machine-readable classification, and
/// [`suggestion()`](Self::suggestion) for recovery guidance.
///
/// # Examples
///
/// ```rust,no_run
/// use kubemq::prelude::*;
///
/// # async fn example() -> kubemq::Result<()> {
/// let client = KubemqClient::builder()
///     .host("localhost").port(50000)
///     .build().await?;
///
/// match client.ping().await {
///     Ok(info) => println!("Connected: {}", info.host),
///     Err(e) if e.is_retryable() => {
///         eprintln!("Retryable error ({}): {}", e.code(), e);
///         let suggestion = e.suggestion();
///         if !suggestion.is_empty() {
///             eprintln!("Suggestion: {}", suggestion);
///         }
///     }
///     Err(e) => eprintln!("Fatal error: {}", e),
/// }
/// # Ok(())
/// # }
/// ```
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum KubemqError {
    /// A transient failure that may succeed on retry with backoff.
    ///
    /// Retryable. Maps to gRPC `UNKNOWN`, `ABORTED`, or `UNAVAILABLE`.
    #[error("{operation} failed on channel \"{channel}\": {message}")]
    Transient {
        /// Machine-readable error code — always [`ErrorCode::Transient`].
        code: ErrorCode,
        /// Human-readable error description.
        message: String,
        /// The operation that failed (e.g., `"send_event"`).
        operation: String,
        /// The target channel name.
        channel: String,
        /// Whether this error can be retried.
        is_retryable: bool,
        /// The underlying error, if any.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        /// Request ID for correlation.
        request_id: String,
        /// Recovery suggestion (e.g., `"Retry the operation."`).
        suggestion: &'static str,
    },

    /// The operation timed out. Retryable with a longer timeout or backoff.
    ///
    /// Maps to gRPC `DEADLINE_EXCEEDED`.
    #[error("{operation} timed out on channel \"{channel}\": {message}")]
    Timeout {
        /// Machine-readable error code — always [`ErrorCode::Timeout`].
        code: ErrorCode,
        /// Human-readable error description.
        message: String,
        /// The operation that timed out (e.g., `"send_command"`).
        operation: String,
        /// The target channel name.
        channel: String,
        /// Whether this error can be retried.
        is_retryable: bool,
        /// The underlying error, if any.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        /// Request ID for correlation.
        request_id: String,
        /// Recovery suggestion.
        suggestion: &'static str,
    },

    /// Authentication failed — the auth token is invalid or expired.
    ///
    /// Not retryable. Maps to gRPC `UNAUTHENTICATED`.
    #[error("authentication failed: {message}")]
    Authentication {
        /// Machine-readable error code — always [`ErrorCode::Authentication`].
        code: ErrorCode,
        /// Human-readable error description.
        message: String,
        /// The operation that failed.
        operation: String,
        /// The underlying error, if any.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        /// Recovery suggestion.
        suggestion: &'static str,
    },

    /// The client lacks permission for this operation on the specified channel.
    ///
    /// Not retryable. Maps to gRPC `PERMISSION_DENIED`.
    #[error("authorization failed on channel \"{channel}\": {message}")]
    Authorization {
        /// Machine-readable error code — always [`ErrorCode::Authorization`].
        code: ErrorCode,
        /// Human-readable error description.
        message: String,
        /// The operation that failed.
        operation: String,
        /// The target channel name.
        channel: String,
        /// The underlying error, if any.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        /// Recovery suggestion.
        suggestion: &'static str,
    },

    /// The request contained invalid arguments or violated a precondition.
    ///
    /// Not retryable — fix the request before retrying. Maps to gRPC
    /// `INVALID_ARGUMENT`, `ALREADY_EXISTS`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`.
    #[error("validation failed: {message}")]
    Validation {
        /// Machine-readable error code — always [`ErrorCode::Validation`].
        code: ErrorCode,
        /// Human-readable error description.
        message: String,
        /// The operation that failed.
        operation: String,
        /// The target channel name.
        channel: String,
        /// Recovery suggestion.
        suggestion: &'static str,
    },

    /// The requested resource (e.g., channel) was not found.
    ///
    /// Not retryable — create the resource first. Maps to gRPC `NOT_FOUND`.
    #[error("not found: {message}")]
    NotFound {
        /// Machine-readable error code — always [`ErrorCode::NotFound`].
        code: ErrorCode,
        /// Human-readable error description.
        message: String,
        /// The operation that failed.
        operation: String,
        /// The target channel name.
        channel: String,
        /// Recovery suggestion.
        suggestion: &'static str,
    },

    /// An unrecoverable server-side error. Do not retry.
    ///
    /// Maps to gRPC `UNIMPLEMENTED`, `INTERNAL`, `DATA_LOSS`.
    #[error("fatal error: {message}")]
    Fatal {
        /// Machine-readable error code — always [`ErrorCode::Fatal`].
        code: ErrorCode,
        /// Human-readable error description.
        message: String,
        /// The operation that failed.
        operation: String,
        /// The underlying error, if any.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        /// Recovery suggestion.
        suggestion: &'static str,
    },

    /// The operation was cancelled, typically by the caller or a context timeout.
    ///
    /// Not retryable. Maps to gRPC `CANCELLED`.
    #[error("operation cancelled: {message}")]
    Cancellation {
        /// Machine-readable error code — always [`ErrorCode::Cancellation`].
        code: ErrorCode,
        /// Human-readable error description.
        message: String,
        /// The operation that was cancelled (e.g., `"send_event"`).
        operation: String,
    },

    /// The request was throttled due to rate limiting. Retryable with backoff.
    ///
    /// Maps to gRPC `RESOURCE_EXHAUSTED`.
    #[error("throttled: {message}")]
    Throttling {
        /// Machine-readable error code — always [`ErrorCode::Throttling`].
        code: ErrorCode,
        /// Human-readable error description.
        message: String,
        /// The operation that was throttled.
        operation: String,
        /// The target channel name.
        channel: String,
        /// Whether this error can be retried.
        is_retryable: bool,
        /// Recovery suggestion.
        suggestion: &'static str,
    },

    /// The internal send buffer is full — the operation cannot be queued.
    ///
    /// Not retryable immediately. Consider reducing send rate or increasing buffer capacity.
    #[error("backpressure: buffer full (capacity={buffer_size}, queued={queued_count})")]
    BufferFull {
        /// Machine-readable error code — always [`ErrorCode::Backpressure`].
        code: ErrorCode,
        /// Maximum buffer capacity.
        buffer_size: usize,
        /// Number of messages currently queued.
        queued_count: usize,
        /// Recovery suggestion.
        suggestion: &'static str,
    },

    /// The stream was broken with unacknowledged messages still in flight.
    ///
    /// Not retryable. The caller should re-send unacknowledged messages.
    #[error("stream broken: {unacknowledged_count} unacknowledged messages")]
    StreamBroken {
        /// IDs of messages that were sent but not acknowledged.
        unacknowledged_ids: Vec<String>,
        /// Number of unacknowledged messages.
        unacknowledged_count: usize,
    },

    /// The client has been closed and cannot be used for further operations.
    ///
    /// Returned when an operation is attempted after calling
    /// [`KubemqClient::close()`](crate::KubemqClient::close).
    #[error("client closed")]
    ClientClosed,

    /// A low-level transport failure occurred.
    ///
    /// Wraps the underlying transport error (e.g., gRPC connection failure).
    #[error("transport error [{operation}]: {source}")]
    Transport {
        /// The operation that encountered the transport error.
        operation: String,
        /// The underlying transport error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A user-provided callback or handler returned an error.
    ///
    /// Wraps the error returned by the user's message handler.
    #[error("handler error [{handler}]: {source}")]
    Handler {
        /// The name of the handler that returned the error.
        handler: String,
        /// The error returned by the handler.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl KubemqError {
    /// Returns `true` if this error is transient and the operation may succeed on retry.
    ///
    /// Retryable variants: [`Transient`](Self::Transient), [`Timeout`](Self::Timeout),
    /// [`Throttling`](Self::Throttling).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::KubemqError;
    ///
    /// fn handle_error(err: &KubemqError) {
    ///     if err.is_retryable() {
    ///         println!("Will retry: {}", err);
    ///     } else {
    ///         eprintln!("Permanent failure: {}", err);
    ///     }
    /// }
    /// ```
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transient { is_retryable, .. } => *is_retryable,
            Self::Timeout { is_retryable, .. } => *is_retryable,
            Self::Throttling { is_retryable, .. } => *is_retryable,
            _ => false,
        }
    }

    /// Returns the machine-readable [`ErrorCode`] for this error.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::{ErrorCode, KubemqError};
    ///
    /// fn classify(err: &KubemqError) {
    ///     match err.code() {
    ///         ErrorCode::Authentication => eprintln!("Check your auth token"),
    ///         ErrorCode::Validation => eprintln!("Fix request parameters"),
    ///         code => eprintln!("Error code: {:?}", code),
    ///     }
    /// }
    /// ```
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Transient { code, .. } => *code,
            Self::Timeout { code, .. } => *code,
            Self::Authentication { code, .. } => *code,
            Self::Authorization { code, .. } => *code,
            Self::Validation { code, .. } => *code,
            Self::NotFound { code, .. } => *code,
            Self::Fatal { code, .. } => *code,
            Self::Cancellation { code, .. } => *code,
            Self::Throttling { code, .. } => *code,
            Self::BufferFull { code, .. } => *code,
            Self::StreamBroken { .. } => ErrorCode::Transient,
            Self::ClientClosed => ErrorCode::Fatal,
            Self::Transport { .. } => ErrorCode::Fatal,
            Self::Handler { .. } => ErrorCode::Fatal,
        }
    }

    /// Returns a human-readable recovery suggestion, or an empty string if none is available.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kubemq::KubemqError;
    ///
    /// fn log_error(err: &KubemqError) {
    ///     eprintln!("Error: {}", err);
    ///     let hint = err.suggestion();
    ///     if !hint.is_empty() {
    ///         eprintln!("Suggestion: {}", hint);
    ///     }
    /// }
    /// ```
    pub fn suggestion(&self) -> &str {
        match self {
            Self::Transient { suggestion, .. } => suggestion,
            Self::Timeout { suggestion, .. } => suggestion,
            Self::Authentication { suggestion, .. } => suggestion,
            Self::Authorization { suggestion, .. } => suggestion,
            Self::Validation { suggestion, .. } => suggestion,
            Self::NotFound { suggestion, .. } => suggestion,
            Self::Fatal { suggestion, .. } => suggestion,
            Self::Throttling { suggestion, .. } => suggestion,
            Self::BufferFull { suggestion, .. } => suggestion,
            Self::Cancellation { .. } => "Operation was cancelled. Check if the client is closing.",
            _ => "",
        }
    }

    /// Maps a tonic gRPC status to the appropriate [`KubemqError`] variant.
    ///
    /// See the [error reference](../docs/errors.md) for the complete mapping table.
    pub(crate) fn from_grpc_status(status: tonic::Status, operation: &str, channel: &str) -> Self {
        match status.code() {
            tonic::Code::Ok => Self::Fatal {
                code: ErrorCode::Fatal,
                message: "gRPC OK status mapped as error (this should not happen)".to_string(),
                operation: operation.to_string(),
                source: None,
                suggestion: "This is a bug in the SDK. Please report it.",
            },
            tonic::Code::Cancelled => Self::Cancellation {
                code: ErrorCode::Cancellation,
                message: status.message().to_string(),
                operation: operation.to_string(),
            },
            tonic::Code::Unknown | tonic::Code::Unavailable | tonic::Code::Aborted => {
                Self::Transient {
                    code: ErrorCode::Transient,
                    message: status.message().to_string(),
                    operation: operation.to_string(),
                    channel: channel.to_string(),
                    is_retryable: true,
                    source: Some(Box::new(status)),
                    request_id: String::new(),
                    suggestion:
                        "Retry the operation. If the problem persists, check server health.",
                }
            }
            tonic::Code::InvalidArgument
            | tonic::Code::AlreadyExists
            | tonic::Code::FailedPrecondition
            | tonic::Code::OutOfRange => Self::Validation {
                code: ErrorCode::Validation,
                message: status.message().to_string(),
                operation: operation.to_string(),
                channel: channel.to_string(),
                suggestion: "Check the request parameters (channel, body, metadata).",
            },
            tonic::Code::DeadlineExceeded => Self::Timeout {
                code: ErrorCode::Timeout,
                message: status.message().to_string(),
                operation: operation.to_string(),
                channel: channel.to_string(),
                is_retryable: true,
                source: Some(Box::new(status)),
                request_id: String::new(),
                suggestion: "Increase the timeout or check server connectivity and firewall rules.",
            },
            tonic::Code::NotFound => Self::NotFound {
                code: ErrorCode::NotFound,
                message: status.message().to_string(),
                operation: operation.to_string(),
                channel: channel.to_string(),
                suggestion: "The channel or queue does not exist. Create it first.",
            },
            tonic::Code::PermissionDenied => Self::Authorization {
                code: ErrorCode::Authorization,
                message: status.message().to_string(),
                operation: operation.to_string(),
                channel: channel.to_string(),
                source: Some(Box::new(status)),
                suggestion: "Verify the client has permissions for this channel.",
            },
            tonic::Code::ResourceExhausted => Self::Throttling {
                code: ErrorCode::Throttling,
                message: status.message().to_string(),
                operation: operation.to_string(),
                channel: channel.to_string(),
                is_retryable: true,
                suggestion: "Reduce request rate or increase server capacity.",
            },
            tonic::Code::DataLoss => Self::Fatal {
                code: ErrorCode::Fatal,
                message: status.message().to_string(),
                operation: operation.to_string(),
                source: Some(Box::new(status)),
                suggestion: "Data loss detected. Check server logs and data integrity.",
            },
            tonic::Code::Unimplemented | tonic::Code::Internal => Self::Fatal {
                code: ErrorCode::Fatal,
                message: status.message().to_string(),
                operation: operation.to_string(),
                source: Some(Box::new(status)),
                suggestion: "This is an unrecoverable server error. Contact support.",
            },
            tonic::Code::Unauthenticated => Self::Authentication {
                code: ErrorCode::Authentication,
                message: status.message().to_string(),
                operation: operation.to_string(),
                source: Some(Box::new(status)),
                suggestion: "Check your auth token. It may have expired.",
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_retryable() {
        let err = KubemqError::Transient {
            code: ErrorCode::Transient,
            message: "test".into(),
            operation: "send".into(),
            channel: "ch1".into(),
            is_retryable: true,
            source: None,
            request_id: String::new(),
            suggestion: "",
        };
        assert!(err.is_retryable());
        assert_eq!(err.code(), ErrorCode::Transient);
    }

    #[test]
    fn test_error_code_not_retryable() {
        let err = KubemqError::Validation {
            code: ErrorCode::Validation,
            message: "bad input".into(),
            operation: "send".into(),
            channel: "ch1".into(),
            suggestion: "fix it",
        };
        assert!(!err.is_retryable());
        assert_eq!(err.code(), ErrorCode::Validation);
        assert_eq!(err.suggestion(), "fix it");
    }

    #[test]
    fn test_client_closed_error() {
        let err = KubemqError::ClientClosed;
        assert!(!err.is_retryable());
        assert_eq!(err.code(), ErrorCode::Fatal);
        assert_eq!(err.suggestion(), "");
    }

    #[test]
    fn test_from_grpc_status_unavailable() {
        let status = tonic::Status::unavailable("server down");
        let err = KubemqError::from_grpc_status(status, "ping", "");
        assert!(err.is_retryable());
        assert_eq!(err.code(), ErrorCode::Transient);
    }

    #[test]
    fn test_from_grpc_status_unauthenticated() {
        let status = tonic::Status::unauthenticated("bad token");
        let err = KubemqError::from_grpc_status(status, "send_event", "ch1");
        assert_eq!(err.code(), ErrorCode::Authentication);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_not_found() {
        let status = tonic::Status::not_found("no such channel");
        let err = KubemqError::from_grpc_status(status, "subscribe", "ch1");
        assert_eq!(err.code(), ErrorCode::NotFound);
    }

    #[test]
    fn test_from_grpc_status_deadline_exceeded() {
        let status = tonic::Status::deadline_exceeded("timed out");
        let err = KubemqError::from_grpc_status(status, "send_command", "ch1");
        assert_eq!(err.code(), ErrorCode::Timeout);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_permission_denied() {
        let status = tonic::Status::permission_denied("no access");
        let err = KubemqError::from_grpc_status(status, "subscribe", "ch1");
        assert_eq!(err.code(), ErrorCode::Authorization);
    }

    #[test]
    fn test_from_grpc_status_internal() {
        let status = tonic::Status::internal("server error");
        let err = KubemqError::from_grpc_status(status, "ping", "");
        assert_eq!(err.code(), ErrorCode::Fatal);
    }

    #[test]
    fn test_from_grpc_status_cancelled() {
        let status = tonic::Status::cancelled("client cancelled");
        let err = KubemqError::from_grpc_status(status, "send_event", "ch1");
        assert_eq!(err.code(), ErrorCode::Cancellation);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_unknown() {
        let status = tonic::Status::unknown("unknown error");
        let err = KubemqError::from_grpc_status(status, "send_event", "ch1");
        assert_eq!(err.code(), ErrorCode::Transient);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_aborted() {
        let status = tonic::Status::aborted("transaction aborted");
        let err = KubemqError::from_grpc_status(status, "send_event", "ch1");
        assert_eq!(err.code(), ErrorCode::Transient);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_invalid_argument() {
        let status = tonic::Status::invalid_argument("bad channel name");
        let err = KubemqError::from_grpc_status(status, "send_event", "ch1");
        assert_eq!(err.code(), ErrorCode::Validation);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_already_exists() {
        let status = tonic::Status::already_exists("channel exists");
        let err = KubemqError::from_grpc_status(status, "create_channel", "ch1");
        assert_eq!(err.code(), ErrorCode::Validation);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_failed_precondition() {
        let status = tonic::Status::failed_precondition("precondition not met");
        let err = KubemqError::from_grpc_status(status, "send_queue", "q1");
        assert_eq!(err.code(), ErrorCode::Validation);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_out_of_range() {
        let status = tonic::Status::out_of_range("index out of range");
        let err = KubemqError::from_grpc_status(status, "receive_queue", "q1");
        assert_eq!(err.code(), ErrorCode::Validation);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_resource_exhausted() {
        let status = tonic::Status::resource_exhausted("quota exceeded");
        let err = KubemqError::from_grpc_status(status, "send_event", "ch1");
        assert_eq!(err.code(), ErrorCode::Throttling);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_unimplemented() {
        let status = tonic::Status::unimplemented("not supported");
        let err = KubemqError::from_grpc_status(status, "ping", "");
        assert_eq!(err.code(), ErrorCode::Fatal);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_from_grpc_status_data_loss() {
        let status = tonic::Status::data_loss("data corrupted");
        let err = KubemqError::from_grpc_status(status, "receive_queue", "q1");
        assert_eq!(err.code(), ErrorCode::Fatal);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_error_display() {
        let err = KubemqError::Transient {
            code: ErrorCode::Transient,
            message: "connection lost".into(),
            operation: "send_event".into(),
            channel: "events.test".into(),
            is_retryable: true,
            source: None,
            request_id: String::new(),
            suggestion: "",
        };
        let display = format!("{}", err);
        assert!(display.contains("send_event"));
        assert!(display.contains("events.test"));
        assert!(display.contains("connection lost"));
    }

    /// REQ-M53: Ok status should produce Fatal, not panic (replaces unreachable!).
    #[test]
    fn test_from_grpc_status_ok_produces_fatal() {
        let status = tonic::Status::ok("");
        let err = KubemqError::from_grpc_status(status, "test_op", "");
        assert!(
            matches!(err, KubemqError::Fatal { .. }),
            "Ok status should map to Fatal, got: {:?}",
            err
        );
        assert_eq!(err.code(), ErrorCode::Fatal);
        assert!(
            err.suggestion().contains("bug"),
            "Suggestion should mention SDK bug: {}",
            err.suggestion()
        );
    }
}
