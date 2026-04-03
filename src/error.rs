/// Machine-readable error codes.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    Transient,
    Timeout,
    Throttling,
    Authentication,
    Authorization,
    Validation,
    NotFound,
    Fatal,
    Cancellation,
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
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum KubemqError {
    #[error("{operation} failed on channel \"{channel}\": {message}")]
    Transient {
        code: ErrorCode,
        message: String,
        operation: String,
        channel: String,
        is_retryable: bool,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        request_id: String,
        suggestion: &'static str,
    },

    #[error("{operation} timed out on channel \"{channel}\": {message}")]
    Timeout {
        code: ErrorCode,
        message: String,
        operation: String,
        channel: String,
        is_retryable: bool,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        request_id: String,
        suggestion: &'static str,
    },

    #[error("authentication failed: {message}")]
    Authentication {
        code: ErrorCode,
        message: String,
        operation: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        suggestion: &'static str,
    },

    #[error("authorization failed on channel \"{channel}\": {message}")]
    Authorization {
        code: ErrorCode,
        message: String,
        operation: String,
        channel: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        suggestion: &'static str,
    },

    #[error("validation failed: {message}")]
    Validation {
        code: ErrorCode,
        message: String,
        operation: String,
        channel: String,
        suggestion: &'static str,
    },

    #[error("not found: {message}")]
    NotFound {
        code: ErrorCode,
        message: String,
        operation: String,
        channel: String,
        suggestion: &'static str,
    },

    #[error("fatal error: {message}")]
    Fatal {
        code: ErrorCode,
        message: String,
        operation: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        suggestion: &'static str,
    },

    #[error("operation cancelled: {message}")]
    Cancellation {
        code: ErrorCode,
        message: String,
        operation: String,
    },

    #[error("throttled: {message}")]
    Throttling {
        code: ErrorCode,
        message: String,
        operation: String,
        channel: String,
        is_retryable: bool,
        suggestion: &'static str,
    },

    #[error("backpressure: buffer full (capacity={buffer_size}, queued={queued_count})")]
    BufferFull {
        code: ErrorCode,
        buffer_size: usize,
        queued_count: usize,
        suggestion: &'static str,
    },

    #[error("stream broken: {unacknowledged_count} unacknowledged messages")]
    StreamBroken {
        unacknowledged_ids: Vec<String>,
        unacknowledged_count: usize,
    },

    #[error("client closed")]
    ClientClosed,

    #[error("transport error [{operation}]: {source}")]
    Transport {
        operation: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("handler error [{handler}]: {source}")]
    Handler {
        handler: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl KubemqError {
    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transient { is_retryable, .. } => *is_retryable,
            Self::Timeout { is_retryable, .. } => *is_retryable,
            Self::Throttling { is_retryable, .. } => *is_retryable,
            _ => false,
        }
    }

    /// Get the error code.
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

    /// Get the suggestion for resolving this error.
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

    /// Map a tonic gRPC status to KubemqError.
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
