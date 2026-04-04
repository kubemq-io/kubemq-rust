/// Information about the KubeMQ broker returned by [`KubemqClient::ping()`](crate::KubemqClient::ping).
#[derive(Debug, Clone)]
pub struct ServerInfo {
    /// Hostname of the KubeMQ broker.
    pub host: String,
    /// Version string of the KubeMQ broker.
    pub version: String,
    /// Unix timestamp (seconds) when the broker started.
    pub server_start_time: i64,
    /// Number of seconds the broker has been running.
    pub server_up_time_seconds: i64,
}
