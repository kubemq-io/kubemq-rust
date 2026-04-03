/// Server information returned by Ping.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub host: String,
    pub version: String,
    pub server_start_time: i64,
    pub server_up_time_seconds: i64,
}
