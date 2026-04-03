//! v2 Axum REST API server for burn-in control.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use tower_http::cors::CorsLayer;

use crate::api::{self, RunConfigApi};
use crate::config::BurnInConfig;
use crate::engine::{Engine, RunState};

pub struct AppState {
    pub engine: Engine,
}

pub async fn run(config: BurnInConfig) -> anyhow::Result<()> {
    let port = config.metrics.port;
    let cors_origins = config.cors.origins.clone();

    let state = Arc::new(AppState {
        engine: Engine::new(config),
    });

    let cors = if cors_origins == "*" {
        CorsLayer::permissive()
    } else {
        CorsLayer::permissive()
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/info", get(info))
        .route("/broker/status", get(broker_status))
        .route("/run/start", post(run_start))
        .route("/run/stop", post(run_stop))
        .route("/run", get(run_full))
        .route("/run/status", get(run_status))
        .route("/run/config", get(run_config))
        .route("/run/report", get(run_report))
        .route("/cleanup", post(cleanup))
        .route("/metrics", get(metrics))
        .route("/status", get(legacy_status))
        .route("/summary", get(legacy_summary))
        .layer(cors)
        .with_state(state.clone());

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("Burn-in API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(state.clone()))
        .await?;

    Ok(())
}

pub async fn run_with_engine(engine: Engine, port: u16) -> anyhow::Result<Arc<AppState>> {
    let state = Arc::new(AppState { engine });

    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/info", get(info))
        .route("/broker/status", get(broker_status))
        .route("/run/start", post(run_start))
        .route("/run/stop", post(run_stop))
        .route("/run", get(run_full))
        .route("/run/status", get(run_status))
        .route("/run/config", get(run_config))
        .route("/run/report", get(run_report))
        .route("/cleanup", post(cleanup))
        .route("/metrics", get(metrics))
        .route("/status", get(legacy_status))
        .route("/summary", get(legacy_summary))
        .layer(cors)
        .with_state(state.clone());

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("Burn-in API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let state_ret = state.clone();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal(state))
            .await
            .ok();
    });

    Ok(state_ret)
}

async fn shutdown_signal(state: Arc<AppState>) {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = ctrl_c => tracing::info!("SIGINT received"),
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
        tracing::info!("Shutdown signal received");
    }

    tracing::info!("Initiating graceful shutdown...");
    let passed = state.engine.graceful_shutdown().await;
    if !passed {
        tracing::warn!("Burn-in verdict: FAILED");
    }
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "alive"}))
}

async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let s = state.engine.state().await;
    match s {
        RunState::Starting | RunState::Stopping => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "not_ready",
                "state": s.to_string(),
            })),
        ),
        _ => (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "ready",
                "state": s.to_string(),
            })),
        ),
    }
}

async fn info(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(state.engine.get_info().await)
}

async fn broker_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(state.engine.get_broker_status().await)
}

async fn run_start(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let raw_value: serde_json::Value = if body.is_empty() {
        serde_json::json!({})
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!("invalid JSON: {}", e),
                    })),
                );
            }
        }
    };

    // Detect v1 format
    let v1_issues = api::detect_v1_format(&raw_value);
    if !v1_issues.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "v1 config format detected",
                "v1_fields": v1_issues,
                "message": "Please use v2 format. See /info for spec version.",
            })),
        );
    }

    let rc: RunConfigApi = match serde_json::from_value(raw_value) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("invalid config: {}", e),
                })),
            );
        }
    };

    let validation_errors = rc.validate();
    if !validation_errors.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "validation failed",
                "errors": validation_errors,
            })),
        );
    }

    let current_state = state.engine.state().await;
    if current_state != RunState::Idle
        && current_state != RunState::Stopped
        && current_state != RunState::Error
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": format!("cannot start run in state {}", current_state),
                "state": current_state.to_string(),
            })),
        );
    }

    match state.engine.start_run(&rc).await {
        Ok((run_id, enabled_count)) => {
            let total_channels = rc.total_channel_count(state.engine.startup_cfg());
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "status": "starting",
                    "run_id": run_id,
                    "message": format!(
                        "run starting with {} channels across {} patterns",
                        total_channels, enabled_count
                    ),
                })),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": e,
            })),
        ),
    }
}

async fn run_stop(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let run_id = state.engine.run_id().await;
    match state.engine.stop_run().await {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "run_id": run_id,
                "state": "stopping",
                "message": "Graceful shutdown initiated",
            })),
        ),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": e,
                "state": state.engine.state().await.to_string(),
            })),
        ),
    }
}

async fn run_full(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(state.engine.get_run_full().await)
}

async fn run_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(state.engine.get_run_status().await)
}

async fn run_config(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.engine.get_run_config().await {
        Some(cfg) => (StatusCode::OK, Json(cfg)),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no run config available"})),
        ),
    }
}

async fn run_report(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.engine.get_run_report().await {
        Some(rpt) => (StatusCode::OK, Json(rpt)),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no report available"})),
        ),
    }
}

async fn cleanup(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let s = state.engine.state().await;
    if s != RunState::Idle && s != RunState::Stopped && s != RunState::Error {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": format!("cannot cleanup in state {}", s),
            })),
        );
    }
    (StatusCode::OK, Json(state.engine.cleanup_channels().await))
}

async fn metrics(State(state): State<Arc<AppState>>) -> String {
    state.engine.metrics_prometheus().await
}

async fn legacy_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    tracing::debug!("GET /status is deprecated, use GET /run/status");
    Json(state.engine.get_run_status().await)
}

async fn legacy_summary(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    tracing::debug!("GET /summary is deprecated, use GET /run/report");
    match state.engine.get_run_report().await {
        Some(rpt) => (StatusCode::OK, Json(rpt)),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no report available"})),
        ),
    }
}
