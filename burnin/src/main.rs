//! KubeMQ Rust SDK Burn-in Test Application
//!
//! Boots idle, accepts POST /run/start for v2 JSON config-driven runs.

mod api;
mod config;
mod engine;
mod metrics;
mod payload;
mod report;
mod server;
mod tracker;
mod worker;

use clap::Parser;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "kubemq-burnin", about = "KubeMQ Rust SDK burn-in soak test")]
struct Args {
    /// Path to YAML config file
    #[arg(short, long, default_value = "burnin-config.yaml")]
    config: PathBuf,

    /// Override broker address (host:port)
    #[arg(long, env = "KUBEMQ_BROKER_ADDRESS")]
    broker_address: Option<String>,

    /// Override run mode (soak, stress, idle)
    #[arg(long)]
    mode: Option<String>,

    /// Override run duration (e.g. "1h", "30m", "1d")
    #[arg(long)]
    duration: Option<String>,

    /// API port
    #[arg(long)]
    api_port: Option<u16>,

    /// Run with a YAML config file (starts immediately)
    #[arg(long)]
    run: Option<PathBuf>,

    /// Cleanup all rust_burnin_* channels and exit
    #[arg(long)]
    cleanup_only: bool,

    /// Validate config and exit
    #[arg(long)]
    validate_config: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let mut cfg = if args.config.exists() {
        let content = std::fs::read_to_string(&args.config)?;
        serde_yaml::from_str::<config::BurnInConfig>(&content)?
    } else {
        tracing::warn!("Config file {:?} not found, using defaults", args.config);
        config::BurnInConfig::default()
    };

    if let Some(addr) = args.broker_address {
        cfg.broker.address = addr;
    }
    if let Some(mode) = args.mode {
        cfg.mode = mode;
    }
    if let Some(duration) = args.duration {
        cfg.duration = duration;
    }
    if let Some(port) = args.api_port {
        cfg.metrics.port = port;
        cfg.api.port = port;
    }

    if args.validate_config {
        let (errors, warnings) = cfg.validate();
        if !warnings.is_empty() {
            for w in &warnings {
                println!("WARNING: {}", w);
            }
        }
        if errors.is_empty() {
            println!("Config is valid");
            std::process::exit(0);
        } else {
            for e in &errors {
                println!("ERROR: {}", e);
            }
            std::process::exit(1);
        }
    }

    if args.cleanup_only {
        tracing::info!("Cleanup-only mode: deleting all rust_burnin_* channels");
        let engine = engine::Engine::new(cfg);
        let result = engine.cleanup_channels().await;
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    tracing::info!(
        "Starting burn-in app: mode=idle, broker={}, port={}",
        cfg.broker.address,
        cfg.metrics.port,
    );

    if let Some(run_file) = args.run {
        let port = cfg.metrics.port;
        let engine = engine::Engine::new(cfg.clone());

        let run_cfg_content = std::fs::read_to_string(&run_file)?;
        let run_config: config::BurnInConfig = serde_yaml::from_str(&run_cfg_content)?;

        let state = server::run_with_engine(engine, port).await?;

        match state.engine.start_run_from_config(run_config).await {
            Ok(run_id) => tracing::info!("Run started from file: run_id={}", run_id),
            Err(e) => tracing::error!("Failed to start run from file: {}", e),
        }

        tokio::signal::ctrl_c().await?;
        state.engine.graceful_shutdown().await;
    } else {
        server::run(cfg).await?;
    }

    Ok(())
}
