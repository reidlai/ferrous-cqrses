use clap::Parser;
use ferrous_cqrses::consumer::{Config, IggyStreamConsumer};
use ferrous_cqrses::saga::{HttpEnterpriseDispatcher, OrderSagaManager, SagaConfig};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SagaConfig::parse();

    if config.healthcheck {
        // High-fidelity healthcheck probe for container runtime:
        // Verifies daemon heartbeat freshness on heartbeat path (default: /tmp/daemon_heartbeat).
        let heartbeat_path_str = std::env::var("HEARTBEAT_PATH")
            .unwrap_or_else(|_| "/tmp/daemon_heartbeat".to_string());
        let heartbeat_path = std::path::Path::new(&heartbeat_path_str);
        if !heartbeat_path.exists() {
            eprintln!(
                "Healthcheck failed: heartbeat file does not exist ({})",
                heartbeat_path.display()
            );
            std::process::exit(1);
        }
        let metadata = match std::fs::metadata(heartbeat_path) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Healthcheck failed: cannot read metadata: {e}");
                std::process::exit(1);
            }
        };
        let modified = match metadata.modified() {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Healthcheck failed: cannot read modified timestamp: {e}");
                std::process::exit(1);
            }
        };
        match modified.elapsed() {
            Ok(elapsed) if elapsed > Duration::from_secs(60) => {
                eprintln!(
                    "Healthcheck failed: daemon heartbeat is stale ({:?} ago)",
                    elapsed
                );
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("Healthcheck failed: clock error: {e}");
                std::process::exit(1);
            }
            _ => return Ok(()),
        }
    }

    // Initialize tracing subscriber with JSON formatter and env-filter
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to initialize tracing subscriber");

    info!(
        server = %config.iggy_server_address,
        stream = %config.stream_name,
        topic = %config.topic_orders_events,
        consumer_group = %config.consumer_group,
        enterprise_settlement_url = %config.enterprise_settlement_url,
        max_retries = config.max_retries,
        "Starting ferrous-cqrses Order saga daemon (order-saga)"
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn signal handler task for graceful shutdown on SIGINT / SIGTERM / Ctrl+C
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(err) => {
                    tracing::error!("Failed to register SIGTERM listener: {err}");
                    return;
                }
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Received shutdown signal (SIGINT/ctrl_c); initiating graceful shutdown");
                    let _ = shutdown_tx.send(true);
                }
                _ = sigterm.recv() => {
                    info!("Received shutdown signal (SIGTERM); initiating graceful shutdown");
                    let _ = shutdown_tx.send(true);
                }
            }
        }

        #[cfg(not(unix))]
        {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received shutdown signal (SIGINT/ctrl_c); initiating graceful shutdown");
                    let _ = shutdown_tx.send(true);
                }
                Err(err) => {
                    tracing::error!("Error listening for shutdown signal: {err}");
                }
            }
        }
    });

    let stream = IggyStreamConsumer::new(Config {
        iggy_server_address: config.iggy_server_address.clone(),
        iggy_username: config.iggy_username.clone(),
        iggy_password: config.iggy_password.clone(),
        stream_name: config.stream_name.clone(),
        topic_orders_events: config.topic_orders_events.clone(),
        consumer_group: config.consumer_group.clone(),
        healthcheck: false,
        ..Default::default()
    });

    let dispatcher = HttpEnterpriseDispatcher::from_config(&config)?;
    let last_active = Arc::new(AtomicU64::new(
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
    ));
    let mut saga_manager = OrderSagaManager::new(config, stream, dispatcher)
        .with_heartbeat_tracker(last_active.clone());

    let heartbeat_path_str = std::env::var("HEARTBEAT_PATH")
        .unwrap_or_else(|_| "/tmp/daemon_heartbeat".to_string());
    let heartbeat_path = std::path::PathBuf::from(heartbeat_path_str);
    let _ = std::fs::write(&heartbeat_path, b"healthy");

    // Spawn periodic heartbeat updater coupled to active saga processing
    let tracker = last_active.clone();
    let heartbeat_target = heartbeat_path.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
            let last = tracker.load(Ordering::Relaxed);
            // Only touch heartbeat file if saga loop has been active within 60 seconds
            if now.saturating_sub(last) <= 60 {
                let _ = std::fs::write(&heartbeat_target, b"healthy");
            }
        }
    });

    info!("Order saga manager initialized; entering processing loop");
    saga_manager.run_loop(shutdown_rx).await?;
    info!("Order saga daemon terminated cleanly");

    Ok(())
}
