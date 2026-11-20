use clap::Parser;
use ferrous_cqrses::consumer::{Config, IggyStreamConsumer};
use ferrous_cqrses::saga::{HttpEnterpriseDispatcher, OrderSagaManager, SagaConfig};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SagaConfig::parse();

    if config.healthcheck {
        // High-fidelity healthcheck probe for container runtime:
        // Verifies daemon heartbeat freshness on /tmp/daemon_heartbeat if present.
        let heartbeat_path = std::path::Path::new("/tmp/daemon_heartbeat");
        if heartbeat_path.exists() {
            if let Ok(metadata) = std::fs::metadata(heartbeat_path) {
                if let Ok(modified) = metadata.modified() {
                    if let Ok(elapsed) = modified.elapsed() {
                        if elapsed > std::time::Duration::from_secs(60) {
                            eprintln!(
                                "Healthcheck failed: daemon heartbeat is stale ({:?} ago)",
                                elapsed
                            );
                            std::process::exit(1);
                        }
                    }
                }
            }
        }
        return Ok(());
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
    let mut saga_manager = OrderSagaManager::new(config, stream, dispatcher);

    // Spawn periodic heartbeat updater on /tmp/daemon_heartbeat for container healthcheck probe
    tokio::spawn(async {
        let heartbeat_path = std::path::Path::new("/tmp/daemon_heartbeat");
        loop {
            let _ = std::fs::write(heartbeat_path, b"healthy");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    info!("Order saga manager initialized; entering processing loop");
    saga_manager.run_loop(shutdown_rx).await?;
    info!("Order saga daemon terminated cleanly");

    Ok(())
}
