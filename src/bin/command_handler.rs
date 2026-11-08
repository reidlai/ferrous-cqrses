use clap::Parser;
use ferrous_cqrses::consumer::{CommandConsumer, Config, IggyStreamConsumer};
use ferrous_cqrses::publisher::IggyStreamPublisher;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let config = Config::parse();

    info!(
        server = %config.iggy_server_address,
        stream = %config.stream_name,
        topic = %config.topic_orders_intake,
        consumer_group = %config.consumer_group,
        "Starting ferrous-cqrses sequential command handler daemon"
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

    let stream = IggyStreamConsumer::new(config.clone());
    let publisher = IggyStreamPublisher::new(config.clone());
    let mut consumer = CommandConsumer::new(config, stream, publisher);

    info!("Command consumer loop initialized; awaiting incoming commands");
    consumer.run_loop(shutdown_rx).await?;
    info!("Command handler daemon terminated cleanly");

    Ok(())
}
