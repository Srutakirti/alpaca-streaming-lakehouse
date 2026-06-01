mod config;
mod kafka;
mod metrics;
mod ws;

use rdkafka::producer::Producer;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = config::Config::from_env()?;
    tracing::debug!(?cfg, "config loaded");
    tracing::info!(
        component = %cfg.component,
        kafka_broker = %cfg.kafka_broker,
        kafka_topic = %cfg.kafka_topic,
        symbols = ?cfg.symbols,
        "wsr starting"
    );

    // Kafka setup: ensure the topic exists and force the TCP handshake before
    // the WS task starts feeding frames.
    kafka::ensure_topic(&cfg).await?;
    let producer = kafka::build_producer(&cfg)?;
    kafka::warm_metadata(&producer, &cfg.kafka_topic)?;

    let metrics = metrics::Metrics::new(cfg.component.clone());
    let shutdown = CancellationToken::new();
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(cfg.channel_capacity);

    let emitter = metrics::emitter(
        metrics.clone(),
        Duration::from_secs(cfg.metrics_interval_secs),
        Some(tx.clone()),
        shutdown.clone(),
    );
    let prod_task = kafka::producer_task(
        producer.clone(),
        cfg.kafka_topic.clone(),
        cfg.clone(),
        metrics.clone(),
        rx,
        shutdown.clone(),
    );

    // Either Ctrl+C (SIGINT) or SIGTERM (Cloud Run shutdown) cancels the
    // token, draining the pipeline gracefully.
    spawn_signal_handler(shutdown.clone());

    // The WS retry loop owns the foreground. It returns when the shutdown
    // token fires or a fatal/exhausted error occurs.
    let ws_result = ws::run(&cfg, tx.clone(), metrics.clone(), shutdown.clone()).await;
    match &ws_result {
        Ok(()) => tracing::info!("ws loop ended"),
        Err(e) => tracing::error!(error = %e, "ws loop ended with error"),
    }

    // Tear down: signal shutdown, close the channel, drain both tasks, flush.
    shutdown.cancel();
    drop(tx);
    let _ = tokio::join!(prod_task, emitter);
    producer.flush(Duration::from_secs(10))?;
    tracing::info!(snapshot = %metrics.snapshot(), "final metrics");

    ws_result.map_err(anyhow::Error::from)
}

/// Cancel `shutdown` on the first of SIGINT (Ctrl+C) or SIGTERM.
fn spawn_signal_handler(shutdown: CancellationToken) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "failed to install SIGTERM handler");
                    return;
                }
            };
            tokio::select! {
                r = tokio::signal::ctrl_c() => {
                    if r.is_ok() {
                        tracing::info!("SIGINT received; shutting down");
                    }
                }
                _ = term.recv() => tracing::info!("SIGTERM received; shutting down"),
            }
        }
        #[cfg(not(unix))]
        {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!("SIGINT received; shutting down");
            }
        }
        shutdown.cancel();
    });
}
