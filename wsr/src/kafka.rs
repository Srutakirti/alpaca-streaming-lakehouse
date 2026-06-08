use crate::config::Config;
use crate::metrics::Metrics;
use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, StreamExt};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::Message;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum Error {
    #[error("kafka client error: {0}")]
    Kafka(#[from] KafkaError),
    #[error("metadata fetch failed: {0}")]
    Metadata(String),
}

fn client_config(cfg: &Config) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &cfg.kafka_broker);
    c
}

pub async fn ensure_topic(cfg: &Config) -> Result<(), Error> {
    let admin: AdminClient<DefaultClientContext> = client_config(cfg).create()?;
    let new_topic = NewTopic::new(&cfg.kafka_topic, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
    let results = admin.create_topics(&[new_topic], &opts).await?;
    for r in results {
        //check if topic was created or topic already exists or error happened
        match r {
            Ok(name) => tracing::info!(topic = %name, "topic created"),
            Err((name, RDKafkaErrorCode::TopicAlreadyExists)) => {
                tracing::info!(topic = %name, "topic already exists")
            }
            Err((name, code)) => {
                return Err(Error::Metadata(format!(
                    "create_topics({name}) failed: {code:?}"
                )));
            }
        }
    }
    Ok(())
}

pub fn build_producer(cfg: &Config) -> Result<FutureProducer, Error> {
    let producer: FutureProducer = client_config(cfg).create()?;
    Ok(producer)
}

pub fn warm_metadata(producer: &FutureProducer, topic: &str) -> Result<(), Error> {
    let md = producer
        .client()
        .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(10)))
        .map_err(|e| Error::Metadata(e.to_string()))?;
    tracing::info!(brokers = md.brokers().len(), topics = md.topics().len(), "metadata warmed");
    Ok(())
}

pub fn producer_task(
    producer: FutureProducer,
    topic: String,
    cfg: Config,
    metrics: Arc<Metrics>,
    mut rx: mpsc::Receiver<Bytes>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut inflight = FuturesUnordered::new();
        let mut draining = false;

        loop {
            tokio::select! {
                biased;

                // Shutdown signalled: stop accepting new messages, drain inflight.
                _ = shutdown.cancelled(), if !draining => {
                    tracing::info!(inflight = inflight.len(), "shutdown: draining inflight deliveries");
                    draining = true;
                }

                // Drain side: poll completions until inflight empties.
                Some(result) = inflight.next(), if !inflight.is_empty() => {
                    match result {
                        Ok(Ok(_delivery)) => {
                            metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                            metrics.touch_last_delivery();
                        }
                        Ok(Err((err, msg))) => {
                            metrics.delivery_failures.fetch_add(1, Ordering::Relaxed);
                            let payload_len = <_ as Message>::payload(&msg).map(|p| p.len()).unwrap_or(0);
                            tracing::warn!(error = %err, payload_len, "delivery failed");
                        }
                        Err(canceled) => {
                            metrics.delivery_failures.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(error = %canceled, "delivery future canceled");
                        }
                    }
                    if draining && inflight.is_empty() {
                        break;
                    }
                }

                // New payload to produce. Guarded so we never exceed max_inflight.
                maybe = rx.recv(), if !draining && inflight.len() < cfg.max_inflight => {
                    match maybe {
                        Some(payload) => {
                            if let Some(fut) = enqueue(&producer, &topic, payload, &metrics).await {
                                inflight.push(fut);
                            }
                        }
                        None => {
                            tracing::info!(inflight = inflight.len(), "channel closed: draining");
                            draining = true;
                            if inflight.is_empty() { break; }
                        }
                    }
                }

                // All other arms disabled — only possible after draining finishes.
                else => break,
            }
        }
        tracing::info!("producer task exited");
    })
}

async fn enqueue(
    producer: &FutureProducer,
    topic: &str,
    payload: Bytes,
    metrics: &Arc<Metrics>,
) -> Option<rdkafka::producer::DeliveryFuture> {
    let mut payload = payload;
    loop {
        let record: FutureRecord<'_, (), [u8]> = FutureRecord::to(topic).payload(&payload);
        match producer.send_result(record) {
            Ok(fut) => return Some(fut),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), rec)) => {
                tracing::warn!(payload_len = rec.payload.map(|p| p.len()), "librdkafka queue full; backing off");
                payload = Bytes::copy_from_slice(rec.payload.unwrap_or(&[]));
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err((e, _)) => {
                metrics.delivery_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(error = %e, "send_result failed synchronously");
                return None;
            }
        }
    }
}

