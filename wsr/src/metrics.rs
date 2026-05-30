use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct Metrics {
    pub component: String,
    pub messages_received: AtomicU64,
    pub messages_sent: AtomicU64,
    pub delivery_failures: AtomicU64,
    pub errors: AtomicU64,
    pub last_message_ts: AtomicI64,
    pub last_delivery_ts: AtomicI64,
    pub connection_status: AtomicBool,
}

impl Metrics {
    pub fn new(component: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            component: component.into(),
            messages_received: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            delivery_failures: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_message_ts: AtomicI64::new(0),
            last_delivery_ts: AtomicI64::new(0),
            connection_status: AtomicBool::new(false),
        })
    }

    pub fn touch_last_message(&self) {
        self.last_message_ts.store(now_ms(), Ordering::Relaxed);
    }

    pub fn touch_last_delivery(&self) {
        self.last_delivery_ts.store(now_ms(), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Value {
        json!({
            "component": self.component,
            "messages_received": self.messages_received.load(Ordering::Relaxed),
            "messages_sent": self.messages_sent.load(Ordering::Relaxed),
            "delivery_failures": self.delivery_failures.load(Ordering::Relaxed),
            "errors": self.errors.load(Ordering::Relaxed),
            "last_message_ts": iso(self.last_message_ts.load(Ordering::Relaxed)),
            "last_delivery_ts": iso(self.last_delivery_ts.load(Ordering::Relaxed)),
            "connection_status": self.connection_status.load(Ordering::Relaxed),
        })
    }
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn iso(ms: i64) -> Value {
    if ms == 0 {
        return Value::Null;
    }
    match DateTime::<Utc>::from_timestamp_millis(ms) {
        Some(dt) => Value::String(dt.to_rfc3339()),
        None => Value::Null,
    }
}

pub fn emitter<T: Send + 'static>(
    metrics: Arc<Metrics>,
    interval: Duration,
    channel: Option<mpsc::Sender<T>>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate first tick so the first emission happens after `interval`.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let mut snap = metrics.snapshot();
                    if let Some(tx) = channel.as_ref() {
                        let cap = tx.max_capacity();
                        let free = tx.capacity();
                        let depth = cap.saturating_sub(free);
                        if let Some(obj) = snap.as_object_mut() {
                            obj.insert("queue_depth".into(), json!(depth));
                            obj.insert("queue_capacity".into(), json!(cap));
                        }
                    }
                    tracing::info!(snapshot = %snap, "metrics");
                }
                _ = shutdown.cancelled() => break,
            }
        }
    })
}
