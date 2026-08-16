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

/// Exit the producer (by cancelling `shutdown`) when no *bar* message has been
/// received within `idle`. Only data frames count — `touch_last_message` is
/// called for Text/Binary frames, never for ping/pong keepalives — so this is
/// the thing that stops the producer from holding the single Alpaca connection
/// open all weekend once the market closes and only keepalives flow.
///
/// Until the first bar arrives, the process start time is the baseline, so a run
/// launched on a closed market (holiday, off-hours) also exits after `idle`.
pub fn watchdog(
    metrics: Arc<Metrics>,
    idle: Duration,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let start_ms = now_ms();
        let idle_ms = idle.as_millis() as i64;
        // Poll several times per window so we react within ~idle/6, but never
        // busier than every 5s for short windows.
        let tick = (idle / 6).max(Duration::from_secs(5));
        let mut ticker = tokio::time::interval(tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // skip the immediate first tick
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let last = metrics.last_message_ts.load(Ordering::Relaxed);
                    let baseline = if last == 0 { start_ms } else { last };
                    if now_ms() - baseline >= idle_ms {
                        tracing::warn!(
                            idle_secs = idle.as_secs(),
                            last_message_ts = %iso(last),
                            "no bar data within idle window; shutting down producer"
                        );
                        shutdown.cancel();
                        break;
                    }
                }
                _ = shutdown.cancelled() => break,
            }
        }
    })
}

pub fn emitter<T: Send + 'static>(
    metrics: Arc<Metrics>,
    interval: Duration,
    channel: Option<mpsc::Sender<T>>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        //ticker just yeilds when the time interval is done, first tick is done immediately
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate first tick so the first emission happens after `interval`.
        ticker.tick().await;
        loop {
            //tokio polls both ticker and shutdown futures  and proceeds on whichever resolves first.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_snapshot_has_defaults() {
        let m = Metrics::new("alpaca-extractor-rs");
        let s = m.snapshot();
        assert_eq!(s["component"], "alpaca-extractor-rs");
        assert_eq!(s["messages_received"], 0);
        assert_eq!(s["messages_sent"], 0);
        assert_eq!(s["delivery_failures"], 0);
        assert_eq!(s["errors"], 0);
        assert_eq!(s["connection_status"], false);
        assert!(s["last_message_ts"].is_null());
        assert!(s["last_delivery_ts"].is_null());
    }

    #[test]
    fn snapshot_reflects_atomic_updates() {
        let m = Metrics::new("wsr-test");
        m.messages_received.fetch_add(3, Ordering::Relaxed);
        m.messages_sent.fetch_add(2, Ordering::Relaxed);
        m.delivery_failures.fetch_add(1, Ordering::Relaxed);
        m.connection_status.store(true, Ordering::Relaxed);
        m.touch_last_message();

        let s = m.snapshot();
        assert_eq!(s["messages_received"], 3);
        assert_eq!(s["messages_sent"], 2);
        assert_eq!(s["delivery_failures"], 1);
        assert_eq!(s["connection_status"], true);
        // touch_last_message wrote a real timestamp -> rendered as an ISO string.
        assert!(s["last_message_ts"].is_string());
        assert!(s["last_delivery_ts"].is_null()); // never touched
    }

    #[test]
    fn iso_renders_zero_as_null_and_nonzero_as_string() {
        assert!(iso(0).is_null());
        let v = iso(1_700_000_000_000);
        assert!(v.is_string());
        assert!(v.as_str().unwrap().contains("2023")); // 2023-11-14T...
    }
}
