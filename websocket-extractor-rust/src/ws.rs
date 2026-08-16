use crate::config::Config;
use crate::metrics::Metrics;
use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWriter = SplitSink<WsStream, Message>;
type WsReader = SplitStream<WsStream>;

/// Errors at the ws module boundary. `Transient` drives a reconnect with
/// backoff; `Fatal` aborts the retry loop immediately.
#[derive(Debug, Error)]
pub enum Error {
    #[error("transient ws error: {0}")]
    Transient(String),
    #[error("fatal ws error: {0}")]
    Fatal(String),
}

/// A single Alpaca control frame (`success` / `subscription` / `error`).
/// Data frames (bars) are forwarded verbatim and never deserialized here.
#[derive(Debug, Deserialize)]
struct ControlFrame {
    #[serde(rename = "T")]
    typ: String,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    bars: Option<Vec<String>>,
}

/// Why `stream_frames` returned without an error.
enum Outcome {
    /// Shutdown token fired — exit the retry loop cleanly.
    Shutdown,
}

/// Connect, authenticate, subscribe, then stream frames into `tx` until the
/// shutdown token fires or a fatal error occurs. Reconnects on transient
/// failures with exponential backoff (reset on each successful connect),
/// giving up after `cfg.max_retries` consecutive failed attempts.
pub async fn run(
    cfg: &Config,
    tx: mpsc::Sender<Bytes>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let backoff_max = Duration::from_secs(cfg.backoff_max_secs);
    let mut backoff = Duration::from_secs(1);
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        let mut connected = false;
        let span = tracing::info_span!("ws_attempt", attempt);

        let result: Result<Outcome, Error> = async {
            let (mut write, mut read) = setup_ws(cfg).await?;
            connected = true;
            metrics.connection_status.store(true, Ordering::Relaxed);
            tracing::info!("ws connected; streaming frames");
            stream_frames(&mut write, &mut read, cfg, &tx, &metrics, &shutdown).await
        }
        .instrument(span)
        .await;

        metrics.connection_status.store(false, Ordering::Relaxed);

        // A healthy connection resets the backoff schedule and consecutive
        // attempt counter — MAX_RETRIES counts only back-to-back failures.
        if connected {
            backoff = Duration::from_secs(1);
            attempt = 0;
        }

        match result {
            Ok(Outcome::Shutdown) => {
                tracing::info!("ws shutdown requested; exiting retry loop");
                return Ok(());
            }
            Err(Error::Fatal(msg)) => {
                tracing::error!(error = %msg, "fatal ws error; giving up");
                return Err(Error::Fatal(msg));
            }
            Err(Error::Transient(msg)) => {
                metrics.errors.fetch_add(1, Ordering::Relaxed);
                if attempt >= cfg.max_retries {
                    tracing::error!(error = %msg, attempt, "max retries exhausted");
                    return Err(Error::Transient(format!("max retries exhausted: {msg}")));
                }
                tracing::warn!(
                    error = %msg,
                    attempt,
                    backoff_secs = backoff.as_secs(),
                    "transient ws error; backing off"
                );
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = shutdown.cancelled() => {
                        tracing::info!("shutdown during backoff; exiting");
                        return Ok(());
                    }
                }
                backoff = (backoff * 2).min(backoff_max);
            }
        }
    }
}

/// Open the WebSocket and walk the Alpaca handshake: connected greeting →
/// auth → subscribe. Returns the split sink/stream ready for streaming.
async fn setup_ws(cfg: &Config) -> Result<(WsWriter, WsReader), Error> {
    let (ws_stream, _) = connect_async(&cfg.ws_uri)
        .await
        .map_err(|e| Error::Transient(format!("connect: {e}")))?;
    let (mut write, mut read) = ws_stream.split();

    let to = Duration::from_secs(cfg.timeout_secs);

    // 1. Server greeting: [{"T":"success","msg":"connected"}]
    expect(&mut read, "success", Some("connected"), to).await?;
    tracing::info!("connected");

    // 2. Auth: send credentials, expect [{"T":"success","msg":"authenticated"}]
    let auth = format!(
        r#"{{"action":"auth","key":"{}","secret":"{}"}}"#,
        cfg.alpaca_key, cfg.alpaca_secret
    );
    write
        .send(Message::Text(auth.into()))
        .await
        .map_err(|e| Error::Transient(format!("send auth: {e}")))?;
    expect(&mut read, "success", Some("authenticated"), to).await?;
    tracing::info!("authenticated");

    // 3. Subscribe to bars, expect [{"T":"subscription","bars":[...]}]
    let bars = serde_json::to_string(&cfg.symbols)
        .map_err(|e| Error::Fatal(format!("serialize symbols: {e}")))?;
    let subscribe = format!(r#"{{"action":"subscribe","bars":{bars}}}"#);
    write
        .send(Message::Text(subscribe.into()))
        .await
        .map_err(|e| Error::Transient(format!("send subscribe: {e}")))?;
    let resp = expect(&mut read, "subscription", None, to).await?;
    tracing::info!(bars = ?resp.bars, "subscribed");

    Ok((write, read))
}

/// How a control frame matched the expected type/msg. Kept separate from IO so
/// the handshake decision logic is unit-testable without a socket.
#[derive(Debug, PartialEq, Eq)]
enum FrameClass {
    /// Frame is the expected type (and msg, if required).
    Match,
    /// Server `error` frame — bad credentials / config; aborts the retry loop.
    Fatal(String),
    /// Wrong type or msg — a transient handshake hiccup; reconnect.
    Mismatch(String),
}

/// Classify a parsed control frame against the expected type/msg. A server
/// `error` frame is `Fatal`; a type or msg mismatch is `Mismatch`.
fn classify_frame(frame: &ControlFrame, want_type: &str, want_msg: Option<&str>) -> FrameClass {
    if frame.typ == "error" {
        let code = frame.code.unwrap_or(-1);
        let m = frame.msg.clone().unwrap_or_default();
        return FrameClass::Fatal(format!("server error {code}: {m}"));
    }
    if frame.typ != want_type {
        return FrameClass::Mismatch(format!("expected T={want_type}, got T={}", frame.typ));
    }
    if let Some(expected) = want_msg {
        let got = frame.msg.as_deref().unwrap_or("");
        if got != expected {
            return FrameClass::Mismatch(format!("expected msg={expected}, got msg={got}"));
        }
    }
    FrameClass::Match
}

/// Await one control frame of the expected type within `to`. A server `error`
/// frame is classified `Fatal` (bad credentials / config); a type/msg mismatch
/// or any IO problem is `Transient`.
async fn expect(
    read: &mut WsReader,
    want_type: &str,
    want_msg: Option<&str>,
    to: Duration,
) -> Result<ControlFrame, Error> {
    let frame = timeout(to, next_control(read))
        .await
        .map_err(|_| Error::Transient(format!("timed out waiting for {want_type}")))??;

    match classify_frame(&frame, want_type, want_msg) {
        FrameClass::Match => Ok(frame),
        FrameClass::Fatal(m) => Err(Error::Fatal(m)),
        FrameClass::Mismatch(m) => Err(Error::Transient(m)),
    }
}

/// Read the next text frame and parse the first element as a `ControlFrame`.
async fn next_control(read: &mut WsReader) -> Result<ControlFrame, Error> {
    let msg = read
        .next()
        .await
        .ok_or_else(|| Error::Transient("stream ended before control frame".into()))?
        .map_err(|e| Error::Transient(format!("ws recv: {e}")))?;

    let text = match msg {
        Message::Text(t) => t,
        other => {
            return Err(Error::Transient(format!(
                "expected text frame, got {other:?}"
            )));
        }
    };

    let arr: Vec<ControlFrame> = serde_json::from_str(&text)
        .map_err(|e| Error::Transient(format!("parse control frame: {e}")))?;
    arr.into_iter()
        .next()
        .ok_or_else(|| Error::Transient("empty control frame array".into()))
}

/// The inner recv loop: forward each data frame verbatim into `tx`, refreshing
/// the idle timeout on every message. Exits on shutdown, idle timeout, server
/// close, or stream error.
async fn stream_frames(
    write: &mut WsWriter,
    read: &mut WsReader,
    cfg: &Config,
    tx: &mpsc::Sender<Bytes>,
    metrics: &Arc<Metrics>,
    shutdown: &CancellationToken,
) -> Result<Outcome, Error> {
    let to = Duration::from_secs(cfg.timeout_secs);
    loop {
        tokio::select! {
            biased;

            // Graceful shutdown: try to close politely, then exit the loop.
            _ = shutdown.cancelled() => {
                let _ = write.send(Message::Close(None)).await;
                return Ok(Outcome::Shutdown);
            }

            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        forward(Bytes::copy_from_slice(text.as_bytes()), tx, metrics).await?;
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        forward(Bytes::copy_from_slice(&bin), tx, metrics).await?;
                    }
                    // Ping/Pong are answered by tungstenite automatically.
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        tracing::warn!(?frame, "server closed connection");
                        return Err(Error::Transient("server closed connection".into()));
                    }
                    Some(Ok(other)) => tracing::debug!(?other, "ignoring frame"),
                    Some(Err(e)) => return Err(Error::Transient(format!("ws recv: {e}"))),
                    None => return Err(Error::Transient("stream ended".into())),
                }
            }

            // Idle timeout — reset on every received message above.
            _ = tokio::time::sleep(to) => {
                let _ = write.send(Message::Close(None)).await;
                return Err(Error::Transient(format!("no message in {}s", cfg.timeout_secs)));
            }
        }
    }
}

/// Count a received frame and hand it to the producer task. A closed channel
/// means the producer task is gone — fatal.
async fn forward(
    bytes: Bytes,
    tx: &mpsc::Sender<Bytes>,
    metrics: &Arc<Metrics>,
) -> Result<(), Error> {
    metrics.messages_received.fetch_add(1, Ordering::Relaxed);
    metrics.touch_last_message();
    tx.send(bytes)
        .await
        .map_err(|_| Error::Fatal("producer channel closed".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse a single Alpaca control object (as `next_control` does per element).
    fn parse(s: &str) -> ControlFrame {
        serde_json::from_str(s).expect("valid control frame json")
    }

    #[test]
    fn connected_greeting_matches() {
        let f = parse(r#"{"T":"success","msg":"connected"}"#);
        assert_eq!(
            classify_frame(&f, "success", Some("connected")),
            FrameClass::Match
        );
    }

    #[test]
    fn authenticated_matches() {
        let f = parse(r#"{"T":"success","msg":"authenticated"}"#);
        assert_eq!(
            classify_frame(&f, "success", Some("authenticated")),
            FrameClass::Match
        );
    }

    #[test]
    fn subscription_matches_when_msg_not_required() {
        let f = parse(r#"{"T":"subscription","bars":["AAPL","TSLA"]}"#);
        assert_eq!(classify_frame(&f, "subscription", None), FrameClass::Match);
    }

    #[test]
    fn error_frame_is_fatal_with_code_and_msg() {
        let f = parse(r#"{"T":"error","code":402,"msg":"auth failed"}"#);
        match classify_frame(&f, "success", Some("authenticated")) {
            FrameClass::Fatal(m) => {
                assert!(m.contains("402"), "got: {m}");
                assert!(m.contains("auth failed"), "got: {m}");
            }
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    #[test]
    fn error_frame_without_code_defaults_to_minus_one() {
        let f = parse(r#"{"T":"error","msg":"bad"}"#);
        match classify_frame(&f, "success", None) {
            FrameClass::Fatal(m) => assert!(m.contains("-1"), "got: {m}"),
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    #[test]
    fn wrong_type_is_mismatch() {
        let f = parse(r#"{"T":"subscription"}"#);
        assert!(matches!(
            classify_frame(&f, "success", Some("authenticated")),
            FrameClass::Mismatch(_)
        ));
    }

    #[test]
    fn right_type_wrong_msg_is_mismatch() {
        let f = parse(r#"{"T":"success","msg":"connected"}"#);
        assert!(matches!(
            classify_frame(&f, "success", Some("authenticated")),
            FrameClass::Mismatch(_)
        ));
    }

    #[test]
    fn data_bar_during_handshake_is_mismatch() {
        // A bar frame (T="b", extra fields) where a control frame was expected.
        let f = parse(r#"{"T":"b","S":"AAPL","o":1.0,"c":1.5}"#);
        assert!(matches!(
            classify_frame(&f, "success", Some("connected")),
            FrameClass::Mismatch(_)
        ));
    }
}
