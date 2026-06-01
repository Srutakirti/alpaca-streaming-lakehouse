#![allow(dead_code)]

mod config;
mod kafka;
mod metrics;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream};
use tokio_tungstenite::MaybeTlsStream;
use tokio::net::TcpStream;
use futures_util::stream::{SplitStream,SplitSink};
use tokio::signal;
use serde_json::Value;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWriter = SplitSink<WsStream, Message>;
type WsReader = SplitStream<WsStream>;

use std::time::Duration;
use tokio::time::timeout;

async fn expect_message_timeout<S>(
    read: &mut S,
    expected_type: &str,
    expected_msg: Option<&str>,
    duration: Duration,
) -> Result<Value, Box<dyn std::error::Error>>
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    match timeout(duration, expect_message(read, expected_type, expected_msg)).await {
        Ok(result) => result,
        Err(_) => Err(format!("timed out waiting for '{}'", expected_type).into()),
    }
}

async fn expect_message<S>(
    read: &mut S,
    expected_type: &str,
    expected_msg: Option<&str>,
) -> Result<Value,Box<dyn std::error::Error>>
where S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
        let msg = read.next().await
        .ok_or("stream ended before expected message")??;

    let text = match msg {
        Message::Text(t) => t,
        other => return Err(format!("expected text frame, got {:?}", other).into()),
    };

    // Alpaca sends arrays of events per frame
    let arr: Vec<Value> = serde_json::from_str(&text)?;
    let first = arr.into_iter().next()
        .ok_or("empty message array")?;

    let t = first.get("T").and_then(|v| v.as_str())
        .ok_or("missing 'T' field")?;

    if t != expected_type {
        // Surface error messages from the server
        if t == "error" {
            let code = first.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
            let m = first.get("msg").and_then(|v| v.as_str()).unwrap_or("");
            return Err(format!("server error {}: {}", code, m).into());
        }
        return Err(format!("expected T={}, got T={}", expected_type, t).into());
    }

    if let Some(expected) = expected_msg {
        let got = first.get("msg").and_then(|v| v.as_str()).unwrap_or("");
        if got != expected {
            return Err(format!("expected msg='{}', got msg='{}'", expected, got).into());
        }
    }

    Ok(first)
}


async fn setup_ws() -> Result<(WsWriter, WsReader), Box<dyn std::error::Error>> {
    let ws_url = "wss://stream.data.alpaca.markets/v2/iex";
    let alpaca_key = std::env::var("ALPACA_KEY")?;
    let alpaca_secret = std::env::var("ALPACA_SECRET")?;
    let auth_message = format!(
    r#"{{"action":"auth","key":"{}","secret":"{}"}}"#,
    alpaca_key, alpaca_secret
);
    let subscribe_message = r#"{"action":"subscribe","bars":["*"]}"#;

    let (ws_stream,_) = connect_async(ws_url).await?;

    let(mut write, mut read) = ws_stream.split();

    // 1. Initial "connected" greeting (server sends this on its own)
    //checking that the first message is success and connected
    //[{"T":"success","msg":"connected"}]
    //Type is T
    expect_message_timeout(&mut read, "success", Some("connected"),Duration::from_secs(120),).await?;
    println!("✓ connected");

    // 2. Send auth, expect "authenticated"
    //[{"T":"success","msg":"authenticated"}]
    write.send(Message::Text(auth_message.clone().into())).await?;
    expect_message_timeout(&mut read, "success", Some("authenticated"),Duration::from_secs(120),).await?;
    println!("✓ authenticated");

    // 3. Send subscribe, expect "subscription" confirmation
    // [{"T":"subscription","bars":["*"]}]
    write.send(Message::Text(subscribe_message.into())).await?;
    let sub = expect_message_timeout(&mut read, "subscription", None,Duration::from_secs(120),).await?;
    println!("✓ subscribed: {}", sub);

    Ok((write, read))


}

#[derive(Debug)]
enum ExitReason {
   ServerClosed,
   Error, 
   StreamEnded,
   Timeout,
   CtrlC 
}

async fn msg_read_loop(read:&mut WsReader , 
    write:&mut  WsWriter, ) -> Result<ExitReason,Box<dyn std::error::Error>> 

{
loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => println!("{}", text),
                    Some(Ok(Message::Binary(bin))) => println!("<binary {} bytes>", bin.len()),
                    Some(Ok(Message::Close(frame))) => {
                        println!("Server closed connection: {:?}", frame);
                        break Ok(ExitReason::ServerClosed);
                    }
                    Some(Ok(other)) => { println!("(frame: {:?})", other);} // Ping/Pong handled automatically
                    Some(Err(e)) => {
                        eprintln!("Error: {}", e);
                        break Ok(ExitReason::Error);
                    }
                    None => {
                        println!("Stream ended");
                        break Ok(ExitReason::StreamEnded);
                    }
                }
            }

            _ = tokio::time::sleep(Duration::from_secs(120)) => {
            eprintln!("No message received in 120s, terminating");
            write.send(Message::Close(None)).await?;
            break Ok(ExitReason::Timeout);
            }

            _ = signal::ctrl_c() => {
                println!("\nShutting down...");
                write.send(Message::Close(None)).await?;
                break Ok(ExitReason::CtrlC);
            }
        }
    }
}

async fn retry_loop(x: i32) -> Result<(),Box<dyn std::error::Error>> {
    for _ in 0..x {

        let (mut write , mut read) = setup_ws().await?;
        
        let attempt = msg_read_loop(&mut read,&mut write).await?;

        match attempt  {
            ExitReason::Timeout | ExitReason::Error=> continue,
            ExitReason::CtrlC => return Ok(()),
            other => return Err(format!("exited unexpectedly: {:?}", other).into()),
        }


    }
    Err("ALl retries done".into())
}

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

    // Checkpoint 5 smoke: push 10 "hello" payloads through the producer task
    // and verify they land in Kafka. Replaced in Checkpoint 7.
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    kafka::ensure_topic(&cfg).await?;
    let m = metrics::Metrics::new(cfg.component.clone());
    let shutdown = CancellationToken::new();
    let producer = kafka::build_producer(&cfg)?;
    kafka::warm_metadata(&producer, &cfg.kafka_topic)?;
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(cfg.channel_capacity);
    let emitter = metrics::emitter(
        m.clone(),
        Duration::from_secs(cfg.metrics_interval_secs),
        Some(tx.clone()),
        shutdown.clone(),
    );
    let prod_task = kafka::producer_task(
        producer.clone(),
        cfg.kafka_topic.clone(),
        cfg.clone(),
        m.clone(),
        rx,
        shutdown.clone(),
    );
    let total: u64 = std::env::var("WSR_SMOKE_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let delay_ms: u64 = std::env::var("WSR_SMOKE_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    for i in 0..total {
        let payload = bytes::Bytes::from(format!(r#"[{{"T":"hello","n":{i}}}]"#));
        tx.send(payload).await?;
        m.messages_received
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        m.touch_last_message();
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }
    drop(tx);
    // Give the producer task a moment to drain naturally before forcing shutdown.
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    let _ = tokio::join!(prod_task, emitter);
    use rdkafka::producer::Producer;
    producer.flush(Duration::from_secs(10))?;
    tracing::info!(snapshot = %m.snapshot(), "final metrics");
    Ok(())
}
