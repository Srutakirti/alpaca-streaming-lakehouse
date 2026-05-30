#![allow(dead_code)]

mod config;

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

    // TODO(checkpoint-7): wire ws + kafka tasks. Smoke check only for now.
    Ok(())
}
