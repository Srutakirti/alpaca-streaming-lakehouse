use anyhow::{Context, Result, anyhow};
use std::fmt;

#[derive(Clone)]
pub struct Config {
    pub alpaca_key: String,
    pub alpaca_secret: String,
    pub ws_uri: String,
    pub symbols: Vec<String>,
    pub kafka_broker: String,
    pub kafka_topic: String,
    pub metrics_interval_secs: u64,
    pub max_retries: u32,
    pub timeout_secs: u64,
    pub backoff_max_secs: u64,
    pub channel_capacity: usize,
    pub max_inflight: usize,
    pub component: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            alpaca_key: req("ALPACA_KEY")?,
            alpaca_secret: req("ALPACA_SECRET")?,
            ws_uri: opt("ALPACA_WS_URI", "wss://stream.data.alpaca.markets/v2/iex"),
            symbols: opt("ALPACA_SYMBOLS", "*")
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            kafka_broker: opt("KAFKA_BROKER", "localhost:9092"),
            kafka_topic: opt("KAFKA_TOPIC", "alpaca-bars"),
            metrics_interval_secs: opt_parse("METRICS_INTERVAL", 10)?,
            max_retries: opt_parse("MAX_RETRIES", 5)?,
            timeout_secs: opt_parse("TIMEOUT", 120)?,
            backoff_max_secs: opt_parse("BACKOFF_MAX", 120)?,
            channel_capacity: opt_parse("CHANNEL_CAPACITY", 1024)?,
            max_inflight: opt_parse("MAX_INFLIGHT", 512)?,
            component: opt("COMPONENT", "alpaca-extractor-rs"),
        })
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("alpaca_key", &redact(&self.alpaca_key))
            .field("alpaca_secret", &"<redacted>")
            .field("ws_uri", &self.ws_uri)
            .field("symbols", &self.symbols)
            .field("kafka_broker", &self.kafka_broker)
            .field("kafka_topic", &self.kafka_topic)
            .field("metrics_interval_secs", &self.metrics_interval_secs)
            .field("max_retries", &self.max_retries)
            .field("timeout_secs", &self.timeout_secs)
            .field("backoff_max_secs", &self.backoff_max_secs)
            .field("channel_capacity", &self.channel_capacity)
            .field("max_inflight", &self.max_inflight)
            .field("component", &self.component)
            .finish()
    }
}

fn redact(s: &str) -> String {
    if s.len() <= 4 {
        "<redacted>".to_string()
    } else {
        format!("{}…<redacted>", &s[..4])
    }
}

fn req(name: &str) -> Result<String> {
    std::env::var(name).map_err(|_| anyhow!("missing env var {name}"))
}

fn opt(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn opt_parse<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(v) => v
            .parse::<T>()
            .map_err(|e| anyhow!("invalid {name}: {e}"))
            .with_context(|| format!("parsing env var {name}")),
        Err(_) => Ok(default),
    }
}
