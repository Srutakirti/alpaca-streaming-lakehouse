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
    pub data_idle_timeout_secs: u64,
    pub backoff_max_secs: u64,
    pub channel_capacity: usize,
    pub max_inflight: usize,
    pub component: String,
}

impl Config {
    /// Build from the process environment.
    pub fn from_env() -> Result<Self> {
        Self::from_getter(&|name| std::env::var(name).ok())
    }

    /// Build from an arbitrary env getter. All process-global env access is
    /// confined to the single closure in `from_env`, so the parsing/default
    /// logic here is pure and unit-testable without mutating the environment.
    fn from_getter(get: &dyn Fn(&str) -> Option<String>) -> Result<Self> {
        Ok(Self {
            alpaca_key: req(get, "ALPACA_KEY")?,
            alpaca_secret: req(get, "ALPACA_SECRET")?,
            ws_uri: opt(
                get,
                "ALPACA_WS_URI",
                "wss://stream.data.alpaca.markets/v2/iex",
            ),
            symbols: opt(get, "ALPACA_SYMBOLS", "*")
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            kafka_broker: opt(get, "KAFKA_BROKER", "localhost:9092"),
            kafka_topic: opt(get, "KAFKA_TOPIC", "alpaca-bars"),
            metrics_interval_secs: opt_parse(get, "METRICS_INTERVAL", 10)?,
            max_retries: opt_parse(get, "MAX_RETRIES", 5)?,
            timeout_secs: opt_parse(get, "TIMEOUT", 120)?,
            // No *bar* data within this window (ping/pong don't count) → the
            // producer exits so it stops holding the single Alpaca connection.
            // 0 disables the watchdog. Default 600s (10 min): liquid symbols
            // emit 1-min bars continuously during market hours, so a 10-min gap
            // means the market is closed (or the feed is dead).
            data_idle_timeout_secs: opt_parse(get, "DATA_IDLE_TIMEOUT", 600)?,
            backoff_max_secs: opt_parse(get, "BACKOFF_MAX", 120)?,
            channel_capacity: opt_parse(get, "CHANNEL_CAPACITY", 1024)?,
            max_inflight: opt_parse(get, "MAX_INFLIGHT", 512)?,
            component: opt(get, "COMPONENT", "alpaca-extractor-rs"),
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
            .field("data_idle_timeout_secs", &self.data_idle_timeout_secs)
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

fn req(get: &dyn Fn(&str) -> Option<String>, name: &str) -> Result<String> {
    get(name).ok_or_else(|| anyhow!("missing env var {name}"))
}

fn opt(get: &dyn Fn(&str) -> Option<String>, name: &str, default: &str) -> String {
    get(name).unwrap_or_else(|| default.to_string())
}

fn opt_parse<T>(get: &dyn Fn(&str) -> Option<String>, name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match get(name) {
        Some(v) => v
            .parse::<T>()
            .map_err(|e| anyhow!("invalid {name}: {e}"))
            .with_context(|| format!("parsing env var {name}")),
        None => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Build a Config from a fixed key/value map (no process env involved).
    fn cfg_from(pairs: &[(&str, &str)]) -> Result<Config> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_getter(&|name| map.get(name).cloned())
    }

    /// The two required vars plus any extras, for the happy-path cases.
    fn with_creds(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
        let mut pairs = vec![
            ("ALPACA_KEY", "PKTEST1234"),
            ("ALPACA_SECRET", "topsecretvalue"),
        ];
        pairs.extend_from_slice(extra);
        pairs
    }

    #[test]
    fn defaults_apply_when_only_required_present() {
        let cfg = cfg_from(&with_creds(&[])).unwrap();
        assert_eq!(cfg.ws_uri, "wss://stream.data.alpaca.markets/v2/iex");
        assert_eq!(cfg.kafka_broker, "localhost:9092");
        assert_eq!(cfg.kafka_topic, "alpaca-bars");
        assert_eq!(cfg.metrics_interval_secs, 10);
        assert_eq!(cfg.max_retries, 5);
        assert_eq!(cfg.timeout_secs, 120);
        assert_eq!(cfg.data_idle_timeout_secs, 600);
        assert_eq!(cfg.backoff_max_secs, 120);
        assert_eq!(cfg.channel_capacity, 1024);
        assert_eq!(cfg.max_inflight, 512);
        assert_eq!(cfg.component, "alpaca-extractor-rs");
        assert_eq!(cfg.symbols, vec!["*".to_string()]);
    }

    #[test]
    fn missing_required_var_errors_with_name() {
        // ALPACA_SECRET absent.
        let err = cfg_from(&[("ALPACA_KEY", "PKTEST1234")]).unwrap_err();
        assert!(err.to_string().contains("ALPACA_SECRET"), "got: {err}");
    }

    #[test]
    fn symbols_are_split_trimmed_and_emptied() {
        let cfg = cfg_from(&with_creds(&[("ALPACA_SYMBOLS", "AAPL, TSLA , ,NVDA")])).unwrap();
        assert_eq!(cfg.symbols, vec!["AAPL", "TSLA", "NVDA"]);
    }

    #[test]
    fn overrides_replace_defaults() {
        let cfg = cfg_from(&with_creds(&[
            ("KAFKA_BROKER", "10.0.0.5:9092"),
            ("KAFKA_TOPIC", "bars-test"),
            ("MAX_RETRIES", "9"),
            ("COMPONENT", "wsr-test"),
        ]))
        .unwrap();
        assert_eq!(cfg.kafka_broker, "10.0.0.5:9092");
        assert_eq!(cfg.kafka_topic, "bars-test");
        assert_eq!(cfg.max_retries, 9);
        assert_eq!(cfg.component, "wsr-test");
    }

    #[test]
    fn invalid_numeric_var_errors_with_name() {
        let err = cfg_from(&with_creds(&[("METRICS_INTERVAL", "not-a-number")])).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("METRICS_INTERVAL"), "got: {msg}");
    }

    #[test]
    fn redact_truncates_and_masks() {
        assert_eq!(redact("abc"), "<redacted>"); // <= 4 chars fully masked
        assert_eq!(redact("PKCP1234"), "PKCP…<redacted>");
    }

    #[test]
    fn debug_never_leaks_secrets() {
        let cfg = cfg_from(&with_creds(&[])).unwrap();
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("<redacted>"));
        assert!(!dbg.contains("topsecretvalue"), "secret leaked: {dbg}");
        assert!(!dbg.contains("PKTEST1234"), "full key leaked: {dbg}");
        assert!(dbg.contains("PKTE")); // key truncated to first 4
    }
}
