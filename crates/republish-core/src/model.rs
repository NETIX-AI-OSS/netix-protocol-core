//! Protocol-neutral data model for the republisher: configured points, discovered
//! devices/points, poll samples, and per-point status.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use proto_api::Addressing;

/// A configured point to poll and republish. Protocol-specific addressing lives
/// in [`PointConfig::addressing`], rendered/edited from the active protocol's
/// `addressing_fields` capabilities.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PointConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Human-friendly device/endpoint label (also used in the default topic).
    #[serde(default)]
    pub device_key: String,
    /// Protocol-native address (e.g. `{object_type, object_instance, property}`,
    /// `{table, address, datatype}`, or `{node_id}`).
    #[serde(default)]
    pub addressing: Addressing,
    /// Explicit MQTT tag path; when empty a default is derived from `device_key`.
    #[serde(default)]
    pub tag_path: String,
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
}

impl Default for PointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            device_key: String::new(),
            addressing: Addressing::new(),
            tag_path: String::new(),
            poll_interval_secs: default_poll_interval_secs(),
        }
    }
}

impl PointConfig {
    /// Compact, human display of the addressing (sorted key=value pairs).
    pub fn addressing_summary(&self) -> String {
        self.addressing
            .iter()
            .map(|(k, v)| format!("{k}={}", json_scalar(v)))
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub fn display_name(&self) -> String {
        let key = if self.device_key.trim().is_empty() {
            "(device)"
        } else {
            self.device_key.as_str()
        };
        format!("{key} [{}]", self.addressing_summary())
    }
}

/// A device/server found by discovery (or entered manually).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredDevice {
    /// Stable, human-friendly key used as `PointConfig::device_key`.
    pub key: String,
    /// Network address (e.g. `192.168.1.10:502`, `opc.tcp://host:4840`).
    pub address: String,
    /// Free-form detail line for the UI (vendor, model, instance, …).
    pub detail: String,
}

/// A point found by browsing a device.
#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveredPoint {
    pub device_key: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub units: Option<String>,
    pub value: Option<TelemetryValue>,
    /// Protocol-native addressing to copy into a [`PointConfig`].
    pub addressing: Addressing,
    /// Suggested MQTT tag path (used to prefill the point editor).
    pub suggested_tag_path: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PointSample {
    pub point: PointConfig,
    pub value: TelemetryValue,
    pub topic: String,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PointFailure {
    pub point: PointConfig,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct PollOutcome {
    pub samples: Vec<PointSample>,
    pub failures: Vec<PointFailure>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DiscoverOutcome {
    pub devices: Vec<DiscoveredDevice>,
    pub warnings: Vec<String>,
}

/// A scalar telemetry value: numeric or text (booleans/enums become text).
#[derive(Debug, Clone, PartialEq)]
pub enum TelemetryValue {
    Number(f64),
    Text(String),
}

impl TelemetryValue {
    pub fn as_json_value(&self) -> serde_json::Value {
        match self {
            Self::Number(value) => serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Self::Text(value) => serde_json::Value::String(value.clone()),
        }
    }
}

impl fmt::Display for TelemetryValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(value) => write!(formatter, "{value:.3}"),
            Self::Text(value) => formatter.write_str(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublishStats {
    pub queued: usize,
    pub published: usize,
    pub failed: usize,
    pub reconnects: usize,
    pub last_error: Option<String>,
}

impl PublishStats {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn record_failure(&mut self, error: impl Into<String>) {
        self.failed += 1;
        self.last_error = Some(error.into());
    }
}

/// Identity used to dedupe points across imports: device key + addressing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointIdentity {
    pub device_key: String,
    pub addressing: Vec<(String, String)>,
}

impl PointIdentity {
    pub fn from_point(point: &PointConfig) -> Self {
        let mut addressing: Vec<(String, String)> = point
            .addressing
            .iter()
            .map(|(k, v)| (k.clone(), json_scalar(v)))
            .collect();
        addressing.sort();
        Self {
            device_key: point.device_key.trim().to_ascii_lowercase(),
            addressing,
        }
    }
}

impl Hash for PointIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.device_key.hash(state);
        self.addressing.hash(state);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PointStatus {
    pub last_value: Option<TelemetryValue>,
    pub last_sample_ms: Option<i64>,
    pub stale: bool,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    pub last_publish_error: Option<String>,
}

impl Default for PointStatus {
    fn default() -> Self {
        Self {
            last_value: None,
            last_sample_ms: None,
            stale: true,
            consecutive_failures: 0,
            last_error: None,
            last_publish_error: None,
        }
    }
}

impl PointStatus {
    pub fn record_sample(&mut self, sample: &PointSample) {
        self.last_value = Some(sample.value.clone());
        self.last_sample_ms = Some(sample.timestamp_ms);
        self.stale = false;
        self.consecutive_failures = 0;
        self.last_error = None;
    }

    pub fn record_read_failure(&mut self, error: impl Into<String>) {
        self.stale = true;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.last_error = Some(error.into());
    }

    pub fn record_publish_success(&mut self) {
        self.last_publish_error = None;
    }

    pub fn record_publish_failure(&mut self, error: impl Into<String>) {
        self.last_publish_error = Some(error.into());
    }
}

/// Render a JSON scalar compactly (no quotes for strings) for display/identity.
pub fn json_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub fn default_true() -> bool {
    true
}

pub fn default_poll_interval_secs() -> u64 {
    10
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(device: &str, addr: &[(&str, serde_json::Value)]) -> PointConfig {
        let mut addressing = Addressing::new();
        for (k, v) in addr {
            addressing.insert((*k).to_string(), v.clone());
        }
        PointConfig {
            device_key: device.to_string(),
            addressing,
            ..PointConfig::default()
        }
    }

    #[test]
    fn telemetry_value_json_encoding() {
        assert_eq!(
            TelemetryValue::Number(12.5).as_json_value(),
            serde_json::json!(12.5)
        );
        assert_eq!(
            TelemetryValue::Text("active".into()).as_json_value(),
            serde_json::json!("active")
        );
    }

    #[test]
    fn point_identity_is_order_independent() {
        let a = point(
            "dev",
            &[("b", serde_json::json!(2)), ("a", serde_json::json!(1))],
        );
        let b = point(
            "DEV",
            &[("a", serde_json::json!(1)), ("b", serde_json::json!(2))],
        );
        assert_eq!(PointIdentity::from_point(&a), PointIdentity::from_point(&b));
    }

    #[test]
    fn point_status_lifecycle() {
        let mut status = PointStatus::default();
        assert!(status.stale);
        status.record_read_failure("timeout");
        assert_eq!(status.consecutive_failures, 1);
        let sample = PointSample {
            point: PointConfig::default(),
            value: TelemetryValue::Number(1.0),
            topic: "t".into(),
            timestamp_ms: 1,
        };
        status.record_sample(&sample);
        assert!(!status.stale);
        assert_eq!(status.consecutive_failures, 0);
    }
}
