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
    /// Display label only — NOT part of a point's identity (see [`PointIdentity`]),
    /// so renaming it never orphans a point's poll/status history. May be spelled
    /// `device_label` in config.
    #[serde(default, alias = "device_label")]
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

    /// Human-friendly device label. Alias for [`PointConfig::device_key`]; config
    /// may spell the field `device_label`, and this accessor returns the same value.
    pub fn device_label(&self) -> &str {
        &self.device_key
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
    ///
    /// For BACnet this is derived from the device object's `OBJECT_NAME` (via
    /// `proto_api::base_key`) when available, so it need not encode the device
    /// instance; use [`DiscoveredDevice::instance`] for instance-based
    /// addressing (browse/refresh) rather than parsing the key.
    pub key: String,
    /// Protocol-native numeric device instance, when the discovery protocol has
    /// one (e.g. the BACnet device instance). Carried alongside the key so
    /// browse/refresh can resolve the device even when the key is a friendly
    /// name that no longer encodes the instance.
    pub instance: Option<u32>,
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

#[derive(Debug, Clone, PartialEq, Default)]
pub struct BrowseOutcome {
    pub points: Vec<DiscoveredPoint>,
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
    /// Samples handed to the outbound channel this cycle (local enqueue attempts).
    pub queued: usize,
    /// Samples accepted into the outbound channel this cycle — a *local* enqueue
    /// success, NOT proof the broker received or accepted them. For real delivery
    /// see [`PublishStats::acked`].
    pub published: usize,
    /// Broker-confirmed deliveries (running total of QoS 1 PubAcks seen on the
    /// connection). This is the honest "delivered" count: it stays flat when the
    /// broker is unreachable or rejects auth even while `published` keeps climbing
    /// as samples pile into the local channel.
    pub acked: usize,
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

/// Identity used to dedupe points and to key poll/status history: the point's
/// protocol addressing (`device_instance`, `object_type`, `object_instance`,
/// `property`, …) only. Deliberately independent of `device_key` so renaming a
/// device's human-friendly label does not orphan a point's poll/status history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointIdentity {
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
        Self { addressing }
    }
}

impl Hash for PointIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
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

/// Result of a BACnet (or protocol-specific) device-table refresh.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RefreshOutcome {
    pub resolved: Vec<u32>,
    pub unresolved: Vec<u32>,
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
    crate::defaults::POLL_INTERVAL_SECS
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use std::collections::HashSet;

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
    fn point_identity_is_stable_across_device_key_rename() {
        // Same addressing, different (renamed) device_key -> identity unchanged,
        // so poll/status history keyed on PointIdentity is not orphaned.
        let addr = &[
            ("device_instance", serde_json::json!(12)),
            ("object_type", serde_json::json!("analogInput")),
            ("object_instance", serde_json::json!(3)),
            ("property", serde_json::json!("presentValue")),
        ];
        let before = point("ahu-12", addr);
        let after = point("ahu-12-renamed", addr);

        let id_before = PointIdentity::from_point(&before);
        let id_after = PointIdentity::from_point(&after);
        assert_eq!(id_before, id_after);

        // Hash equality too (identity is used as a HashMap key).
        let mut set = HashSet::new();
        set.insert(id_before);
        assert!(set.contains(&id_after));
    }

    #[test]
    fn point_identity_differs_across_distinct_addressing() {
        // Identical device_key but distinct addressing -> distinct identities.
        let a = point(
            "ahu-12",
            &[
                ("object_type", serde_json::json!("analogInput")),
                ("object_instance", serde_json::json!(3)),
            ],
        );
        let b = point(
            "ahu-12",
            &[
                ("object_type", serde_json::json!("analogInput")),
                ("object_instance", serde_json::json!(4)),
            ],
        );
        assert_ne!(PointIdentity::from_point(&a), PointIdentity::from_point(&b));
    }

    #[test]
    fn device_label_alias_deserializes_and_accessor_matches() {
        // Config may spell the field `device_label`.
        let cfg: PointConfig =
            serde_json::from_str(r#"{"device_label":"boiler-3"}"#).expect("device_label alias");
        assert_eq!(cfg.device_key, "boiler-3");
        assert_eq!(cfg.device_label(), "boiler-3");
    }

    #[test]
    fn addressing_summary_is_sorted_key_value_pairs() {
        let cfg = point(
            "dev",
            &[
                ("object_type", serde_json::json!("analogInput")),
                ("object_instance", serde_json::json!(3)),
                ("device_instance", serde_json::json!(12)),
            ],
        );
        // Addressing iterates in sorted key order; strings render without quotes.
        assert_eq!(
            cfg.addressing_summary(),
            "device_instance=12 object_instance=3 object_type=analogInput"
        );
    }

    #[test]
    fn display_name_uses_device_key_when_present() {
        let cfg = point("ahu-12", &[("object_instance", serde_json::json!(3))]);
        assert_eq!(cfg.display_name(), "ahu-12 [object_instance=3]");
    }

    #[test]
    fn display_name_falls_back_to_placeholder_when_key_blank_or_whitespace() {
        // Empty device_key -> "(device)" placeholder.
        let blank = point("", &[("object_instance", serde_json::json!(3))]);
        assert_eq!(blank.display_name(), "(device) [object_instance=3]");
        // Whitespace-only device_key also treated as blank.
        let ws = point("   ", &[("object_instance", serde_json::json!(3))]);
        assert_eq!(ws.display_name(), "(device) [object_instance=3]");
    }

    #[test]
    fn telemetry_value_display_formats() {
        // Numbers render with 3 decimal places; text renders verbatim.
        assert_eq!(TelemetryValue::Number(1.5).to_string(), "1.500");
        assert_eq!(TelemetryValue::Number(-0.1).to_string(), "-0.100");
        assert_eq!(TelemetryValue::Text("active".into()).to_string(), "active");
    }

    #[test]
    fn point_status_publish_success_and_failure() {
        let mut status = PointStatus::default();
        status.record_publish_failure("broker down");
        assert_eq!(status.last_publish_error.as_deref(), Some("broker down"));
        // A read failure does not clear the publish error.
        status.record_read_failure("timeout");
        assert_eq!(status.last_publish_error.as_deref(), Some("broker down"));
        // A publish success clears it.
        status.record_publish_success();
        assert_eq!(status.last_publish_error, None);
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
