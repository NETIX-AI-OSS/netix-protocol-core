//! Republisher configuration (TOML): selected protocol + per-protocol connection
//! settings, MQTT/TLS target, configured points, and UI preferences.

use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use proto_api::Addressing;

use crate::model::{default_true, PointConfig};
use crate::topic::{telemetry_topic, validate_publish_topic};

const CONFIG_FILE_NAME: &str = "config.toml";
pub const CURRENT_CONFIG_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    #[serde(default = "current_version")]
    pub version: u32,
    /// Selected protocol id (registry key). Empty until the user picks one.
    #[serde(default)]
    pub protocol: String,
    /// Per-protocol connection settings, keyed by protocol id.
    #[serde(default)]
    pub connections: BTreeMap<String, Addressing>,
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub points: Vec<PointConfig>,
    /// Run-from-discovery: when `true` and no points are enabled, the republisher
    /// discovers devices, browses their points, and polls the discovered set
    /// (built in memory), so `config.toml` collapses to connection-only for a
    /// self-describing protocol like BACnet. When `false` (default), a start with
    /// no enabled points fails loud instead of spinning forever publishing
    /// nothing — see [`crate::worker::spawn_republisher`].
    #[serde(default)]
    pub discover_on_start: bool,
    /// Provenance marker for a config emitted by the simulator: the SHA-256 hex
    /// of the canonical simulator config bytes it was generated from (see
    /// `sim-core::republisher_export`). Absent for hand-written or GUI-authored
    /// configs. When present it lets a loader detect *drift* — the simulator
    /// config changed but this republisher config was never regenerated, so its
    /// addresses are stale — via [`AppConfig::check_sim_config_drift`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sim_config_checksum: Option<String>,
    #[serde(default)]
    pub ui: UiPreferences,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MqttConfig {
    #[serde(default = "default_mqtt_host")]
    pub host: String,
    #[serde(default = "default_mqtt_port")]
    pub port: u16,
    #[serde(default = "default_true")]
    pub use_tls: bool,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
    #[serde(default = "default_health_topic")]
    pub health_topic: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// Name of an environment variable that holds the MQTT password. When set
    /// and the variable is present at load time, the password is read from the
    /// environment (env wins) and the secret itself is **never** written to the
    /// config file — only this variable *name* is persisted. This is the
    /// supported way to keep the broker secret out of plaintext on disk.
    #[serde(default)]
    pub password_env: Option<String>,
    #[serde(default)]
    pub ca_cert_path: Option<String>,
    #[serde(default)]
    pub client_cert_path: Option<String>,
    #[serde(default)]
    pub client_key_path: Option<String>,
    #[serde(default)]
    pub client_key_passphrase: Option<String>,
    #[serde(default)]
    pub remember_secrets: bool,
    #[serde(default)]
    pub retain: bool,
    #[serde(default = "default_keep_alive_secs")]
    pub keep_alive_secs: u64,
    /// How telemetry is serialised onto MQTT (default: bare scalar per point).
    #[serde(default)]
    pub payload_format: PayloadFormat,
    /// Topic prefix for `netix_envelope` publishes; the leading slash is
    /// preserved so `/Netix/Sim/Device/<id>/telemetry` matches a subscription
    /// on `/Netix/Sim/Device/#`.
    #[serde(default = "default_device_topic_prefix")]
    pub device_topic_prefix: String,
    /// Start republishing automatically on launch (no manual "Start" click).
    #[serde(default)]
    pub autostart: bool,
}

/// How telemetry is serialised onto MQTT.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PayloadFormat {
    /// One message per point: the bare JSON scalar value, on the point's tag
    /// topic (`<topic_prefix>/<tag_path>`).
    Scalar,
    /// One message per device: a `{reason,time,id,points:[{pointName,data,status}]}`
    /// envelope on `<device_topic_prefix>/<id>/telemetry`, where `id` is the
    /// point's `device_key` and `pointName` is its `tag_path`. This is the
    /// pipeline default (see [`crate::defaults::PAYLOAD_FORMAT`]).
    #[default]
    NetixEnvelope,
}

impl PayloadFormat {
    /// All variants, in menu order, for the settings picker.
    pub const ALL: [Self; 2] = [Self::Scalar, Self::NetixEnvelope];

    /// The config/TOML token for this format (matches the serde `snake_case`
    /// rename). Used by the simulator's emit so the emitted string and the
    /// deserialised value can never drift apart.
    pub fn as_config_token(&self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::NetixEnvelope => "netix_envelope",
        }
    }
}

impl std::fmt::Display for PayloadFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            Self::Scalar => "Scalar (value per topic)",
            Self::NetixEnvelope => "Netix envelope (per device)",
        };
        f.write_str(label)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UiTheme {
    Auto,
    Light,
    Dark,
}

impl std::fmt::Display for UiTheme {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => formatter.write_str("Auto"),
            Self::Light => formatter.write_str("Light"),
            Self::Dark => formatter.write_str("Dark"),
        }
    }
}

impl UiTheme {
    pub const ALL: [Self; 3] = [Self::Auto, Self::Light, Self::Dark];
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UiPreferences {
    #[serde(default = "default_ui_theme")]
    pub theme: UiTheme,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            version: CURRENT_CONFIG_VERSION,
            protocol: String::new(),
            connections: BTreeMap::new(),
            mqtt: MqttConfig::default(),
            points: Vec::new(),
            discover_on_start: false,
            sim_config_checksum: None,
            ui: UiPreferences::default(),
        }
    }
}

impl AppConfig {
    /// Connection settings for the active protocol (empty map if unset).
    pub fn connection(&self) -> Addressing {
        self.connections
            .get(&self.protocol)
            .cloned()
            .unwrap_or_default()
    }

    /// Mutable connection settings for the active protocol, created on demand.
    pub fn connection_mut(&mut self) -> &mut Addressing {
        self.connections.entry(self.protocol.clone()).or_default()
    }

    /// Stamps the in-memory config with the current version and applies upgrades.
    pub fn migrate(&mut self) {
        if self.version < 2 {
            if let Some(conn) = self.connections.get_mut("bacnet") {
                let legacy_port = conn
                    .get("port")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(47808);
                if legacy_port == 47808 {
                    conn.insert("port".into(), serde_json::json!(0));
                }
            }
        }
        self.version = CURRENT_CONFIG_VERSION;
    }

    /// The stored simulator-config provenance checksum, if this config was
    /// emitted by the simulator (see [`sim_config_checksum`](Self::sim_config_checksum)).
    pub fn sim_config_checksum(&self) -> Option<&str> {
        self.sim_config_checksum.as_deref()
    }

    /// Detect simulator-config drift: compare the stored
    /// [`sim_config_checksum`](Self::sim_config_checksum) (the simulator config
    /// this republisher config was emitted from) against `current_checksum`
    /// (the checksum of the simulator config in effect now). On mismatch this
    /// logs a `Warning` and returns the message; the caller decides what to do.
    ///
    /// This is intentionally **non-fatal**: it returns `None` (and logs nothing)
    /// when no checksum is stored — a hand-written or GUI-authored config has no
    /// simulator provenance to drift from — and when the checksums match. A
    /// mismatch means the simulator config changed but this config was never
    /// regenerated, so its addresses may be stale; regenerate to resync.
    pub fn check_sim_config_drift(&self, current_checksum: &str) -> Option<String> {
        let stored = self.sim_config_checksum.as_deref()?;
        if stored == current_checksum {
            return None;
        }
        let message = format!(
            "Simulator config drift: this republisher config was emitted from a \
             simulator config with checksum {stored}, but the current simulator \
             config hashes to {current_checksum}. The republisher may be polling \
             stale addresses — regenerate the config (simulator \
             --emit-republisher-config) so BACnet addresses stay in sync."
        );
        log::warn!("{message}");
        Some(message)
    }

    pub fn sanitized_for_save(&self) -> Self {
        let mut clone = self.clone();
        if !clone.mqtt.remember_secrets {
            clone.mqtt.password = None;
            clone.mqtt.client_key_passphrase = None;
        }
        clone
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.mqtt.host.trim().is_empty() {
            return Err("MQTT host cannot be empty".to_string());
        }
        if self.mqtt.port == 0 {
            return Err("MQTT port cannot be 0".to_string());
        }
        if self.mqtt.topic_prefix.trim().is_empty() {
            return Err("MQTT topic prefix cannot be empty".to_string());
        }
        validate_publish_topic(&self.mqtt.health_topic)
            .map_err(|error| format!("MQTT health topic is invalid: {error}"))?;
        let cert_set = self
            .mqtt
            .client_cert_path
            .as_ref()
            .is_some_and(|v| !v.trim().is_empty());
        let key_set = self
            .mqtt
            .client_key_path
            .as_ref()
            .is_some_and(|v| !v.trim().is_empty());
        if cert_set != key_set {
            return Err(
                "MQTT client certificate and client key paths must be configured together"
                    .to_string(),
            );
        }
        let envelope = self.mqtt.payload_format == PayloadFormat::NetixEnvelope;
        if envelope {
            if self.mqtt.device_topic_prefix.trim().is_empty() {
                return Err("MQTT device topic prefix cannot be empty".to_string());
            }
            validate_publish_topic(&crate::topic::device_envelope_topic(&self.mqtt, "sample"))
                .map_err(|error| format!("MQTT device topic is invalid: {error}"))?;
        }
        for point in &self.points {
            if point.enabled && point.poll_interval_secs == 0 {
                return Err(format!(
                    "{} poll interval cannot be 0",
                    point.display_name()
                ));
            }
            // In envelope mode the per-point scalar topic is unused; the device
            // topic is validated above instead.
            if point.enabled && !envelope {
                validate_publish_topic(&telemetry_topic(&self.mqtt, point)).map_err(|error| {
                    format!("{} MQTT topic is invalid: {error}", point.display_name())
                })?;
            }
        }
        Ok(())
    }
}

impl MqttConfig {
    /// If [`password_env`](Self::password_env) names an environment variable
    /// that is set (and non-empty), populate [`password`](Self::password) from
    /// it. The secret is taken from the process environment at load time and is
    /// never written back to disk (env wins; the file stores only the variable
    /// *name*). Returns `true` if a password was resolved from the environment.
    pub fn resolve_password_env(&mut self) -> bool {
        let Some(var) = self.password_env.as_deref().map(str::trim) else {
            return false;
        };
        if var.is_empty() {
            return false;
        }
        match std::env::var(var) {
            Ok(value) if !value.trim().is_empty() => {
                self.password = Some(value);
                true
            }
            _ => false,
        }
    }
}

/// A warning message when a plaintext MQTT secret is (or is about to be) written
/// to the config file — i.e. `remember_secrets = true` and a password or client
/// key passphrase is present. Returns `None` when no plaintext secret is being
/// persisted. Callers log this at save and load time so operators are told to
/// prefer `password_env` (env indirection) over on-disk plaintext.
pub fn plaintext_secret_warning(mqtt: &MqttConfig) -> Option<String> {
    let non_empty = |value: &Option<String>| value.as_deref().is_some_and(|v| !v.trim().is_empty());
    let persists_plaintext = mqtt.remember_secrets
        && (non_empty(&mqtt.password) || non_empty(&mqtt.client_key_passphrase));
    persists_plaintext.then(|| {
        "MQTT secret(s) are stored in PLAINTEXT in the config file \
         (remember_secrets = true). Prefer `password_env` to load the MQTT \
         password from an environment variable so the secret is never written \
         to disk."
            .to_string()
    })
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            host: default_mqtt_host(),
            port: default_mqtt_port(),
            use_tls: crate::defaults::USE_TLS,
            client_id: default_client_id(),
            topic_prefix: default_topic_prefix(),
            health_topic: default_health_topic(),
            username: None,
            password: None,
            password_env: None,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            client_key_passphrase: None,
            remember_secrets: false,
            retain: false,
            keep_alive_secs: default_keep_alive_secs(),
            payload_format: crate::defaults::PAYLOAD_FORMAT,
            device_topic_prefix: default_device_topic_prefix(),
            autostart: crate::defaults::AUTOSTART,
        }
    }
}

impl Default for UiPreferences {
    fn default() -> Self {
        Self {
            theme: default_ui_theme(),
        }
    }
}

pub fn config_path() -> Result<PathBuf> {
    let project_dirs = ProjectDirs::from("com", "netix", "republisher")
        .context("failed to resolve OS config directory")?;
    Ok(project_dirs.config_dir().join(CONFIG_FILE_NAME))
}

pub fn load_or_default() -> (AppConfig, PathBuf, String) {
    let path = match config_path() {
        Ok(path) => path,
        Err(error) => {
            return (
                AppConfig::default(),
                PathBuf::from(CONFIG_FILE_NAME),
                error.to_string(),
            )
        }
    };

    match load_from_path(&path) {
        Ok(config) => (config, path, "Loaded saved configuration".to_string()),
        Err(error) if path.exists() => (
            AppConfig::default(),
            path,
            format!("Using defaults; config load failed: {error:#}"),
        ),
        Err(_) => (
            AppConfig::default(),
            path,
            "Using default configuration".to_string(),
        ),
    }
}

pub fn load_from_path(path: &Path) -> Result<AppConfig> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut config: AppConfig =
        toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
    config.migrate();
    // A plaintext secret in the loaded file: warn (it should not be on disk).
    if let Some(message) = plaintext_secret_warning(&config.mqtt) {
        log::warn!("{message}");
    }
    // Env indirection wins over anything on disk: if `password_env` names a set
    // variable, the password comes from the environment, not the file.
    config.mqtt.resolve_password_env();
    Ok(config)
}

pub fn save_to_path(path: &Path, config: &AppConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    // Persisting a plaintext secret: warn and point at the env-var alternative.
    if let Some(message) = plaintext_secret_warning(&config.mqtt) {
        log::warn!("{message}");
    }
    let raw =
        toml::to_string_pretty(&config.sanitized_for_save()).context("failed to encode config")?;
    fs::write(path, raw).with_context(|| format!("failed to write {}", path.display()))
}

fn current_version() -> u32 {
    CURRENT_CONFIG_VERSION
}

fn default_mqtt_host() -> String {
    "localhost".to_string()
}

fn default_mqtt_port() -> u16 {
    crate::defaults::MQTT_PORT
}

fn default_client_id() -> String {
    crate::defaults::generate_client_id()
}

fn default_topic_prefix() -> String {
    "Netix/Site".to_string()
}

fn default_health_topic() -> String {
    "Netix/Site/_health/republisher".to_string()
}

fn default_device_topic_prefix() -> String {
    crate::defaults::DEVICE_TOPIC_PREFIX.to_string()
}

fn default_keep_alive_secs() -> u64 {
    crate::defaults::KEEP_ALIVE_SECS
}

fn default_ui_theme() -> UiTheme {
    UiTheme::Auto
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_omits_secrets_unless_remembered() {
        let mut config = AppConfig::default();
        config.mqtt.password = Some("secret".into());
        config.mqtt.remember_secrets = false;
        assert_eq!(config.sanitized_for_save().mqtt.password, None);
        config.mqtt.remember_secrets = true;
        assert_eq!(
            config.sanitized_for_save().mqtt.password.as_deref(),
            Some("secret")
        );
    }

    #[test]
    fn password_env_resolves_secret_from_environment() {
        // Unique var name so the process-global env mutation can't race other tests.
        let var = "REPUBLISH_CORE_TEST_PW_ENV_A1B2";
        std::env::set_var(var, "s3cr3t-from-env");

        let mut mqtt = MqttConfig {
            password_env: Some(var.to_string()),
            // A stale plaintext value on disk must lose to the environment.
            password: Some("stale-on-disk".to_string()),
            ..MqttConfig::default()
        };
        let resolved = mqtt.resolve_password_env();
        std::env::remove_var(var);

        assert!(resolved, "env-named password should resolve");
        assert_eq!(mqtt.password.as_deref(), Some("s3cr3t-from-env"));

        // No env var present -> nothing resolved, existing password untouched.
        let mut mqtt = MqttConfig {
            password_env: Some(var.to_string()),
            password: Some("keep-me".to_string()),
            ..MqttConfig::default()
        };
        assert!(!mqtt.resolve_password_env());
        assert_eq!(mqtt.password.as_deref(), Some("keep-me"));

        // The env-var name is safe to persist (it is not the secret itself),
        // and the resolved secret is stripped from a save with remember_secrets off.
        let config = AppConfig {
            mqtt: MqttConfig {
                password_env: Some(var.to_string()),
                password: Some("resolved-secret".to_string()),
                remember_secrets: false,
                ..MqttConfig::default()
            },
            ..AppConfig::default()
        };
        let saved = config.sanitized_for_save();
        assert_eq!(
            saved.mqtt.password_env.as_deref(),
            Some(var),
            "password_env name is persisted"
        );
        assert_eq!(
            saved.mqtt.password, None,
            "the secret itself is not persisted"
        );
    }

    #[test]
    fn plaintext_persist_emits_warning() {
        // remember_secrets + a password => a plaintext secret hits disk => warn.
        let mut mqtt = MqttConfig {
            remember_secrets: true,
            password: Some("plaintext".into()),
            ..MqttConfig::default()
        };
        assert!(
            plaintext_secret_warning(&mqtt)
                .unwrap()
                .contains("PLAINTEXT"),
            "plaintext persist should produce a warning recommending password_env"
        );
        assert!(plaintext_secret_warning(&mqtt)
            .unwrap()
            .contains("password_env"));

        // A client key passphrase is likewise a plaintext secret.
        mqtt.password = None;
        mqtt.client_key_passphrase = Some("phrase".into());
        assert!(plaintext_secret_warning(&mqtt).is_some());

        // remember_secrets off => secrets are stripped on save => no warning.
        mqtt.remember_secrets = false;
        assert!(plaintext_secret_warning(&mqtt).is_none());

        // No secret set => nothing to warn about even with remember_secrets on.
        let clean = MqttConfig {
            remember_secrets: true,
            ..MqttConfig::default()
        };
        assert!(plaintext_secret_warning(&clean).is_none());
    }

    #[test]
    fn config_round_trips_toml() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        let mut config = AppConfig {
            protocol: "modbus".into(),
            discover_on_start: true,
            ..Default::default()
        };
        config
            .connection_mut()
            .insert("host".into(), serde_json::json!("192.168.1.50"));
        let mut point = PointConfig {
            device_key: "PLC1".into(),
            tag_path: "PLC1/Temp".into(),
            ..PointConfig::default()
        };
        point
            .addressing
            .insert("address".into(), serde_json::json!(40001));
        config.points.push(point);

        save_to_path(&path, &config).unwrap();
        let loaded = load_from_path(&path).unwrap();

        assert_eq!(loaded.protocol, "modbus");
        // discover_on_start is serialized and survives the round-trip; a config
        // file predating the field still parses (serde default = false).
        assert!(loaded.discover_on_start);
        assert!(!AppConfig::default().discover_on_start);
        assert_eq!(loaded.points.len(), 1);
        assert_eq!(loaded.points[0].tag_path, "PLC1/Temp");
        assert_eq!(
            loaded.connection().get("host"),
            Some(&serde_json::json!("192.168.1.50"))
        );
    }

    #[test]
    fn migrate_rewrites_legacy_bacnet_port() {
        let mut config = AppConfig {
            version: 1,
            ..AppConfig::default()
        };
        config.connections.insert("bacnet".into(), {
            let mut conn = Addressing::new();
            conn.insert("port".into(), serde_json::json!(47808));
            conn
        });
        config.migrate();
        assert_eq!(config.version, CURRENT_CONFIG_VERSION);
        assert_eq!(
            config.connections["bacnet"].get("port"),
            Some(&serde_json::json!(0))
        );
    }

    #[test]
    fn validate_rejects_empty_host_and_zero_port() {
        let mut config = AppConfig::default();
        config.mqtt.host = "  ".into();
        assert!(config.validate().is_err());

        let mut config = AppConfig::default();
        config.mqtt.port = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_cert_without_key() {
        let mut config = AppConfig::default();
        config.mqtt.client_cert_path = Some("/tmp/cert.pem".into());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("configured together"));
    }

    #[test]
    fn validate_rejects_zero_poll_interval_for_enabled_points() {
        let mut config = AppConfig::default();
        config.points.push(PointConfig {
            enabled: true,
            poll_interval_secs: 0,
            ..PointConfig::default()
        });
        assert!(config.validate().is_err());
    }

    #[test]
    fn sim_config_checksum_round_trips_and_detects_drift() {
        // No provenance stored -> nothing to drift from, never warns.
        let plain = AppConfig::default();
        assert_eq!(plain.sim_config_checksum(), None);
        assert_eq!(plain.check_sim_config_drift("anything"), None);

        // Stored checksum survives a save/load round-trip.
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        let config = AppConfig {
            sim_config_checksum: Some("abc123".into()),
            ..AppConfig::default()
        };
        save_to_path(&path, &config).unwrap();
        let raw = fs::read_to_string(&path).unwrap();
        assert!(
            raw.contains("sim_config_checksum = \"abc123\""),
            "emitted toml must carry the provenance checksum, got:\n{raw}"
        );
        let loaded = load_from_path(&path).unwrap();
        assert_eq!(loaded.sim_config_checksum(), Some("abc123"));

        // Matching checksum -> no drift, no warning message.
        assert_eq!(loaded.check_sim_config_drift("abc123"), None);
        // A different current checksum -> drift is detected and reported.
        let warning = loaded
            .check_sim_config_drift("def456")
            .expect("mismatching checksum must be flagged as drift");
        assert!(warning.contains("drift"));
        assert!(warning.contains("abc123"));
        assert!(warning.contains("def456"));
    }

    #[test]
    fn payload_format_defaults_envelope_and_round_trips() {
        // Default is the Netix envelope so a hand/GUI config matches the pipeline
        // (and the simulator emit) without extra tweaking — one source of truth.
        assert_eq!(
            MqttConfig::default().payload_format,
            PayloadFormat::NetixEnvelope
        );
        // Pipeline-correct defaults are the safe/right ones out of the box.
        assert_eq!(MqttConfig::default().port, crate::defaults::MQTT_PORT);
        assert!(MqttConfig::default().use_tls);
        assert!(!MqttConfig::default().autostart);
        // Each default config gets a unique, prefixed client id (no broker thrash).
        assert!(MqttConfig::default()
            .client_id
            .starts_with(crate::defaults::CLIENT_ID_PREFIX));
        assert_ne!(
            MqttConfig::default().client_id,
            MqttConfig::default().client_id
        );

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        let mut config = AppConfig {
            protocol: "bacnet".into(),
            mqtt: MqttConfig {
                payload_format: PayloadFormat::NetixEnvelope,
                autostart: true,
                ..MqttConfig::default()
            },
            ..AppConfig::default()
        };
        let mut point = PointConfig {
            device_key: "ahu-12".into(),
            tag_path: "discharge-air-temp".into(),
            ..PointConfig::default()
        };
        point
            .addressing
            .insert("device_instance".into(), serde_json::json!(10100));
        config.points.push(point);
        // Envelope configs validate the device topic, not the per-point topics.
        assert!(config.validate().is_ok(), "{:?}", config.validate());

        save_to_path(&path, &config).unwrap();
        let loaded = load_from_path(&path).unwrap();
        assert_eq!(loaded.mqtt.payload_format, PayloadFormat::NetixEnvelope);
        assert_eq!(loaded.mqtt.device_topic_prefix, "/Netix/Sim/Device");
        assert!(loaded.mqtt.autostart);
    }

    #[test]
    fn payload_format_display_and_config_token() {
        // The config token must match the serde snake_case rename exactly so the
        // simulator emit and the deserialised value never drift.
        assert_eq!(PayloadFormat::Scalar.as_config_token(), "scalar");
        assert_eq!(
            PayloadFormat::NetixEnvelope.as_config_token(),
            "netix_envelope"
        );
        // Human-facing labels for the settings picker.
        assert_eq!(
            PayloadFormat::Scalar.to_string(),
            "Scalar (value per topic)"
        );
        assert_eq!(
            PayloadFormat::NetixEnvelope.to_string(),
            "Netix envelope (per device)"
        );
        // ALL is in menu order.
        assert_eq!(
            PayloadFormat::ALL,
            [PayloadFormat::Scalar, PayloadFormat::NetixEnvelope]
        );
    }

    #[test]
    fn ui_theme_display_and_all() {
        assert_eq!(UiTheme::Auto.to_string(), "Auto");
        assert_eq!(UiTheme::Light.to_string(), "Light");
        assert_eq!(UiTheme::Dark.to_string(), "Dark");
        assert_eq!(UiTheme::ALL, [UiTheme::Auto, UiTheme::Light, UiTheme::Dark]);
    }

    #[test]
    fn validate_rejects_empty_topic_prefix() {
        let mut config = AppConfig::default();
        config.mqtt.topic_prefix = "  ".into();
        assert_eq!(
            config.validate().unwrap_err(),
            "MQTT topic prefix cannot be empty"
        );
    }

    #[test]
    fn validate_rejects_empty_device_topic_prefix_in_envelope_mode() {
        // Default payload_format is NetixEnvelope, so the device prefix is required.
        let mut config = AppConfig::default();
        config.mqtt.device_topic_prefix = "   ".into();
        assert_eq!(
            config.validate().unwrap_err(),
            "MQTT device topic prefix cannot be empty"
        );
    }

    #[test]
    fn validate_rejects_invalid_health_topic() {
        let mut config = AppConfig::default();
        // A wildcard is illegal in a publish topic; the health topic is validated
        // verbatim (unlike per-point topics it is not sanitised first).
        config.mqtt.health_topic = "Netix/Site/#".into();
        assert!(config
            .validate()
            .unwrap_err()
            .contains("health topic is invalid"));
    }

    #[test]
    fn validate_rejects_point_whose_scalar_topic_is_empty() {
        // Scalar mode validates each enabled point's telemetry topic. A prefix that
        // survives the non-empty check but normalises to nothing ("#"), paired with
        // a tag_path that sanitises to nothing ("###"), yields an empty publish
        // topic -> the per-point map_err error message closure fires.
        let mut config = AppConfig::default();
        config.mqtt.payload_format = PayloadFormat::Scalar;
        config.mqtt.topic_prefix = "#".into();
        config.points.push(PointConfig {
            enabled: true,
            poll_interval_secs: 5,
            device_key: "dev".into(),
            tag_path: "###".into(),
            ..PointConfig::default()
        });
        let err = config.validate().unwrap_err();
        assert!(
            err.contains("MQTT topic is invalid"),
            "expected per-point topic error, got: {err}"
        );
    }

    #[test]
    fn validate_accepts_scalar_mode_with_enabled_point() {
        // Scalar mode exercises the per-point telemetry-topic validation branch
        // (skipped in envelope mode). A sane point passes.
        let mut config = AppConfig::default();
        config.mqtt.payload_format = PayloadFormat::Scalar;
        config.points.push(PointConfig {
            enabled: true,
            poll_interval_secs: 5,
            device_key: "dev".into(),
            tag_path: "dev/temp".into(),
            ..PointConfig::default()
        });
        assert!(config.validate().is_ok(), "{:?}", config.validate());

        // A disabled point with a zero poll interval is ignored (branch: !enabled).
        config.points.push(PointConfig {
            enabled: false,
            poll_interval_secs: 0,
            ..PointConfig::default()
        });
        assert!(config.validate().is_ok());
    }

    #[test]
    fn resolve_password_env_no_var_or_blank_var_is_noop() {
        // password_env unset -> nothing to resolve.
        let mut mqtt = MqttConfig::default();
        assert!(!mqtt.resolve_password_env());
        assert_eq!(mqtt.password, None);

        // password_env present but blank -> treated as unset.
        let mut mqtt = MqttConfig {
            password_env: Some("   ".into()),
            ..MqttConfig::default()
        };
        assert!(!mqtt.resolve_password_env());
        assert_eq!(mqtt.password, None);

        // password_env names a var that is set but empty -> not resolved.
        let var = "REPUBLISH_CORE_TEST_PW_ENV_EMPTY_C3D4";
        std::env::set_var(var, "");
        let mut mqtt = MqttConfig {
            password_env: Some(var.into()),
            ..MqttConfig::default()
        };
        let resolved = mqtt.resolve_password_env();
        std::env::remove_var(var);
        assert!(!resolved);
        assert_eq!(mqtt.password, None);
    }

    #[test]
    fn migrate_leaves_non_default_and_current_configs_untouched() {
        // A non-default legacy bacnet port must survive migration verbatim.
        let mut config = AppConfig {
            version: 1,
            ..AppConfig::default()
        };
        config.connections.insert("bacnet".into(), {
            let mut conn = Addressing::new();
            conn.insert("port".into(), serde_json::json!(502));
            conn
        });
        config.migrate();
        assert_eq!(
            config.connections["bacnet"].get("port"),
            Some(&serde_json::json!(502)),
            "a non-default port is not a legacy default and must be left alone"
        );

        // A v1 bacnet connection with the port key absent defaults to 47808 and is
        // rewritten to the sentinel 0.
        let mut config = AppConfig {
            version: 1,
            ..AppConfig::default()
        };
        config
            .connections
            .insert("bacnet".into(), Addressing::new());
        config.migrate();
        assert_eq!(
            config.connections["bacnet"].get("port"),
            Some(&serde_json::json!(0))
        );

        // Already-current version: migrate only stamps the version, no rewrites.
        let mut config = AppConfig::default();
        config.connections.insert("bacnet".into(), {
            let mut conn = Addressing::new();
            conn.insert("port".into(), serde_json::json!(47808));
            conn
        });
        config.migrate();
        assert_eq!(config.version, CURRENT_CONFIG_VERSION);
        assert_eq!(
            config.connections["bacnet"].get("port"),
            Some(&serde_json::json!(47808)),
            "at the current version the legacy rewrite must not run"
        );

        // A v1 config with no bacnet connection at all: nothing to rewrite.
        let mut config = AppConfig {
            version: 1,
            ..AppConfig::default()
        };
        config.migrate();
        assert_eq!(config.version, CURRENT_CONFIG_VERSION);
        assert!(config.connections.is_empty());
    }

    #[test]
    fn load_from_path_surfaces_read_and_parse_errors() {
        // Missing file -> read error with a path-bearing context.
        let missing = std::path::Path::new("/nonexistent/republish-core/does-not-exist.toml");
        let err = load_from_path(missing).unwrap_err();
        assert!(format!("{err:#}").contains("failed to read"));

        // Present but malformed TOML -> parse error.
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        fs::write(&path, "this is = = not valid toml {{{").unwrap();
        let err = load_from_path(&path).unwrap_err();
        assert!(format!("{err:#}").contains("failed to parse"));
    }

    #[test]
    fn load_defaults_version_and_resolves_env_on_load() {
        // A file omitting `version` deserialises via the serde default
        // (current_version) and is then migrated to the current version.
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        let var = "REPUBLISH_CORE_TEST_PW_ENV_LOAD_E5F6";
        std::env::set_var(var, "loaded-from-env");
        fs::write(
            &path,
            format!("[mqtt]\npassword_env = \"{var}\"\npassword = \"stale\"\n"),
        )
        .unwrap();
        let loaded = load_from_path(&path);
        std::env::remove_var(var);
        let loaded = loaded.unwrap();
        assert_eq!(loaded.version, CURRENT_CONFIG_VERSION);
        // Env indirection wins over the stale on-disk password at load time.
        assert_eq!(loaded.mqtt.password.as_deref(), Some("loaded-from-env"));
    }

    #[test]
    fn save_then_load_preserves_remembered_plaintext_secret() {
        // remember_secrets = true keeps the plaintext secret through a save/load
        // (both save and load emit the plaintext warning path).
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        let config = AppConfig {
            mqtt: MqttConfig {
                remember_secrets: true,
                password: Some("kept-plain".into()),
                client_key_passphrase: Some("kept-phrase".into()),
                ..MqttConfig::default()
            },
            ..AppConfig::default()
        };
        save_to_path(&path, &config).unwrap();
        let raw = fs::read_to_string(&path).unwrap();
        assert!(raw.contains("kept-plain"));
        let loaded = load_from_path(&path).unwrap();
        assert_eq!(loaded.mqtt.password.as_deref(), Some("kept-plain"));
        assert_eq!(
            loaded.mqtt.client_key_passphrase.as_deref(),
            Some("kept-phrase")
        );
    }

    #[test]
    fn save_creates_missing_parent_directory() {
        // save_to_path creates the parent dir chain on demand.
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("a").join("b").join("config.toml");
        assert!(!path.parent().unwrap().exists());
        save_to_path(&path, &AppConfig::default()).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn save_errors_when_parent_cannot_be_created() {
        // A regular file standing where a parent directory should be makes
        // create_dir_all fail, surfacing the path-bearing context.
        let temp = tempfile::tempdir().unwrap();
        let blocker = temp.path().join("blocker");
        fs::write(&blocker, "not a directory").unwrap();
        let path = blocker.join("config.toml");
        let err = save_to_path(&path, &AppConfig::default()).unwrap_err();
        assert!(format!("{err:#}").contains("failed to create"));
    }

    #[test]
    fn config_path_and_load_or_default_via_xdg() {
        // config_path()/load_or_default() resolve a real OS config dir; pin it to a
        // temp dir via XDG_CONFIG_HOME so the three load_or_default branches are
        // deterministic. XDG_CONFIG_HOME is process-global, so all assertions live
        // in this single serial test.
        let temp = tempfile::tempdir().unwrap();
        let prev = std::env::var_os("XDG_CONFIG_HOME");
        std::env::set_var("XDG_CONFIG_HOME", temp.path());

        let path = config_path().expect("config path resolves under XDG_CONFIG_HOME");
        assert!(path.ends_with("republisher/config.toml"), "got {path:?}");
        assert!(path.starts_with(temp.path()));

        // No file yet -> defaults with the "using default configuration" message.
        // (Full equality can't be used: a fresh default mints a random client_id.)
        let (config, reported, message) = load_or_default();
        assert_eq!(config.protocol, "");
        assert!(config.points.is_empty());
        assert_eq!(config.version, CURRENT_CONFIG_VERSION);
        assert_eq!(reported, path);
        assert_eq!(message, "Using default configuration");

        // A valid saved file -> loaded, "Loaded saved configuration".
        let saved = AppConfig {
            protocol: "modbus".into(),
            ..AppConfig::default()
        };
        save_to_path(&path, &saved).unwrap();
        let (config, _, message) = load_or_default();
        assert_eq!(config.protocol, "modbus");
        assert_eq!(message, "Loaded saved configuration");

        // A malformed file that exists -> defaults, "config load failed".
        fs::write(&path, "= = broken").unwrap();
        let (config, _, message) = load_or_default();
        assert_eq!(config.protocol, "");
        assert!(config.points.is_empty());
        assert!(
            message.contains("config load failed"),
            "got message: {message}"
        );

        // Restore the prior environment.
        match prev {
            Some(value) => std::env::set_var("XDG_CONFIG_HOME", value),
            None => std::env::remove_var("XDG_CONFIG_HOME"),
        }
    }
}
