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
        for point in &self.points {
            if point.enabled && point.poll_interval_secs == 0 {
                return Err(format!(
                    "{} poll interval cannot be 0",
                    point.display_name()
                ));
            }
            if point.enabled {
                validate_publish_topic(&telemetry_topic(&self.mqtt, point)).map_err(|error| {
                    format!("{} MQTT topic is invalid: {error}", point.display_name())
                })?;
            }
        }
        Ok(())
    }
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            host: default_mqtt_host(),
            port: default_mqtt_port(),
            use_tls: true,
            client_id: default_client_id(),
            topic_prefix: default_topic_prefix(),
            health_topic: default_health_topic(),
            username: None,
            password: None,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            client_key_passphrase: None,
            remember_secrets: false,
            retain: false,
            keep_alive_secs: default_keep_alive_secs(),
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
    Ok(config)
}

pub fn save_to_path(path: &Path, config: &AppConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
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
    8883
}

fn default_client_id() -> String {
    "netix-republisher".to_string()
}

fn default_topic_prefix() -> String {
    "Netix/Site".to_string()
}

fn default_health_topic() -> String {
    "Netix/Site/_health/republisher".to_string()
}

fn default_keep_alive_secs() -> u64 {
    30
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
    fn config_round_trips_toml() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        let mut config = AppConfig {
            protocol: "modbus".into(),
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
        config.connections.insert(
            "bacnet".into(),
            {
                let mut conn = Addressing::new();
                conn.insert("port".into(), serde_json::json!(47808));
                conn
            },
        );
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
}
