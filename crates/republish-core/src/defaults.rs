//! The single source of truth for pipeline-correct republisher defaults.
//!
//! Both the config struct defaults ([`MqttConfig`](crate::config::MqttConfig),
//! [`PointConfig`](crate::model::PointConfig)) and the simulator's `config.toml`
//! emit (`sim-core::republisher_export`) reference these constants and the
//! [`generate_client_id`] generator. Because there is exactly one source, a
//! hand-written, GUI-authored, or emitted config can never diverge from the
//! built-in defaults — the "emit hardcodes the right value while the struct
//! default silently stays wrong" anti-pattern that RCA #3 killed.

use uuid::Uuid;

use crate::config::PayloadFormat;

/// Default MQTT broker port (TLS listener).
pub const MQTT_PORT: u16 = 8883;

/// Default: connect to the broker over TLS.
pub const USE_TLS: bool = true;

/// Default telemetry serialisation: the Netix per-device envelope the platform
/// MQTT workers ingest (`{reason,time,id,points:[{pointName,data,status}]}`).
pub const PAYLOAD_FORMAT: PayloadFormat = PayloadFormat::NetixEnvelope;

/// Envelope device-topic prefix. Keeps its leading slash so a publish to
/// `/Netix/Sim/Device/<id>/telemetry` matches a subscription on
/// `/Netix/Sim/Device/#` (a leading-slash-stripped topic would not).
pub const DEVICE_TOPIC_PREFIX: &str = "/Netix/Sim/Device";

/// Default: do NOT start republishing on launch. Starting is a deliberate
/// operator action (safety) — a freshly loaded config never publishes on its own.
pub const AUTOSTART: bool = false;

/// Default per-point poll cadence, in seconds.
pub const POLL_INTERVAL_SECS: u64 = 30;

/// Default MQTT keep-alive, in seconds.
pub const KEEP_ALIVE_SECS: u64 = 30;

/// Prefix for generated MQTT client ids (see [`generate_client_id`]).
pub const CLIENT_ID_PREFIX: &str = "netix-republisher-";

/// Generate a per-instance-unique MQTT client id (`netix-republisher-<uuid v4>`).
///
/// Two republishers sharing a client id mutually kick each other off the broker;
/// a v4 uuid suffix makes collisions astronomically unlikely. The value is stored
/// in the config struct and serialised, so it stays **stable across reconnects**
/// (one id per saved config) while remaining **unique across instances**.
pub fn generate_client_id() -> String {
    format!("{CLIENT_ID_PREFIX}{}", Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_client_id_is_prefixed_and_unique() {
        let a = generate_client_id();
        let b = generate_client_id();
        assert!(a.starts_with(CLIENT_ID_PREFIX));
        assert!(b.starts_with(CLIENT_ID_PREFIX));
        // v4 uuid suffix -> two generated ids never collide.
        assert_ne!(a, b);
        // "netix-republisher-" + 36-char uuid.
        assert_eq!(a.len(), CLIENT_ID_PREFIX.len() + 36);
    }

    #[test]
    #[allow(
        clippy::assertions_on_constants,
        reason = "these assert the pipeline-correct values of the shared Defaults constants"
    )]
    fn pipeline_defaults_are_envelope_ready() {
        assert_eq!(PAYLOAD_FORMAT, PayloadFormat::NetixEnvelope);
        assert!(USE_TLS);
        assert!(!AUTOSTART);
        assert_eq!(MQTT_PORT, 8883);
        assert_eq!(POLL_INTERVAL_SECS, 30);
        assert_eq!(KEEP_ALIVE_SECS, 30);
        assert!(DEVICE_TOPIC_PREFIX.starts_with('/'));
    }
}
