//! The single source of truth for pipeline-correct republisher defaults, referenced by both config struct defaults and the simulator's emit so they can never diverge.

use uuid::Uuid;

use crate::config::PayloadFormat;

/// Default MQTT broker port (TLS listener).
pub const MQTT_PORT: u16 = 8883;

/// Default: connect to the broker over TLS.
pub const USE_TLS: bool = true;

/// Default telemetry serialisation: the Netix per-device envelope the platform MQTT workers ingest.
pub const PAYLOAD_FORMAT: PayloadFormat = PayloadFormat::NetixEnvelope;

/// Envelope device-topic prefix; keeps its leading slash so publishes match a subscription on `/Netix/Sim/Device/#`.
pub const DEVICE_TOPIC_PREFIX: &str = "/Netix/Sim/Device";

/// Default: do NOT start republishing on launch; starting is a deliberate operator action for safety.
pub const AUTOSTART: bool = false;

/// Default per-point poll cadence, in seconds.
pub const POLL_INTERVAL_SECS: u64 = 30;

/// Default MQTT keep-alive, in seconds.
pub const KEEP_ALIVE_SECS: u64 = 30;

/// Prefix for generated MQTT client ids (see [`generate_client_id`]).
pub const CLIENT_ID_PREFIX: &str = "netix-republisher-";

/// Generates a per-instance-unique MQTT client id (`netix-republisher-<uuid v4>`); stored/serialised so it stays stable across reconnects but unique across instances.
pub fn generate_client_id() -> String {
    format!("{CLIENT_ID_PREFIX}{}", Uuid::new_v4())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
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
