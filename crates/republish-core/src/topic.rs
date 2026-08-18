//! MQTT topic construction and validation (protocol-neutral).

use crate::config::MqttConfig;
use crate::model::{json_scalar, PointConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicError {
    Empty,
    Wildcard,
}

impl std::fmt::Display for TopicError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => formatter.write_str("MQTT publish topic cannot be empty"),
            Self::Wildcard => formatter.write_str("MQTT publish topic cannot contain # or +"),
        }
    }
}

impl std::error::Error for TopicError {}

pub fn telemetry_topic(config: &MqttConfig, point: &PointConfig) -> String {
    let tag_path = if point.tag_path.trim().is_empty() {
        default_tag_path(point)
    } else {
        point.tag_path.clone()
    };
    join_topic(&[&normalize_prefix(&config.topic_prefix), &tag_path])
}

/// Topic for a `netix_envelope` device publish: `<device_topic_prefix>/<id>/telemetry`.
///
/// Unlike [`telemetry_topic`], the configured prefix is preserved verbatim
/// (only its trailing slash trimmed) so a leading slash survives — a publish to
/// `/Netix/Sim/Device/<id>/telemetry` must match a subscription on
/// `/Netix/Sim/Device/#`, which a leading-slash-stripped topic would not. Only
/// the `id` segment is sanitised.
pub fn device_envelope_topic(config: &MqttConfig, id: &str) -> String {
    let prefix = config.device_topic_prefix.trim_end().trim_end_matches('/');
    format!("{}/{}/telemetry", prefix, sanitize_segment(id))
}

/// Default tag path when a point has no explicit `tag_path`: the device key
/// followed by a slug of the addressing values.
pub fn default_tag_path(point: &PointConfig) -> String {
    let device = if point.device_key.trim().is_empty() {
        "device".to_string()
    } else {
        point.device_key.clone()
    };
    let addr = point
        .addressing
        .values()
        .map(json_scalar)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    if addr.is_empty() {
        sanitize_segment(&device)
    } else {
        format!("{}/{}", sanitize_segment(&device), sanitize_segment(&addr))
    }
}

pub fn validate_publish_topic(topic: &str) -> Result<(), TopicError> {
    let trimmed = topic.trim();
    if trimmed.is_empty() {
        return Err(TopicError::Empty);
    }
    if trimmed.contains('#') || trimmed.contains('+') {
        return Err(TopicError::Wildcard);
    }
    Ok(())
}

pub fn normalize_prefix(prefix: &str) -> String {
    prefix
        .trim()
        .trim_end_matches('#')
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.trim().is_empty())
        .map(sanitize_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn join_topic(parts: &[&str]) -> String {
    parts
        .iter()
        .flat_map(|part| part.split('/'))
        .filter(|segment| !segment.trim().is_empty())
        .map(sanitize_segment)
        .collect::<Vec<_>>()
        .join("/")
}

pub fn sanitize_segment(value: &str) -> String {
    let mut sanitized = value
        .trim()
        .chars()
        .map(|character| match character {
            '/' | '#' | '+' | ' ' | '\t' | '\n' | '\r' => '_',
            character if character.is_control() => '_',
            character => character,
        })
        .collect::<String>();

    while sanitized.contains("__") {
        sanitized = sanitized.replace("__", "_");
    }
    sanitized.trim_matches('_').to_string()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use proto_api::Addressing;

    fn point(device: &str, addr: &[(&str, serde_json::Value)], tag: &str) -> PointConfig {
        let mut addressing = Addressing::new();
        for (k, v) in addr {
            addressing.insert((*k).to_string(), v.clone());
        }
        PointConfig {
            device_key: device.to_string(),
            addressing,
            tag_path: tag.to_string(),
            ..PointConfig::default()
        }
    }

    #[test]
    fn normalizes_abstract_subscription_prefix() {
        assert_eq!(normalize_prefix("Netix/NC-9/#"), "Netix/NC-9");
        assert_eq!(normalize_prefix("/Netix//Site/"), "Netix/Site");
    }

    #[test]
    fn generates_default_topic_from_device_and_addressing() {
        let config = MqttConfig::default();
        let p = point(
            "Jace Neo",
            &[
                ("object_type", serde_json::json!("analog_input")),
                ("object_instance", serde_json::json!(2)),
            ],
            "",
        );
        // BTreeMap orders keys: object_instance, object_type -> "2_analog_input"
        assert_eq!(
            telemetry_topic(&config, &p),
            "Netix/Site/Jace_Neo/2_analog_input"
        );
    }

    #[test]
    fn explicit_tag_path_wins() {
        let config = MqttConfig::default();
        let p = point("dev", &[], "AHU1/Supply Temp");
        assert_eq!(telemetry_topic(&config, &p), "Netix/Site/AHU1/Supply_Temp");
    }

    #[test]
    fn rejects_wildcards() {
        assert!(validate_publish_topic("Netix/Site/AHU1/temp").is_ok());
        assert_eq!(
            validate_publish_topic("Netix/Site/#").unwrap_err(),
            TopicError::Wildcard
        );
        // The '+' single-level wildcard is rejected too.
        assert_eq!(
            validate_publish_topic("Netix/+/temp").unwrap_err(),
            TopicError::Wildcard
        );
    }

    #[test]
    fn rejects_empty_and_whitespace_topics() {
        assert_eq!(validate_publish_topic("").unwrap_err(), TopicError::Empty);
        assert_eq!(
            validate_publish_topic("   \t ").unwrap_err(),
            TopicError::Empty
        );
    }

    #[test]
    fn topic_error_display_messages() {
        assert_eq!(
            TopicError::Empty.to_string(),
            "MQTT publish topic cannot be empty"
        );
        assert_eq!(
            TopicError::Wildcard.to_string(),
            "MQTT publish topic cannot contain # or +"
        );
    }

    #[test]
    fn default_tag_path_falls_back_to_literal_device_when_key_blank() {
        // Blank device_key and no addressing -> literal "device" segment.
        let p = point("   ", &[], "");
        assert_eq!(default_tag_path(&p), "device");
    }

    #[test]
    fn default_tag_path_uses_device_alone_when_no_addressing() {
        // Device present but no addressing values -> just the sanitised device.
        let p = point("Boiler Room", &[], "");
        assert_eq!(default_tag_path(&p), "Boiler_Room");
    }

    #[test]
    fn default_tag_path_skips_null_and_empty_addressing_values() {
        // json_scalar(Null) yields "" which is filtered out, leaving addr empty.
        let p = point("dev", &[("property", serde_json::Value::Null)], "");
        assert_eq!(default_tag_path(&p), "dev");
    }

    #[test]
    fn sanitize_segment_collapses_runs_of_underscores() {
        // Repeated illegal chars collapse to one underscore, ends trimmed.
        assert_eq!(sanitize_segment("__a///b  c__"), "a_b_c");
        assert_eq!(sanitize_segment("#+ /"), "");
    }

    #[test]
    fn device_envelope_topic_preserves_leading_slash() {
        let config = MqttConfig::default();
        // Default prefix keeps its leading slash so it matches `/Netix/Sim/Device/#`.
        assert_eq!(
            device_envelope_topic(&config, "ahu-12"),
            "/Netix/Sim/Device/ahu-12/telemetry"
        );
        // The id segment is sanitised; a trailing prefix slash is trimmed.
        let trailing = MqttConfig {
            device_topic_prefix: "/Netix/Sim/Device/".to_string(),
            ..MqttConfig::default()
        };
        assert_eq!(
            device_envelope_topic(&trailing, "pump room/1"),
            "/Netix/Sim/Device/pump_room_1/telemetry"
        );
    }
}
