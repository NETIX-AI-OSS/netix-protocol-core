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
    }
}
