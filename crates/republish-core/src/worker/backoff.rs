//! Per-device exponential backoff when all reads fail.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::log::LogLevel;
use crate::model::{PointConfig, PollOutcome};

pub const DEVICE_BACKOFF_INITIAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct DeviceBackoff {
    pub delay: Duration,
    pub until: Instant,
}

/// Escalates backoff for devices where every read failed this cycle and clears
/// it for devices that produced at least one sample.
pub fn update_device_backoffs(
    backoffs: &mut HashMap<u32, DeviceBackoff>,
    polled_devices: &HashSet<u32>,
    outcome: &PollOutcome,
    now: Instant,
    max_delay: Duration,
    device_instance: fn(&PointConfig) -> Option<u32>,
) -> Vec<(LogLevel, String)> {
    let healthy = outcome
        .samples
        .iter()
        .filter_map(|sample| device_instance(&sample.point))
        .collect::<HashSet<_>>();
    let failed = outcome
        .failures
        .iter()
        .filter_map(|failure| device_instance(&failure.point))
        .collect::<HashSet<_>>();

    let mut messages = Vec::new();
    for &device in polled_devices {
        if healthy.contains(&device) {
            if backoffs.remove(&device).is_some() {
                messages.push((
                    LogLevel::Info,
                    format!("device {device} responding again; backoff cleared"),
                ));
            }
        } else if failed.contains(&device) {
            let delay = match backoffs.get(&device) {
                Some(backoff) => backoff.delay.saturating_mul(2).min(max_delay),
                None => DEVICE_BACKOFF_INITIAL.min(max_delay),
            };
            backoffs.insert(
                device,
                DeviceBackoff {
                    delay,
                    until: now + delay,
                },
            );
            messages.push((
                LogLevel::Warning,
                format!(
                    "device {device}: all reads failed; next attempt in {}s",
                    delay.as_secs()
                ),
            ));
        }
    }
    messages
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use proto_api::Addressing;

    fn point_on_device(device_instance: u32) -> PointConfig {
        let mut addressing = Addressing::new();
        addressing.insert("device_instance".into(), serde_json::json!(device_instance));
        PointConfig {
            enabled: true,
            device_key: format!("device_{device_instance}"),
            addressing,
            ..PointConfig::default()
        }
    }

    fn device_instance(point: &PointConfig) -> Option<u32> {
        match point.addressing.get("device_instance")? {
            serde_json::Value::Number(n) => n.as_u64().map(|v| v as u32),
            _ => None,
        }
    }

    #[test]
    fn backoff_doubles_on_failure_and_clears_on_success() {
        let now = Instant::now();
        let mut backoffs = HashMap::new();
        let polled = HashSet::from([100u32]);
        let fail = PollOutcome {
            failures: vec![crate::model::PointFailure {
                point: point_on_device(100),
                error: "timeout".into(),
            }],
            ..PollOutcome::default()
        };
        update_device_backoffs(
            &mut backoffs,
            &polled,
            &fail,
            now,
            Duration::from_secs(300),
            device_instance,
        );
        assert_eq!(backoffs.get(&100).unwrap().delay, DEVICE_BACKOFF_INITIAL);

        let ok = PollOutcome {
            samples: vec![crate::model::PointSample {
                point: point_on_device(100),
                value: crate::model::TelemetryValue::Number(1.0),
                topic: String::new(),
                timestamp_ms: 0,
            }],
            ..PollOutcome::default()
        };
        update_device_backoffs(
            &mut backoffs,
            &polled,
            &ok,
            now,
            Duration::from_secs(300),
            device_instance,
        );
        assert!(!backoffs.contains_key(&100));
    }

    fn failure_outcome(device: u32) -> PollOutcome {
        PollOutcome {
            failures: vec![crate::model::PointFailure {
                point: point_on_device(device),
                error: "timeout".into(),
            }],
            ..PollOutcome::default()
        }
    }

    #[test]
    fn backoff_doubles_then_clamps_at_max_delay() {
        let now = Instant::now();
        let mut backoffs = HashMap::new();
        let polled = HashSet::from([7u32]);
        let max = Duration::from_secs(15);

        // First failure: no prior backoff -> initial (clamped to max if smaller).
        let messages = update_device_backoffs(
            &mut backoffs,
            &polled,
            &failure_outcome(7),
            now,
            max,
            device_instance,
        );
        assert_eq!(backoffs.get(&7).unwrap().delay, DEVICE_BACKOFF_INITIAL);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, LogLevel::Warning);
        assert!(messages[0].1.contains("all reads failed"));
        assert_eq!(
            backoffs.get(&7).unwrap().until,
            now + DEVICE_BACKOFF_INITIAL
        );

        // Second failure: 10s * 2 = 20s, clamped down to the 15s max.
        update_device_backoffs(
            &mut backoffs,
            &polled,
            &failure_outcome(7),
            now,
            max,
            device_instance,
        );
        assert_eq!(backoffs.get(&7).unwrap().delay, max);
    }

    #[test]
    fn clear_message_emitted_when_device_recovers() {
        let now = Instant::now();
        let mut backoffs = HashMap::new();
        let polled = HashSet::from([9u32]);
        update_device_backoffs(
            &mut backoffs,
            &polled,
            &failure_outcome(9),
            now,
            Duration::from_secs(300),
            device_instance,
        );
        let ok = PollOutcome {
            samples: vec![crate::model::PointSample {
                point: point_on_device(9),
                value: crate::model::TelemetryValue::Number(1.0),
                topic: String::new(),
                timestamp_ms: 0,
            }],
            ..PollOutcome::default()
        };
        let messages = update_device_backoffs(
            &mut backoffs,
            &polled,
            &ok,
            now,
            Duration::from_secs(300),
            device_instance,
        );
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, LogLevel::Info);
        assert!(messages[0].1.contains("backoff cleared"));
    }

    #[test]
    fn device_without_numeric_instance_is_ignored() {
        // A non-numeric device_instance resolves to None, so the point neither
        // clears nor escalates a backoff.
        let now = Instant::now();
        let mut backoffs = HashMap::new();
        let mut addressing = Addressing::new();
        addressing.insert("device_instance".into(), serde_json::json!("not-a-number"));
        let point = PointConfig {
            addressing,
            ..PointConfig::default()
        };
        let outcome = PollOutcome {
            failures: vec![crate::model::PointFailure {
                point,
                error: "x".into(),
            }],
            ..PollOutcome::default()
        };
        // The unresolved device is not in polled_devices, so nothing happens.
        let messages = update_device_backoffs(
            &mut backoffs,
            &HashSet::new(),
            &outcome,
            now,
            Duration::from_secs(300),
            device_instance,
        );
        assert!(messages.is_empty());
        assert!(backoffs.is_empty());
    }
}
