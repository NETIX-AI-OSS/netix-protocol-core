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
}
