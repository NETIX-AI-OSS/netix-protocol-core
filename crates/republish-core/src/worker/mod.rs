//! Background worker: discovery, browse, bulk scan, and poll→publish loops.

mod backoff;
mod events;
mod runtime;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use proto_api::Addressing;

pub use events::{RepublisherLifecycle, WorkerChannel, WorkerEvent};

use crate::config::MqttConfig;
use crate::import::{merge_imported_points, point_from_discovered};
use crate::log::LogLevel;
use crate::model::{PointConfig, PointFailure, PointIdentity, PointSample, PointStatus, PublishStats};
use crate::mqtt::{publish_health, HealthSnapshot, RumqttPublisher};
use crate::protocol::RepublishFactory;

use backoff::{update_device_backoffs, DeviceBackoff};
use events::log;
use runtime::run_async;

const POLL_TICK: Duration = Duration::from_millis(500);
const HEALTH_INTERVAL: Duration = Duration::from_secs(30);
const CLIENT_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_DEVICE_BACKOFF_MAX: Duration = Duration::from_secs(300);
const DEVICE_RERESOLVE_INTERVAL: Duration = Duration::from_secs(60);
const DEVICE_TABLE_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(240);

#[derive(Default)]
struct RefreshStateChange {
    newly_resolved: Vec<u32>,
    newly_unresolved: HashSet<u32>,
}

fn device_instance(point: &PointConfig) -> Option<u32> {
    match point.addressing.get("device_instance")? {
        serde_json::Value::Number(n) => n.as_u64().map(|v| v as u32),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Points eligible for polling this cycle: enabled, resolved, not in backoff,
/// and past their poll interval.
fn due_points(
    now: Instant,
    points: &[PointConfig],
    unresolved_devices: &HashSet<u32>,
    device_backoffs: &HashMap<u32, DeviceBackoff>,
    last_poll: &HashMap<PointIdentity, Instant>,
) -> Vec<PointConfig> {
    points
        .iter()
        .filter(|p| p.enabled)
        .filter(|p| {
            if let Some(instance) = device_instance(p) {
                if unresolved_devices.contains(&instance) {
                    return false;
                }
                if let Some(backoff) = device_backoffs.get(&instance) {
                    if now < backoff.until {
                        return false;
                    }
                }
            }
            let id = PointIdentity::from_point(p);
            match last_poll.get(&id) {
                Some(at) => now.duration_since(*at).as_secs() >= p.poll_interval_secs,
                None => true,
            }
        })
        .cloned()
        .collect()
}

fn device_backoff_max(conn: &Addressing) -> Duration {
    match conn.get("device_backoff_max_secs") {
        Some(serde_json::Value::Number(n)) => {
            Duration::from_secs(n.as_u64().unwrap_or(300).max(10))
        }
        Some(serde_json::Value::String(s)) => {
            Duration::from_secs(s.trim().parse::<u64>().unwrap_or(300).max(10))
        }
        _ => DEFAULT_DEVICE_BACKOFF_MAX,
    }
}

fn unique_device_instances(points: &[PointConfig]) -> Vec<u32> {
    let mut instances: Vec<u32> = points
        .iter()
        .filter(|p| p.enabled)
        .filter_map(device_instance)
        .collect();
    instances.sort_unstable();
    instances.dedup();
    instances
}

fn apply_refresh_state(
    unresolved_devices: &mut HashSet<u32>,
    device_backoffs: &mut HashMap<u32, DeviceBackoff>,
    refresh: &crate::model::RefreshOutcome,
) -> RefreshStateChange {
    let mut change = RefreshStateChange::default();

    for &device in &refresh.resolved {
        if unresolved_devices.remove(&device) {
            change.newly_resolved.push(device);
        }
        device_backoffs.remove(&device);
    }
    change.newly_resolved.sort_unstable();
    change.newly_resolved.dedup();

    for &device in &refresh.unresolved {
        if unresolved_devices.insert(device) {
            change.newly_unresolved.insert(device);
        }
    }

    change
}

fn record_unresolved_failures(
    sender: &Sender<WorkerEvent>,
    points: &[PointConfig],
    unresolved_devices: &HashSet<u32>,
    point_status: &mut HashMap<PointIdentity, PointStatus>,
) {
    if unresolved_devices.is_empty() {
        return;
    }
    let failures = points
        .iter()
        .filter(|point| {
            point.enabled
                && device_instance(point)
                    .is_some_and(|instance| unresolved_devices.contains(&instance))
        })
        .map(|point| PointFailure {
            point: point.clone(),
            error: format!(
                "device {} not in I-Am cache",
                device_instance(point).unwrap_or(0)
            ),
        })
        .collect::<Vec<_>>();
    if failures.is_empty() {
        return;
    }
    for failure in &failures {
        point_status
            .entry(PointIdentity::from_point(&failure.point))
            .or_default()
            .record_read_failure(failure.error.clone());
    }
    let _ = sender.send(WorkerEvent::Failures(failures));
}

fn emit_refresh_state_change(
    sender: &Sender<WorkerEvent>,
    points: &[PointConfig],
    label: &str,
    change: RefreshStateChange,
    point_status: &mut HashMap<PointIdentity, PointStatus>,
) {
    if !change.newly_resolved.is_empty() {
        log(
            sender,
            LogLevel::Info,
            format!(
                "{} device(s) resolved during {label}: {:?}",
                change.newly_resolved.len(),
                change.newly_resolved
            ),
        );
    }
    if !change.newly_unresolved.is_empty() {
        let newly_unresolved: Vec<u32> = change.newly_unresolved.iter().copied().collect();
        log(
            sender,
            LogLevel::Warning,
            format!(
                "{} device(s) unresolved during {label}: {:?}",
                newly_unresolved.len(),
                newly_unresolved
            ),
        );
        record_unresolved_failures(
            sender,
            points,
            &change.newly_unresolved,
            point_status,
        );
    }
}

fn publish_samples(
    sender: &Sender<WorkerEvent>,
    publisher: &mut RumqttPublisher,
    mqtt: &MqttConfig,
    samples: &[PointSample],
    point_status: &mut HashMap<PointIdentity, PointStatus>,
) -> PublishStats {
    let mut stats = PublishStats::empty();
    for sample in samples {
        stats.queued += 1;
        let identity = PointIdentity::from_point(&sample.point);
        let payload = match serde_json::to_vec(&sample.value.as_json_value()) {
            Ok(payload) => payload,
            Err(error) => {
                let message = error.to_string();
                stats.record_failure(message.clone());
                if let Some(status) = point_status.get_mut(&identity) {
                    status.record_publish_failure(&message);
                }
                let _ = sender.send(WorkerEvent::PointPublish {
                    identity,
                    error: Some(message),
                });
                continue;
            }
        };
        match publisher.try_enqueue_sample(&sample.topic, payload, mqtt.retain) {
            Ok(()) => {
                stats.published += 1;
                if let Some(status) = point_status.get_mut(&identity) {
                    status.record_publish_success();
                }
                let _ = sender.send(WorkerEvent::PointPublish {
                    identity,
                    error: None,
                });
            }
            Err(error) => {
                let message = error.to_string();
                stats.record_failure(message.clone());
                if let Some(status) = point_status.get_mut(&identity) {
                    status.record_publish_failure(&message);
                }
                let _ = sender.send(WorkerEvent::PointPublish {
                    identity,
                    error: Some(message),
                });
            }
        }
    }

    stats.reconnects = publisher.reconnect_count();
    if stats.last_error.is_none() {
        stats.last_error = publisher.last_connection_error();
    }
    stats
}

/// Discover devices/servers for the selected protocol.
pub fn spawn_discovery(sender: Sender<WorkerEvent>, factory: RepublishFactory, conn: Addressing) {
    std::thread::spawn(move || {
        run_async(sender.clone(), async move {
            let proto = factory();
            match proto.discover(&conn).await {
                Ok(outcome) => {
                    for warning in &outcome.warnings {
                        log(&sender, LogLevel::Warning, warning.clone());
                    }
                    let count = outcome.devices.len();
                    let _ = sender.send(WorkerEvent::Devices(outcome));
                    let _ = sender.send(WorkerEvent::Finished(format!(
                        "Discovery found {count} device(s)"
                    )));
                }
                Err(error) => {
                    log(
                        &sender,
                        LogLevel::Error,
                        format!("Discovery failed: {error:#}"),
                    );
                    let _ = sender.send(WorkerEvent::Finished("Discovery failed".into()));
                }
            }
        });
    });
}

/// Browse one device's points.
pub fn spawn_browse(
    sender: Sender<WorkerEvent>,
    factory: RepublishFactory,
    conn: Addressing,
    device: crate::model::DiscoveredDevice,
) {
    std::thread::spawn(move || {
        run_async(sender.clone(), async move {
            let proto = factory();
            match proto.browse(&conn, &device).await {
                Ok(outcome) => {
                    for warning in outcome.warnings {
                        log(&sender, LogLevel::Warning, warning);
                    }
                    let count = outcome.points.len();
                    let _ = sender.send(WorkerEvent::Points(outcome.points));
                    let _ = sender.send(WorkerEvent::Finished(format!(
                        "Browsed {count} point(s) on {}",
                        device.key
                    )));
                }
                Err(error) => {
                    log(
                        &sender,
                        LogLevel::Error,
                        format!("Browse failed: {error:#}"),
                    );
                    let _ = sender.send(WorkerEvent::Finished("Browse failed".into()));
                }
            }
        });
    });
}

/// Browse every discovered device and merge results into the point table.
pub fn spawn_scan_all_objects(
    sender: Sender<WorkerEvent>,
    factory: RepublishFactory,
    conn: Addressing,
    devices: Vec<crate::model::DiscoveredDevice>,
    existing_points: Vec<PointConfig>,
) {
    std::thread::spawn(move || {
        run_async(sender.clone(), async move {
            let total = devices.len();
            log(
                &sender,
                LogLevel::Info,
                format!("Scanning object lists for {total} device(s)"),
            );
            let _ = sender.send(WorkerEvent::ScanProgress {
                device_key: String::new(),
                current: 0,
                total,
            });

            let proto = factory();
            let mut imported = Vec::new();
            let mut failures = 0usize;
            for (idx, device) in devices.iter().enumerate() {
                match proto.browse(&conn, device).await {
                    Ok(outcome) => {
                        for warning in outcome.warnings {
                            log(&sender, LogLevel::Warning, warning);
                        }
                        let count = outcome.points.len();
                        for point in outcome.points {
                            imported.push(point_from_discovered(&point, 10));
                        }
                        log(
                            &sender,
                            LogLevel::Info,
                            format!("[{}/{}] {}: {count} object(s)", idx + 1, total, device.key),
                        );
                    }
                    Err(error) => {
                        failures += 1;
                        log(
                            &sender,
                            LogLevel::Warning,
                            format!("{}: scan failed: {error:#}", device.key),
                        );
                    }
                }
                let _ = sender.send(WorkerEvent::ScanProgress {
                    device_key: device.key.clone(),
                    current: idx + 1,
                    total,
                });
            }

            let merge = merge_imported_points(&existing_points, &imported);
            let added = merge.added;
            let updated = merge.updated;
            let total_points = merge.points.len();
            let _ = sender.send(WorkerEvent::BulkTagImport(merge));
            let _ = sender.send(WorkerEvent::Finished(format!(
                "Scanned {total} device(s) ({failures} failure(s)) — {added} point(s) added, {updated} updated, {total_points} total"
            )));
        });
    });
}

/// Poll configured points once and publish results to MQTT.
pub fn spawn_poll_once(
    sender: Sender<WorkerEvent>,
    factory: RepublishFactory,
    conn: Addressing,
    mqtt: MqttConfig,
    points: Vec<PointConfig>,
) {
    std::thread::spawn(move || {
        run_async(sender.clone(), async move {
            let proto = factory();
            let enabled: Vec<PointConfig> = points.into_iter().filter(|p| p.enabled).collect();
            if enabled.is_empty() {
                let _ = sender.send(WorkerEvent::Finished("No enabled points to poll".into()));
                return;
            }
            let mut publisher = match RumqttPublisher::new(&mqtt) {
                Ok(publisher) => publisher,
                Err(error) => {
                    log(
                        &sender,
                        LogLevel::Error,
                        format!("MQTT publisher failed: {error:#}"),
                    );
                    let _ = sender.send(WorkerEvent::Finished("Poll once failed".into()));
                    return;
                }
            };
            match proto.poll(&conn, &enabled).await {
                Ok(outcome) => {
                    for warning in outcome.warnings {
                        log(&sender, LogLevel::Warning, warning);
                    }
                    if !outcome.failures.is_empty() {
                        let _ = sender.send(WorkerEvent::Failures(outcome.failures));
                    }
                    if !outcome.samples.is_empty() {
                        let mut samples = outcome.samples;
                        for sample in &mut samples {
                            sample.topic = crate::topic::telemetry_topic(&mqtt, &sample.point);
                        }
                        let mut point_status = HashMap::new();
                        let stats =
                            publish_samples(&sender, &mut publisher, &mqtt, &samples, &mut point_status);
                        let _ = sender.send(WorkerEvent::Samples(samples));
                        let _ = sender.send(WorkerEvent::PublishStatus(stats));
                    }
                    let _ = sender.send(WorkerEvent::Finished("Poll once complete".into()));
                }
                Err(error) => {
                    log(&sender, LogLevel::Error, format!("Poll failed: {error:#}"));
                    let _ = sender.send(WorkerEvent::Finished("Poll once failed".into()));
                }
            }
        });
    });
}

/// Run the continuous poll→publish loop until `stop` is set.
pub fn spawn_republisher(
    sender: Sender<WorkerEvent>,
    factory: RepublishFactory,
    conn: Addressing,
    mqtt: MqttConfig,
    points: Vec<PointConfig>,
    stop: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let fail_sender = sender.clone();
        let completed = run_async(sender.clone(), async move {
            let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Starting));
            let proto = factory();
            let backoff_max = device_backoff_max(&conn);
            let mut publisher = match RumqttPublisher::new(&mqtt) {
                Ok(publisher) => publisher,
                Err(error) => {
                    let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Failed(
                        error.to_string(),
                    )));
                    return;
                }
            };
            let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Running));

            let device_instances = unique_device_instances(&points);
            let mut unresolved_devices: HashSet<u32> = HashSet::new();
            match proto.refresh_devices(&conn, &device_instances).await {
                Ok(refresh) => {
                    if !refresh.unresolved.is_empty() {
                        log(
                            &sender,
                            LogLevel::Warning,
                            format!(
                                "{} of {} device(s) not in I-Am cache; their points will be skipped (resolution retried every {}s)",
                                refresh.unresolved.len(),
                                device_instances.len(),
                                DEVICE_RERESOLVE_INTERVAL.as_secs()
                            ),
                        );
                    }
                    unresolved_devices = refresh.unresolved.into_iter().collect();
                }
                Err(error) => {
                    log(
                        &sender,
                        LogLevel::Warning,
                        format!("Device table refresh failed: {error:#}"),
                    );
                }
            }

            let mut last_poll: HashMap<PointIdentity, Instant> = HashMap::new();
            let mut point_status: HashMap<PointIdentity, PointStatus> = HashMap::new();
            record_unresolved_failures(&sender, &points, &unresolved_devices, &mut point_status);
            let mut device_backoffs: HashMap<u32, DeviceBackoff> = HashMap::new();
            let mut last_resolve_attempt = Instant::now();
            let mut last_full_refresh = Instant::now();
            let mut last_health = Instant::now()
                .checked_sub(HEALTH_INTERVAL)
                .unwrap_or_else(Instant::now);
            let mut cycle_published = 0usize;
            let mut cycle_failed_reads = 0usize;
            let mut cycle_failed_publishes = 0usize;
            let mut reconnects = 0usize;
            let mut last_error: Option<String> = None;

            while !stop.load(Ordering::Relaxed) {
                let now = Instant::now();
                let mut refreshed_this_iteration = false;
                if last_full_refresh.elapsed() >= DEVICE_TABLE_KEEPALIVE_INTERVAL {
                    last_full_refresh = Instant::now();
                    last_resolve_attempt = Instant::now();
                    refreshed_this_iteration = true;
                    match proto.refresh_devices(&conn, &device_instances).await {
                        Ok(refresh) => {
                            let change = apply_refresh_state(
                                &mut unresolved_devices,
                                &mut device_backoffs,
                                &refresh,
                            );
                            emit_refresh_state_change(
                                &sender,
                                &points,
                                "device table keepalive",
                                change,
                                &mut point_status,
                            );
                        }
                        Err(error) => {
                            log(
                                &sender,
                                LogLevel::Warning,
                                format!("Device table keepalive failed: {error:#}"),
                            );
                        }
                    }
                }

                if !refreshed_this_iteration
                    && !unresolved_devices.is_empty()
                    && last_resolve_attempt.elapsed() >= DEVICE_RERESOLVE_INTERVAL
                {
                    last_resolve_attempt = Instant::now();
                    let targets: Vec<u32> = unresolved_devices.iter().copied().collect();
                    match proto.refresh_devices(&conn, &targets).await {
                        Ok(refresh) => {
                            let change = apply_refresh_state(
                                &mut unresolved_devices,
                                &mut device_backoffs,
                                &refresh,
                            );
                            emit_refresh_state_change(
                                &sender,
                                &points,
                                "device re-resolution",
                                change,
                                &mut point_status,
                            );
                        }
                        Err(error) => {
                            log(
                                &sender,
                                LogLevel::Warning,
                                format!("Device re-resolution failed: {error:#}"),
                            );
                        }
                    }
                }

                let due = due_points(
                    now,
                    &points,
                    &unresolved_devices,
                    &device_backoffs,
                    &last_poll,
                );

                if !due.is_empty() {
                    let polled_devices: HashSet<u32> =
                        due.iter().filter_map(device_instance).collect();
                    match proto.poll(&conn, &due).await {
                        Ok(outcome) => {
                            for message in update_device_backoffs(
                                &mut device_backoffs,
                                &polled_devices,
                                &outcome,
                                now,
                                backoff_max,
                                device_instance,
                            ) {
                                log(&sender, message.0, message.1);
                            }
                            for point in &due {
                                last_poll.insert(PointIdentity::from_point(point), now);
                            }
                            for warning in outcome.warnings {
                                log(&sender, LogLevel::Warning, warning);
                            }
                            cycle_failed_reads += outcome.failures.len();
                            for failure in &outcome.failures {
                                let id = PointIdentity::from_point(&failure.point);
                                let status = point_status.entry(id).or_default();
                                status.record_read_failure(&failure.error);
                            }
                            if !outcome.failures.is_empty() {
                                let _ = sender.send(WorkerEvent::Failures(outcome.failures));
                            }
                            if !outcome.samples.is_empty() {
                                let mut samples = outcome.samples;
                                for sample in &mut samples {
                                    sample.topic =
                                        crate::topic::telemetry_topic(&mqtt, &sample.point);
                                    let id = PointIdentity::from_point(&sample.point);
                                    let status = point_status.entry(id).or_default();
                                    status.record_sample(sample);
                                }
                                let stats = publish_samples(
                                    &sender,
                                    &mut publisher,
                                    &mqtt,
                                    &samples,
                                    &mut point_status,
                                );
                                cycle_published += stats.published;
                                cycle_failed_publishes += stats.failed;
                                reconnects = stats.reconnects;
                                if stats.last_error.is_some() {
                                    last_error = stats.last_error.clone();
                                }
                                let _ = sender.send(WorkerEvent::Samples(samples));
                                let _ = sender.send(WorkerEvent::PublishStatus(stats));
                            }
                        }
                        Err(error) => {
                            last_error = Some(error.to_string());
                            log(&sender, LogLevel::Error, format!("Poll failed: {error:#}"));
                        }
                    }
                }

                if last_health.elapsed() >= HEALTH_INTERVAL {
                    let stale_points = point_status.values().filter(|s| s.stale).count();
                    let snapshot = HealthSnapshot {
                        published: cycle_published,
                        failed_reads: cycle_failed_reads,
                        failed_publishes: cycle_failed_publishes,
                        stale_points,
                        reconnects,
                        last_error: last_error.clone(),
                    };
                    if let Err(error) = publish_health(&mut publisher, &mqtt, snapshot).await {
                        log(
                            &sender,
                            LogLevel::Warning,
                            format!("Health publish failed: {error:#}"),
                        );
                    }
                    last_health = Instant::now();
                    cycle_published = 0;
                    cycle_failed_reads = 0;
                    cycle_failed_publishes = 0;
                }

                tokio::time::sleep(POLL_TICK).await;
            }

            let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Stopping));
            tokio::time::sleep(CLIENT_STOP_TIMEOUT).await;
            let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Stopped));
        });
        if !completed {
            let _ = fail_sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Failed(
                "Worker thread crashed".into(),
            )));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;
    use crate::model::{PointConfig, RefreshOutcome};

    fn bacnet_point(device: u32, enabled: bool) -> PointConfig {
        let mut addressing = Addressing::new();
        addressing.insert("device_instance".into(), serde_json::json!(device));
        PointConfig {
            enabled,
            device_key: format!("d{device}"),
            addressing,
            ..PointConfig::default()
        }
    }

    #[test]
    fn apply_refresh_state_clears_resolved_and_tracks_new_unresolved() {
        let mut unresolved = HashSet::from([100u32, 200u32]);
        let mut backoffs = HashMap::from([(
            100u32,
            DeviceBackoff {
                delay: Duration::from_secs(10),
                until: Instant::now(),
            },
        )]);
        let refresh = RefreshOutcome {
            resolved: vec![100],
            unresolved: vec![300],
        };

        let change = apply_refresh_state(&mut unresolved, &mut backoffs, &refresh);

        assert!(!unresolved.contains(&100));
        assert!(unresolved.contains(&200));
        assert!(unresolved.contains(&300));
        assert_eq!(change.newly_resolved, vec![100]);
        assert!(change.newly_unresolved.contains(&300));
        assert!(!backoffs.contains_key(&100));
    }

    #[test]
    fn record_unresolved_failures_emits_failures_and_updates_status() {
        let (tx, rx) = unbounded();
        let points = vec![bacnet_point(42, true), bacnet_point(99, true)];
        let unresolved = HashSet::from([42u32]);
        let mut status = HashMap::new();

        record_unresolved_failures(&tx, &points, &unresolved, &mut status);

        match rx.try_recv().unwrap() {
            WorkerEvent::Failures(failures) => {
                assert_eq!(failures.len(), 1);
                assert!(failures[0].error.contains("not in I-Am cache"));
            }
            other => panic!("expected Failures, got {other:?}"),
        }
        let identity = PointIdentity::from_point(&points[0]);
        assert_eq!(status.get(&identity).unwrap().consecutive_failures, 1);
    }

    #[test]
    fn due_points_skips_backoff_until_window() {
        let now = Instant::now();
        let point = bacnet_point(100, true);
        let mut backoffs = HashMap::from([(
            100u32,
            DeviceBackoff {
                delay: Duration::from_secs(30),
                until: now + Duration::from_secs(30),
            },
        )]);
        let due = due_points(now, std::slice::from_ref(&point), &HashSet::new(), &backoffs, &HashMap::new());
        assert!(due.is_empty());

        backoffs.get_mut(&100).unwrap().until = now - Duration::from_secs(1);
        let due = due_points(now, std::slice::from_ref(&point), &HashSet::new(), &backoffs, &HashMap::new());
        assert_eq!(due.len(), 1);
    }

    #[test]
    fn due_points_respects_poll_interval() {
        let now = Instant::now();
        let mut point = bacnet_point(100, true);
        point.poll_interval_secs = 60;
        let mut last_poll = HashMap::new();
        last_poll.insert(PointIdentity::from_point(&point), now - Duration::from_secs(30));
        let due = due_points(now, std::slice::from_ref(&point), &HashSet::new(), &HashMap::new(), &last_poll);
        assert!(due.is_empty());

        last_poll.insert(PointIdentity::from_point(&point), now - Duration::from_secs(60));
        let due = due_points(now, std::slice::from_ref(&point), &HashSet::new(), &HashMap::new(), &last_poll);
        assert_eq!(due.len(), 1);
    }
}
