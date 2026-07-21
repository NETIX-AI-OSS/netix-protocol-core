//! Background worker: discovery, browse, bulk scan, and poll→publish loops.

mod backoff;
mod events;
mod runtime;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use proto_api::{Addressing, BrowseKind, Capabilities, DiscoveryKind};

pub use events::{RepublisherLifecycle, WorkerChannel, WorkerEvent, WorkerReceiver, WorkerSender};

use crate::config::{MqttConfig, PayloadFormat};
use crate::import::{merge_imported_points, point_from_discovered};
use crate::log::LogLevel;
use crate::model::{
    PointConfig, PointFailure, PointIdentity, PointSample, PointStatus, PublishStats,
};
use crate::mqtt::{publish_health, HealthSnapshot, RumqttPublisher};
use crate::protocol::{RepublishFactory, RepublishProtocol};

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
        record_unresolved_failures(sender, points, &change.newly_unresolved, point_status);
    }
}

fn publish_samples(
    sender: &Sender<WorkerEvent>,
    publisher: &mut RumqttPublisher,
    mqtt: &MqttConfig,
    samples: &[PointSample],
    point_status: &mut HashMap<PointIdentity, PointStatus>,
) -> PublishStats {
    if mqtt.payload_format == PayloadFormat::NetixEnvelope {
        return publish_envelope(sender, publisher, mqtt, samples, point_status);
    }
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
    stats.acked = publisher.acked_count();
    if stats.last_error.is_none() {
        stats.last_error = publisher
            .connection_fatal_error()
            .or_else(|| publisher.last_connection_error());
    }
    stats
}

/// Publish one `netix_envelope` message per device (grouping the batch's samples
/// by `device_key`): `{"reason","time","id","points":[{"pointName","data",
/// "status"}]}` on `<device_topic_prefix>/<id>/telemetry`. This matches the
/// envelope platform MQTT workers ingest, so a demo device's telemetry lands on
/// the historian tag `<id>-<pointName>`.
fn publish_envelope(
    sender: &Sender<WorkerEvent>,
    publisher: &mut RumqttPublisher,
    mqtt: &MqttConfig,
    samples: &[PointSample],
    point_status: &mut HashMap<PointIdentity, PointStatus>,
) -> PublishStats {
    let mut stats = PublishStats::empty();

    // Group by device id, preserving first-seen order.
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<&PointSample>> = HashMap::new();
    for sample in samples {
        stats.queued += 1;
        let id = envelope_device_id(&sample.point);
        groups
            .entry(id.clone())
            .or_insert_with(|| {
                order.push(id.clone());
                Vec::new()
            })
            .push(sample);
    }

    for id in &order {
        let group = &groups[id];
        let mut time_ms: i64 = 0;
        let points: Vec<serde_json::Value> = group
            .iter()
            .map(|sample| {
                time_ms = time_ms.max(sample.timestamp_ms);
                serde_json::json!({
                    "pointName": envelope_point_name(&sample.point),
                    "data": sample.value.to_string(),
                    "status": "ok",
                })
            })
            .collect();
        let envelope = serde_json::json!({
            "reason": "CHANGE_OF_VALUE",
            "time": time_ms,
            "id": id,
            "points": points,
        });
        let topic = crate::topic::device_envelope_topic(mqtt, id);
        let result = serde_json::to_vec(&envelope)
            .map_err(|error| error.to_string())
            .and_then(|payload| {
                publisher
                    .try_enqueue_sample(&topic, payload, mqtt.retain)
                    .map_err(|error| error.to_string())
            });
        match result {
            Ok(()) => {
                stats.published += group.len();
                for sample in group {
                    let identity = PointIdentity::from_point(&sample.point);
                    if let Some(status) = point_status.get_mut(&identity) {
                        status.record_publish_success();
                    }
                    let _ = sender.send(WorkerEvent::PointPublish {
                        identity,
                        error: None,
                    });
                }
            }
            Err(message) => {
                for sample in group {
                    stats.record_failure(message.clone());
                    let identity = PointIdentity::from_point(&sample.point);
                    if let Some(status) = point_status.get_mut(&identity) {
                        status.record_publish_failure(&message);
                    }
                    let _ = sender.send(WorkerEvent::PointPublish {
                        identity,
                        error: Some(message.clone()),
                    });
                }
            }
        }
    }

    stats.reconnects = publisher.reconnect_count();
    stats.acked = publisher.acked_count();
    if stats.last_error.is_none() {
        stats.last_error = publisher
            .connection_fatal_error()
            .or_else(|| publisher.last_connection_error());
    }
    stats
}

/// Envelope `id` for a point: its `device_key`, or a display-name fallback.
fn envelope_device_id(point: &PointConfig) -> String {
    let key = point.device_key.trim();
    if key.is_empty() {
        point.display_name()
    } else {
        key.to_string()
    }
}

/// Envelope `pointName` for a point: its `tag_path`, or a display-name fallback.
fn envelope_point_name(point: &PointConfig) -> String {
    let path = point.tag_path.trim();
    if path.is_empty() {
        point.display_name()
    } else {
        path.to_string()
    }
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
                        let stats = publish_samples(
                            &sender,
                            &mut publisher,
                            &mqtt,
                            &samples,
                            &mut point_status,
                        );
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

/// Whether an adapter can build a point set from discovery: it both discovers
/// devices *and* browses their points. Manual-only discovery or a no-op browse
/// cannot produce points, so `discover_on_start` has nothing to work with.
fn supports_discovery(caps: &Capabilities) -> bool {
    caps.discovery != DiscoveryKind::ManualOnly && caps.browse != BrowseKind::None
}

/// The lifecycle/log message when a start has no points to publish. Split out so
/// the exact wording is asserted by a unit test and stays identical in both the
/// `Warning` log and the `Failed` lifecycle event.
fn no_points_message(discover_on_start: bool, discovery_supported: bool) -> String {
    if discover_on_start {
        if discovery_supported {
            "discover_on_start=true but discovery found no pollable points; nothing to publish"
                .to_string()
        } else {
            "discover_on_start=true but the selected protocol does not support discovery; \
             nothing to publish"
                .to_string()
        }
    } else {
        "no enabled points and discover_on_start=false; nothing to publish".to_string()
    }
}

/// Discover devices, browse each, and build an in-memory, identity-faithful point
/// set — the runtime half of "run-from-discovery" (RCA §4 Fix B). Reuses the
/// shared browse→import path ([`point_from_discovered`] + [`merge_imported_points`]
/// for identity-keyed dedupe), so the points it builds carry the same
/// `device_key`/`tag_path` the GUI **Discover** button and the emitted config
/// produce. Returns the built points plus any discovery/browse warnings for the
/// caller to surface. Lets a connection-only config poll a self-describing BACnet
/// source with no hand-authored `config.toml` points.
pub async fn discover_points(
    proto: &dyn RepublishProtocol,
    conn: &Addressing,
) -> anyhow::Result<(Vec<PointConfig>, Vec<String>)> {
    let mut warnings = Vec::new();
    let discovered = proto.discover(conn).await?;
    warnings.extend(discovered.warnings);

    let mut imported: Vec<PointConfig> = Vec::new();
    for device in &discovered.devices {
        match proto.browse(conn, device).await {
            Ok(outcome) => {
                warnings.extend(outcome.warnings);
                for point in outcome.points {
                    imported.push(point_from_discovered(
                        &point,
                        crate::defaults::POLL_INTERVAL_SECS,
                    ));
                }
            }
            Err(error) => {
                warnings.push(format!("{}: browse failed: {error:#}", device.key));
            }
        }
    }

    // Identity-keyed dedupe via the shared merge (the same path the GUI bulk scan
    // uses), starting from an empty base so we get exactly the discovered set.
    let merged = merge_imported_points(&[], &imported);
    Ok((merged.points, warnings))
}

/// Run the continuous poll→publish loop until `stop` is set.
///
/// With no enabled points the worker no longer spins forever publishing nothing
/// (RCA #2/#4): if `discover_on_start` is set and the adapter supports discovery
/// it discovers→browses→builds a point set in memory and polls that; otherwise it
/// emits a loud `Warning` + `Failed` lifecycle event and stops.
pub fn spawn_republisher(
    sender: Sender<WorkerEvent>,
    factory: RepublishFactory,
    conn: Addressing,
    mqtt: MqttConfig,
    points: Vec<PointConfig>,
    discover_on_start: bool,
    stop: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let fail_sender = sender.clone();
        let completed = run_async(sender.clone(), async move {
            let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Starting));
            let proto = factory();
            let backoff_max = device_backoff_max(&conn);

            // Resolve the working point set BEFORE opening the broker link. With no
            // enabled points we either discover-then-poll (discover_on_start) or
            // fail loud — never enter the loop with an empty due set and publish
            // nothing silently (RCA #2/#4).
            let mut points = points;
            if !points.iter().any(|p| p.enabled) {
                let discovery_supported = supports_discovery(proto.capabilities());
                if discover_on_start && discovery_supported {
                    log(
                        &sender,
                        LogLevel::Info,
                        "No enabled points; discover_on_start=true — discovering devices to poll"
                            .to_string(),
                    );
                    match discover_points(proto.as_ref(), &conn).await {
                        Ok((discovered, warnings)) => {
                            for warning in warnings {
                                log(&sender, LogLevel::Warning, warning);
                            }
                            log(
                                &sender,
                                LogLevel::Info,
                                format!(
                                    "discover_on_start built {} point(s) from discovery",
                                    discovered.len()
                                ),
                            );
                            points = discovered;
                        }
                        Err(error) => {
                            let message = format!("discover_on_start discovery failed: {error:#}");
                            log(&sender, LogLevel::Error, message.clone());
                            let _ = sender.send(WorkerEvent::Lifecycle(
                                RepublisherLifecycle::Failed(message),
                            ));
                            return;
                        }
                    }
                }
                // Still nothing to publish: warn loud and fail the lifecycle rather
                // than looping forever over an empty point set.
                if !points.iter().any(|p| p.enabled) {
                    let message = no_points_message(discover_on_start, discovery_supported);
                    log(&sender, LogLevel::Warning, message.clone());
                    let _ = sender
                        .send(WorkerEvent::Lifecycle(RepublisherLifecycle::Failed(message)));
                    return;
                }
            }

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
            let mut acked = 0usize;
            let mut last_error: Option<String> = None;
            // A broker auth/config rejection (bad password, not authorized) never
            // self-heals, so warn the operator once instead of publishing into a
            // channel that will never be delivered — the "looks healthy, delivers
            // nothing" failure this RCA targets.
            let mut fatal_reported = false;

            while !stop.load(Ordering::Relaxed) {
                let now = Instant::now();

                if !fatal_reported {
                    if let Some(message) = publisher.connection_fatal_error() {
                        // Warn (not Failed): the worker keeps running so the broker
                        // link can recover if credentials are fixed, but the error
                        // is now loud in the log, health payload and last_error —
                        // and the `acked` counter stays at zero so the box no longer
                        // looks healthy while delivering nothing.
                        log(
                            &sender,
                            LogLevel::Warning,
                            format!("MQTT connection rejected by broker: {message}"),
                        );
                        last_error = Some(message);
                        fatal_reported = true;
                    }
                }

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
                                acked = stats.acked;
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
                        acked,
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
    use crate::model::{
        BrowseOutcome, DiscoverOutcome, DiscoveredDevice, PointConfig, PollOutcome, RefreshOutcome,
    };
    use crossbeam_channel::unbounded;
    use std::sync::OnceLock;

    /// A minimal manual-only adapter: no discovery, no browse. Used to exercise
    /// the zero-points startup paths without any network or MQTT dependency.
    struct ManualProto;

    fn manual_caps() -> &'static Capabilities {
        static CAPS: OnceLock<Capabilities> = OnceLock::new();
        CAPS.get_or_init(|| Capabilities {
            id: "manual",
            display_name: "Manual",
            discovery: DiscoveryKind::ManualOnly,
            browse: BrowseKind::None,
            connection_fields: Vec::new(),
            addressing_fields: Vec::new(),
            default_port: 0,
        })
    }

    #[async_trait::async_trait]
    impl RepublishProtocol for ManualProto {
        fn capabilities(&self) -> &Capabilities {
            manual_caps()
        }
        async fn discover(&self, _conn: &Addressing) -> anyhow::Result<DiscoverOutcome> {
            Ok(DiscoverOutcome::default())
        }
        async fn browse(
            &self,
            _conn: &Addressing,
            _device: &DiscoveredDevice,
        ) -> anyhow::Result<BrowseOutcome> {
            Ok(BrowseOutcome::default())
        }
        async fn poll(
            &self,
            _conn: &Addressing,
            _points: &[PointConfig],
        ) -> anyhow::Result<PollOutcome> {
            Ok(PollOutcome::default())
        }
    }

    fn manual_factory() -> Box<dyn RepublishProtocol> {
        Box::new(ManualProto)
    }

    /// Drain worker events until a `Failed` lifecycle arrives (or timeout).
    fn wait_for_failed(rx: &crossbeam_channel::Receiver<WorkerEvent>) -> Option<String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(WorkerEvent::Lifecycle(RepublisherLifecycle::Failed(message))) => {
                    return Some(message)
                }
                Ok(_) => continue,
                Err(_) => continue,
            }
        }
        None
    }

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
        let due = due_points(
            now,
            std::slice::from_ref(&point),
            &HashSet::new(),
            &backoffs,
            &HashMap::new(),
        );
        assert!(due.is_empty());

        backoffs.get_mut(&100).unwrap().until = now - Duration::from_secs(1);
        let due = due_points(
            now,
            std::slice::from_ref(&point),
            &HashSet::new(),
            &backoffs,
            &HashMap::new(),
        );
        assert_eq!(due.len(), 1);
    }

    #[test]
    fn due_points_respects_poll_interval() {
        let now = Instant::now();
        let mut point = bacnet_point(100, true);
        point.poll_interval_secs = 60;
        let mut last_poll = HashMap::new();
        last_poll.insert(
            PointIdentity::from_point(&point),
            now - Duration::from_secs(30),
        );
        let due = due_points(
            now,
            std::slice::from_ref(&point),
            &HashSet::new(),
            &HashMap::new(),
            &last_poll,
        );
        assert!(due.is_empty());

        last_poll.insert(
            PointIdentity::from_point(&point),
            now - Duration::from_secs(60),
        );
        let due = due_points(
            now,
            std::slice::from_ref(&point),
            &HashSet::new(),
            &HashMap::new(),
            &last_poll,
        );
        assert_eq!(due.len(), 1);
    }

    #[test]
    fn supports_discovery_requires_discovery_and_browse() {
        // A manual-only, no-browse adapter cannot build points from discovery.
        assert!(!supports_discovery(manual_caps()));

        let broadcast = Capabilities {
            discovery: DiscoveryKind::Broadcast,
            browse: BrowseKind::ObjectList,
            ..manual_caps().clone()
        };
        assert!(supports_discovery(&broadcast));

        // Discovery without browse still can't produce points.
        let no_browse = Capabilities {
            discovery: DiscoveryKind::Broadcast,
            browse: BrowseKind::None,
            ..manual_caps().clone()
        };
        assert!(!supports_discovery(&no_browse));
    }

    #[test]
    fn no_points_message_wording_is_stable() {
        // The false/no-discover message matches the RCA-specified wording exactly.
        let off = no_points_message(false, true);
        assert!(off.contains("no enabled points and discover_on_start=false"), "{off}");

        assert!(no_points_message(true, false).contains("does not support discovery"));
        assert!(no_points_message(true, true).contains("found no pollable points"));

        for message in [
            no_points_message(false, false),
            no_points_message(true, false),
            no_points_message(true, true),
        ] {
            assert!(message.contains("nothing to publish"), "{message}");
        }
    }

    #[test]
    fn zero_points_without_discover_fails_loud() {
        // No enabled points + discover_on_start=false must emit a Failed lifecycle
        // event (not spin forever). The empty-points check short-circuits before
        // the MQTT publisher is created, so no broker is needed here.
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        spawn_republisher(
            tx,
            manual_factory,
            Addressing::new(),
            MqttConfig::default(),
            Vec::new(),
            false,
            Arc::clone(&stop),
        );

        let message = wait_for_failed(&rx).expect("expected Failed lifecycle for zero points");
        stop.store(true, Ordering::Relaxed);
        assert!(
            message.contains("no enabled points and discover_on_start=false"),
            "got: {message}"
        );
        assert!(message.contains("nothing to publish"), "got: {message}");
    }

    #[test]
    fn zero_points_with_discover_but_no_devices_fails_loud() {
        // discover_on_start=true against a manual-only adapter (no discovery) also
        // fails loud rather than spinning — surfacing that the protocol can't
        // self-describe.
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        spawn_republisher(
            tx,
            manual_factory,
            Addressing::new(),
            MqttConfig::default(),
            Vec::new(),
            true,
            Arc::clone(&stop),
        );

        let message = wait_for_failed(&rx).expect("expected Failed lifecycle");
        stop.store(true, Ordering::Relaxed);
        assert!(
            message.contains("does not support discovery"),
            "got: {message}"
        );
    }
}
