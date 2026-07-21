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
use crate::mqtt::{publish_health, HealthSnapshot, MqttPublisher, RumqttPublisher};
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

/// The cadence knobs of the continuous republisher loop. Extracted so tests can
/// drive [`run_republisher`] with tiny intervals while production keeps the real
/// timings via [`LoopIntervals::default`] (the compile-time consts above).
struct LoopIntervals {
    /// How often to publish a health snapshot.
    health: Duration,
    /// How often to retry resolving still-unresolved devices.
    reresolve: Duration,
    /// How often to do a full device-table keepalive refresh.
    keepalive: Duration,
    /// Sleep between poll cycles.
    poll_tick: Duration,
    /// Grace period after `Stopping` before emitting `Stopped`.
    client_stop_timeout: Duration,
}

impl Default for LoopIntervals {
    fn default() -> Self {
        Self {
            health: HEALTH_INTERVAL,
            reresolve: DEVICE_RERESOLVE_INTERVAL,
            keepalive: DEVICE_TABLE_KEEPALIVE_INTERVAL,
            poll_tick: POLL_TICK,
            client_stop_timeout: CLIENT_STOP_TIMEOUT,
        }
    }
}

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

async fn publish_samples<P: MqttPublisher + Send>(
    sender: &Sender<WorkerEvent>,
    publisher: &mut P,
    mqtt: &MqttConfig,
    samples: &[PointSample],
    point_status: &mut HashMap<PointIdentity, PointStatus>,
) -> PublishStats {
    if mqtt.payload_format == PayloadFormat::NetixEnvelope {
        return publish_envelope(sender, publisher, mqtt, samples, point_status).await;
    }
    let mut stats = PublishStats::empty();
    for sample in samples {
        stats.queued += 1;
        let identity = PointIdentity::from_point(&sample.point);
        // as_json_value() yields a serde_json::Value; serializing one is infallible.
        let payload = serde_json::to_vec(&sample.value.as_json_value())
            .expect("serde_json::Value always serializes");
        match publisher.publish(&sample.topic, payload, mqtt.retain).await {
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
async fn publish_envelope<P: MqttPublisher + Send>(
    sender: &Sender<WorkerEvent>,
    publisher: &mut P,
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
        // envelope is a serde_json::Value; serializing one is infallible.
        let payload = serde_json::to_vec(&envelope).expect("serde_json::Value always serializes");
        let result = publisher
            .publish(&topic, payload, mqtt.retain)
            .await
            .map_err(|error| error.to_string());
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
                        )
                        .await;
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
                    let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Failed(
                        message,
                    )));
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
            run_republisher(
                &sender,
                proto.as_ref(),
                &conn,
                &mqtt,
                &points,
                &stop,
                &mut publisher,
                LoopIntervals::default(),
            )
            .await;
        });
        if !completed {
            let _ = fail_sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Failed(
                "Worker thread crashed".into(),
            )));
        }
    });
}

/// The continuous poll→publish loop, given an already-resolved (non-empty) point
/// set and an already-built publisher. Emits `Running`, primes the device table,
/// then loops on `intervals` until `stop` is set, closing with `Stopping`/`Stopped`.
///
/// Split out of [`spawn_republisher`] so the loop is testable without a broker: it
/// is generic over the [`MqttPublisher`] trait (production passes the real
/// `RumqttPublisher`) and takes its cadence via [`LoopIntervals`] (production uses
/// [`LoopIntervals::default`], i.e. the module consts — unchanged behavior).
#[allow(clippy::too_many_arguments)]
async fn run_republisher<P: MqttPublisher + Send>(
    sender: &Sender<WorkerEvent>,
    proto: &dyn RepublishProtocol,
    conn: &Addressing,
    mqtt: &MqttConfig,
    points: &[PointConfig],
    stop: &AtomicBool,
    publisher: &mut P,
    intervals: LoopIntervals,
) {
    let backoff_max = device_backoff_max(conn);
    let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Running));

    let device_instances = unique_device_instances(points);
    let mut unresolved_devices: HashSet<u32> = HashSet::new();
    match proto.refresh_devices(conn, &device_instances).await {
        Ok(refresh) => {
            if !refresh.unresolved.is_empty() {
                log(
                    sender,
                    LogLevel::Warning,
                    format!(
                        "{} of {} device(s) not in I-Am cache; their points will be skipped (resolution retried every {}s)",
                        refresh.unresolved.len(),
                        device_instances.len(),
                        intervals.reresolve.as_secs()
                    ),
                );
            }
            unresolved_devices = refresh.unresolved.into_iter().collect();
        }
        Err(error) => {
            log(
                sender,
                LogLevel::Warning,
                format!("Device table refresh failed: {error:#}"),
            );
        }
    }

    let mut last_poll: HashMap<PointIdentity, Instant> = HashMap::new();
    let mut point_status: HashMap<PointIdentity, PointStatus> = HashMap::new();
    record_unresolved_failures(sender, points, &unresolved_devices, &mut point_status);
    let mut device_backoffs: HashMap<u32, DeviceBackoff> = HashMap::new();
    let mut last_resolve_attempt = Instant::now();
    let mut last_full_refresh = Instant::now();
    let mut last_health = Instant::now()
        .checked_sub(intervals.health)
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
                    sender,
                    LogLevel::Warning,
                    format!("MQTT connection rejected by broker: {message}"),
                );
                last_error = Some(message);
                fatal_reported = true;
            }
        }

        let mut refreshed_this_iteration = false;
        if last_full_refresh.elapsed() >= intervals.keepalive {
            last_full_refresh = Instant::now();
            last_resolve_attempt = Instant::now();
            refreshed_this_iteration = true;
            match proto.refresh_devices(conn, &device_instances).await {
                Ok(refresh) => {
                    let change = apply_refresh_state(
                        &mut unresolved_devices,
                        &mut device_backoffs,
                        &refresh,
                    );
                    emit_refresh_state_change(
                        sender,
                        points,
                        "device table keepalive",
                        change,
                        &mut point_status,
                    );
                }
                Err(error) => {
                    log(
                        sender,
                        LogLevel::Warning,
                        format!("Device table keepalive failed: {error:#}"),
                    );
                }
            }
        }

        if !refreshed_this_iteration
            && !unresolved_devices.is_empty()
            && last_resolve_attempt.elapsed() >= intervals.reresolve
        {
            last_resolve_attempt = Instant::now();
            let targets: Vec<u32> = unresolved_devices.iter().copied().collect();
            match proto.refresh_devices(conn, &targets).await {
                Ok(refresh) => {
                    let change = apply_refresh_state(
                        &mut unresolved_devices,
                        &mut device_backoffs,
                        &refresh,
                    );
                    emit_refresh_state_change(
                        sender,
                        points,
                        "device re-resolution",
                        change,
                        &mut point_status,
                    );
                }
                Err(error) => {
                    log(
                        sender,
                        LogLevel::Warning,
                        format!("Device re-resolution failed: {error:#}"),
                    );
                }
            }
        }

        let due = due_points(
            now,
            points,
            &unresolved_devices,
            &device_backoffs,
            &last_poll,
        );

        if !due.is_empty() {
            let polled_devices: HashSet<u32> = due.iter().filter_map(device_instance).collect();
            match proto.poll(conn, &due).await {
                Ok(outcome) => {
                    for message in update_device_backoffs(
                        &mut device_backoffs,
                        &polled_devices,
                        &outcome,
                        now,
                        backoff_max,
                        device_instance,
                    ) {
                        log(sender, message.0, message.1);
                    }
                    for point in &due {
                        last_poll.insert(PointIdentity::from_point(point), now);
                    }
                    for warning in outcome.warnings {
                        log(sender, LogLevel::Warning, warning);
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
                            sample.topic = crate::topic::telemetry_topic(mqtt, &sample.point);
                            let id = PointIdentity::from_point(&sample.point);
                            let status = point_status.entry(id).or_default();
                            status.record_sample(sample);
                        }
                        let stats =
                            publish_samples(sender, publisher, mqtt, &samples, &mut point_status)
                                .await;
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
                    log(sender, LogLevel::Error, format!("Poll failed: {error:#}"));
                }
            }
        }

        if last_health.elapsed() >= intervals.health {
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
            if let Err(error) = publish_health(publisher, mqtt, snapshot).await {
                log(
                    sender,
                    LogLevel::Warning,
                    format!("Health publish failed: {error:#}"),
                );
            }
            last_health = Instant::now();
            cycle_published = 0;
            cycle_failed_reads = 0;
            cycle_failed_publishes = 0;
        }

        tokio::time::sleep(intervals.poll_tick).await;
    }

    let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Stopping));
    tokio::time::sleep(intervals.client_stop_timeout).await;
    let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Stopped));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PayloadFormat;
    use crate::model::{
        BrowseOutcome, DiscoverOutcome, DiscoveredDevice, DiscoveredPoint, PointConfig,
        PollOutcome, RefreshOutcome, TelemetryValue,
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
    fn record_unresolved_failures_noop_when_no_enabled_point_matches() {
        // The unresolved set is non-empty, but no enabled point references those
        // devices -> nothing is emitted and no status is touched.
        let (tx, rx) = unbounded();
        let points = vec![bacnet_point(1, true)]; // device 1, but 999 is unresolved
        let unresolved = HashSet::from([999u32]);
        let mut status = HashMap::new();

        record_unresolved_failures(&tx, &points, &unresolved, &mut status);

        assert!(rx.try_recv().is_err(), "no Failures should be emitted");
        assert!(status.is_empty());
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
        assert!(
            off.contains("no enabled points and discover_on_start=false"),
            "{off}"
        );

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

    // ---- Pure-helper coverage -------------------------------------------------

    #[test]
    fn device_instance_parses_number_string_and_rejects_others() {
        // Numeric addressing value.
        let mut num = Addressing::new();
        num.insert("device_instance".into(), serde_json::json!(7));
        let point = PointConfig {
            addressing: num,
            ..PointConfig::default()
        };
        assert_eq!(device_instance(&point), Some(7));

        // String addressing value (trimmed + parsed).
        let mut text = Addressing::new();
        text.insert("device_instance".into(), serde_json::json!("  42 "));
        let point = PointConfig {
            addressing: text,
            ..PointConfig::default()
        };
        assert_eq!(device_instance(&point), Some(42));

        // Unparseable string -> None.
        let mut bad = Addressing::new();
        bad.insert("device_instance".into(), serde_json::json!("not-a-number"));
        let point = PointConfig {
            addressing: bad,
            ..PointConfig::default()
        };
        assert_eq!(device_instance(&point), None);

        // Non-number/non-string JSON -> None.
        let mut arr = Addressing::new();
        arr.insert("device_instance".into(), serde_json::json!([1, 2]));
        let point = PointConfig {
            addressing: arr,
            ..PointConfig::default()
        };
        assert_eq!(device_instance(&point), None);

        // Missing key -> None.
        assert_eq!(device_instance(&PointConfig::default()), None);
    }

    #[test]
    fn device_backoff_max_reads_number_string_and_default_with_clamp() {
        // Numeric override, clamped to a floor of 10s.
        let mut conn = Addressing::new();
        conn.insert("device_backoff_max_secs".into(), serde_json::json!(120));
        assert_eq!(device_backoff_max(&conn), Duration::from_secs(120));

        let mut low = Addressing::new();
        low.insert("device_backoff_max_secs".into(), serde_json::json!(1));
        assert_eq!(device_backoff_max(&low), Duration::from_secs(10));

        // String override.
        let mut text = Addressing::new();
        text.insert("device_backoff_max_secs".into(), serde_json::json!("45"));
        assert_eq!(device_backoff_max(&text), Duration::from_secs(45));

        // Unparseable string falls back to 300 (then clamped, still 300).
        let mut bad = Addressing::new();
        bad.insert("device_backoff_max_secs".into(), serde_json::json!("oops"));
        assert_eq!(device_backoff_max(&bad), Duration::from_secs(300));

        // Missing key -> the compile-time default.
        assert_eq!(
            device_backoff_max(&Addressing::new()),
            DEFAULT_DEVICE_BACKOFF_MAX
        );
    }

    #[test]
    fn unique_device_instances_dedupes_sorts_and_skips_disabled() {
        let points = vec![
            bacnet_point(30, true),
            bacnet_point(10, true),
            bacnet_point(30, true),  // duplicate
            bacnet_point(20, false), // disabled -> excluded
        ];
        assert_eq!(unique_device_instances(&points), vec![10, 30]);
    }

    #[test]
    fn envelope_id_and_point_name_fall_back_to_display_name() {
        // Populated device_key / tag_path are used verbatim.
        let mut addressing = Addressing::new();
        addressing.insert("object_instance".into(), serde_json::json!(3));
        let point = PointConfig {
            device_key: "AHU-1".into(),
            tag_path: "AHU-1/SupplyTemp".into(),
            addressing: addressing.clone(),
            ..PointConfig::default()
        };
        assert_eq!(envelope_device_id(&point), "AHU-1");
        assert_eq!(envelope_point_name(&point), "AHU-1/SupplyTemp");

        // Blank device_key / tag_path fall back to the display name.
        let blank = PointConfig {
            device_key: "  ".into(),
            tag_path: "  ".into(),
            addressing,
            ..PointConfig::default()
        };
        let display = blank.display_name();
        assert_eq!(envelope_device_id(&blank), display);
        assert_eq!(envelope_point_name(&blank), display);
    }

    #[test]
    fn emit_refresh_state_change_logs_both_transitions_and_records_failures() {
        let (tx, rx) = unbounded();
        let points = vec![bacnet_point(500, true)];
        let change = RefreshStateChange {
            newly_resolved: vec![100],
            newly_unresolved: HashSet::from([500u32]),
        };
        let mut status = HashMap::new();

        emit_refresh_state_change(&tx, &points, "keepalive", change, &mut status);

        let mut saw_resolved_log = false;
        let mut saw_unresolved_log = false;
        let mut saw_failures = false;
        while let Ok(event) = rx.try_recv() {
            match event {
                WorkerEvent::Log(LogLevel::Info, message)
                    if message.contains("resolved during") =>
                {
                    saw_resolved_log = true;
                }
                WorkerEvent::Log(LogLevel::Warning, message)
                    if message.contains("unresolved during") =>
                {
                    saw_unresolved_log = true;
                }
                WorkerEvent::Failures(failures) => {
                    saw_failures = failures
                        .iter()
                        .any(|f| f.error.contains("not in I-Am cache"));
                }
                _ => {}
            }
        }
        assert!(saw_resolved_log, "expected a resolved Info log");
        assert!(saw_unresolved_log, "expected an unresolved Warning log");
        assert!(
            saw_failures,
            "expected Failures for the newly-unresolved device"
        );
        // The unresolved point's status got a recorded read failure.
        let id = PointIdentity::from_point(&points[0]);
        assert_eq!(status.get(&id).unwrap().consecutive_failures, 1);
    }

    // ---- A scripted fake protocol driven via `conn` flags ---------------------
    //
    // The republisher factory type is a bare `fn()` pointer that cannot capture
    // state, so scenario configuration is threaded through the `conn` Addressing
    // (which the worker forwards to every protocol call). This keeps a single
    // fake + factory yet lets each test pick discover/browse/poll/refresh
    // behavior race-free (each call gets its own `conn`).
    struct ScriptedProto;

    fn scripted_caps() -> &'static Capabilities {
        static CAPS: OnceLock<Capabilities> = OnceLock::new();
        CAPS.get_or_init(|| Capabilities {
            id: "scripted",
            display_name: "Scripted",
            discovery: DiscoveryKind::Broadcast,
            browse: BrowseKind::ObjectList,
            connection_fields: Vec::new(),
            addressing_fields: Vec::new(),
            default_port: 0,
        })
    }

    fn flag(conn: &Addressing, key: &str) -> bool {
        conn.get(key) == Some(&serde_json::json!(true))
    }

    fn count(conn: &Addressing, key: &str, default: usize) -> usize {
        match conn.get(key) {
            Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(default as u64) as usize,
            _ => default,
        }
    }

    fn u32_list(conn: &Addressing, key: &str) -> Vec<u32> {
        match conn.get(key) {
            Some(serde_json::Value::Array(values)) => values
                .iter()
                .filter_map(|v| v.as_u64().map(|n| n as u32))
                .collect(),
            _ => Vec::new(),
        }
    }

    #[async_trait::async_trait]
    impl RepublishProtocol for ScriptedProto {
        fn capabilities(&self) -> &Capabilities {
            scripted_caps()
        }

        async fn discover(&self, conn: &Addressing) -> anyhow::Result<DiscoverOutcome> {
            if flag(conn, "discover_err") {
                anyhow::bail!("discover boom");
            }
            let mut devices = Vec::new();
            for i in 0..count(conn, "devices", 0) {
                devices.push(DiscoveredDevice {
                    key: format!("d{i}"),
                    instance: Some(1000 + i as u32),
                    address: "127.0.0.1".into(),
                    detail: String::new(),
                });
            }
            if flag(conn, "bad_device") {
                devices.push(DiscoveredDevice {
                    key: "d-bad".into(),
                    instance: Some(9000),
                    address: "x".into(),
                    detail: String::new(),
                });
            }
            let mut warnings = Vec::new();
            if flag(conn, "discover_warn") {
                warnings.push("discover warning".into());
            }
            Ok(DiscoverOutcome { devices, warnings })
        }

        async fn browse(
            &self,
            conn: &Addressing,
            device: &DiscoveredDevice,
        ) -> anyhow::Result<BrowseOutcome> {
            if device.key == "d-bad" || flag(conn, "browse_err") {
                anyhow::bail!("browse boom for {}", device.key);
            }
            let mut points = Vec::new();
            for i in 0..count(conn, "browse_points", 1) {
                let mut addressing = Addressing::new();
                addressing.insert(
                    "device_instance".into(),
                    serde_json::json!(device.instance.unwrap_or(0)),
                );
                addressing.insert("object_instance".into(), serde_json::json!(i));
                points.push(DiscoveredPoint {
                    device_key: device.key.clone(),
                    name: Some(format!("pt{i}")),
                    description: None,
                    units: None,
                    value: None,
                    addressing,
                    suggested_tag_path: format!("{}/pt{i}", device.key),
                });
            }
            let mut warnings = Vec::new();
            if flag(conn, "browse_warn") {
                warnings.push("browse warning".into());
            }
            Ok(BrowseOutcome { points, warnings })
        }

        async fn poll(
            &self,
            conn: &Addressing,
            points: &[PointConfig],
        ) -> anyhow::Result<PollOutcome> {
            if flag(conn, "poll_err") {
                anyhow::bail!("poll boom");
            }
            let mut samples = Vec::new();
            let mut failures = Vec::new();
            for (i, point) in points.iter().enumerate() {
                if flag(conn, "poll_fail_points") {
                    failures.push(PointFailure {
                        point: point.clone(),
                        error: "read timeout".into(),
                    });
                } else {
                    samples.push(PointSample {
                        point: point.clone(),
                        value: TelemetryValue::Number(i as f64),
                        topic: String::new(),
                        timestamp_ms: 1000 + i as i64,
                    });
                }
            }
            let mut warnings = Vec::new();
            if flag(conn, "poll_warn") {
                warnings.push("poll warning".into());
            }
            Ok(PollOutcome {
                samples,
                failures,
                warnings,
            })
        }

        async fn refresh_devices(
            &self,
            conn: &Addressing,
            device_instances: &[u32],
        ) -> anyhow::Result<RefreshOutcome> {
            if flag(conn, "refresh_err") {
                anyhow::bail!("refresh boom");
            }
            let unresolved = u32_list(conn, "unresolved");
            let resolved = device_instances
                .iter()
                .copied()
                .filter(|i| !unresolved.contains(i))
                .collect();
            Ok(RefreshOutcome {
                resolved,
                unresolved,
            })
        }
    }

    fn scripted_factory() -> Box<dyn RepublishProtocol> {
        Box::new(ScriptedProto)
    }

    /// An MQTT config that never reaches a broker: the event loop stays in
    /// connect/backoff so `publish` only fills the outbound channel.
    fn offline_mqtt(format: PayloadFormat) -> MqttConfig {
        MqttConfig {
            host: "127.0.0.1".into(),
            port: 1,
            use_tls: false,
            payload_format: format,
            ..MqttConfig::default()
        }
    }

    // ---- A broker-free MqttPublisher fake for driving run_republisher ---------
    //
    // Records the topics it publishes and can be told to fail every publish
    // (exercising the loop's publish-failure and health-failure branches) or to
    // report a fatal broker rejection (the "connection rejected" branch) — none of
    // which need a real broker.
    #[derive(Default)]
    struct FakePublisher {
        published: Vec<String>,
        fail: bool,
        fatal: Option<String>,
    }

    impl MqttPublisher for FakePublisher {
        fn publish<'a>(
            &'a mut self,
            topic: &'a str,
            _payload: Vec<u8>,
            _retain: bool,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>>
        {
            Box::pin(async move {
                if self.fail {
                    anyhow::bail!("fake publish failed");
                }
                self.published.push(topic.to_string());
                Ok(())
            })
        }

        fn connection_fatal_error(&self) -> Option<String> {
            self.fatal.clone()
        }
    }

    /// Tiny loop cadences so the whole loop (health, keepalive, re-resolve, poll,
    /// shutdown grace) exercises in milliseconds instead of minutes.
    fn tiny_intervals() -> LoopIntervals {
        LoopIntervals {
            health: Duration::from_millis(5),
            reresolve: Duration::from_millis(2),
            keepalive: Duration::from_millis(3),
            poll_tick: Duration::from_millis(1),
            client_stop_timeout: Duration::from_millis(1),
        }
    }

    /// Drain the worker channel (non-blocking) into a Vec until `done` matches an
    /// event or the deadline passes, then flip `stop` so a concurrently-`join!`ed
    /// [`run_republisher`] returns. The short async sleep yields to that loop
    /// future on the current-thread runtime.
    async fn drive_until(
        rx: &crossbeam_channel::Receiver<WorkerEvent>,
        stop: &AtomicBool,
        mut done: impl FnMut(&WorkerEvent) -> bool,
    ) -> Vec<WorkerEvent> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut events = Vec::new();
        loop {
            while let Ok(event) = rx.try_recv() {
                let matched = done(&event);
                events.push(event);
                if matched {
                    stop.store(true, Ordering::Relaxed);
                    return events;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                stop.store(true, Ordering::Relaxed);
                return events;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// A protocol whose device-table refresh reports the targets unresolved on the
    /// first call, then on every later call either resolves them (recovery) or
    /// errors — driving the loop's keepalive / re-resolution success and failure
    /// branches with an observable state transition.
    struct RefreshScript {
        calls: std::sync::atomic::AtomicUsize,
        fail_after_first: bool,
    }

    impl RefreshScript {
        fn recovering() -> Self {
            Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                fail_after_first: false,
            }
        }
        fn failing() -> Self {
            Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                fail_after_first: true,
            }
        }
    }

    #[async_trait::async_trait]
    impl RepublishProtocol for RefreshScript {
        fn capabilities(&self) -> &Capabilities {
            scripted_caps()
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
        async fn refresh_devices(
            &self,
            _conn: &Addressing,
            device_instances: &[u32],
        ) -> anyhow::Result<RefreshOutcome> {
            let n = self.calls.fetch_add(1, Ordering::Relaxed);
            if n == 0 {
                Ok(RefreshOutcome {
                    resolved: Vec::new(),
                    unresolved: device_instances.to_vec(),
                })
            } else if self.fail_after_first {
                anyhow::bail!("refresh boom")
            } else {
                Ok(RefreshOutcome {
                    resolved: device_instances.to_vec(),
                    unresolved: Vec::new(),
                })
            }
        }
    }

    // ---- run_republisher loop coverage (fake publisher, tiny intervals) -------

    #[tokio::test]
    async fn run_republisher_publishes_samples_and_health_then_stops() {
        let (tx, rx) = unbounded();
        let stop = AtomicBool::new(false);
        let mut publisher = FakePublisher::default();
        let conn = Addressing::new();
        let mqtt = offline_mqtt(PayloadFormat::Scalar);
        let points = vec![bacnet_point(10, true)];

        let (_, mut events) = tokio::join!(
            run_republisher(
                &tx,
                &ScriptedProto,
                &conn,
                &mqtt,
                &points,
                &stop,
                &mut publisher,
                tiny_intervals(),
            ),
            drive_until(&rx, &stop, |e| matches!(e, WorkerEvent::PublishStatus(_))),
        );
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        assert!(is_running(&events), "expected a Running lifecycle");
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Samples(s) if !s.is_empty())));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::PointPublish { error: None, .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::PublishStatus(_))));
        // Health published (on the first tick) to the configured health topic, and
        // at least one telemetry publish to a different topic.
        assert!(
            publisher.published.contains(&mqtt.health_topic),
            "expected a health publish, got: {:?}",
            publisher.published
        );
        assert!(publisher.published.iter().any(|t| *t != mqtt.health_topic));
        // Graceful shutdown tail after stop.
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Lifecycle(RepublisherLifecycle::Stopping))));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Lifecycle(RepublisherLifecycle::Stopped))));
    }

    #[tokio::test]
    async fn run_republisher_records_publish_and_health_failures() {
        let (tx, rx) = unbounded();
        let stop = AtomicBool::new(false);
        // Every publish fails: sample publishes AND the health publish.
        let mut publisher = FakePublisher {
            fail: true,
            ..FakePublisher::default()
        };
        let conn = Addressing::new();
        let mqtt = offline_mqtt(PayloadFormat::Scalar);
        let points = vec![bacnet_point(10, true)];

        let (_, mut events) = tokio::join!(
            run_republisher(
                &tx,
                &ScriptedProto,
                &conn,
                &mqtt,
                &points,
                &stop,
                &mut publisher,
                tiny_intervals(),
            ),
            drive_until(&rx, &stop, |e| matches!(e, WorkerEvent::PublishStatus(_))),
        );
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        // Publish failure path: a failed PointPublish and a non-zero failure count.
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::PointPublish { error: Some(_), .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::PublishStatus(s) if s.failed > 0)));
        // Health publish failure is logged (non-fatal).
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::Log(LogLevel::Warning, m) if m.contains("Health publish failed")
        )));
    }

    #[tokio::test]
    async fn run_republisher_reresolves_unresolved_device() {
        let (tx, rx) = unbounded();
        let stop = AtomicBool::new(false);
        let mut publisher = FakePublisher::default();
        let conn = Addressing::new();
        let mqtt = offline_mqtt(PayloadFormat::Scalar);
        let points = vec![bacnet_point(42, true)];
        let proto = RefreshScript::recovering();
        // Keepalive far away so the re-resolution branch (not the full keepalive)
        // is what fires and recovers device 42.
        let intervals = LoopIntervals {
            keepalive: Duration::from_secs(60),
            ..tiny_intervals()
        };

        let (_, mut events) = tokio::join!(
            run_republisher(
                &tx,
                &proto,
                &conn,
                &mqtt,
                &points,
                &stop,
                &mut publisher,
                intervals,
            ),
            drive_until(&rx, &stop, |e| matches!(
                e,
                WorkerEvent::Log(LogLevel::Info, m) if m.contains("resolved during device re-resolution")
            )),
        );
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        assert!(is_running(&events));
        // Initial refresh reported device 42 unresolved: a warning + Failures.
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::Log(LogLevel::Warning, m) if m.contains("not in I-Am cache")
        )));
        assert!(events.iter().any(|e| matches!(e, WorkerEvent::Failures(f)
            if f.iter().any(|x| x.error.contains("not in I-Am cache")))));
        // Re-resolution then recovered it.
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::Log(LogLevel::Info, m) if m.contains("resolved during device re-resolution")
        )));
    }

    #[tokio::test]
    async fn run_republisher_keepalive_refresh_recovers_device() {
        let (tx, rx) = unbounded();
        let stop = AtomicBool::new(false);
        let mut publisher = FakePublisher::default();
        let conn = Addressing::new();
        let mqtt = offline_mqtt(PayloadFormat::Scalar);
        let points = vec![bacnet_point(42, true)];
        let proto = RefreshScript::recovering();
        // Tiny keepalive, far-away re-resolve: the full keepalive refresh is what
        // fires (covering its Ok arm) and recovers device 42.
        let intervals = LoopIntervals {
            keepalive: Duration::from_millis(3),
            reresolve: Duration::from_secs(60),
            ..tiny_intervals()
        };

        let (_, mut events) = tokio::join!(
            run_republisher(
                &tx,
                &proto,
                &conn,
                &mqtt,
                &points,
                &stop,
                &mut publisher,
                intervals,
            ),
            drive_until(&rx, &stop, |e| matches!(
                e,
                WorkerEvent::Log(LogLevel::Info, m) if m.contains("resolved during device table keepalive")
            )),
        );
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        assert!(is_running(&events));
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::Log(LogLevel::Info, m) if m.contains("resolved during device table keepalive")
        )));
    }

    #[tokio::test]
    async fn run_republisher_logs_reresolution_failure() {
        let (tx, rx) = unbounded();
        let stop = AtomicBool::new(false);
        let mut publisher = FakePublisher::default();
        let conn = Addressing::new();
        let mqtt = offline_mqtt(PayloadFormat::Scalar);
        let points = vec![bacnet_point(42, true)];
        // Initial refresh reports device 42 unresolved (so the re-resolve branch is
        // eligible), then every later refresh errors — covering its Err arm.
        let proto = RefreshScript::failing();
        let intervals = LoopIntervals {
            keepalive: Duration::from_secs(60),
            ..tiny_intervals()
        };

        let (_, mut events) = tokio::join!(
            run_republisher(
                &tx,
                &proto,
                &conn,
                &mqtt,
                &points,
                &stop,
                &mut publisher,
                intervals,
            ),
            drive_until(&rx, &stop, |e| matches!(
                e,
                WorkerEvent::Log(LogLevel::Warning, m) if m.contains("Device re-resolution failed")
            )),
        );
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        assert!(is_running(&events));
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::Log(LogLevel::Warning, m) if m.contains("Device re-resolution failed")
        )));
    }

    #[tokio::test]
    async fn run_republisher_reports_poll_error() {
        let (tx, rx) = unbounded();
        let stop = AtomicBool::new(false);
        let mut publisher = FakePublisher::default();
        let mut conn = Addressing::new();
        conn.insert("poll_err".into(), serde_json::json!(true));
        let mqtt = offline_mqtt(PayloadFormat::Scalar);
        let points = vec![bacnet_point(10, true)];

        let (_, mut events) = tokio::join!(
            run_republisher(
                &tx,
                &ScriptedProto,
                &conn,
                &mqtt,
                &points,
                &stop,
                &mut publisher,
                tiny_intervals(),
            ),
            drive_until(
                &rx,
                &stop,
                |e| matches!(e, WorkerEvent::Log(LogLevel::Error, m) if m.contains("Poll failed"))
            ),
        );
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        assert!(is_running(&events));
        assert!(events.iter().any(
            |e| matches!(e, WorkerEvent::Log(LogLevel::Error, m) if m.contains("Poll failed"))
        ));
    }

    #[tokio::test]
    async fn run_republisher_logs_refresh_and_keepalive_failures() {
        let (tx, rx) = unbounded();
        let stop = AtomicBool::new(false);
        let mut publisher = FakePublisher::default();
        let mut conn = Addressing::new();
        conn.insert("refresh_err".into(), serde_json::json!(true));
        let mqtt = offline_mqtt(PayloadFormat::Scalar);
        let points = vec![bacnet_point(10, true)];

        // tiny keepalive => the keepalive refresh fires (and also fails) inside the
        // loop, distinct from the initial pre-loop refresh failure.
        let (_, mut events) = tokio::join!(
            run_republisher(
                &tx,
                &ScriptedProto,
                &conn,
                &mqtt,
                &points,
                &stop,
                &mut publisher,
                tiny_intervals(),
            ),
            drive_until(
                &rx,
                &stop,
                |e| matches!(e, WorkerEvent::Log(LogLevel::Warning, m) if m.contains("keepalive failed"))
            ),
        );
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        assert!(is_running(&events));
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::Log(LogLevel::Warning, m) if m.contains("Device table refresh failed")
        )));
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::Log(LogLevel::Warning, m) if m.contains("Device table keepalive failed")
        )));
    }

    #[tokio::test]
    async fn run_republisher_warns_once_on_fatal_broker_rejection() {
        let (tx, rx) = unbounded();
        let stop = AtomicBool::new(false);
        let mut publisher = FakePublisher {
            fatal: Some("not authorized".to_string()),
            ..FakePublisher::default()
        };
        let conn = Addressing::new();
        let mqtt = offline_mqtt(PayloadFormat::Scalar);
        let points = vec![bacnet_point(10, true)];

        let (_, mut events) = tokio::join!(
            run_republisher(
                &tx,
                &ScriptedProto,
                &conn,
                &mqtt,
                &points,
                &stop,
                &mut publisher,
                tiny_intervals(),
            ),
            drive_until(
                &rx,
                &stop,
                |e| matches!(e, WorkerEvent::Log(LogLevel::Warning, m) if m.contains("rejected by broker"))
            ),
        );
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        assert!(is_running(&events));
        let rejections = events
            .iter()
            .filter(|e| matches!(e, WorkerEvent::Log(LogLevel::Warning, m) if m.contains("rejected by broker")))
            .count();
        // Warned, and only once (fatal_reported latches).
        assert_eq!(
            rejections, 1,
            "fatal rejection should be warned exactly once"
        );
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::Log(LogLevel::Warning, m) if m.contains("not authorized")
        )));
    }

    /// Collect worker events until `done` returns true or the deadline passes.
    fn collect_until(
        rx: &crossbeam_channel::Receiver<WorkerEvent>,
        timeout: Duration,
        mut done: impl FnMut(&WorkerEvent) -> bool,
    ) -> Vec<WorkerEvent> {
        let deadline = Instant::now() + timeout;
        let mut events = Vec::new();
        while Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(event) => {
                    let stop = done(&event);
                    events.push(event);
                    if stop {
                        break;
                    }
                }
                Err(_) => continue,
            }
        }
        events
    }

    fn is_finished(events: &[WorkerEvent], needle: &str) -> bool {
        events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Finished(m) if m.contains(needle)))
    }

    fn is_running(events: &[WorkerEvent]) -> bool {
        events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Lifecycle(RepublisherLifecycle::Running)))
    }

    // ---- spawn_discovery ------------------------------------------------------

    #[test]
    fn spawn_discovery_emits_devices_warnings_and_finished() {
        let (tx, rx) = unbounded();
        let mut conn = Addressing::new();
        conn.insert("devices".into(), serde_json::json!(2));
        conn.insert("discover_warn".into(), serde_json::json!(true));
        spawn_discovery(tx, scripted_factory, conn);

        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        assert!(
            events
                .iter()
                .any(|e| matches!(e, WorkerEvent::Devices(o) if o.devices.len() == 2)),
            "expected 2 discovered devices"
        );
        assert!(events.iter().any(
            |e| matches!(e, WorkerEvent::Log(LogLevel::Warning, m) if m == "discover warning")
        ));
        assert!(is_finished(&events, "Discovery found 2 device"));
    }

    #[test]
    fn spawn_discovery_reports_failure() {
        let (tx, rx) = unbounded();
        let mut conn = Addressing::new();
        conn.insert("discover_err".into(), serde_json::json!(true));
        spawn_discovery(tx, scripted_factory, conn);

        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        assert!(events.iter().any(
            |e| matches!(e, WorkerEvent::Log(LogLevel::Error, m) if m.contains("Discovery failed"))
        ));
        assert!(is_finished(&events, "Discovery failed"));
    }

    // ---- spawn_browse ---------------------------------------------------------

    #[test]
    fn spawn_browse_emits_points_and_finished() {
        let (tx, rx) = unbounded();
        let mut conn = Addressing::new();
        conn.insert("browse_points".into(), serde_json::json!(3));
        conn.insert("browse_warn".into(), serde_json::json!(true));
        let device = DiscoveredDevice {
            key: "AHU-7".into(),
            instance: Some(7),
            address: "127.0.0.1".into(),
            detail: String::new(),
        };
        spawn_browse(tx, scripted_factory, conn, device);

        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Points(p) if p.len() == 3)));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Log(LogLevel::Warning, m) if m == "browse warning")));
        assert!(is_finished(&events, "Browsed 3 point(s) on AHU-7"));
    }

    #[test]
    fn spawn_browse_reports_failure() {
        let (tx, rx) = unbounded();
        let device = DiscoveredDevice {
            key: "d-bad".into(),
            instance: Some(1),
            address: "x".into(),
            detail: String::new(),
        };
        spawn_browse(tx, scripted_factory, Addressing::new(), device);

        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        assert!(events.iter().any(
            |e| matches!(e, WorkerEvent::Log(LogLevel::Error, m) if m.contains("Browse failed"))
        ));
        assert!(is_finished(&events, "Browse failed"));
    }

    // ---- spawn_scan_all_objects ----------------------------------------------

    #[test]
    fn spawn_scan_all_objects_merges_points_and_reports_failures() {
        let (tx, rx) = unbounded();
        let mut conn = Addressing::new();
        conn.insert("browse_points".into(), serde_json::json!(2));
        conn.insert("browse_warn".into(), serde_json::json!(true));
        let devices = vec![
            DiscoveredDevice {
                key: "d0".into(),
                instance: Some(1000),
                address: "127.0.0.1".into(),
                detail: String::new(),
            },
            DiscoveredDevice {
                key: "d-bad".into(), // browse fails for this one
                instance: Some(9000),
                address: "x".into(),
                detail: String::new(),
            },
        ];
        spawn_scan_all_objects(tx, scripted_factory, conn, devices, Vec::new());

        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        // Progress emitted (initial + one per device).
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::ScanProgress { total, .. } if *total == 2)));
        // The good device contributed 2 points to the bulk merge.
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::BulkTagImport(m) if m.added == 2)));
        assert!(is_finished(&events, "1 failure(s)"));
        assert!(is_finished(&events, "2 point(s) added"));
    }

    // ---- spawn_poll_once ------------------------------------------------------

    #[test]
    fn spawn_poll_once_with_no_enabled_points_finishes_early() {
        let (tx, rx) = unbounded();
        let mut disabled = bacnet_point(1, false);
        disabled.enabled = false;
        spawn_poll_once(
            tx,
            scripted_factory,
            Addressing::new(),
            offline_mqtt(PayloadFormat::Scalar),
            vec![disabled],
        );
        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        assert!(is_finished(&events, "No enabled points to poll"));
    }

    #[test]
    fn spawn_poll_once_polls_publishes_and_finishes() {
        let (tx, rx) = unbounded();
        let points = vec![bacnet_point(10, true), bacnet_point(20, true)];
        spawn_poll_once(
            tx,
            scripted_factory,
            Addressing::new(),
            offline_mqtt(PayloadFormat::Scalar),
            points,
        );
        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        // Scalar path enqueues each sample and emits a per-point publish event.
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::PointPublish { error: None, .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Samples(s) if s.len() == 2)));
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::PublishStatus(stats) if stats.published == 2
        )));
        assert!(is_finished(&events, "Poll once complete"));
    }

    #[test]
    fn spawn_poll_once_envelope_path_publishes_grouped_by_device() {
        let (tx, rx) = unbounded();
        let points = vec![bacnet_point(10, true), bacnet_point(10, true)];
        spawn_poll_once(
            tx,
            scripted_factory,
            Addressing::new(),
            offline_mqtt(PayloadFormat::NetixEnvelope),
            points,
        );
        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::PointPublish { error: None, .. })));
        assert!(is_finished(&events, "Poll once complete"));
    }

    #[test]
    fn spawn_poll_once_surfaces_read_failures_and_warnings() {
        let (tx, rx) = unbounded();
        let mut conn = Addressing::new();
        conn.insert("poll_fail_points".into(), serde_json::json!(true));
        conn.insert("poll_warn".into(), serde_json::json!(true));
        spawn_poll_once(
            tx,
            scripted_factory,
            conn,
            offline_mqtt(PayloadFormat::Scalar),
            vec![bacnet_point(10, true)],
        );
        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Failures(f) if f.len() == 1)));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Log(LogLevel::Warning, m) if m == "poll warning")));
        assert!(is_finished(&events, "Poll once complete"));
    }

    #[test]
    fn spawn_poll_once_reports_poll_failure() {
        let (tx, rx) = unbounded();
        let mut conn = Addressing::new();
        conn.insert("poll_err".into(), serde_json::json!(true));
        spawn_poll_once(
            tx,
            scripted_factory,
            conn,
            offline_mqtt(PayloadFormat::Scalar),
            vec![bacnet_point(10, true)],
        );
        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        assert!(events.iter().any(
            |e| matches!(e, WorkerEvent::Log(LogLevel::Error, m) if m.contains("Poll failed"))
        ));
        assert!(is_finished(&events, "Poll once failed"));
    }

    // ---- discover_points ------------------------------------------------------

    #[tokio::test]
    async fn discover_points_builds_from_discovery_and_browse() {
        let mut conn = Addressing::new();
        conn.insert("devices".into(), serde_json::json!(2));
        conn.insert("browse_points".into(), serde_json::json!(2));
        conn.insert("discover_warn".into(), serde_json::json!(true));
        conn.insert("browse_warn".into(), serde_json::json!(true));

        let (points, warnings) = discover_points(&ScriptedProto, &conn).await.unwrap();
        // 2 devices x 2 points each, all identity-distinct.
        assert_eq!(points.len(), 4);
        assert!(points.iter().all(|p| p.enabled));
        assert!(warnings.iter().any(|w| w == "discover warning"));
        assert!(warnings.iter().any(|w| w == "browse warning"));
    }

    #[tokio::test]
    async fn discover_points_records_per_device_browse_failure() {
        let mut conn = Addressing::new();
        conn.insert("devices".into(), serde_json::json!(1));
        conn.insert("browse_points".into(), serde_json::json!(1));
        conn.insert("bad_device".into(), serde_json::json!(true));

        let (points, warnings) = discover_points(&ScriptedProto, &conn).await.unwrap();
        // Only the good device yields a point; the bad one adds a browse warning.
        assert_eq!(points.len(), 1);
        assert!(warnings.iter().any(|w| w.contains("browse failed")));
    }

    #[tokio::test]
    async fn discover_points_propagates_discovery_error() {
        let mut conn = Addressing::new();
        conn.insert("discover_err".into(), serde_json::json!(true));
        assert!(discover_points(&ScriptedProto, &conn).await.is_err());
    }

    // ---- spawn_republisher decision paths (no broker) -------------------------

    #[test]
    fn spawn_republisher_discover_on_start_builds_points_and_runs() {
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        let mut conn = Addressing::new();
        conn.insert("devices".into(), serde_json::json!(1));
        conn.insert("browse_points".into(), serde_json::json!(1));
        conn.insert("browse_warn".into(), serde_json::json!(true));
        spawn_republisher(
            tx,
            scripted_factory,
            conn,
            offline_mqtt(PayloadFormat::Scalar),
            Vec::new(),
            true,
            Arc::clone(&stop),
        );
        // Drive until a publish cycle completes (proves it built points, entered
        // the loop, polled, and published without a broker).
        let events = collect_until(&rx, Duration::from_secs(6), |e| {
            matches!(e, WorkerEvent::PublishStatus(_))
        });
        stop.store(true, Ordering::Relaxed);
        assert!(is_running(&events), "expected a Running lifecycle");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, WorkerEvent::Log(LogLevel::Info, m)
                    if m.contains("built 1 point(s) from discovery"))),
            "expected the discover_on_start build log"
        );
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::PublishStatus(_))));
    }

    #[test]
    fn spawn_republisher_discover_on_start_discovery_error_fails_loud() {
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        let mut conn = Addressing::new();
        conn.insert("discover_err".into(), serde_json::json!(true));
        spawn_republisher(
            tx,
            scripted_factory,
            conn,
            offline_mqtt(PayloadFormat::Scalar),
            Vec::new(),
            true,
            Arc::clone(&stop),
        );
        let message = wait_for_failed(&rx).expect("expected Failed lifecycle");
        stop.store(true, Ordering::Relaxed);
        assert!(
            message.contains("discover_on_start discovery failed"),
            "got: {message}"
        );
    }

    #[test]
    fn spawn_republisher_discover_on_start_no_points_fails_loud() {
        // Discovery is supported but yields zero devices -> no pollable points.
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        let mut conn = Addressing::new();
        conn.insert("devices".into(), serde_json::json!(0));
        spawn_republisher(
            tx,
            scripted_factory,
            conn,
            offline_mqtt(PayloadFormat::Scalar),
            Vec::new(),
            true,
            Arc::clone(&stop),
        );
        let message = wait_for_failed(&rx).expect("expected Failed lifecycle");
        stop.store(true, Ordering::Relaxed);
        assert!(
            message.contains("discovery found no pollable points"),
            "got: {message}"
        );
    }

    #[test]
    fn spawn_republisher_runs_configured_points_and_publishes() {
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        // Two resolved devices, refresh reports all resolved (no unresolved list).
        let points = vec![bacnet_point(10, true), bacnet_point(20, true)];
        spawn_republisher(
            tx,
            scripted_factory,
            Addressing::new(),
            offline_mqtt(PayloadFormat::Scalar),
            points,
            false,
            Arc::clone(&stop),
        );
        let events = collect_until(&rx, Duration::from_secs(6), |e| {
            matches!(e, WorkerEvent::PublishStatus(_))
        });
        stop.store(true, Ordering::Relaxed);
        assert!(is_running(&events));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Samples(s) if !s.is_empty())));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::PublishStatus(_))));
    }

    #[test]
    fn spawn_republisher_warns_and_skips_unresolved_devices() {
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        // Device 42 is reported unresolved by refresh -> its points are skipped
        // and a Failures event is emitted rather than polling it.
        let mut conn = Addressing::new();
        conn.insert("unresolved".into(), serde_json::json!([42]));
        let points = vec![bacnet_point(42, true)];
        spawn_republisher(
            tx,
            scripted_factory,
            conn,
            offline_mqtt(PayloadFormat::Scalar),
            points,
            false,
            Arc::clone(&stop),
        );
        let events = collect_until(&rx, Duration::from_secs(6), |e| {
            matches!(e, WorkerEvent::Failures(_))
        });
        stop.store(true, Ordering::Relaxed);
        assert!(is_running(&events));
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::Log(LogLevel::Warning, m) if m.contains("not in I-Am cache")
        )));
        assert!(events.iter().any(|e| matches!(e, WorkerEvent::Failures(f)
                if f.iter().any(|x| x.error.contains("not in I-Am cache")))));
    }

    #[test]
    fn spawn_republisher_warns_when_initial_refresh_fails() {
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        let mut conn = Addressing::new();
        conn.insert("refresh_err".into(), serde_json::json!(true));
        let points = vec![bacnet_point(10, true)];
        spawn_republisher(
            tx,
            scripted_factory,
            conn,
            offline_mqtt(PayloadFormat::Scalar),
            points,
            false,
            Arc::clone(&stop),
        );
        // Refresh failing is non-fatal: the worker still reaches Running and (with
        // no unresolved set) polls the point.
        let events = collect_until(&rx, Duration::from_secs(6), |e| {
            matches!(e, WorkerEvent::PublishStatus(_))
        });
        stop.store(true, Ordering::Relaxed);
        assert!(is_running(&events));
        assert!(events.iter().any(|e| matches!(
            e,
            WorkerEvent::Log(LogLevel::Warning, m) if m.contains("Device table refresh failed")
        )));
    }

    // ---- publish_samples / publish_envelope (direct, no broker) ---------------

    #[tokio::test]
    async fn publish_samples_scalar_enqueues_and_emits_events() {
        let (tx, rx) = unbounded();
        let mut publisher = RumqttPublisher::new(&offline_mqtt(PayloadFormat::Scalar)).unwrap();
        let mqtt = offline_mqtt(PayloadFormat::Scalar);
        let sample = PointSample {
            point: bacnet_point(1, true),
            value: TelemetryValue::Number(3.5),
            topic: "Netix/A".into(),
            timestamp_ms: 1,
        };
        let mut status = HashMap::new();
        status.insert(
            PointIdentity::from_point(&sample.point),
            PointStatus::default(),
        );

        let stats = publish_samples(
            &tx,
            &mut publisher,
            &mqtt,
            std::slice::from_ref(&sample),
            &mut status,
        )
        .await;
        assert_eq!(stats.queued, 1);
        assert_eq!(stats.published, 1);
        assert_eq!(stats.failed, 0);
        // Success recorded on the point status and a publish event emitted.
        assert!(status
            .get(&PointIdentity::from_point(&sample.point))
            .unwrap()
            .last_publish_error
            .is_none());
        assert!(rx
            .try_iter()
            .any(|e| matches!(e, WorkerEvent::PointPublish { error: None, .. })));
    }

    #[tokio::test]
    async fn publish_samples_envelope_groups_and_emits_events() {
        let (tx, rx) = unbounded();
        let mut publisher =
            RumqttPublisher::new(&offline_mqtt(PayloadFormat::NetixEnvelope)).unwrap();
        let mqtt = offline_mqtt(PayloadFormat::NetixEnvelope);
        // Two samples for the same device -> one envelope publish, two point events.
        let s1 = PointSample {
            point: bacnet_point(1, true),
            value: TelemetryValue::Number(1.0),
            topic: String::new(),
            timestamp_ms: 5,
        };
        let s2 = PointSample {
            point: bacnet_point(1, true),
            value: TelemetryValue::Text("on".into()),
            topic: String::new(),
            timestamp_ms: 9,
        };
        let mut status = HashMap::new();
        status.insert(PointIdentity::from_point(&s1.point), PointStatus::default());

        let stats = publish_samples(&tx, &mut publisher, &mqtt, &[s1, s2], &mut status).await;
        assert_eq!(stats.queued, 2);
        assert_eq!(stats.published, 2);
        let publishes = rx
            .try_iter()
            .filter(|e| matches!(e, WorkerEvent::PointPublish { error: None, .. }))
            .count();
        assert_eq!(publishes, 2);
    }

    #[tokio::test]
    async fn publish_samples_counts_and_reports_channel_full_failures() {
        let (tx, rx) = unbounded();
        let mut publisher = RumqttPublisher::new(&offline_mqtt(PayloadFormat::Scalar)).unwrap();
        let mqtt = offline_mqtt(PayloadFormat::Scalar);
        // Flood well past the outbound channel capacity so enqueue fails fast.
        let point = bacnet_point(1, true);
        let mut status = HashMap::new();
        status.insert(PointIdentity::from_point(&point), PointStatus::default());
        let samples: Vec<PointSample> = (0..5000)
            .map(|i| PointSample {
                point: point.clone(),
                value: TelemetryValue::Number(i as f64),
                topic: "Netix/Flood".into(),
                timestamp_ms: i as i64,
            })
            .collect();

        let stats = publish_samples(&tx, &mut publisher, &mqtt, &samples, &mut status).await;
        assert_eq!(stats.queued, 5000);
        assert!(stats.failed > 0, "channel-full drops should be counted");
        assert_eq!(stats.published + stats.failed, 5000);
        assert!(stats
            .last_error
            .as_deref()
            .unwrap()
            .contains("failed to enqueue"));
        // A publish-failure event and a recorded publish error surfaced.
        assert!(rx
            .try_iter()
            .any(|e| matches!(e, WorkerEvent::PointPublish { error: Some(_), .. })));
        assert!(status
            .get(&PointIdentity::from_point(&point))
            .unwrap()
            .last_publish_error
            .is_some());
    }

    #[tokio::test]
    async fn publish_envelope_counts_channel_full_failures() {
        let (tx, rx) = unbounded();
        let mut publisher =
            RumqttPublisher::new(&offline_mqtt(PayloadFormat::NetixEnvelope)).unwrap();
        let mqtt = offline_mqtt(PayloadFormat::NetixEnvelope);
        // Distinct devices -> one envelope publish each, so enqueues pile up and
        // eventually fail once the outbound channel is saturated.
        let mut status = HashMap::new();
        let samples: Vec<PointSample> = (0..6000u32)
            .map(|i| {
                let point = bacnet_point(i, true);
                status.insert(PointIdentity::from_point(&point), PointStatus::default());
                PointSample {
                    point,
                    value: TelemetryValue::Number(i as f64),
                    topic: String::new(),
                    timestamp_ms: i as i64,
                }
            })
            .collect();

        let stats = publish_samples(&tx, &mut publisher, &mqtt, &samples, &mut status).await;
        assert_eq!(stats.queued, 6000);
        assert!(stats.failed > 0, "saturated channel should record failures");
        assert!(rx
            .try_iter()
            .any(|e| matches!(e, WorkerEvent::PointPublish { error: Some(_), .. })));
    }

    /// A TLS config with a CA path that holds no certificates: `build_transport`
    /// (hence `RumqttPublisher::new`) fails without needing a broker.
    fn broken_tls_mqtt() -> (MqttConfig, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("empty-ca.pem");
        std::fs::write(&ca, b"not a certificate\n").unwrap();
        let cfg = MqttConfig {
            host: "127.0.0.1".into(),
            port: 1,
            use_tls: true,
            ca_cert_path: Some(ca.to_string_lossy().into_owned()),
            ..MqttConfig::default()
        };
        (cfg, dir)
    }

    #[test]
    fn spawn_poll_once_fails_when_publisher_cannot_be_built() {
        let (tx, rx) = unbounded();
        let (cfg, _dir) = broken_tls_mqtt();
        spawn_poll_once(
            tx,
            scripted_factory,
            Addressing::new(),
            cfg,
            vec![bacnet_point(10, true)],
        );
        let events = collect_until(&rx, Duration::from_secs(5), |e| {
            matches!(e, WorkerEvent::Finished(_))
        });
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Log(LogLevel::Error, m) if m.contains("MQTT publisher failed"))));
        assert!(is_finished(&events, "Poll once failed"));
    }

    #[test]
    fn spawn_republisher_fails_when_publisher_cannot_be_built() {
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        let (cfg, _dir) = broken_tls_mqtt();
        spawn_republisher(
            tx,
            scripted_factory,
            Addressing::new(),
            cfg,
            vec![bacnet_point(10, true)],
            false,
            Arc::clone(&stop),
        );
        let message = wait_for_failed(&rx).expect("expected Failed lifecycle");
        stop.store(true, Ordering::Relaxed);
        // spawn_republisher surfaces the top-level context of the build error.
        assert!(
            message.contains("failed to load MQTT CA certificate"),
            "got: {message}"
        );
    }

    #[test]
    fn spawn_republisher_loop_records_read_failures_and_warnings() {
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        let mut conn = Addressing::new();
        conn.insert("poll_fail_points".into(), serde_json::json!(true));
        conn.insert("poll_warn".into(), serde_json::json!(true));
        spawn_republisher(
            tx,
            scripted_factory,
            conn,
            offline_mqtt(PayloadFormat::Scalar),
            vec![bacnet_point(10, true)],
            false,
            Arc::clone(&stop),
        );
        // The loop polls, gets read failures + a warning, and emits Failures.
        let events = collect_until(&rx, Duration::from_secs(6), |e| {
            matches!(e, WorkerEvent::Failures(_))
        });
        stop.store(true, Ordering::Relaxed);
        assert!(is_running(&events));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Failures(f) if !f.is_empty())));
        assert!(events
            .iter()
            .any(|e| matches!(e, WorkerEvent::Log(LogLevel::Warning, m) if m == "poll warning")));
    }

    #[test]
    fn spawn_republisher_loop_reports_poll_error() {
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        let mut conn = Addressing::new();
        conn.insert("poll_err".into(), serde_json::json!(true));
        spawn_republisher(
            tx,
            scripted_factory,
            conn,
            offline_mqtt(PayloadFormat::Scalar),
            vec![bacnet_point(10, true)],
            false,
            Arc::clone(&stop),
        );
        let events = collect_until(
            &rx,
            Duration::from_secs(6),
            |e| matches!(e, WorkerEvent::Log(LogLevel::Error, m) if m.contains("Poll failed")),
        );
        stop.store(true, Ordering::Relaxed);
        assert!(events.iter().any(
            |e| matches!(e, WorkerEvent::Log(LogLevel::Error, m) if m.contains("Poll failed"))
        ));
    }

    #[test]
    fn spawn_republisher_emits_graceful_shutdown_lifecycle() {
        // Cover the Stopping -> (drain grace) -> Stopped tail: stop the worker and
        // wait past CLIENT_STOP_TIMEOUT for the final lifecycle events.
        let (tx, rx) = unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        spawn_republisher(
            tx,
            scripted_factory,
            Addressing::new(),
            offline_mqtt(PayloadFormat::Scalar),
            vec![bacnet_point(10, true)],
            false,
            Arc::clone(&stop),
        );
        // Wait until it is Running, then request stop.
        let running = collect_until(&rx, Duration::from_secs(5), is_running_single);
        assert!(is_running(&running));
        stop.store(true, Ordering::Relaxed);

        // CLIENT_STOP_TIMEOUT is 5s between Stopping and Stopped.
        let tail = collect_until(&rx, Duration::from_secs(9), |e| {
            matches!(e, WorkerEvent::Lifecycle(RepublisherLifecycle::Stopped))
        });
        assert!(tail
            .iter()
            .any(|e| matches!(e, WorkerEvent::Lifecycle(RepublisherLifecycle::Stopping))));
        assert!(tail
            .iter()
            .any(|e| matches!(e, WorkerEvent::Lifecycle(RepublisherLifecycle::Stopped))));
    }

    fn is_running_single(event: &WorkerEvent) -> bool {
        matches!(event, WorkerEvent::Lifecycle(RepublisherLifecycle::Running))
    }
}
