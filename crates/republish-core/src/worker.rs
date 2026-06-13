//! Background worker: runs discovery, browse, and the continuous poll→publish
//! loop on dedicated threads, communicating with the UI over a channel.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{unbounded, Receiver, Sender};

use proto_api::Addressing;

use crate::config::MqttConfig;
use crate::log::LogLevel;
use crate::model::{
    DiscoverOutcome, DiscoveredDevice, DiscoveredPoint, PointConfig, PointFailure, PointIdentity,
    PointSample, PublishStats,
};
use crate::mqtt::{publish_health, HealthSnapshot, RumqttPublisher};
use crate::protocol::RepublishFactory;

const POLL_TICK: Duration = Duration::from_millis(500);
const HEALTH_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepublisherLifecycle {
    Starting,
    Running,
    Stopped,
    Failed(String),
}

/// Messages emitted by worker threads, drained by the UI.
pub enum WorkerEvent {
    Log(LogLevel, String),
    Devices(DiscoverOutcome),
    Points(Vec<DiscoveredPoint>),
    Samples(Vec<PointSample>),
    Failures(Vec<PointFailure>),
    PublishStatus(PublishStats),
    Lifecycle(RepublisherLifecycle),
    Finished(String),
}

/// A bidirectional channel pair the UI holds.
pub struct WorkerChannel {
    pub sender: Sender<WorkerEvent>,
    pub receiver: Receiver<WorkerEvent>,
}

impl Default for WorkerChannel {
    fn default() -> Self {
        let (sender, receiver) = unbounded();
        Self { sender, receiver }
    }
}

impl WorkerChannel {
    pub fn new() -> Self {
        Self::default()
    }
}

fn log(sender: &Sender<WorkerEvent>, level: LogLevel, message: impl Into<String>) {
    let _ = sender.send(WorkerEvent::Log(level, message.into()));
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("failed to build tokio runtime")
}

/// Discover devices/servers for the selected protocol.
pub fn spawn_discovery(sender: Sender<WorkerEvent>, factory: RepublishFactory, conn: Addressing) {
    std::thread::spawn(move || {
        runtime().block_on(async move {
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
    device: DiscoveredDevice,
) {
    std::thread::spawn(move || {
        runtime().block_on(async move {
            let proto = factory();
            match proto.browse(&conn, &device).await {
                Ok(points) => {
                    let count = points.len();
                    let _ = sender.send(WorkerEvent::Points(points));
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
        runtime().block_on(async move {
            let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Starting));
            let proto = factory();
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

            let mut last_poll: HashMap<PointIdentity, Instant> = HashMap::new();
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
                let due: Vec<PointConfig> = points
                    .iter()
                    .filter(|p| p.enabled)
                    .filter(|p| {
                        let id = PointIdentity::from_point(p);
                        match last_poll.get(&id) {
                            Some(at) => now.duration_since(*at).as_secs() >= p.poll_interval_secs,
                            None => true,
                        }
                    })
                    .cloned()
                    .collect();

                if !due.is_empty() {
                    match proto.poll(&conn, &due).await {
                        Ok(outcome) => {
                            for point in &due {
                                last_poll.insert(PointIdentity::from_point(point), now);
                            }
                            for warning in outcome.warnings {
                                log(&sender, LogLevel::Warning, warning);
                            }
                            cycle_failed_reads += outcome.failures.len();
                            if !outcome.failures.is_empty() {
                                let _ = sender.send(WorkerEvent::Failures(outcome.failures));
                            }
                            if !outcome.samples.is_empty() {
                                // Adapters return values without an MQTT topic; the
                                // worker owns the MqttConfig, so it builds the topics.
                                let mut samples = outcome.samples;
                                for sample in &mut samples {
                                    sample.topic =
                                        crate::topic::telemetry_topic(&mqtt, &sample.point);
                                }
                                let stats = publisher.enqueue_samples(&mqtt, &samples);
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
                    let snapshot = HealthSnapshot {
                        published: cycle_published,
                        failed_reads: cycle_failed_reads,
                        failed_publishes: cycle_failed_publishes,
                        stale_points: 0,
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

            let _ = sender.send(WorkerEvent::Lifecycle(RepublisherLifecycle::Stopped));
        });
    });
}
