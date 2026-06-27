use crate::config::MqttConfig;
use crate::model::{PointSample, PublishStats};
use anyhow::{anyhow, Context, Result};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, TlsConfiguration, Transport};
use serde_json::json;
use std::fs;
use std::future::Future;
use std::io::BufReader;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
// Bounds how many QoS 1 publishes can sit waiting for the event loop. While the broker
// is unreachable the channel fills and try_publish() fails fast — samples are dropped
// and counted, never blocking the poll loop. Sized for full-fleet bursts (~1500 points).
const OUTBOUND_CHANNEL_CAPACITY: usize = 4096;

pub trait MqttPublisher {
    fn publish<'a>(
        &'a mut self,
        topic: &'a str,
        payload: Vec<u8>,
        retain: bool,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectBackoff {
    current: Duration,
    max: Duration,
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self {
            current: BACKOFF_INITIAL,
            max: BACKOFF_MAX,
        }
    }
}

impl ReconnectBackoff {
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = self.current.saturating_mul(2).min(self.max);
        delay
    }

    pub fn reset(&mut self) {
        self.current = BACKOFF_INITIAL;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    pub published: usize,
    pub failed_reads: usize,
    pub failed_publishes: usize,
    pub stale_points: usize,
    pub reconnects: usize,
    pub last_error: Option<String>,
}

impl HealthSnapshot {
    pub fn status(&self) -> &'static str {
        if self.failed_reads == 0 && self.failed_publishes == 0 && self.stale_points == 0 {
            "ok"
        } else {
            "degraded"
        }
    }
}

#[derive(Default)]
struct ConnectionState {
    connected: AtomicBool,
    reconnects: AtomicUsize,
    last_error: Mutex<Option<String>>,
}

impl ConnectionState {
    fn record_connack(&self) {
        self.connected.store(true, Ordering::Relaxed);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = None;
        }
    }

    fn record_error(&self, error: impl Into<String>) {
        if self.connected.swap(false, Ordering::Relaxed) {
            self.reconnects.fetch_add(1, Ordering::Relaxed);
        }
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(error.into());
        }
    }
}

pub struct RumqttPublisher {
    client: AsyncClient,
    state: Arc<ConnectionState>,
    eventloop_task: tokio::task::JoinHandle<()>,
}

impl RumqttPublisher {
    /// Must be called from within a tokio runtime: the event loop runs in a spawned task.
    pub fn new(config: &MqttConfig) -> Result<Self> {
        let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);
        options.set_keep_alive(Duration::from_secs(config.keep_alive_secs.max(5)));
        options.set_transport(build_transport(config)?);
        if let Some(username) = config.username.as_deref().filter(|value| !value.is_empty()) {
            options.set_credentials(username, config.password.clone().unwrap_or_default());
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            anyhow!("RumqttPublisher::new must be called from within a tokio runtime")
        })?;
        let (client, mut eventloop) = AsyncClient::new(options, OUTBOUND_CHANNEL_CAPACITY);
        let state = Arc::new(ConnectionState::default());

        // The event loop runs in its own task so the request channel always drains.
        // Driving it from the publishing task deadlocks: once the channel fills,
        // publish().await blocks waiting for space that only the (then never-polled)
        // event loop could free.
        let eventloop_task = runtime.spawn({
            let state = Arc::clone(&state);
            async move {
                let mut backoff = ReconnectBackoff::default();
                loop {
                    match eventloop.poll().await {
                        Ok(Event::Incoming(Packet::ConnAck(_))) => {
                            state.record_connack();
                            backoff.reset();
                        }
                        Ok(_) => {}
                        Err(error) => {
                            state.record_error(error.to_string());
                            sleep(backoff.next_delay()).await;
                        }
                    }
                }
            }
        });

        Ok(Self {
            client,
            state,
            eventloop_task,
        })
    }

    /// Hand a publish to the event loop without blocking. Fails fast when the
    /// outbound channel is full (broker down long enough to back up the queue).
    fn enqueue(&self, topic: &str, payload: Vec<u8>, retain: bool) -> Result<()> {
        self.client
            .try_publish(topic, QoS::AtLeastOnce, retain, payload)
            .map_err(|error| anyhow!("failed to enqueue MQTT publish to {topic}: {error}"))
    }

    pub fn enqueue_samples(
        &mut self,
        config: &MqttConfig,
        samples: &[PointSample],
    ) -> PublishStats {
        let mut stats = PublishStats::empty();
        for sample in samples {
            stats.queued += 1;
            let payload = match serde_json::to_vec(&sample.value.as_json_value()) {
                Ok(payload) => payload,
                Err(error) => {
                    stats.record_failure(error.to_string());
                    continue;
                }
            };
            match self.enqueue(&sample.topic, payload, config.retain) {
                Ok(()) => stats.published += 1,
                Err(error) => stats.record_failure(error.to_string()),
            }
        }

        stats.reconnects = self.reconnect_count();
        if stats.last_error.is_none() {
            stats.last_error = self.last_connection_error();
        }
        stats
    }

    pub(crate) fn try_enqueue_sample(
        &self,
        topic: &str,
        payload: Vec<u8>,
        retain: bool,
    ) -> Result<()> {
        self.enqueue(topic, payload, retain)
    }

    pub fn reconnect_count(&self) -> usize {
        self.state.reconnects.load(Ordering::Relaxed)
    }

    pub fn last_connection_error(&self) -> Option<String> {
        self.state
            .last_error
            .lock()
            .ok()
            .and_then(|value| value.clone())
    }
}

impl Drop for RumqttPublisher {
    fn drop(&mut self) {
        self.client.try_disconnect().ok();
        self.eventloop_task.abort();
    }
}

impl MqttPublisher for RumqttPublisher {
    fn publish<'a>(
        &'a mut self,
        topic: &'a str,
        payload: Vec<u8>,
        retain: bool,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.enqueue(topic, payload, retain) })
    }
}

pub async fn publish_samples<P: MqttPublisher + Send>(
    publisher: &mut P,
    config: &MqttConfig,
    samples: &[PointSample],
) -> PublishStats {
    let mut stats = PublishStats::empty();

    for sample in samples {
        stats.queued += 1;
        let payload = match serde_json::to_vec(&sample.value.as_json_value()) {
            Ok(payload) => payload,
            Err(error) => {
                stats.record_failure(error.to_string());
                continue;
            }
        };

        match publisher
            .publish(&sample.topic, payload, config.retain)
            .await
        {
            Ok(()) => stats.published += 1,
            Err(error) => stats.record_failure(error.to_string()),
        }
    }

    stats
}

pub async fn publish_health<P: MqttPublisher + Send>(
    publisher: &mut P,
    config: &MqttConfig,
    snapshot: HealthSnapshot,
) -> Result<()> {
    let payload = json!({
        "status": snapshot.status(),
        "published": snapshot.published,
        "failed_reads": snapshot.failed_reads,
        "failed_publishes": snapshot.failed_publishes,
        "stale_points": snapshot.stale_points,
        "reconnects": snapshot.reconnects,
        "last_error": snapshot.last_error,
        "timestamp": crate::model::now_millis(),
    });
    publisher
        .publish(
            &config.health_topic,
            serde_json::to_vec(&payload).context("failed to encode health payload")?,
            true,
        )
        .await
}

fn build_transport(config: &MqttConfig) -> Result<Transport> {
    if !config.use_tls {
        return Ok(Transport::tcp());
    }

    let client_auth = match (&config.client_cert_path, &config.client_key_path) {
        (Some(cert_path), Some(key_path)) => Some((
            load_cert_chain(Path::new(cert_path))
                .with_context(|| format!("failed to load MQTT client certificate {cert_path}"))?,
            load_private_key(Path::new(key_path))
                .with_context(|| format!("failed to load MQTT client key {key_path}"))?,
        )),
        _ => None,
    };

    if client_auth.is_none() && config.ca_cert_path.is_none() {
        return Ok(Transport::tls_with_default_config());
    }

    let roots = if let Some(ca_path) = &config.ca_cert_path {
        load_root_store_from_file(Path::new(ca_path))
            .with_context(|| format!("failed to load MQTT CA certificate {ca_path}"))?
    } else {
        load_native_root_store().context("failed to load platform TLS root certificates")?
    };

    let builder =
        rumqttc::tokio_rustls::rustls::ClientConfig::builder().with_root_certificates(roots);
    let tls_config = if let Some((certs, key)) = client_auth {
        builder
            .with_client_auth_cert(certs, key)
            .context("failed to configure MQTT client certificate")?
    } else {
        builder.with_no_client_auth()
    };

    Ok(Transport::tls_with_config(TlsConfiguration::Rustls(
        Arc::new(tls_config),
    )))
}

fn load_root_store_from_file(path: &Path) -> Result<rumqttc::tokio_rustls::rustls::RootCertStore> {
    let mut roots = rumqttc::tokio_rustls::rustls::RootCertStore::empty();
    let certs = load_cert_chain(path)?;
    let (added, ignored) = roots.add_parsable_certificates(certs);
    if added == 0 {
        return Err(anyhow!(
            "no usable CA certificates found; ignored {ignored}"
        ));
    }
    Ok(roots)
}

fn load_native_root_store() -> Result<rumqttc::tokio_rustls::rustls::RootCertStore> {
    let mut roots = rumqttc::tokio_rustls::rustls::RootCertStore::empty();
    let result = rustls_native_certs::load_native_certs();
    for cert in result.certs {
        roots
            .add(cert)
            .context("failed to add native TLS root certificate")?;
    }
    if roots.is_empty() {
        return Err(anyhow!(
            "no native TLS root certificates loaded: {:?}",
            result.errors
        ));
    }
    Ok(roots)
}

fn load_cert_chain(
    path: &Path,
) -> Result<Vec<rumqttc::tokio_rustls::rustls::pki_types::CertificateDer<'static>>> {
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut reader = BufReader::new(raw.as_slice());
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to parse certificates from {}", path.display()))
}

fn load_private_key(
    path: &Path,
) -> Result<rumqttc::tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>> {
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut reader = BufReader::new(raw.as_slice());
    rustls_pemfile::private_key(&mut reader)
        .with_context(|| format!("failed to parse private key from {}", path.display()))?
        .ok_or_else(|| anyhow!("no private key found in {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{PointConfig, TelemetryValue};

    #[derive(Default)]
    struct FakePublisher {
        calls: Vec<(String, Vec<u8>, bool)>,
        fail: bool,
    }

    impl MqttPublisher for FakePublisher {
        fn publish<'a>(
            &'a mut self,
            topic: &'a str,
            payload: Vec<u8>,
            retain: bool,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                if self.fail {
                    anyhow::bail!("publish failed");
                }
                self.calls.push((topic.to_string(), payload, retain));
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn publishes_json_scalars_with_retain_flag() {
        let mut publisher = FakePublisher::default();
        let config = MqttConfig {
            retain: true,
            ..MqttConfig::default()
        };
        let sample = PointSample {
            point: PointConfig::default(),
            value: TelemetryValue::Number(22.5),
            topic: "Netix/Site/AHU1/Temp".to_string(),
            timestamp_ms: 1,
        };

        let stats = publish_samples(&mut publisher, &config, &[sample]).await;

        assert_eq!(stats.queued, 1);
        assert_eq!(stats.published, 1);
        assert_eq!(publisher.calls[0].0, "Netix/Site/AHU1/Temp");
        assert_eq!(publisher.calls[0].1, b"22.5");
        assert!(publisher.calls[0].2);
    }

    #[tokio::test]
    async fn health_payload_reports_degraded_state() {
        let mut publisher = FakePublisher::default();
        let config = MqttConfig::default();

        publish_health(
            &mut publisher,
            &config,
            HealthSnapshot {
                published: 1,
                failed_reads: 2,
                failed_publishes: 3,
                stale_points: 4,
                reconnects: 5,
                last_error: Some("network".to_string()),
            },
        )
        .await
        .unwrap();

        let payload: serde_json::Value = serde_json::from_slice(&publisher.calls[0].1).unwrap();
        assert_eq!(payload["status"], "degraded");
        assert_eq!(payload["published"], 1);
        assert_eq!(payload["stale_points"], 4);
        assert_eq!(payload["reconnects"], 5);
    }

    #[test]
    fn reconnect_backoff_caps_and_resets() {
        let mut backoff = ReconnectBackoff::default();

        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        for _ in 0..10 {
            backoff.next_delay();
        }
        assert_eq!(backoff.next_delay(), Duration::from_secs(30));

        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    }

    #[test]
    fn connack_clears_previous_connection_error() {
        let state = ConnectionState::default();
        state.connected.store(true, Ordering::Relaxed);

        state.record_error("network closed");
        assert_eq!(state.reconnects.load(Ordering::Relaxed), 1);
        assert_eq!(
            state.last_error.lock().unwrap().as_deref(),
            Some("network closed")
        );

        state.record_connack();

        assert!(state.connected.load(Ordering::Relaxed));
        assert_eq!(state.reconnects.load(Ordering::Relaxed), 1);
        assert_eq!(*state.last_error.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn failed_publish_updates_counters() {
        let mut publisher = FakePublisher {
            fail: true,
            ..FakePublisher::default()
        };
        let sample = PointSample {
            point: PointConfig::default(),
            value: TelemetryValue::Text("active".to_string()),
            topic: "Netix/Site/AHU1/mode".to_string(),
            timestamp_ms: 1,
        };

        let stats = publish_samples(&mut publisher, &MqttConfig::default(), &[sample]).await;

        assert_eq!(stats.queued, 1);
        assert_eq!(stats.published, 0);
        assert_eq!(stats.failed, 1);
        assert!(stats
            .last_error
            .as_deref()
            .unwrap()
            .contains("publish failed"));
    }
}
