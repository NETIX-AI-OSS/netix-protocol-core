use crate::config::MqttConfig;
use crate::model::{PointSample, PublishStats};
use anyhow::{anyhow, Context, Result};
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event, MqttOptions, Packet, QoS, TlsConfiguration, Transport,
};
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
    /// Samples enqueued to the outbound channel (local attempts, not delivery).
    pub published: usize,
    /// Broker-confirmed deliveries (running total of QoS 1 PubAcks).
    pub acked: usize,
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
    /// Running total of broker-confirmed QoS 1 deliveries (PubAcks).
    acked: AtomicUsize,
    /// Sticky flag set when the broker *rejects* the connection with a non-Success
    /// CONNACK return code (bad username/password, not authorized, …). Unlike a
    /// transport drop this will not self-heal on retry, so it is surfaced as a
    /// fatal connection error rather than counted as a reconnect.
    fatal: AtomicBool,
    last_error: Mutex<Option<String>>,
}

impl ConnectionState {
    /// Handle a broker CONNACK. A `Success` code means the link is live; any other
    /// code is a fatal auth/config rejection — the connection is NOT counted as up
    /// (see [`ConnectionState::record_fatal`]).
    fn record_connack(&self, code: ConnectReturnCode) {
        if code == ConnectReturnCode::Success {
            self.connected.store(true, Ordering::Relaxed);
            self.fatal.store(false, Ordering::Relaxed);
            if let Ok(mut last_error) = self.last_error.lock() {
                *last_error = None;
            }
        } else {
            self.record_fatal(connack_error_message(code));
        }
    }

    /// Record a fatal, non-self-healing connection error (broker rejected the
    /// connection). Marks the link down and sets the sticky `fatal` flag so the
    /// error is surfaced to the operator instead of being silently retried.
    fn record_fatal(&self, error: impl Into<String>) {
        self.connected.store(false, Ordering::Relaxed);
        self.fatal.store(true, Ordering::Relaxed);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(error.into());
        }
    }

    /// Count a broker-confirmed QoS 1 delivery.
    fn record_puback(&self) {
        self.acked.fetch_add(1, Ordering::Relaxed);
    }

    fn record_error(&self, error: impl Into<String>) {
        if self.connected.swap(false, Ordering::Relaxed) {
            self.reconnects.fetch_add(1, Ordering::Relaxed);
        }
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(error.into());
        }
    }

    #[cfg(test)]
    fn connection_fatal_error_for_test(&self) -> Option<String> {
        if self.fatal.load(Ordering::Relaxed) {
            self.last_error.lock().ok().and_then(|value| value.clone())
        } else {
            None
        }
    }
}

/// Human-readable explanation for a non-Success MQTT CONNACK return code.
fn connack_error_message(code: ConnectReturnCode) -> String {
    let reason = match code {
        ConnectReturnCode::Success => "connection accepted",
        ConnectReturnCode::RefusedProtocolVersion => "unacceptable protocol version",
        ConnectReturnCode::BadClientId => "client identifier rejected",
        ConnectReturnCode::ServiceUnavailable => "service unavailable",
        ConnectReturnCode::BadUserNamePassword => "bad username or password",
        ConnectReturnCode::NotAuthorized => "not authorized",
    };
    format!(
        "MQTT broker refused the connection: {reason} (CONNACK {code:?}); \
         check credentials and broker permissions"
    )
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
                        Ok(Event::Incoming(Packet::ConnAck(connack))) => {
                            // Inspect the return code: a non-Success code (bad auth,
                            // not authorized, …) is a fatal rejection, NOT a healthy
                            // connection. record_connack sets the fatal error state.
                            state.record_connack(connack.code);
                            backoff.reset();
                        }
                        Ok(Event::Incoming(Packet::PubAck(_))) => {
                            // Broker confirmed a QoS 1 delivery — the honest counter.
                            state.record_puback();
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
        stats.acked = self.acked_count();
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

    /// Running total of broker-confirmed QoS 1 deliveries (PubAcks). The honest
    /// "delivered" counter — distinct from local enqueue attempts.
    pub fn acked_count(&self) -> usize {
        self.state.acked.load(Ordering::Relaxed)
    }

    pub fn last_connection_error(&self) -> Option<String> {
        self.state
            .last_error
            .lock()
            .ok()
            .and_then(|value| value.clone())
    }

    /// A human message when the broker has *rejected* the connection (bad auth,
    /// not authorized, …) — a fatal, non-self-healing error worth surfacing to the
    /// operator. `None` while the connection is healthy or only transiently down.
    pub fn connection_fatal_error(&self) -> Option<String> {
        if self.state.fatal.load(Ordering::Relaxed) {
            self.last_connection_error()
        } else {
            None
        }
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
        // Local enqueue attempts this interval — NOT proof of delivery.
        "published": snapshot.published,
        "queued": snapshot.published,
        // Broker-confirmed deliveries (running total of QoS 1 PubAcks).
        "acked": snapshot.acked,
        "delivered": snapshot.acked,
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
                acked: 7,
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
        assert_eq!(payload["queued"], 1);
        // Broker-confirmed deliveries are surfaced distinctly from local attempts.
        assert_eq!(payload["acked"], 7);
        assert_eq!(payload["delivered"], 7);
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

        state.record_connack(ConnectReturnCode::Success);

        assert!(state.connected.load(Ordering::Relaxed));
        assert_eq!(state.reconnects.load(Ordering::Relaxed), 1);
        assert_eq!(*state.last_error.lock().unwrap(), None);
        assert!(state.connection_fatal_error_for_test().is_none());
    }

    #[test]
    fn connack_failure_code_sets_fatal_error_and_does_not_connect() {
        let state = ConnectionState::default();

        // A bad-auth CONNACK must NOT mark the link up, and must surface a fatal,
        // human-readable error rather than being counted as a reconnect.
        state.record_connack(ConnectReturnCode::BadUserNamePassword);

        assert!(!state.connected.load(Ordering::Relaxed));
        assert_eq!(state.reconnects.load(Ordering::Relaxed), 0);
        assert!(state.fatal.load(Ordering::Relaxed));
        let message = state.last_error.lock().unwrap().clone().unwrap();
        assert!(message.contains("bad username or password"), "{message}");
        assert_eq!(
            state.connection_fatal_error_for_test().as_deref(),
            Some(&message[..])
        );

        // A later successful CONNACK clears the fatal state.
        state.record_connack(ConnectReturnCode::Success);
        assert!(state.connected.load(Ordering::Relaxed));
        assert!(!state.fatal.load(Ordering::Relaxed));
        assert!(state.connection_fatal_error_for_test().is_none());
    }

    #[test]
    fn connack_not_authorized_is_fatal() {
        let state = ConnectionState::default();
        state.record_connack(ConnectReturnCode::NotAuthorized);
        assert!(state.fatal.load(Ordering::Relaxed));
        assert!(state
            .last_error
            .lock()
            .unwrap()
            .as_deref()
            .unwrap()
            .contains("not authorized"));
    }

    #[test]
    fn puback_increments_acked_counter() {
        let state = ConnectionState::default();
        assert_eq!(state.acked.load(Ordering::Relaxed), 0);
        state.record_puback();
        state.record_puback();
        assert_eq!(state.acked.load(Ordering::Relaxed), 2);
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

    fn write_test_tls_material(
        dir: &Path,
    ) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let ca = rcgen::generate_simple_self_signed(vec!["mqtt.local".into()]).unwrap();
        let ca_path = dir.join("ca.pem");
        std::fs::write(&ca_path, ca.cert.pem()).unwrap();

        let mut params = rcgen::CertificateParams::new(vec!["client.local".into()]).unwrap();
        params.is_ca = rcgen::IsCa::NoCa;
        let client_key = rcgen::KeyPair::generate().unwrap();
        let client_cert = params.self_signed(&client_key).unwrap();
        let client_cert_path = dir.join("client.pem");
        let client_key_path = dir.join("client.key");
        std::fs::write(&client_cert_path, client_cert.pem()).unwrap();
        std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();
        (ca_path, client_cert_path, client_key_path)
    }

    #[test]
    fn build_transport_tcp_when_tls_disabled() {
        let cfg = MqttConfig {
            use_tls: false,
            ..MqttConfig::default()
        };
        assert!(matches!(build_transport(&cfg).unwrap(), Transport::Tcp));
    }

    #[test]
    fn build_transport_uses_default_tls_without_custom_paths() {
        let cfg = MqttConfig {
            use_tls: true,
            ..MqttConfig::default()
        };
        assert!(matches!(build_transport(&cfg).unwrap(), Transport::Tls(_)));
    }

    #[test]
    fn build_transport_loads_custom_ca_and_client_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let (ca_path, cert_path, key_path) = write_test_tls_material(dir.path());
        let cfg = MqttConfig {
            use_tls: true,
            ca_cert_path: Some(ca_path.to_string_lossy().into_owned()),
            client_cert_path: Some(cert_path.to_string_lossy().into_owned()),
            client_key_path: Some(key_path.to_string_lossy().into_owned()),
            ..MqttConfig::default()
        };
        assert!(matches!(build_transport(&cfg).unwrap(), Transport::Tls(_)));
    }

    #[test]
    fn build_transport_allows_client_cert_without_key_at_transport_layer() {
        let dir = tempfile::tempdir().unwrap();
        let (ca_path, cert_path, _) = write_test_tls_material(dir.path());
        let cfg = MqttConfig {
            use_tls: true,
            ca_cert_path: Some(ca_path.to_string_lossy().into_owned()),
            client_cert_path: Some(cert_path.to_string_lossy().into_owned()),
            client_key_path: None,
            ..MqttConfig::default()
        };
        assert!(build_transport(&cfg).is_ok());
    }

    #[test]
    fn health_snapshot_status_is_ok_without_failures() {
        let snapshot = HealthSnapshot {
            published: 9,
            acked: 9,
            failed_reads: 0,
            failed_publishes: 0,
            stale_points: 0,
            reconnects: 2,
            last_error: None,
        };
        // Reconnects alone do not degrade status — only read/publish/stale failures do.
        assert_eq!(snapshot.status(), "ok");
    }

    #[test]
    fn health_snapshot_status_degrades_on_stale_points_only() {
        let snapshot = HealthSnapshot {
            published: 1,
            acked: 1,
            failed_reads: 0,
            failed_publishes: 0,
            stale_points: 1,
            reconnects: 0,
            last_error: None,
        };
        assert_eq!(snapshot.status(), "degraded");
    }

    #[test]
    fn health_snapshot_is_serializable_snapshot_value() {
        // The snapshot is a plain value: clone/equality hold so the poll loop can
        // diff successive snapshots.
        let snapshot = HealthSnapshot {
            published: 3,
            acked: 2,
            failed_reads: 1,
            failed_publishes: 0,
            stale_points: 0,
            reconnects: 4,
            last_error: Some("boom".to_string()),
        };
        assert_eq!(snapshot.clone(), snapshot);
    }

    #[test]
    fn connack_error_message_describes_every_return_code() {
        // Every non-Success arm must produce an operator-actionable reason, and the
        // Success arm is still well-formed even though record_connack never routes it here.
        for (code, needle) in [
            (ConnectReturnCode::Success, "connection accepted"),
            (
                ConnectReturnCode::RefusedProtocolVersion,
                "unacceptable protocol version",
            ),
            (ConnectReturnCode::BadClientId, "client identifier rejected"),
            (ConnectReturnCode::ServiceUnavailable, "service unavailable"),
            (
                ConnectReturnCode::BadUserNamePassword,
                "bad username or password",
            ),
            (ConnectReturnCode::NotAuthorized, "not authorized"),
        ] {
            let message = connack_error_message(code);
            assert!(message.contains(needle), "{code:?}: {message}");
            assert!(message.contains("MQTT broker refused the connection"));
        }
    }

    #[test]
    fn connack_bad_client_id_and_service_unavailable_are_fatal() {
        for code in [
            ConnectReturnCode::RefusedProtocolVersion,
            ConnectReturnCode::BadClientId,
            ConnectReturnCode::ServiceUnavailable,
        ] {
            let state = ConnectionState::default();
            state.record_connack(code);
            assert!(state.fatal.load(Ordering::Relaxed), "{code:?}");
            assert!(!state.connected.load(Ordering::Relaxed), "{code:?}");
            assert_eq!(state.reconnects.load(Ordering::Relaxed), 0, "{code:?}");
            assert!(
                state.connection_fatal_error_for_test().is_some(),
                "{code:?}"
            );
        }
    }

    #[test]
    fn build_transport_uses_native_roots_with_client_cert_and_no_ca() {
        // client cert present but no explicit CA → the platform's native root store
        // is loaded and combined with the client certificate.
        let dir = tempfile::tempdir().unwrap();
        let (_, cert_path, key_path) = write_test_tls_material(dir.path());
        let cfg = MqttConfig {
            use_tls: true,
            ca_cert_path: None,
            client_cert_path: Some(cert_path.to_string_lossy().into_owned()),
            client_key_path: Some(key_path.to_string_lossy().into_owned()),
            ..MqttConfig::default()
        };
        assert!(matches!(build_transport(&cfg).unwrap(), Transport::Tls(_)));
    }

    #[test]
    fn build_transport_rejects_ca_file_without_certificates() {
        let dir = tempfile::tempdir().unwrap();
        let bogus_ca = dir.path().join("empty-ca.pem");
        std::fs::write(&bogus_ca, b"not a certificate at all\n").unwrap();
        let cfg = MqttConfig {
            use_tls: true,
            ca_cert_path: Some(bogus_ca.to_string_lossy().into_owned()),
            ..MqttConfig::default()
        };
        let error = build_transport(&cfg)
            .err()
            .expect("expected a CA parse error");
        assert!(
            format!("{error:#}").contains("no usable CA certificates"),
            "{error:#}"
        );
    }

    #[test]
    fn rumqtt_publisher_new_requires_a_tokio_runtime() {
        // Called with no current runtime it must fail fast rather than panic later.
        let cfg = MqttConfig {
            use_tls: false,
            ..MqttConfig::default()
        };
        let error = RumqttPublisher::new(&cfg)
            .err()
            .expect("expected a missing-runtime error");
        assert!(
            format!("{error}").contains("within a tokio runtime"),
            "{error}"
        );
    }

    fn test_sample(topic: &str) -> PointSample {
        PointSample {
            point: PointConfig::default(),
            value: TelemetryValue::Number(1.0),
            topic: topic.to_string(),
            timestamp_ms: 1,
        }
    }

    #[tokio::test]
    async fn rumqtt_publisher_enqueues_without_a_broker_and_reports_counters() {
        // A never-reachable broker: the event loop stays in connect/backoff, so
        // try_publish only enqueues into the outbound channel — no delivery.
        let cfg = MqttConfig {
            host: "127.0.0.1".to_string(),
            port: 1,
            use_tls: false,
            retain: true,
            // Non-empty credentials exercise the set_credentials path in new().
            username: Some("edge".to_string()),
            password: Some("secret".to_string()),
            ..MqttConfig::default()
        };
        let mut publisher = RumqttPublisher::new(&cfg).unwrap();

        assert_eq!(publisher.reconnect_count(), 0);
        assert_eq!(publisher.acked_count(), 0);
        assert!(publisher.connection_fatal_error().is_none());

        let samples = [test_sample("Netix/A"), test_sample("Netix/B")];
        let stats = publisher.enqueue_samples(&cfg, &samples);
        assert_eq!(stats.queued, 2);
        assert_eq!(stats.published, 2);
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.acked, 0);
        assert_eq!(stats.reconnects, 0);

        // Direct enqueue helpers succeed too (channel has room).
        publisher
            .try_enqueue_sample("Netix/C", b"1".to_vec(), false)
            .unwrap();
        MqttPublisher::publish(&mut publisher, "Netix/D", b"1".to_vec(), true)
            .await
            .unwrap();
        // Drop aborts the background task.
    }

    #[tokio::test]
    async fn rumqtt_publisher_records_transient_error_when_broker_refuses() {
        // Connecting to a closed local port yields a transport error the event loop
        // records — a transient (non-fatal) failure, so no fatal error is surfaced.
        let cfg = MqttConfig {
            host: "127.0.0.1".to_string(),
            port: 1,
            use_tls: false,
            ..MqttConfig::default()
        };
        let publisher = RumqttPublisher::new(&cfg).unwrap();

        let mut recorded = None;
        for _ in 0..100 {
            if let Some(error) = publisher.last_connection_error() {
                recorded = Some(error);
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        let recorded = recorded.expect("event loop should have recorded a connection error");
        assert!(!recorded.is_empty());
        // A transport refusal is not a fatal auth/config rejection.
        assert!(publisher.connection_fatal_error().is_none());
    }

    #[tokio::test]
    async fn enqueue_samples_counts_failures_when_outbound_channel_is_full() {
        // With no broker the channel never drains; enqueuing past its capacity forces
        // try_publish to fail fast so samples are dropped and counted rather than blocking.
        let cfg = MqttConfig {
            host: "127.0.0.1".to_string(),
            port: 1,
            use_tls: false,
            ..MqttConfig::default()
        };
        let mut publisher = RumqttPublisher::new(&cfg).unwrap();

        let overflow = OUTBOUND_CHANNEL_CAPACITY + 500;
        let samples: Vec<PointSample> = (0..overflow).map(|_| test_sample("Netix/Flood")).collect();
        let stats = publisher.enqueue_samples(&cfg, &samples);

        assert_eq!(stats.queued, overflow);
        assert_eq!(stats.published + stats.failed, overflow);
        assert!(stats.failed > 0, "channel-full drops should be counted");
        assert!(stats.published <= OUTBOUND_CHANNEL_CAPACITY);
        assert!(
            stats
                .last_error
                .as_deref()
                .unwrap()
                .contains("failed to enqueue MQTT publish"),
            "{:?}",
            stats.last_error
        );
    }
}
