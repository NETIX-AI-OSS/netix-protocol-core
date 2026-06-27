//! BACnet/IP republisher adapter: Who-Is discovery, object-list browse, and
//! ReadProperty(Multiple) polling, mapped onto the generic republisher trait.

mod refresh;
mod value;

#[cfg(feature = "republish")]
pub mod test_support;

use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};
use bacnet_client::client::BACnetClient;
use bacnet_services::common::PropertyReference;
use bacnet_services::rpm::ReadAccessSpecification;
use bacnet_transport::bip::{BipTransport, ForeignDeviceConfig};
use bacnet_types::enums::{ObjectType, PropertyIdentifier};
use bacnet_types::primitives::ObjectIdentifier;
use futures_util::stream::{self, StreamExt};
use proto_api::{Addressing, Capabilities};
use republish_core::model::{
    json_scalar, now_millis, DiscoverOutcome, DiscoveredDevice, DiscoveredPoint, PointConfig,
    PointFailure, PointSample, PollOutcome, RefreshOutcome,
};
use republish_core::network::{ipv4_interfaces, NetworkInterface};
use republish_core::RepublishProtocol;

use refresh::refresh_device_table;
use value::{
    decode_object_id, decode_scalar_value, decode_unsigned, object_type_from_text,
    object_type_name, property_identifier_from_text,
};

type BacnetIpClient = BACnetClient<BipTransport>;

const DISCOVERY_BROADCAST_PASSES: usize = 3;
pub(crate) const MAX_BROWSE_OBJECTS: usize = 512;

pub struct BacnetRepublishProtocol {
    caps: Capabilities,
    session: tokio::sync::Mutex<Option<PollSession>>,
}

struct PollSession {
    fingerprint: ConnFingerprint,
    bind_interface: Ipv4Addr,
    client: BacnetIpClient,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConnFingerprint {
    interface: Ipv4Addr,
    port: u16,
    broadcast: Ipv4Addr,
    discovery_window_ms: u64,
    apdu_timeout_ms: u64,
    bbmd: Option<(Ipv4Addr, u16, u16)>,
}

impl ConnFingerprint {
    fn from_cfg(cfg: &ConnCfg) -> Self {
        Self {
            interface: cfg.interface,
            port: cfg.port,
            broadcast: cfg.broadcast,
            discovery_window_ms: cfg.discovery_window_ms,
            apdu_timeout_ms: cfg.apdu_timeout_ms,
            bbmd: cfg.bbmd.as_ref().map(|b| (b.address, b.port, b.ttl_secs as u16)),
        }
    }
}

impl BacnetRepublishProtocol {
    pub fn new(caps: Capabilities) -> Self {
        Self {
            caps,
            session: tokio::sync::Mutex::new(None),
        }
    }

    async fn ensure_poll_client(
        &self,
        cfg: &ConnCfg,
        interfaces: &[NetworkInterface],
    ) -> Result<tokio::sync::MutexGuard<'_, Option<PollSession>>> {
        let mut guard = self.session.lock().await;
        let bind = resolve_bind_interface(cfg, interfaces);
        let fingerprint = ConnFingerprint::from_cfg(cfg);
        let needs_rebuild = match guard.as_ref() {
            None => true,
            Some(session) => {
                session.fingerprint != fingerprint || session.bind_interface != bind
            }
        };
        if needs_rebuild {
            if let Some(mut old) = guard.take() {
                old.client.stop().await.ok();
            }
            let client = build_client(cfg, bind).await?;
            *guard = Some(PollSession {
                fingerprint,
                bind_interface: bind,
                client,
            });
        }
        Ok(guard)
    }
}

pub fn republish_factory() -> Box<dyn RepublishProtocol> {
    Box::new(BacnetRepublishProtocol::new(crate::capabilities()))
}

struct ConnCfg {
    interface: Ipv4Addr,
    discover_all_interfaces: bool,
    bind_failure_policy: BindFailurePolicy,
    port: u16,
    broadcast: Ipv4Addr,
    bbmd: Option<BbmdCfg>,
    discovery_window_ms: u64,
    apdu_timeout_ms: u64,
    poll_concurrency: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BindFailurePolicy {
    Skip,
    Strict,
}

struct BbmdCfg {
    address: Ipv4Addr,
    port: u16,
    ttl_secs: u16,
}

fn conn_bool(conn: &Addressing, key: &str) -> bool {
    match conn.get(key) {
        Some(serde_json::Value::Bool(v)) => *v,
        Some(serde_json::Value::String(s)) => s.eq_ignore_ascii_case("true") || s == "1",
        Some(serde_json::Value::Number(n)) => n.as_u64().is_some_and(|v| v != 0),
        _ => false,
    }
}

fn conn_str(conn: &Addressing, key: &str) -> Option<String> {
    conn.get(key)
        .map(json_scalar)
        .filter(|s| !s.trim().is_empty())
}

fn conn_u64(conn: &Addressing, key: &str) -> Option<u64> {
    match conn.get(key)? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn parse_conn(conn: &Addressing, interfaces: &[NetworkInterface]) -> ConnCfg {
    let interface = conn_str(conn, "interface")
        .and_then(|s| s.parse::<Ipv4Addr>().ok())
        .or_else(|| interfaces.first().map(|i| i.addr))
        .unwrap_or(Ipv4Addr::UNSPECIFIED);
    let port = conn_u64(conn, "port").unwrap_or(0) as u16;
    let broadcast = conn_str(conn, "broadcast_address")
        .and_then(|s| s.parse::<Ipv4Addr>().ok())
        .unwrap_or(Ipv4Addr::BROADCAST);
    let bbmd = conn_str(conn, "bbmd_address")
        .and_then(|s| s.parse::<Ipv4Addr>().ok())
        .map(|address| BbmdCfg {
            address,
            port: conn_u64(conn, "bbmd_port").unwrap_or(47808) as u16,
            ttl_secs: conn_u64(conn, "bbmd_ttl_secs").unwrap_or(300) as u16,
        });
    let bind_failure_policy = match conn_str(conn, "bind_failure_policy")
        .unwrap_or_else(|| "skip".into())
        .to_ascii_lowercase()
        .as_str()
    {
        "strict" => BindFailurePolicy::Strict,
        _ => BindFailurePolicy::Skip,
    };
    ConnCfg {
        interface,
        discover_all_interfaces: conn_bool(conn, "discover_all_interfaces"),
        bind_failure_policy,
        port,
        broadcast,
        bbmd,
        discovery_window_ms: conn_u64(conn, "discovery_window_ms").unwrap_or(3000),
        apdu_timeout_ms: conn_u64(conn, "apdu_timeout_ms").unwrap_or(2000),
        poll_concurrency: conn_u64(conn, "poll_concurrency").unwrap_or(8) as usize,
    }
}

async fn build_client(cfg: &ConnCfg, interface: Ipv4Addr) -> Result<BacnetIpClient> {
    let mut transport = BipTransport::new(interface, cfg.port, cfg.broadcast);
    if let Some(bbmd) = &cfg.bbmd {
        transport.register_as_foreign_device(ForeignDeviceConfig {
            bbmd_ip: bbmd.address,
            bbmd_port: bbmd.port,
            ttl: bbmd.ttl_secs,
        });
    }
    BACnetClient::<BipTransport>::generic_builder()
        .transport(transport)
        .apdu_timeout_ms(cfg.apdu_timeout_ms)
        .build()
        .await
        .map_err(|error| anyhow!(error.to_string()))
}

fn target_interfaces(cfg: &ConnCfg, interfaces: &[NetworkInterface]) -> Vec<Ipv4Addr> {
    if cfg.discover_all_interfaces {
        if interfaces.is_empty() {
            return vec![Ipv4Addr::UNSPECIFIED];
        }
        let mut addrs: Vec<Ipv4Addr> = interfaces.iter().map(|i| i.addr).collect();
        addrs.sort();
        addrs.dedup();
        addrs
    } else {
        vec![cfg.interface]
    }
}

fn resolve_bind_interface(cfg: &ConnCfg, interfaces: &[NetworkInterface]) -> Ipv4Addr {
    if cfg.interface.is_unspecified() {
        interfaces
            .first()
            .map(|i| i.addr)
            .unwrap_or(Ipv4Addr::UNSPECIFIED)
    } else {
        cfg.interface
    }
}

fn format_bip_mac(mac: &[u8]) -> String {
    if mac.len() >= 6 {
        let ip = Ipv4Addr::new(mac[0], mac[1], mac[2], mac[3]);
        let port = u16::from_be_bytes([mac[4], mac[5]]);
        format!("{ip}:{port}")
    } else {
        mac.iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":")
    }
}

fn device_key(instance: u32) -> String {
    format!("device_{instance}")
}

fn instance_from_key(key: &str) -> Option<u32> {
    key.strip_prefix("device_").and_then(|n| n.parse().ok())
}

async fn collect_devices(client: &BacnetIpClient) -> Vec<DiscoveredDevice> {
    let mut devices = client
        .discovered_devices()
        .await
        .into_iter()
        .map(|device| {
            let instance = device.object_identifier.instance_number();
            DiscoveredDevice {
                key: device_key(instance),
                address: format_bip_mac(device.mac_address.as_slice()),
                detail: format!("instance {instance}, vendor {}", device.vendor_id),
            }
        })
        .collect::<Vec<_>>();
    devices.sort_by(|a, b| a.key.cmp(&b.key));
    devices
}

#[async_trait::async_trait]
impl RepublishProtocol for BacnetRepublishProtocol {
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn discover(&self, conn: &Addressing) -> Result<DiscoverOutcome> {
        let interfaces = ipv4_interfaces();
        let cfg = parse_conn(conn, &interfaces);
        let mut by_key: HashMap<String, DiscoveredDevice> = HashMap::new();
        let mut warnings = Vec::new();

        for interface in target_interfaces(&cfg, &interfaces) {
            let mut client = match build_client(&cfg, interface).await {
                Ok(client) => client,
                Err(error) => {
                    let message = format!("bind failed on {interface}: {error:#}");
                    if cfg.bind_failure_policy == BindFailurePolicy::Strict {
                        return Err(anyhow!(message));
                    }
                    warnings.push(message);
                    continue;
                }
            };
            for _ in 0..DISCOVERY_BROADCAST_PASSES {
                if client.who_is(None, None).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(cfg.discovery_window_ms)).await;
                for device in collect_devices(&client).await {
                    by_key.insert(device.key.clone(), device);
                }
            }
            client.stop().await.ok();
        }

        let mut devices: Vec<DiscoveredDevice> = by_key.into_values().collect();
        devices.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(DiscoverOutcome { devices, warnings })
    }

    async fn browse(
        &self,
        conn: &Addressing,
        device: &DiscoveredDevice,
    ) -> Result<Vec<DiscoveredPoint>> {
        let device_instance = instance_from_key(&device.key)
            .ok_or_else(|| anyhow!("cannot determine device instance from '{}'", device.key))?;
        let interfaces = ipv4_interfaces();
        let cfg = parse_conn(conn, &interfaces);
        let mut client = build_client(&cfg, cfg.interface).await?;
        refresh_device_table(&client, &cfg, &[device_instance]).await?;

        let result = scan_objects(&client, device_instance, &device.key).await;
        client.stop().await.ok();
        result
    }

    async fn poll(&self, conn: &Addressing, points: &[PointConfig]) -> Result<PollOutcome> {
        let enabled: Vec<PointConfig> = points.iter().filter(|p| p.enabled).cloned().collect();
        if enabled.is_empty() {
            return Ok(PollOutcome::default());
        }
        let interfaces = ipv4_interfaces();
        let cfg = parse_conn(conn, &interfaces);
        let mut guard = self.ensure_poll_client(&cfg, &interfaces).await?;
        let client = &mut guard.as_mut().expect("session initialized").client;
        poll_with_client(client, &enabled, cfg.poll_concurrency).await
    }

    async fn refresh_devices(
        &self,
        conn: &Addressing,
        device_instances: &[u32],
    ) -> Result<RefreshOutcome> {
        let interfaces = ipv4_interfaces();
        let cfg = parse_conn(conn, &interfaces);
        let mut guard = self.ensure_poll_client(&cfg, &interfaces).await?;
        let client = &guard.as_mut().expect("session initialized").client;
        refresh_device_table(client, &cfg, device_instances).await
    }
}

async fn scan_objects(
    client: &BacnetIpClient,
    device_instance: u32,
    dev_key: &str,
) -> Result<Vec<DiscoveredPoint>> {
    let device_oid = ObjectIdentifier::new(ObjectType::DEVICE, device_instance)?;
    let count_ack = client
        .read_property_from_device(
            device_instance,
            device_oid,
            PropertyIdentifier::OBJECT_LIST,
            Some(0),
        )
        .await
        .context("failed to read objectList length")?;
    let count =
        decode_unsigned(&count_ack.property_value).context("decode objectList length")? as usize;

    let mut points = Vec::new();
    for index in 1..=count.min(MAX_BROWSE_OBJECTS) {
        let Ok(ack) = client
            .read_property_from_device(
                device_instance,
                device_oid,
                PropertyIdentifier::OBJECT_LIST,
                Some(index as u32),
            )
            .await
        else {
            continue;
        };
        let Ok((object_type, object_instance)) = decode_object_id(&ack.property_value) else {
            continue;
        };
        if object_type == ObjectType::DEVICE {
            continue;
        }
        let object_identifier = ObjectIdentifier::new(object_type, object_instance)?;
        let name = read_scalar(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::OBJECT_NAME,
        )
        .await
        .map(|v| v.to_string())
        .filter(|v| !v.trim().is_empty());
        let description = read_scalar(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::DESCRIPTION,
        )
        .await
        .map(|v| v.to_string())
        .filter(|v| !v.trim().is_empty());
        let units = read_scalar(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::UNITS,
        )
        .await
        .map(|v| v.to_string())
        .filter(|v| !v.trim().is_empty());
        let present = read_scalar(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::PRESENT_VALUE,
        )
        .await;

        let type_name = object_type_name(object_type);
        let mut addressing = Addressing::new();
        addressing.insert("device_instance".into(), serde_json::json!(device_instance));
        addressing.insert("object_type".into(), serde_json::json!(type_name.clone()));
        addressing.insert("object_instance".into(), serde_json::json!(object_instance));
        addressing.insert("property".into(), serde_json::json!("present_value"));
        let point_name = name
            .clone()
            .unwrap_or_else(|| format!("{type_name}_{object_instance}"));
        points.push(DiscoveredPoint {
            device_key: dev_key.to_string(),
            name,
            description,
            units,
            value: present,
            addressing,
            suggested_tag_path: format!("{dev_key}/{point_name}"),
        });
    }
    Ok(points)
}

pub(crate) async fn read_scalar(
    client: &BacnetIpClient,
    device_instance: u32,
    object_identifier: ObjectIdentifier,
    property_identifier: PropertyIdentifier,
) -> Option<republish_core::TelemetryValue> {
    client
        .read_property_from_device(
            device_instance,
            object_identifier,
            property_identifier,
            None,
        )
        .await
        .ok()
        .and_then(|ack| decode_scalar_value(&ack.property_value).ok())
}

struct PollRequest {
    point: PointConfig,
    device_instance: u32,
    object_identifier: ObjectIdentifier,
    property_identifier: PropertyIdentifier,
}

fn addr_str(point: &PointConfig, key: &str) -> Option<String> {
    point
        .addressing
        .get(key)
        .map(json_scalar)
        .filter(|s| !s.trim().is_empty())
}

fn addr_u32(point: &PointConfig, key: &str) -> Option<u32> {
    match point.addressing.get(key)? {
        serde_json::Value::Number(n) => n.as_u64().map(|v| v as u32),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

impl PollRequest {
    fn from_point(point: PointConfig) -> Result<Self> {
        let device_instance = addr_u32(&point, "device_instance")
            .ok_or_else(|| anyhow!("missing device_instance"))?;
        let type_text = addr_str(&point, "object_type").unwrap_or_default();
        let object_type = object_type_from_text(&type_text)
            .ok_or_else(|| anyhow!("unknown object type '{type_text}'"))?;
        let object_instance = addr_u32(&point, "object_instance").unwrap_or(0);
        let prop_text = addr_str(&point, "property").unwrap_or_else(|| "present_value".into());
        let property_identifier = property_identifier_from_text(&prop_text)
            .ok_or_else(|| anyhow!("unknown property '{prop_text}'"))?;
        let object_identifier = ObjectIdentifier::new(object_type, object_instance)
            .context("invalid object identifier")?;
        Ok(Self {
            point,
            device_instance,
            object_identifier,
            property_identifier,
        })
    }
}

pub(crate) async fn poll_with_client(
    client: &BacnetIpClient,
    points: &[PointConfig],
    concurrency: usize,
) -> Result<PollOutcome> {
    let mut by_device: HashMap<u32, Vec<PollRequest>> = HashMap::new();
    let mut failures = Vec::new();
    for point in points.iter().cloned() {
        match PollRequest::from_point(point.clone()) {
            Ok(request) => by_device
                .entry(request.device_instance)
                .or_default()
                .push(request),
            Err(error) => failures.push(PointFailure {
                point,
                error: error.to_string(),
            }),
        }
    }

    let group_results = stream::iter(by_device)
        .map(|(device_instance, requests)| async move {
            match read_group_rpm(client, device_instance, &requests).await {
                Ok(samples) => (None, samples, Vec::new()),
                Err(error) => {
                    let warning = format!(
                        "RPM failed for device {device_instance}; used fallback: {error:#}"
                    );
                    let (samples, fails) = read_group_individual(client, &requests).await;
                    (Some(warning), samples, fails)
                }
            }
        })
        .buffer_unordered(concurrency.max(1))
        .collect::<Vec<_>>()
        .await;

    let mut samples = Vec::new();
    let mut warnings = Vec::new();
    for (warning, mut group_samples, mut group_failures) in group_results {
        if let Some(warning) = warning {
            warnings.push(warning);
        }
        samples.append(&mut group_samples);
        failures.append(&mut group_failures);
    }

    Ok(PollOutcome {
        samples,
        failures,
        warnings,
    })
}

fn sample(point: &PointConfig, value: republish_core::TelemetryValue) -> PointSample {
    PointSample {
        point: point.clone(),
        value,
        topic: String::new(), // filled in by the worker
        timestamp_ms: now_millis(),
    }
}

async fn read_group_rpm(
    client: &BacnetIpClient,
    device_instance: u32,
    requests: &[PollRequest],
) -> Result<Vec<PointSample>> {
    let specs = requests
        .iter()
        .map(|request| ReadAccessSpecification {
            object_identifier: request.object_identifier,
            list_of_property_references: vec![PropertyReference {
                property_identifier: request.property_identifier,
                property_array_index: None,
            }],
        })
        .collect::<Vec<_>>();

    let ack = client
        .read_property_multiple_from_device(device_instance, specs)
        .await?;
    let mut samples = Vec::new();
    let mut seen = HashSet::<usize>::new();
    for result in ack.list_of_read_access_results {
        for element in result.list_of_results {
            let Some((index, request)) = requests.iter().enumerate().find(|(_, request)| {
                request.object_identifier == result.object_identifier
                    && request.property_identifier == element.property_identifier
            }) else {
                continue;
            };
            let Some(value_bytes) = element.property_value else {
                continue;
            };
            let value = decode_scalar_value(&value_bytes)
                .with_context(|| format!("failed to decode {}", request.point.display_name()))?;
            seen.insert(index);
            samples.push(sample(&request.point, value));
        }
    }
    if seen.len() != requests.len() {
        return Err(anyhow!(
            "RPM returned {} of {} properties",
            seen.len(),
            requests.len()
        ));
    }
    Ok(samples)
}

async fn read_group_individual(
    client: &BacnetIpClient,
    requests: &[PollRequest],
) -> (Vec<PointSample>, Vec<PointFailure>) {
    let mut samples = Vec::new();
    let mut failures = Vec::new();
    for request in requests {
        let result = client
            .read_property_from_device(
                request.device_instance,
                request.object_identifier,
                request.property_identifier,
                None,
            )
            .await
            .map_err(|error| error.to_string())
            .and_then(|ack| decode_scalar_value(&ack.property_value).map_err(|e| e.to_string()));
        match result {
            Ok(value) => samples.push(sample(&request.point, value)),
            Err(error) => failures.push(PointFailure {
                point: request.point.clone(),
                error,
            }),
        }
    }
    (samples, failures)
}
