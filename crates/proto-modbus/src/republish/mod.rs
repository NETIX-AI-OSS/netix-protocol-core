//! Modbus TCP republisher adapter: manual endpoint connection, register-range
//! browse, and value polling with per-point datatype/word-order/scale decoding.

mod scan;

use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};
use futures_util::stream::{self, StreamExt};
use proto_api::{Addressing, Capabilities};
use republish_core::model::{
    now_millis, DiscoverOutcome, DiscoveredDevice, DiscoveredPoint, PointConfig, PointFailure,
    PointSample, PollOutcome, TelemetryValue,
};
use republish_core::RepublishProtocol;
use tokio::time::timeout;
use tokio_modbus::client::Context;
use tokio_modbus::prelude::Reader;
use tokio_modbus::Slave;

/// How many registers `browse` scans from each table.
const BROWSE_COUNT: u16 = 32;

pub struct ModbusRepublishProtocol {
    caps: Capabilities,
}

pub fn republish_factory() -> Box<dyn RepublishProtocol> {
    Box::new(ModbusRepublishProtocol {
        caps: crate::capabilities(),
    })
}

struct ConnParams {
    host: String,
    port: u16,
    unit: u8,
    timeout: Duration,
    scan_concurrency: usize,
    max_hosts: u32,
}

impl Clone for ConnParams {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            port: self.port,
            unit: self.unit,
            timeout: self.timeout,
            scan_concurrency: self.scan_concurrency,
            max_hosts: self.max_hosts,
        }
    }
}

fn conn_str(conn: &Addressing, key: &str) -> Option<String> {
    conn.get(key).map(republish_core::model::json_scalar)
}

fn conn_u64(conn: &Addressing, key: &str) -> Option<u64> {
    match conn.get(key)? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn parse_conn(conn: &Addressing) -> Result<ConnParams> {
    let host = conn_str(conn, "host")
        .filter(|h| !h.trim().is_empty())
        .ok_or_else(|| anyhow!("Modbus host is required"))?;
    let port = conn_u64(conn, "port").unwrap_or(502) as u16;
    let unit = conn_u64(conn, "unit_id").unwrap_or(1) as u8;
    let timeout_ms = conn_u64(conn, "timeout_ms").unwrap_or(1000);
    Ok(ConnParams {
        host: host.trim().to_string(),
        port,
        unit,
        timeout: Duration::from_millis(timeout_ms.max(100)),
        scan_concurrency: conn_u64(conn, "scan_concurrency").unwrap_or(32).max(1) as usize,
        max_hosts: conn_u64(conn, "max_hosts").unwrap_or(256).max(1) as u32,
    })
}

fn socket_addr(host: &str, port: u16) -> Result<SocketAddr> {
    format!("{host}:{port}")
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve {host}:{port}"))?
        .next()
        .ok_or_else(|| anyhow!("no address for {host}:{port}"))
}

fn endpoint_label(host: &str, port: u16) -> String {
    format!("{host}:{port}")
}

async fn connect_addr(
    addr: SocketAddr,
    unit: u8,
    connect_timeout: Duration,
    label: &str,
) -> Result<Context> {
    let ctx = timeout(
        connect_timeout,
        tokio_modbus::client::tcp::connect_slave(addr, Slave(unit)),
    )
    .await
    .map_err(|_| anyhow!("connect to {label} timed out"))?
    .with_context(|| format!("failed to connect to {label}"))?;
    Ok(ctx)
}

async fn connect(params: &ConnParams) -> Result<Context> {
    let addr = socket_addr(&params.host, params.port)?;
    connect_addr(
        addr,
        params.unit,
        params.timeout,
        &endpoint_label(&params.host, params.port),
    )
    .await
}

async fn probe_host(ip: Ipv4Addr, params: &ConnParams) -> Option<DiscoveredDevice> {
    let addr = SocketAddr::from((ip, params.port));
    let host_label = endpoint_label(&ip.to_string(), params.port);
    let mut ctx = connect_addr(addr, params.unit, params.timeout, &host_label)
        .await
        .ok()?;
    ctx.read_holding_registers(0, 1).await.ok()?.ok()?;
    Some(DiscoveredDevice {
        key: format!("modbus-{}", host_label.replace([':', '.'], "-")),
        address: host_label,
        detail: format!("unit {}", params.unit),
    })
}

#[async_trait::async_trait]
impl RepublishProtocol for ModbusRepublishProtocol {
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn discover(&self, conn: &Addressing) -> Result<DiscoverOutcome> {
        let params = parse_conn(conn)?;
        let targets = scan::parse_host_targets(&params.host, params.max_hosts)?;
        if targets.len() == 1 && !params.host.contains('/') {
            let device = probe_host(targets[0], &params).await.ok_or_else(|| {
                anyhow!(
                    "no Modbus response at {}",
                    endpoint_label(&params.host, params.port)
                )
            })?;
            return Ok(DiscoverOutcome {
                devices: vec![device],
                warnings: vec![],
            });
        }

        let found = stream::iter(targets)
            .map(|ip| {
                let params = params.clone();
                async move { probe_host(ip, &params).await }
            })
            .buffer_unordered(params.scan_concurrency)
            .filter_map(async |device| device)
            .collect::<Vec<_>>()
            .await;

        let mut devices = found;
        devices.sort_by(|a, b| a.address.cmp(&b.address));
        devices.dedup_by(|a, b| a.address == b.address);
        Ok(DiscoverOutcome {
            devices,
            warnings: vec![],
        })
    }

    async fn browse(
        &self,
        conn: &Addressing,
        device: &DiscoveredDevice,
    ) -> Result<Vec<DiscoveredPoint>> {
        let params = parse_conn(conn)?;
        let mut ctx = connect(&params).await?;
        let mut points = Vec::new();

        // Scan holding and input registers one at a time (Modbus reads are
        // all-or-nothing, so a fixed-width scan fails entirely past the device's
        // last register). Stop at the first address that isn't readable.
        for table in ["holding", "input"] {
            for addr in 0..BROWSE_COUNT {
                let read = match table {
                    "holding" => ctx.read_holding_registers(addr, 1).await,
                    _ => ctx.read_input_registers(addr, 1).await,
                };
                let Ok(Ok(regs)) = read else {
                    break;
                };
                let value = TelemetryValue::Number(regs.first().copied().unwrap_or(0) as f64);
                push_register_point(&mut points, device, table, addr, "u16", value);
            }
        }
        for table in ["coil", "discrete"] {
            for addr in 0..BROWSE_COUNT {
                let read = match table {
                    "coil" => ctx.read_coils(addr, 1).await,
                    _ => ctx.read_discrete_inputs(addr, 1).await,
                };
                let Ok(Ok(bits)) = read else {
                    break;
                };
                let value = TelemetryValue::Number(if bits.first().copied().unwrap_or(false) {
                    1.0
                } else {
                    0.0
                });
                push_register_point(&mut points, device, table, addr, "bool", value);
            }
        }
        Ok(points)
    }

    async fn poll(&self, conn: &Addressing, points: &[PointConfig]) -> Result<PollOutcome> {
        let params = parse_conn(conn)?;
        let mut ctx = connect(&params).await?;
        let mut outcome = PollOutcome::default();
        for point in points {
            match read_point(&mut ctx, &params, point).await {
                Ok(value) => outcome.samples.push(PointSample {
                    point: point.clone(),
                    value,
                    topic: String::new(), // filled in by the worker
                    timestamp_ms: now_millis(),
                }),
                Err(error) => outcome.failures.push(PointFailure {
                    point: point.clone(),
                    error: format!("{error:#}"),
                }),
            }
        }
        Ok(outcome)
    }
}

fn push_register_point(
    points: &mut Vec<DiscoveredPoint>,
    device: &DiscoveredDevice,
    table: &str,
    addr: u16,
    datatype: &str,
    value: TelemetryValue,
) {
    let mut addressing = Addressing::new();
    addressing.insert("table".into(), serde_json::json!(table));
    addressing.insert("address".into(), serde_json::json!(addr as u64));
    addressing.insert("datatype".into(), serde_json::json!(datatype));
    addressing.insert("word_order".into(), serde_json::json!("big"));
    points.push(DiscoveredPoint {
        device_key: device.key.clone(),
        name: Some(format!("{table}[{addr}]")),
        description: None,
        units: None,
        value: Some(value),
        addressing,
        suggested_tag_path: format!("{}/{table}_{addr}", device.key),
    });
}

fn addr_str(point: &PointConfig, key: &str) -> Option<String> {
    point
        .addressing
        .get(key)
        .map(republish_core::model::json_scalar)
}

fn addr_u64(point: &PointConfig, key: &str) -> Option<u64> {
    match point.addressing.get(key)? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

async fn read_point(
    ctx: &mut Context,
    params: &ConnParams,
    point: &PointConfig,
) -> Result<TelemetryValue> {
    let table = addr_str(point, "table").unwrap_or_else(|| "holding".into());
    let address = addr_u64(point, "address").unwrap_or(0) as u16;
    let datatype = addr_str(point, "datatype").unwrap_or_else(|| "u16".into());
    let word_order = addr_str(point, "word_order").unwrap_or_else(|| "big".into());
    let scale = addr_str(point, "scale")
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|s| *s != 0.0)
        .unwrap_or(1.0);

    // Bit tables → boolean-ish numbers.
    if table == "coil" || table == "discrete" {
        let read = if table == "coil" {
            timeout(params.timeout, ctx.read_coils(address, 1)).await
        } else {
            timeout(params.timeout, ctx.read_discrete_inputs(address, 1)).await
        }
        .map_err(|_| anyhow!("read timed out"))?
        .map_err(|e| anyhow!("io error: {e}"))?
        .map_err(|e| anyhow!("modbus exception: {e:?}"))?;
        let bit = read.first().copied().unwrap_or(false);
        return Ok(TelemetryValue::Number(if bit { 1.0 } else { 0.0 }));
    }

    let words = match datatype.as_str() {
        "u32" | "i32" | "f32" => 2,
        _ => 1,
    };
    let regs = if table == "input" {
        timeout(params.timeout, ctx.read_input_registers(address, words)).await
    } else {
        timeout(params.timeout, ctx.read_holding_registers(address, words)).await
    }
    .map_err(|_| anyhow!("read timed out"))?
    .map_err(|e| anyhow!("io error: {e}"))?
    .map_err(|e| anyhow!("modbus exception: {e:?}"))?;

    let number = decode(&regs, &datatype, &word_order)?;
    Ok(TelemetryValue::Number(number * scale))
}

fn decode(regs: &[u16], datatype: &str, word_order: &str) -> Result<f64> {
    let little = word_order == "little";
    let combined = |hi: u16, lo: u16| -> u32 {
        if little {
            ((lo as u32) << 16) | hi as u32
        } else {
            ((hi as u32) << 16) | lo as u32
        }
    };
    Ok(match datatype {
        "u16" => *regs.first().ok_or_else(|| anyhow!("no register"))? as f64,
        "i16" => (*regs.first().ok_or_else(|| anyhow!("no register"))? as i16) as f64,
        "u32" => {
            let bits = combined(regs[0], *regs.get(1).unwrap_or(&0));
            bits as f64
        }
        "i32" => {
            let bits = combined(regs[0], *regs.get(1).unwrap_or(&0));
            (bits as i32) as f64
        }
        "f32" => {
            let bits = combined(regs[0], *regs.get(1).unwrap_or(&0));
            f32::from_bits(bits) as f64
        }
        other => return Err(anyhow!("unknown datatype {other}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_f32_big_and_little() {
        let bits = 42.5f32.to_bits();
        let hi = (bits >> 16) as u16;
        let lo = (bits & 0xFFFF) as u16;
        assert_eq!(decode(&[hi, lo], "f32", "big").unwrap(), 42.5);
        assert_eq!(decode(&[lo, hi], "f32", "little").unwrap(), 42.5);
    }

    #[test]
    fn decode_signed_16() {
        assert_eq!(decode(&[0xFFFF], "i16", "big").unwrap(), -1.0);
        assert_eq!(decode(&[0x0005], "u16", "big").unwrap(), 5.0);
    }
}
