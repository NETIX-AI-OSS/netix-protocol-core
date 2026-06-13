//! Modbus TCP republisher adapter: manual endpoint connection, register-range
//! browse, and value polling with per-point datatype/word-order/scale decoding.

use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};
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
    addr: SocketAddr,
    unit: u8,
    timeout: Duration,
    label: String,
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
    let addr = format!("{}:{}", host.trim(), port)
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve {host}:{port}"))?
        .next()
        .ok_or_else(|| anyhow!("no address for {host}:{port}"))?;
    Ok(ConnParams {
        addr,
        unit,
        timeout: Duration::from_millis(timeout_ms.max(100)),
        label: format!("{}:{}", host.trim(), port),
    })
}

async fn connect(params: &ConnParams) -> Result<Context> {
    let ctx = timeout(
        params.timeout,
        tokio_modbus::client::tcp::connect_slave(params.addr, Slave(params.unit)),
    )
    .await
    .map_err(|_| anyhow!("connect to {} timed out", params.label))?
    .with_context(|| format!("failed to connect to {}", params.label))?;
    Ok(ctx)
}

#[async_trait::async_trait]
impl RepublishProtocol for ModbusRepublishProtocol {
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn discover(&self, conn: &Addressing) -> Result<DiscoverOutcome> {
        let params = parse_conn(conn)?;
        // Modbus TCP has no native discovery; verify connectivity and report the
        // configured endpoint as a single device.
        let mut ctx = connect(&params).await?;
        let _ = ctx.read_holding_registers(0, 1).await; // touch the connection
        let device = DiscoveredDevice {
            key: format!("modbus-{}", params.label.replace([':', '.'], "-")),
            address: params.label.clone(),
            detail: format!("unit {}", params.unit),
        };
        Ok(DiscoverOutcome {
            devices: vec![device],
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
                    break; // reached the end of this table's range
                };
                let value = regs.first().copied().unwrap_or(0);
                let mut addressing = Addressing::new();
                addressing.insert("table".into(), serde_json::json!(table));
                addressing.insert("address".into(), serde_json::json!(addr as u64));
                addressing.insert("datatype".into(), serde_json::json!("u16"));
                addressing.insert("word_order".into(), serde_json::json!("big"));
                points.push(DiscoveredPoint {
                    device_key: device.key.clone(),
                    name: Some(format!("{table}[{addr}]")),
                    description: None,
                    units: None,
                    value: Some(TelemetryValue::Number(value as f64)),
                    addressing,
                    suggested_tag_path: format!("{}/{table}_{addr}", device.key),
                });
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
