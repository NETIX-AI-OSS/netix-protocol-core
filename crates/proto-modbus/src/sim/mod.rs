//! Modbus TCP simulator adapter: serves the shared simulation as a read-only Modbus image (holding/input registers + coils/discrete inputs).

mod map;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use proto_api::{Addressing, Capabilities};
use sim_core::{AppMetrics, SimProtocol, SimServeContext, Simulation};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_modbus::prelude::{
    ConformityLevel, DeviceIdObject, ExceptionCode, ReadCode, ReadDeviceIdentificationResponse,
    Request, Response,
};
use tokio_modbus::server::tcp::{accept_tcp_connection, Server};
use tokio_modbus::server::Service;

use map::{build_modbus_map, ModbusMap};

/// Read Device Identification objects (function 43 / MEI 14) the simulator answers with, by object id, from the
/// `vendor_name` / `product_code` / `revision` / `product_name` / `model_name` options, so it can stand in for a
/// particular meter. None configured keeps the function unsupported, as a bare register map would be.
fn identity_objects(options: &Addressing) -> Vec<(u8, String)> {
    let text = |key: &str| {
        options
            .get(key)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let Some(vendor_name) = text("vendor_name") else {
        return Vec::new();
    };
    // VendorName, ProductCode and MajorMinorRevision are the mandatory basic category.
    let mut objects = vec![
        (0x00, vendor_name),
        (0x01, text("product_code").unwrap_or_default()),
        (0x02, text("revision").unwrap_or_else(|| "1.0".to_string())),
    ];
    objects.extend(text("product_name").map(|name| (0x04, name)));
    objects.extend(text("model_name").map(|name| (0x05, name)));
    objects
}

/// The answer to one Read Device Identification request: a whole category from `object_id` (stream access), or
/// that one object (individual access).
fn identify(
    objects: &[(u8, String)],
    read_code: ReadCode,
    object_id: u8,
) -> Result<Response, ExceptionCode> {
    if objects.is_empty() {
        return Err(ExceptionCode::IllegalFunction);
    }
    let in_category = |id: u8| match read_code {
        ReadCode::Basic => id <= 0x02,
        ReadCode::Specific => id == object_id,
        ReadCode::Regular | ReadCode::Extended => true,
    };
    let device_id_objects: Vec<DeviceIdObject> = objects
        .iter()
        .filter(|(id, _)| *id >= object_id && in_category(*id))
        .map(|(id, value)| DeviceIdObject {
            id: *id,
            value: value.clone().into_bytes().into(),
        })
        .collect();
    if device_id_objects.is_empty() {
        return Err(ExceptionCode::IllegalDataAddress);
    }
    Ok(Response::ReadDeviceIdentification(
        ReadDeviceIdentificationResponse {
            read_code,
            conformity_level: ConformityLevel::RegularIdentification,
            more_follows: false,
            next_object_id: 0,
            device_id_objects,
        },
    ))
}

/// Per-connection Modbus service backed by the live simulation.
struct ModbusService {
    map: Arc<ModbusMap>,
    identity: Arc<Vec<(u8, String)>>,
    sim: Arc<Mutex<Simulation>>,
    metrics: Arc<AppMetrics>,
    peer: String,
}

impl Service for ModbusService {
    type Request = Request<'static>;
    type Response = Response;
    type Exception = ExceptionCode;
    type Future = Pin<Box<dyn Future<Output = Result<Response, ExceptionCode>> + Send>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        if let Request::ReadDeviceIdentification(read_code, object_id) = req {
            self.metrics
                .record_request("read_device_identification", Some(&self.peer));
            let answer = identify(&self.identity, read_code, object_id);
            return Box::pin(async move { answer });
        }
        let map = Arc::clone(&self.map);
        let sim = Arc::clone(&self.sim);
        let metrics = Arc::clone(&self.metrics);
        let peer = self.peer.clone();
        Box::pin(async move { handle_request(req, map, sim, metrics, peer).await })
    }
}

async fn handle_request(
    req: Request<'static>,
    map: Arc<ModbusMap>,
    sim: Arc<Mutex<Simulation>>,
    metrics: Arc<AppMetrics>,
    peer: String,
) -> Result<Response, ExceptionCode> {
    match req {
        Request::ReadHoldingRegisters(addr, qty) => {
            metrics.record_request("read_holding_registers", Some(&peer));
            let sim = sim.lock().await;
            map.read_registers(&sim, addr, qty)
                .map(Response::ReadHoldingRegisters)
                .ok_or(ExceptionCode::IllegalDataAddress)
        }
        Request::ReadInputRegisters(addr, qty) => {
            metrics.record_request("read_input_registers", Some(&peer));
            let sim = sim.lock().await;
            map.read_registers(&sim, addr, qty)
                .map(Response::ReadInputRegisters)
                .ok_or(ExceptionCode::IllegalDataAddress)
        }
        Request::ReadCoils(addr, qty) => {
            metrics.record_request("read_coils", Some(&peer));
            let sim = sim.lock().await;
            map.read_bits(&sim, addr, qty)
                .map(Response::ReadCoils)
                .ok_or(ExceptionCode::IllegalDataAddress)
        }
        Request::ReadDiscreteInputs(addr, qty) => {
            metrics.record_request("read_discrete_inputs", Some(&peer));
            let sim = sim.lock().await;
            map.read_bits(&sim, addr, qty)
                .map(Response::ReadDiscreteInputs)
                .ok_or(ExceptionCode::IllegalDataAddress)
        }
        // Simulator exposes sensor data only; writes/other functions rejected.
        _ => Err(ExceptionCode::IllegalFunction),
    }
}

/// Simulator-side Modbus adapter.
pub struct ModbusSimProtocol {
    caps: Capabilities,
}

/// Factory used by the registry to construct the adapter from config options.
pub fn sim_factory(_options: &Addressing) -> anyhow::Result<Box<dyn SimProtocol>> {
    Ok(Box::new(ModbusSimProtocol {
        caps: crate::capabilities(),
    }))
}

#[async_trait::async_trait]
impl SimProtocol for ModbusSimProtocol {
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn serve(self: Box<Self>, ctx: SimServeContext) -> anyhow::Result<()> {
        let unit_id = ctx
            .options
            .get("unit_id")
            .and_then(|v| v.as_u64())
            .unwrap_or(1);

        let map = Arc::new(build_modbus_map(&ctx.sim.lock().await.devices));
        let identity = Arc::new(identity_objects(&ctx.options));
        let addr: SocketAddr = format!("0.0.0.0:{}", ctx.port).parse()?;
        let listener = TcpListener::bind(addr).await?;
        ctx.log_line(format!(
            "Modbus TCP listening on {addr} (unit {unit_id}, {} registers, {} bits)",
            map.num_registers(),
            map.num_bits()
        ));

        let sim = Arc::clone(&ctx.sim);
        let metrics = Arc::clone(&ctx.metrics);
        let server = Server::new(listener);

        let on_connected = move |stream, socket_addr: SocketAddr| {
            let map = Arc::clone(&map);
            let identity = Arc::clone(&identity);
            let sim = Arc::clone(&sim);
            let metrics = Arc::clone(&metrics);
            async move {
                let new_service = move |peer: SocketAddr| {
                    Ok(Some(Arc::new(ModbusService {
                        map: Arc::clone(&map),
                        identity: Arc::clone(&identity),
                        sim: Arc::clone(&sim),
                        metrics: Arc::clone(&metrics),
                        peer: peer.to_string(),
                    })))
                };
                accept_tcp_connection(stream, socket_addr, new_service)
            }
        };
        let on_process_error = |err| log::error!("Modbus connection error: {err}");

        // `serve_until` consumes the server and needs an owned 'static abort future.
        let abort = ctx.cancel.clone().cancelled_owned();
        server
            .serve_until(&on_connected, on_process_error, abort)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pm5320() -> Vec<(u8, String)> {
        let mut options = Addressing::new();
        options.insert(
            "vendor_name".into(),
            serde_json::json!(" Schneider Electric "),
        );
        options.insert("product_code".into(), serde_json::json!("METSEPM5320"));
        options.insert("model_name".into(), serde_json::json!("PM5320"));
        identity_objects(&options)
    }

    fn ids(response: Result<Response, ExceptionCode>) -> Vec<u8> {
        match response {
            Ok(Response::ReadDeviceIdentification(answer)) => answer
                .device_id_objects
                .iter()
                .map(|object| object.id)
                .collect(),
            other => panic!("expected an identification, got {other:?}"),
        }
    }

    #[test]
    fn identity_comes_from_the_options_with_the_mandatory_basic_objects() {
        assert_eq!(
            pm5320(),
            vec![
                (0x00, "Schneider Electric".to_string()),
                (0x01, "METSEPM5320".to_string()),
                (0x02, "1.0".to_string()),
                (0x05, "PM5320".to_string()),
            ]
        );
    }

    #[test]
    fn without_a_vendor_the_function_stays_unsupported() {
        let mut options = Addressing::new();
        options.insert("model_name".into(), serde_json::json!("PM5320"));
        assert!(identity_objects(&options).is_empty());
        assert!(matches!(
            identify(&[], ReadCode::Regular, 0),
            Err(ExceptionCode::IllegalFunction)
        ));
    }

    #[test]
    fn each_category_returns_its_own_objects() {
        let objects = pm5320();
        assert_eq!(ids(identify(&objects, ReadCode::Basic, 0)), vec![0, 1, 2]);
        assert_eq!(
            ids(identify(&objects, ReadCode::Regular, 0)),
            vec![0, 1, 2, 5]
        );
        assert_eq!(ids(identify(&objects, ReadCode::Regular, 2)), vec![2, 5]);
        assert_eq!(ids(identify(&objects, ReadCode::Specific, 5)), vec![5]);
        assert!(matches!(
            identify(&objects, ReadCode::Specific, 4),
            Err(ExceptionCode::IllegalDataAddress)
        ));
    }
}
