//! Modbus TCP simulator adapter: serves the shared simulation as a read-only
//! Modbus image (holding/input registers + coils/discrete inputs).

mod map;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use proto_api::{Addressing, Capabilities};
use sim_core::{AppMetrics, SimProtocol, SimServeContext, Simulation};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_modbus::prelude::{ExceptionCode, Request, Response};
use tokio_modbus::server::tcp::{accept_tcp_connection, Server};
use tokio_modbus::server::Service;

use map::{build_modbus_map, ModbusMap};

/// Per-connection Modbus service backed by the live simulation.
struct ModbusService {
    map: Arc<ModbusMap>,
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
        // The simulator exposes sensor data only; writes and other functions are
        // rejected.
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
            let sim = Arc::clone(&sim);
            let metrics = Arc::clone(&metrics);
            async move {
                let new_service = move |peer: SocketAddr| {
                    Ok(Some(Arc::new(ModbusService {
                        map: Arc::clone(&map),
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
