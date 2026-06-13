//! The simulator-side protocol extension point.
//!
//! A protocol adapter (e.g. `proto-bacnet`, `proto-modbus`, `proto-opcua`)
//! implements [`SimProtocol`] to expose the shared, protocol-agnostic
//! [`Simulation`](crate::simulation::Simulation) on the wire. The core owns the
//! simulation and its tick loop; the adapter only reads live values and answers
//! protocol requests.
//!
//! Adapters are looked up by id in a [`SimRegistry`], which the binary
//! populates with whichever protocols are compiled in.

use std::collections::HashMap;
use std::sync::Arc;

use proto_api::{Addressing, Capabilities};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::app::{AppLog, AppMetrics};
use crate::simulation::Simulation;

/// Everything an adapter needs to serve the simulation.
pub struct SimServeContext {
    /// Shared simulation state, kept current by the core's tick loop.
    pub sim: Arc<Mutex<Simulation>>,
    /// Request/error counters surfaced in the TUI.
    pub metrics: Arc<AppMetrics>,
    /// In-memory log buffer shown in the TUI (None in headless mode — use the
    /// `log` crate facade there).
    pub log: Option<Arc<AppLog>>,
    /// Port to bind (already resolved from config or the adapter default).
    pub port: u16,
    /// Protocol-specific options from the config `protocols[].options` table.
    pub options: Addressing,
    /// Fires when the simulator is shutting down; adapters should stop serving.
    pub cancel: CancellationToken,
}

impl SimServeContext {
    /// Push a line to the TUI log if present, otherwise to the `log` facade.
    pub fn log_line(&self, line: impl Into<String>) {
        let line = line.into();
        match &self.log {
            Some(buf) => buf.push(line),
            None => log::info!("{line}"),
        }
    }
}

/// A protocol server that exposes the shared simulation.
#[async_trait::async_trait]
pub trait SimProtocol: Send + Sync {
    /// Declarative description of the protocol (id, default port, …).
    fn capabilities(&self) -> &Capabilities;

    /// Bind listeners and serve until [`SimServeContext::cancel`] fires.
    async fn serve(self: Box<Self>, ctx: SimServeContext) -> anyhow::Result<()>;
}

/// Constructs an adapter instance from its config options.
pub type SimFactory = fn(&Addressing) -> anyhow::Result<Box<dyn SimProtocol>>;

/// Maps protocol id → adapter factory. The binary registers every compiled-in
/// protocol here; the core resolves the configured protocols against it.
#[derive(Default)]
pub struct SimRegistry {
    factories: HashMap<String, SimFactory>,
}

impl SimRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, id: &str, factory: SimFactory) {
        self.factories.insert(id.to_string(), factory);
    }

    pub fn get(&self, id: &str) -> Option<SimFactory> {
        self.factories.get(id).copied()
    }

    pub fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.factories.keys().cloned().collect();
        ids.sort();
        ids
    }
}
