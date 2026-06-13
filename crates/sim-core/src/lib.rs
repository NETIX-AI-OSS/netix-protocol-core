//! Protocol-agnostic core of the generic simulator: the simulation engine,
//! config model, TUI, and app lifecycle. Concrete protocols plug in through the
//! [`SimProtocol`] trait and are resolved at runtime via a [`SimRegistry`].

pub mod app;
pub mod config;
pub mod protocol;
pub mod simulation;
pub mod tui;

pub use app::{
    bootstrap_config, build_simulation, detect_run_mode, parse_args, restart_process, run, AppLog,
    AppMetrics, RunMode,
};
pub use config::{ConfigError, ProtocolInstanceConfig, SimulatorConfig};
pub use protocol::{SimFactory, SimProtocol, SimRegistry, SimServeContext};
pub use simulation::Simulation;
