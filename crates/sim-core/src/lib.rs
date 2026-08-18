//! Protocol-agnostic core of the generic simulator; concrete protocols plug in through [`SimProtocol`] and are resolved at runtime via [`SimRegistry`].

pub mod app;
pub mod config;
pub mod protocol;
pub mod republisher_export;
pub mod simulation;
pub mod tui;

pub use app::{
    bootstrap_config, build_simulation, detect_run_mode, parse_args, restart_process, run, AppLog,
    AppMetrics, CliArgs, RunMode,
};
pub use config::{ConfigError, ProtocolInstanceConfig, SimulatorConfig};
pub use protocol::{SimFactory, SimProtocol, SimRegistry, SimServeContext};
pub use republisher_export::emit_republisher_config;
pub use simulation::Simulation;
