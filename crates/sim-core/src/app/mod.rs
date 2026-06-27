pub mod log;
pub mod metrics;
pub mod snapshot;

use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ::log::{error, info, warn};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::config::{ConfigError, SimulatorConfig};
use crate::protocol::{SimRegistry, SimServeContext};
use crate::simulation::Simulation;

pub use log::AppLog;
pub use metrics::AppMetrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Tui,
    Headless,
}

#[derive(Debug, Clone)]
pub struct AppMeta {
    pub building_name: String,
    pub config_path: PathBuf,
    /// Human-readable summary of the protocols being served.
    pub protocol_label: String,
    pub started_at: Instant,
}

pub struct AppContext {
    pub meta: AppMeta,
    pub simulation: Arc<Mutex<Simulation>>,
    pub metrics: Arc<AppMetrics>,
    pub log: Arc<AppLog>,
}

pub fn detect_run_mode(no_tui_flag: bool) -> RunMode {
    if no_tui_flag {
        return RunMode::Headless;
    }
    if env_no_tui() {
        return RunMode::Headless;
    }
    if std::io::stdout().is_terminal() {
        RunMode::Tui
    } else {
        RunMode::Headless
    }
}

/// Returns true when headless mode is requested via environment variable.
///
/// Accepts both the generic `SIM_NO_TUI` and the legacy BACnet name
/// `BACNET_SIM_NO_TUI` for backward compatibility with existing scripts.
fn env_no_tui() -> bool {
    ["SIM_NO_TUI", "BACNET_SIM_NO_TUI"].into_iter().any(|name| {
        std::env::var(name)
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

pub fn parse_args() -> (bool, PathBuf) {
    let mut no_tui = false;
    let mut config_path = std::env::var("CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config.yaml"));

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--no-tui" => no_tui = true,
            "--config" | "-c" => {
                if let Some(path) = args.next() {
                    config_path = PathBuf::from(path);
                }
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other if other.starts_with('-') => {
                eprintln!("Unknown flag: {other}");
                print_help();
                std::process::exit(2);
            }
            path => config_path = PathBuf::from(path),
        }
    }

    (no_tui, config_path)
}

fn print_help() {
    eprintln!(
        "Usage: simulator [OPTIONS] [CONFIG_PATH]\n\n\
         Options:\n\
           --no-tui          Log-only mode (also SIM_NO_TUI=1 or BACNET_SIM_NO_TUI=1)\n\
           -c, --config PATH Config file (default: config.yaml)\n\
           -h, --help        Show this help\n"
    );
}

/// Start the simulation tick loop and every configured protocol listener, then
/// run the TUI (or block headless). `registry` carries the protocol adapters
/// compiled into the binary.
pub fn run(
    mode: RunMode,
    config_path: PathBuf,
    config: SimulatorConfig,
    simulation: Simulation,
    registry: SimRegistry,
) -> Result<(), Box<dyn std::error::Error>> {
    let device_count = simulation.devices.len();
    let point_count = simulation.total_points();
    let protocols = config.effective_protocols();

    let metrics = Arc::new(AppMetrics::new());
    let app_log = Arc::new(AppLog::new());
    let sim_arc = Arc::new(Mutex::new(simulation));
    let cancel = CancellationToken::new();

    let rt = tokio::runtime::Runtime::new()?;

    // Simulation tick loop — protocol-agnostic, owned by the core. Advances all
    // point values once per second using wall-clock elapsed time.
    {
        let sim_clone = sim_arc.clone();
        let cancel_tick = cancel.clone();
        rt.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut last_tick = Instant::now();
            loop {
                tokio::select! {
                    _ = cancel_tick.cancelled() => break,
                    _ = interval.tick() => {
                        let elapsed = last_tick.elapsed().as_secs_f64();
                        last_tick = Instant::now();
                        let mut sim = sim_clone.lock().await;
                        sim.update(elapsed);
                    }
                }
            }
        });
    }

    // Spawn each configured protocol listener.
    let mut labels: Vec<String> = Vec::new();
    let mut served = 0usize;
    for proto in &protocols {
        let Some(factory) = registry.get(&proto.id) else {
            warn!(
                "unknown protocol '{}' (available: {})",
                proto.id,
                registry.ids().join(", ")
            );
            continue;
        };
        let adapter = match factory(&proto.options) {
            Ok(adapter) => adapter,
            Err(e) => {
                error!("failed to construct protocol '{}': {e}", proto.id);
                continue;
            }
        };
        let port = proto
            .port
            .unwrap_or_else(|| adapter.capabilities().default_port);
        labels.push(format!("{} :{}", proto.id, port));
        served += 1;

        let ctx = SimServeContext {
            sim: sim_arc.clone(),
            metrics: Arc::clone(&metrics),
            log: if mode == RunMode::Tui {
                Some(Arc::clone(&app_log))
            } else {
                None
            },
            port,
            options: proto.options.clone(),
            cancel: cancel.clone(),
        };
        let id = proto.id.clone();
        rt.spawn(async move {
            if let Err(e) = adapter.serve(ctx).await {
                error!("protocol '{id}' stopped with error: {e}");
            }
        });
    }

    if served == 0 {
        return Err("no protocol listeners could be started; check the `protocols` config".into());
    }
    metrics.set_listening(true);
    let protocol_label = labels.join(", ");

    let meta = AppMeta {
        building_name: config.building.name.clone(),
        config_path: config_path.clone(),
        protocol_label: protocol_label.clone(),
        started_at: Instant::now(),
    };

    match mode {
        RunMode::Headless => {
            info!("Using config at {}", config_path.display());
            info!(
                "Loaded configuration for building: {} ({} devices, {} points)",
                meta.building_name, device_count, point_count
            );
            info!("Serving protocols: {protocol_label}");
            rt.block_on(async {
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            });
        }
        RunMode::Tui => {
            app_log.push(format!(
                "Loaded {} devices, {} points from {}",
                device_count,
                point_count,
                config_path.display()
            ));
            app_log.push(format!("Serving protocols: {protocol_label}"));
            let ctx = AppContext {
                meta,
                simulation: sim_arc,
                metrics,
                log: app_log,
            };
            crate::tui::run(rt, ctx)?;
            cancel.cancel();
        }
    }

    Ok(())
}

pub fn restart_process() -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;
    let cwd = std::env::current_dir()?;
    std::process::Command::new(exe).current_dir(cwd).spawn()?;
    Ok(())
}

/// Ensure a config file exists (writing the bundled sample if missing) and load
/// it. Returns the parsed config or a [`ConfigError`] for the binary to report.
pub fn bootstrap_config(config_path: &PathBuf) -> Result<SimulatorConfig, ConfigError> {
    SimulatorConfig::ensure_config_file(config_path)?;
    let config_path_str = config_path.to_string_lossy();
    SimulatorConfig::load_from_file(&config_path_str)
}

pub fn build_simulation(config: &SimulatorConfig) -> Result<Simulation, ConfigError> {
    Simulation::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_env(name: &str, value: Option<&str>, f: impl FnOnce()) {
        let previous = std::env::var(name).ok();
        match value {
            Some(v) => unsafe { std::env::set_var(name, v) },
            None => unsafe { std::env::remove_var(name) },
        }
        f();
        match previous {
            Some(v) => unsafe { std::env::set_var(name, v) },
            None => unsafe { std::env::remove_var(name) },
        }
    }

    #[test]
    fn sim_no_tui_env_triggers_headless() {
        with_env("SIM_NO_TUI", Some("1"), || {
            with_env("BACNET_SIM_NO_TUI", None, || {
                assert_eq!(detect_run_mode(false), RunMode::Headless);
            });
        });
    }

    #[test]
    fn bacnet_sim_no_tui_alias_triggers_headless() {
        with_env("BACNET_SIM_NO_TUI", Some("true"), || {
            with_env("SIM_NO_TUI", None, || {
                assert_eq!(detect_run_mode(false), RunMode::Headless);
            });
        });
    }
}
