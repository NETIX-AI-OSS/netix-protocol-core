use std::sync::Arc;

use chrono::Local;
use tokio::sync::Mutex;

use crate::app::metrics::AppMetrics;
use crate::app::AppMeta;
use crate::simulation::Simulation;

#[derive(Debug, Clone)]
pub struct DeviceRow {
    pub device_id: u32,
    pub name: String,
    pub point_count: usize,
}

#[derive(Debug, Clone)]
pub struct PointRow {
    pub label: String,
    pub object_type: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct AppSnapshot {
    pub building_name: String,
    pub config_path: String,
    /// Human-readable summary of the protocols being served, e.g.
    /// `"bacnet :47808, modbus :502"`.
    pub protocol_label: String,
    pub uptime_secs: u64,
    pub device_count: usize,
    pub point_count: usize,
    pub occupancy_pct: f64,
    pub outside_temp_c: f64,
    pub listening: bool,
    pub request_count: u64,
    pub error_count: u64,
    /// Per-kind request breakdown (label, count), sorted by label.
    pub named_requests: Vec<(String, u64)>,
    pub last_client: Option<String>,
    pub devices: Vec<DeviceRow>,
    pub log_lines: Vec<String>,
}

pub fn device_points(sim: &Simulation, device_id: u32) -> Vec<PointRow> {
    let Some(device) = sim.devices.iter().find(|d| d.device_id == device_id) else {
        return Vec::new();
    };
    device
        .points
        .iter()
        .map(|p| PointRow {
            label: p.label.clone(),
            object_type: p.object_type.clone(),
            value: format_point_value(&p.value),
        })
        .collect()
}

fn format_point_value(value: &crate::simulation::profiles::PointValue) -> String {
    use crate::simulation::profiles::PointValue;
    match value {
        PointValue::Real(v) => format!("{v:.2}"),
        PointValue::Boolean(b) => b.to_string(),
        PointValue::Unsigned(u) => u.to_string(),
    }
}

pub async fn build_snapshot(
    meta: &AppMeta,
    simulation: &Arc<Mutex<Simulation>>,
    metrics: &Arc<AppMetrics>,
    log_lines: Vec<String>,
) -> AppSnapshot {
    let sim = simulation.lock().await;
    let now = Local::now();
    let occupancy = sim.engine.get_occupancy(now);
    let outside_temp = sim.engine.get_outside_temp(now);
    let device_count = sim.devices.len();
    let point_count = sim.total_points();
    let devices: Vec<DeviceRow> = sim
        .devices
        .iter()
        .map(|d| DeviceRow {
            device_id: d.device_id,
            name: d.name.clone(),
            point_count: d.points.len(),
        })
        .collect();
    drop(sim);

    AppSnapshot {
        building_name: meta.building_name.clone(),
        config_path: meta.config_path.display().to_string(),
        protocol_label: meta.protocol_label.clone(),
        uptime_secs: meta.started_at.elapsed().as_secs(),
        device_count,
        point_count,
        occupancy_pct: occupancy * 100.0,
        outside_temp_c: outside_temp,
        listening: metrics.is_listening(),
        request_count: metrics.request_count(),
        error_count: metrics.error_count(),
        named_requests: metrics.named_counts(),
        last_client: metrics.last_client(),
        devices,
        log_lines,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Instant;

    use crate::config::SimulatorConfig;
    use crate::simulation::Simulation;

    #[tokio::test]
    async fn snapshot_from_simulation() {
        let cfg = SimulatorConfig::load_default_embedded().unwrap();
        let building_name = cfg.building.name.clone();
        let sim = Simulation::new(&cfg).unwrap();
        let sim_arc = Arc::new(Mutex::new(sim));
        let meta = AppMeta {
            building_name: building_name.clone(),
            config_path: PathBuf::from("config.yaml"),
            protocol_label: "bacnet :47808".to_string(),
            started_at: Instant::now(),
        };
        let metrics = Arc::new(AppMetrics::new());
        metrics.set_listening(true);
        let snap = build_snapshot(&meta, &sim_arc, &metrics, vec![]).await;
        assert_eq!(snap.building_name, building_name);
        assert!(snap.device_count > 0);
        assert!(snap.point_count > 0);
        assert!(snap.listening);
    }
}
