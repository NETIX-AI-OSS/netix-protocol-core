pub mod models;
pub mod profiles;
pub mod seasonality;

use chrono::Local;
use proto_api::PointValue as NeutralValue;

use crate::config::{ConfigError, SimulatorConfig};
use models::SimulatedDevice;
use seasonality::SeasonalityEngine;

/// The protocol-agnostic simulation state: a set of devices whose point values
/// are advanced by [`Simulation::update`] on a fixed tick.
pub struct Simulation {
    pub devices: Vec<SimulatedDevice>,
    pub engine: SeasonalityEngine,
}

impl Simulation {
    pub fn new(config: &SimulatorConfig) -> Result<Self, ConfigError> {
        let engine = SeasonalityEngine::new(config.seasonality.weekly_schedule.clone());
        let expanded = config.expand()?;
        let devices = expanded.iter().map(SimulatedDevice::from_spec).collect();
        Ok(Self { devices, engine })
    }

    pub fn total_points(&self) -> usize {
        self.devices.iter().map(|d| d.points.len()).sum()
    }

    pub fn update(&mut self, dt_seconds: f64) {
        let now = Local::now();
        let occupancy = self.engine.get_occupancy(now) as f32;
        let outside_temp = self.engine.get_outside_temp(now) as f32;
        let now_secs = now.timestamp() as f64 + now.timestamp_subsec_micros() as f64 / 1_000_000.0;

        for device in &mut self.devices {
            device.tick(dt_seconds as f32, now_secs, occupancy, outside_temp);
        }
    }

    /// Current value of a point, in the neutral representation adapters consume.
    /// Looks the point up by raw object-type string + instance within a device.
    pub fn neutral_value(
        &self,
        device_id: u32,
        object_type: &str,
        instance: u32,
    ) -> Option<NeutralValue> {
        let device = self.devices.iter().find(|d| d.device_id == device_id)?;
        let point = device.find_point(object_type, instance)?;
        Some(point.neutral_value())
    }
}
