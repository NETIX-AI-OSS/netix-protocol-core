//! Projection of the protocol-agnostic simulation into Modbus address space.
//!
//! Points are laid out deterministically in device/point order:
//! - **Analog** points → one 32-bit IEEE-754 float across two consecutive
//!   registers (big-endian / high word first).
//! - **Multi-state** points → one 16-bit register.
//! - **Binary** points → one bit.
//!
//! Holding and input registers are mirrored (both answer from the same register
//! cells); coils and discrete inputs are likewise mirrored. This keeps the
//! simulator usable by clients that poll either table.

use proto_api::PointKind;
use sim_core::simulation::models::SimulatedDevice;
use sim_core::Simulation;

#[derive(Debug, Clone)]
enum RegEncoding {
    U16,
    F32High,
    F32Low,
}

#[derive(Debug, Clone)]
struct RegCell {
    device_id: u32,
    object_type: String,
    instance: u32,
    enc: RegEncoding,
}

#[derive(Debug, Clone)]
struct BitCell {
    device_id: u32,
    object_type: String,
    instance: u32,
}

/// A read-only Modbus image of the simulation. Values are computed live on each
/// read from the current simulation state.
#[derive(Debug)]
pub struct ModbusMap {
    regs: Vec<RegCell>,
    bits: Vec<BitCell>,
}

impl ModbusMap {
    pub fn num_registers(&self) -> usize {
        self.regs.len()
    }

    pub fn num_bits(&self) -> usize {
        self.bits.len()
    }

    /// Read `qty` registers starting at `addr`, or `None` if the range is out of
    /// bounds (maps to a Modbus IllegalDataAddress exception).
    pub fn read_registers(&self, sim: &Simulation, addr: u16, qty: u16) -> Option<Vec<u16>> {
        let end = (addr as usize).checked_add(qty as usize)?;
        if end > self.regs.len() {
            return None;
        }
        Some(
            (addr as usize..end)
                .map(|i| self.reg_word(sim, i))
                .collect(),
        )
    }

    /// Read `qty` bits (coils / discrete inputs) starting at `addr`, or `None`
    /// if the range is out of bounds.
    pub fn read_bits(&self, sim: &Simulation, addr: u16, qty: u16) -> Option<Vec<bool>> {
        let end = (addr as usize).checked_add(qty as usize)?;
        if end > self.bits.len() {
            return None;
        }
        Some(
            (addr as usize..end)
                .map(|i| {
                    let cell = &self.bits[i];
                    sim.neutral_value(cell.device_id, &cell.object_type, cell.instance)
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                })
                .collect(),
        )
    }

    fn reg_word(&self, sim: &Simulation, idx: usize) -> u16 {
        let cell = &self.regs[idx];
        let value = sim.neutral_value(cell.device_id, &cell.object_type, cell.instance);
        match cell.enc {
            RegEncoding::U16 => value
                .and_then(|v| v.as_f64())
                .map(|f| f.round().clamp(0.0, u16::MAX as f64) as u16)
                .unwrap_or(0),
            RegEncoding::F32High => f32_word(value, true),
            RegEncoding::F32Low => f32_word(value, false),
        }
    }
}

fn f32_word(value: Option<proto_api::PointValue>, high: bool) -> u16 {
    let f = value.and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
    let bits = f.to_bits();
    if high {
        (bits >> 16) as u16
    } else {
        (bits & 0xFFFF) as u16
    }
}

/// Build the Modbus image from the simulation's devices.
pub fn build_modbus_map(devices: &[SimulatedDevice]) -> ModbusMap {
    let mut regs = Vec::new();
    let mut bits = Vec::new();
    for device in devices {
        for point in &device.points {
            match point.kind {
                PointKind::Analog => {
                    regs.push(RegCell {
                        device_id: device.device_id,
                        object_type: point.object_type.clone(),
                        instance: point.instance,
                        enc: RegEncoding::F32High,
                    });
                    regs.push(RegCell {
                        device_id: device.device_id,
                        object_type: point.object_type.clone(),
                        instance: point.instance,
                        enc: RegEncoding::F32Low,
                    });
                }
                PointKind::MultiState => regs.push(RegCell {
                    device_id: device.device_id,
                    object_type: point.object_type.clone(),
                    instance: point.instance,
                    enc: RegEncoding::U16,
                }),
                PointKind::Binary => bits.push(BitCell {
                    device_id: device.device_id,
                    object_type: point.object_type.clone(),
                    instance: point.instance,
                }),
                // Text values have no natural Modbus representation; skip them.
                PointKind::Text => {}
            }
        }
    }
    ModbusMap { regs, bits }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sim_core::config::{
        AssetInstanceSpec, AssetTemplate, BuildingConfig, IdPolicy, SeasonalityConfig,
        SimulatorConfig, TemplatePointSpec, WeeklySchedule,
    };
    use sim_core::simulation::profiles::ProfileSpec;
    use sim_core::Simulation;
    use std::collections::HashMap;

    fn sim_with(points: Vec<TemplatePointSpec>) -> Simulation {
        let mut templates = HashMap::new();
        templates.insert(
            "tpl".to_string(),
            AssetTemplate {
                description: String::new(),
                points,
            },
        );
        let cfg = SimulatorConfig {
            building: BuildingConfig {
                name: "T".into(),
                location: None,
                timezone: None,
            },
            seasonality: SeasonalityConfig {
                weekly_schedule: WeeklySchedule {
                    weekday_occupancy: vec![],
                    weekend_occupancy: vec![],
                },
            },
            id_policy: IdPolicy {
                device_id_base: 1000,
                per_template_block: 100,
            },
            templates,
            instances: vec![AssetInstanceSpec {
                template: "tpl".into(),
                name_prefix: "DEV".into(),
                zone: None,
                count: 1,
            }],
            protocols: vec![],
        };
        Simulation::new(&cfg).unwrap()
    }

    fn tp(label: &str, ot: &str, profile: ProfileSpec) -> TemplatePointSpec {
        TemplatePointSpec {
            label: label.into(),
            object_type: ot.into(),
            units: None,
            profile,
        }
    }

    #[test]
    fn analog_encodes_as_big_endian_f32_pair() {
        let sim = sim_with(vec![tp(
            "temp",
            "analog_input",
            ProfileSpec::Constant { value: 42.5 },
        )]);
        let map = build_modbus_map(&sim.devices);
        assert_eq!(map.num_registers(), 2);
        let regs = map.read_registers(&sim, 0, 2).unwrap();
        let bits = ((regs[0] as u32) << 16) | regs[1] as u32;
        assert_eq!(f32::from_bits(bits), 42.5);
    }

    #[test]
    fn multi_state_encodes_as_single_register() {
        let sim = sim_with(vec![tp(
            "mode",
            "multi_state_value",
            ProfileSpec::ConstantState { value: 7 },
        )]);
        let map = build_modbus_map(&sim.devices);
        assert_eq!(map.num_registers(), 1);
        assert_eq!(map.read_registers(&sim, 0, 1).unwrap(), vec![7]);
    }

    #[test]
    fn binary_encodes_as_bit() {
        let sim = sim_with(vec![tp(
            "occ",
            "binary_value",
            ProfileSpec::ConstantBool { value: true },
        )]);
        let map = build_modbus_map(&sim.devices);
        assert_eq!(map.num_bits(), 1);
        assert_eq!(map.read_bits(&sim, 0, 1).unwrap(), vec![true]);
    }

    #[test]
    fn out_of_range_reads_return_none() {
        let sim = sim_with(vec![tp(
            "temp",
            "analog_input",
            ProfileSpec::Constant { value: 1.0 },
        )]);
        let map = build_modbus_map(&sim.devices);
        assert!(map.read_registers(&sim, 0, 5).is_none());
        assert!(map.read_bits(&sim, 0, 1).is_none()); // no bits at all
    }
}
