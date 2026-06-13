use std::collections::HashMap;

use proto_api::{PointKind, PointValue as NeutralValue};

use crate::config::{DeviceSpec, PointSpec};
use crate::simulation::profiles::{PointValue, ProfileState, TickCtx};

#[derive(Debug, Clone)]
pub struct SimulatedPoint {
    pub label: String,
    /// Neutral category (analog/binary/multi-state) used by all adapters.
    pub kind: PointKind,
    /// Raw object-type string from config (e.g. `"analog_input"`). Adapters that
    /// need finer distinctions than [`PointKind`] (BACnet object types) keep it.
    pub object_type: String,
    pub instance: u32,
    /// Raw engineering-unit string from config (e.g. `"degrees_celsius"`). Each
    /// adapter maps this to its own representation (BACnet enum, OPC UA EU info).
    pub units: Option<String>,
    pub value: PointValue,
    pub profile: ProfileState,
}

impl SimulatedPoint {
    pub fn from_spec(spec: &PointSpec) -> Option<Self> {
        let kind = point_kind_from_object_type(&spec.object_type)?;
        let profile = ProfileState::from_spec(&spec.profile);
        let value = profile.initial_value();
        Some(SimulatedPoint {
            label: spec.label.clone(),
            kind,
            object_type: spec.object_type.trim().to_ascii_lowercase(),
            instance: spec.instance,
            units: spec.units.clone(),
            value,
            profile,
        })
    }

    /// Current value in the protocol-neutral representation that adapters consume.
    pub fn neutral_value(&self) -> NeutralValue {
        match self.value {
            PointValue::Real(v) => NeutralValue::Float(v as f64),
            PointValue::Boolean(b) => NeutralValue::Bool(b),
            PointValue::Unsigned(u) => NeutralValue::UInt(u as u64),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimulatedDevice {
    pub device_id: u32,
    pub name: String,
    pub points: Vec<SimulatedPoint>,
}

impl SimulatedDevice {
    pub fn from_spec(spec: &DeviceSpec) -> Self {
        let points = spec
            .points
            .iter()
            .filter_map(SimulatedPoint::from_spec)
            .collect();
        SimulatedDevice {
            device_id: spec.device_id,
            name: spec.name.clone(),
            points,
        }
    }

    pub fn tick(&mut self, dt: f32, now_secs: f64, occupancy: f32, outside_temp: f32) {
        let mut siblings: HashMap<String, f32> = HashMap::with_capacity(self.points.len());
        // Pre-seed with current values so DerivedConstant/Integrator referencing yet-unticked
        // points still get a stable starting value.
        for p in &self.points {
            if let Some(v) = p.value.as_f32() {
                siblings.insert(p.label.clone(), v);
            }
        }
        for p in &mut self.points {
            let ctx = TickCtx {
                dt,
                now_secs,
                occupancy,
                outside_temp,
                siblings: &siblings,
            };
            let new_value = p.profile.tick(&ctx);
            p.value = new_value;
            if let Some(v) = new_value.as_f32() {
                siblings.insert(p.label.clone(), v);
            }
        }
    }

    /// Look up a point by its raw object-type string and instance.
    pub fn find_point(&self, object_type: &str, instance: u32) -> Option<&SimulatedPoint> {
        let needle = object_type.trim().to_ascii_lowercase();
        self.points
            .iter()
            .find(|p| p.object_type == needle && p.instance == instance)
    }
}

/// Map a config object-type string to a neutral [`PointKind`]. Returns `None`
/// for unknown types, which causes the point to be skipped (matching the old
/// simulator behaviour).
pub fn point_kind_from_object_type(value: &str) -> Option<PointKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "analog_input" | "analog_output" | "analog_value" => Some(PointKind::Analog),
        "binary_input" | "binary_output" | "binary_value" => Some(PointKind::Binary),
        "multi_state_input" | "multi_state_output" | "multi_state_value" => {
            Some(PointKind::MultiState)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DeviceSpec, PointSpec};
    use crate::simulation::profiles::ProfileSpec;

    #[test]
    fn point_kind_from_object_type_known_values() {
        assert_eq!(
            point_kind_from_object_type("analog_input"),
            Some(PointKind::Analog)
        );
        assert_eq!(
            point_kind_from_object_type("analog_output"),
            Some(PointKind::Analog)
        );
        assert_eq!(
            point_kind_from_object_type("binary_value"),
            Some(PointKind::Binary)
        );
        assert_eq!(
            point_kind_from_object_type("multi_state_input"),
            Some(PointKind::MultiState)
        );
    }

    #[test]
    fn point_kind_from_object_type_case_insensitive_and_trimmed() {
        assert_eq!(
            point_kind_from_object_type("  ANALOG_INPUT "),
            Some(PointKind::Analog)
        );
    }

    #[test]
    fn point_kind_from_object_type_unknown_returns_none() {
        assert_eq!(point_kind_from_object_type("unknown_type"), None);
        assert_eq!(point_kind_from_object_type(""), None);
    }

    #[test]
    fn neutral_value_maps_each_variant() {
        let real = SimulatedPoint::from_spec(&PointSpec {
            label: "temp".into(),
            object_type: "analog_input".into(),
            units: None,
            instance: 1,
            profile: ProfileSpec::Constant { value: 22.5 },
        })
        .unwrap();
        assert!(matches!(real.neutral_value(), NeutralValue::Float(_)));

        let boolean = SimulatedPoint::from_spec(&PointSpec {
            label: "occ".into(),
            object_type: "binary_value".into(),
            units: None,
            instance: 1,
            profile: ProfileSpec::ConstantBool { value: true },
        })
        .unwrap();
        assert_eq!(boolean.neutral_value(), NeutralValue::Bool(true));

        let state = SimulatedPoint::from_spec(&PointSpec {
            label: "mode".into(),
            object_type: "multi_state_value".into(),
            units: None,
            instance: 1,
            profile: ProfileSpec::ConstantState { value: 2 },
        })
        .unwrap();
        assert_eq!(state.neutral_value(), NeutralValue::UInt(2));
    }

    #[test]
    fn find_point_matches_object_type_and_instance() {
        let dev = SimulatedDevice::from_spec(&DeviceSpec {
            device_id: 5001,
            name: "AHU".into(),
            points: vec![PointSpec {
                label: "power".into(),
                object_type: "analog_value".into(),
                units: None,
                instance: 3,
                profile: ProfileSpec::Constant { value: 100.0 },
            }],
        });
        assert!(dev.find_point("analog_value", 3).is_some());
        assert!(dev.find_point("ANALOG_VALUE", 3).is_some());
        assert!(dev.find_point("analog_value", 4).is_none());
    }

    #[test]
    fn simulated_device_tick_preseeds_siblings_for_derived_constant() {
        let spec = DeviceSpec {
            device_id: 5001,
            name: "AHU".into(),
            points: vec![
                PointSpec {
                    label: "power".into(),
                    object_type: "analog_value".into(),
                    units: None,
                    instance: 1,
                    profile: ProfileSpec::Constant { value: 100.0 },
                },
                PointSpec {
                    label: "power_copy".into(),
                    object_type: "analog_value".into(),
                    units: None,
                    instance: 2,
                    profile: ProfileSpec::DerivedConstant {
                        from: "power".into(),
                    },
                },
            ],
        };
        let mut device = SimulatedDevice::from_spec(&spec);
        device.tick(1.0, 0.0, 0.0, 25.0);

        let copy = device.find_point("analog_value", 2).unwrap();
        let v = copy.value.as_f32().unwrap();
        assert!(
            (v - 100.0).abs() < 1e-3,
            "derived copy should equal power=100.0, got {v}"
        );
    }
}
