//! BACnet-flavoured device/point index built from the protocol-agnostic
//! simulation. This is where the neutral `object_type` strings and `units`
//! strings carried by `sim-core` are mapped to BACnet `ObjectType` enums and the
//! ASHRAE engineering-units enumeration.

use bacnet_rs::object::ObjectType;
use sim_core::simulation::models::SimulatedDevice;

#[derive(Debug, Clone)]
pub struct PointEntry {
    pub object_type: ObjectType,
    /// Raw object-type string (e.g. `"analog_input"`) used to look the live
    /// value up in the simulation.
    pub object_type_str: String,
    pub instance: u32,
    pub label: String,
    pub units: u32,
}

#[derive(Debug, Clone)]
pub struct DeviceEntry {
    pub device_id: u32,
    pub name: String,
    pub points: Vec<PointEntry>,
}

impl DeviceEntry {
    pub fn object_list_len(&self) -> u32 {
        self.points.len() as u32
    }

    pub fn object_list_entry(&self, index: u32) -> Option<(ObjectType, u32)> {
        if index == 0 {
            return None;
        }
        let point = self.points.get((index - 1) as usize)?;
        Some((point.object_type, point.instance))
    }

    pub fn find_point(&self, object_type: ObjectType, instance: u32) -> Option<&PointEntry> {
        self.points
            .iter()
            .find(|point| point.object_type == object_type && point.instance == instance)
    }
}

pub fn build_device_registry(devices: &[SimulatedDevice]) -> Vec<DeviceEntry> {
    devices
        .iter()
        .map(|d| {
            let mut points: Vec<PointEntry> = d
                .points
                .iter()
                .map(|p| PointEntry {
                    object_type: object_type_from_str(&p.object_type)
                        .unwrap_or(ObjectType::AnalogValue),
                    object_type_str: p.object_type.clone(),
                    instance: p.instance,
                    label: p.label.clone(),
                    units: units_from_str(p.units.as_deref()),
                })
                .collect();
            points.sort_by_key(|p| {
                let type_key: u32 = p.object_type.into();
                (type_key, p.instance)
            });
            DeviceEntry {
                device_id: d.device_id,
                name: d.name.clone(),
                points,
            }
        })
        .collect()
}

pub fn object_type_from_str(value: &str) -> Option<ObjectType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "analog_input" => Some(ObjectType::AnalogInput),
        "analog_output" => Some(ObjectType::AnalogOutput),
        "analog_value" => Some(ObjectType::AnalogValue),
        "binary_input" => Some(ObjectType::BinaryInput),
        "binary_output" => Some(ObjectType::BinaryOutput),
        "binary_value" => Some(ObjectType::BinaryValue),
        "multi_state_input" => Some(ObjectType::MultiStateInput),
        "multi_state_output" => Some(ObjectType::MultiStateOutput),
        "multi_state_value" => Some(ObjectType::MultiStateValue),
        _ => None,
    }
}

pub fn units_from_str(value: Option<&str>) -> u32 {
    // BACnet engineering-units enumeration (ANSI/ASHRAE Std 135).
    match value.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("degrees_celsius") => 62,
        Some("degrees_fahrenheit") => 64,
        Some("degrees_kelvin") => 63,
        Some("percent") => 98,
        Some("percent_relative_humidity") => 29,
        Some("parts_per_million") => 96,
        Some("parts_per_billion") => 97,
        Some("watts") => 47,
        Some("kilowatts") => 48,
        Some("kilovolt_amperes") => 49,
        Some("kilovolt_amperes_reactive") => 52,
        Some("watt_hours") => 18,
        Some("kilowatt_hours") => 19,
        Some("volts") => 5,
        Some("amperes") => 3,
        Some("hertz") => 27,
        Some("pascals") => 53,
        Some("kilopascals") => 54,
        Some("bar") => 55,
        Some("cubic_feet_per_minute") => 84,
        Some("liters_per_second") => 87,
        Some("cubic_meters_per_hour") => 135,
        Some("cubic_meters") => 80,
        Some("liters") => 82,
        Some("meters_per_second") => 74,
        Some("minutes") => 72,
        Some("hours") => 71,
        Some("seconds") => 73,
        Some("no_units") | None => 95,
        Some(_) => 95,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_type_from_str_known_and_unknown() {
        assert_eq!(
            object_type_from_str("analog_input"),
            Some(ObjectType::AnalogInput)
        );
        assert_eq!(
            object_type_from_str("  MULTI_STATE_VALUE "),
            Some(ObjectType::MultiStateValue)
        );
        assert_eq!(object_type_from_str("nope"), None);
    }

    #[test]
    fn units_from_str_maps_and_defaults() {
        assert_eq!(units_from_str(Some("degrees_celsius")), 62);
        assert_eq!(units_from_str(Some("kilowatt_hours")), 19);
        assert_eq!(units_from_str(None), 95);
        assert_eq!(units_from_str(Some("flux")), 95);
    }
}
