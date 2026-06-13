use std::collections::HashMap;

use bacnet_rs::object::{ObjectIdentifier, ObjectType, PropertyIdentifier};
use bacnet_rs::property::{self, PropertyValue};
use bacnet_rs::service::{ReadPropertyRequest, ReadPropertyResponse};
use proto_api::PointValue as NeutralValue;
use sim_core::Simulation;

use crate::sim::registry::{DeviceEntry, PointEntry};

use super::{MAX_APDU_LENGTH, VENDOR_ID};

/// A lightweight description of a single property read operation.
pub(crate) struct PropertyRead {
    pub object_identifier: ObjectIdentifier,
    pub property_identifier: PropertyIdentifier,
    pub property_array_index: Option<u32>,
}

impl PropertyRead {
    pub fn from_request(request: &ReadPropertyRequest) -> Self {
        Self {
            object_identifier: request.object_identifier,
            property_identifier: request.property_identifier,
            property_array_index: request.property_array_index,
        }
    }
}

/// Convert a neutral simulation value to its BACnet property representation. The
/// simulation engine only produces Float/Bool/UInt; Int/Text are mapped
/// best-effort for completeness.
fn neutral_to_property(value: NeutralValue) -> PropertyValue {
    match value {
        NeutralValue::Float(v) => PropertyValue::Real(v as f32),
        NeutralValue::Bool(b) => PropertyValue::Boolean(b),
        NeutralValue::UInt(u) => PropertyValue::Unsigned(u),
        NeutralValue::Int(i) => PropertyValue::Real(i as f32),
        NeutralValue::Text(s) => PropertyValue::CharacterString(s),
    }
}

pub fn handle_read_property(
    service_data: &[u8],
    devices: &[DeviceEntry],
    simulation: &Simulation,
) -> Option<Vec<u8>> {
    let request = ReadPropertyRequest::decode(service_data).ok()?;
    let value = resolve_property_read(&PropertyRead::from_request(&request), devices, simulation)?;
    let response = ReadPropertyResponse {
        object_identifier: request.object_identifier,
        property_identifier: request.property_identifier,
        property_array_index: request.property_array_index,
        property_values: vec![value],
    };
    let mut ack = Vec::new();
    response.encode(&mut ack).ok()?;
    Some(ack)
}

fn index_devices_by_id(devices: &[DeviceEntry]) -> HashMap<u32, &DeviceEntry> {
    devices.iter().map(|d| (d.device_id, d)).collect()
}

pub(crate) fn resolve_property_read(
    read: &PropertyRead,
    devices: &[DeviceEntry],
    simulation: &Simulation,
) -> Option<PropertyValue> {
    let object_type = read.object_identifier.object_type;
    let instance = read.object_identifier.instance;

    if object_type == ObjectType::Device {
        let index = index_devices_by_id(devices);
        let device = index.get(&instance)?;
        return read_device_property(device, read.property_identifier, read.property_array_index);
    }

    for device in devices {
        let Some(point) = device.find_point(object_type, instance) else {
            continue;
        };
        return read_point_property(
            simulation,
            device.device_id,
            &device.name,
            point,
            read.property_identifier,
        );
    }

    None
}

pub(crate) fn encode_property_value_bytes(value: &PropertyValue) -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    property::encode_property_value(value, &mut bytes).ok()?;
    Some(bytes)
}

fn read_device_property(
    device: &DeviceEntry,
    property: PropertyIdentifier,
    array_index: Option<u32>,
) -> Option<PropertyValue> {
    match property {
        PropertyIdentifier::ObjectList => match array_index {
            Some(0) => Some(PropertyValue::Unsigned(device.object_list_len() as u64)),
            Some(index) => {
                let (object_type, instance) = device.object_list_entry(index)?;
                Some(PropertyValue::ObjectIdentifier(ObjectIdentifier::new(
                    object_type,
                    instance,
                )))
            }
            None => None,
        },
        PropertyIdentifier::ObjectName => Some(PropertyValue::CharacterString(device.name.clone())),
        PropertyIdentifier::ObjectIdentifier => Some(PropertyValue::ObjectIdentifier(
            ObjectIdentifier::new(ObjectType::Device, device.device_id),
        )),
        PropertyIdentifier::VendorIdentifier => Some(PropertyValue::Unsigned(VENDOR_ID as u64)),
        PropertyIdentifier::MaxApduLengthAccepted => {
            Some(PropertyValue::Unsigned(MAX_APDU_LENGTH as u64))
        }
        // 4 = NoSegmentation. Clients should chunk RPMs; the server does not segment.
        PropertyIdentifier::SegmentationSupported => Some(PropertyValue::Enumerated(4)),
        _ => None,
    }
}

fn read_point_property(
    simulation: &Simulation,
    device_id: u32,
    device_name: &str,
    point: &PointEntry,
    property: PropertyIdentifier,
) -> Option<PropertyValue> {
    match property {
        PropertyIdentifier::ObjectName => Some(PropertyValue::CharacterString(format!(
            "{} {}",
            device_name,
            point.label.replace('_', " ")
        ))),
        PropertyIdentifier::Description => {
            Some(PropertyValue::CharacterString(point.label.clone()))
        }
        PropertyIdentifier::PresentValue => simulation
            .neutral_value(device_id, &point.object_type_str, point.instance)
            .map(neutral_to_property),
        PropertyIdentifier::Units => Some(PropertyValue::Enumerated(point.units)),
        PropertyIdentifier::ObjectIdentifier => Some(PropertyValue::ObjectIdentifier(
            ObjectIdentifier::new(point.object_type, point.instance),
        )),
        PropertyIdentifier::ObjectType => {
            let raw: u32 = point.object_type.into();
            Some(PropertyValue::Enumerated(raw))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::registry::build_device_registry;
    use bacnet_rs::object::ObjectType;
    use sim_core::config::{
        AssetInstanceSpec, AssetTemplate, BuildingConfig, IdPolicy, SeasonalityConfig,
        SimulatorConfig, TemplatePointSpec, WeeklySchedule,
    };
    use sim_core::simulation::profiles::ProfileSpec;
    use sim_core::Simulation;
    use std::collections::HashMap;

    fn single_device_config(points: Vec<TemplatePointSpec>) -> SimulatorConfig {
        let mut templates = HashMap::new();
        templates.insert(
            "tpl".to_string(),
            AssetTemplate {
                description: String::new(),
                points,
            },
        );
        SimulatorConfig {
            building: BuildingConfig {
                name: "Test Building".into(),
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
        }
    }

    fn make_simulation_and_registry(
        points: Vec<TemplatePointSpec>,
    ) -> (Simulation, Vec<DeviceEntry>) {
        let cfg = single_device_config(points);
        let sim = Simulation::new(&cfg).expect("simulation");
        let registry = build_device_registry(&sim.devices);
        (sim, registry)
    }

    fn pt(
        label: &str,
        object_type: &str,
        units: Option<&str>,
        profile: ProfileSpec,
    ) -> TemplatePointSpec {
        TemplatePointSpec {
            label: label.into(),
            object_type: object_type.into(),
            units: units.map(|u| u.to_string()),
            profile,
        }
    }

    #[test]
    fn device_object_name_returns_device_name() {
        let (sim, registry) = make_simulation_and_registry(vec![pt(
            "sat",
            "analog_input",
            None,
            ProfileSpec::Constant { value: 20.0 },
        )]);
        let device = &registry[0];
        let read = PropertyRead {
            object_identifier: ObjectIdentifier::new(ObjectType::Device, device.device_id),
            property_identifier: PropertyIdentifier::ObjectName,
            property_array_index: None,
        };
        let result = resolve_property_read(&read, &registry, &sim);
        assert!(
            matches!(result, Some(PropertyValue::CharacterString(ref s)) if s.starts_with("DEV"))
        );
    }

    #[test]
    fn device_vendor_and_max_apdu_and_segmentation() {
        let (sim, registry) = make_simulation_and_registry(vec![pt(
            "sat",
            "analog_input",
            None,
            ProfileSpec::Constant { value: 20.0 },
        )]);
        let device = &registry[0];
        let mk = |prop| PropertyRead {
            object_identifier: ObjectIdentifier::new(ObjectType::Device, device.device_id),
            property_identifier: prop,
            property_array_index: None,
        };
        assert_eq!(
            resolve_property_read(&mk(PropertyIdentifier::VendorIdentifier), &registry, &sim),
            Some(PropertyValue::Unsigned(VENDOR_ID as u64))
        );
        assert_eq!(
            resolve_property_read(
                &mk(PropertyIdentifier::MaxApduLengthAccepted),
                &registry,
                &sim
            ),
            Some(PropertyValue::Unsigned(MAX_APDU_LENGTH as u64))
        );
        assert_eq!(
            resolve_property_read(
                &mk(PropertyIdentifier::SegmentationSupported),
                &registry,
                &sim
            ),
            Some(PropertyValue::Enumerated(4))
        );
    }

    #[test]
    fn device_object_list_index_0_returns_count() {
        let (sim, registry) = make_simulation_and_registry(vec![
            pt(
                "sat",
                "analog_input",
                None,
                ProfileSpec::Constant { value: 20.0 },
            ),
            pt(
                "rat",
                "analog_input",
                None,
                ProfileSpec::Constant { value: 22.0 },
            ),
        ]);
        let device = &registry[0];
        let read = PropertyRead {
            object_identifier: ObjectIdentifier::new(ObjectType::Device, device.device_id),
            property_identifier: PropertyIdentifier::ObjectList,
            property_array_index: Some(0),
        };
        assert_eq!(
            resolve_property_read(&read, &registry, &sim),
            Some(PropertyValue::Unsigned(2))
        );
    }

    #[test]
    fn point_present_value_real_and_units_and_object_type() {
        let (sim, registry) = make_simulation_and_registry(vec![pt(
            "temp",
            "analog_input",
            Some("degrees_celsius"),
            ProfileSpec::Constant { value: 42.0 },
        )]);
        let device = &registry[0];
        let point = device.find_point(ObjectType::AnalogInput, 1).unwrap();
        assert!(matches!(
            read_point_property(
                &sim,
                device.device_id,
                &device.name,
                point,
                PropertyIdentifier::PresentValue
            ),
            Some(PropertyValue::Real(_))
        ));
        assert_eq!(
            read_point_property(
                &sim,
                device.device_id,
                &device.name,
                point,
                PropertyIdentifier::Units
            ),
            Some(PropertyValue::Enumerated(62))
        );
        // AnalogInput raw value = 0
        assert_eq!(
            read_point_property(
                &sim,
                device.device_id,
                &device.name,
                point,
                PropertyIdentifier::ObjectType
            ),
            Some(PropertyValue::Enumerated(0))
        );
    }

    #[test]
    fn point_present_value_unsigned_for_multi_state() {
        let (sim, registry) = make_simulation_and_registry(vec![pt(
            "mode",
            "multi_state_value",
            None,
            ProfileSpec::ConstantState { value: 3 },
        )]);
        let device = &registry[0];
        let point = device.find_point(ObjectType::MultiStateValue, 1).unwrap();
        assert!(matches!(
            read_point_property(
                &sim,
                device.device_id,
                &device.name,
                point,
                PropertyIdentifier::PresentValue
            ),
            Some(PropertyValue::Unsigned(_))
        ));
    }

    #[test]
    fn resolve_returns_none_for_unknown_device_id() {
        let (sim, registry) = make_simulation_and_registry(vec![pt(
            "sat",
            "analog_input",
            None,
            ProfileSpec::Constant { value: 20.0 },
        )]);
        let read = PropertyRead {
            object_identifier: ObjectIdentifier::new(ObjectType::Device, 99999),
            property_identifier: PropertyIdentifier::ObjectName,
            property_array_index: None,
        };
        assert!(resolve_property_read(&read, &registry, &sim).is_none());
    }
}
