use bacnet_services::rpm::{
    ReadAccessResult, ReadPropertyMultipleACK, ReadPropertyMultipleRequest, ReadResultElement,
};
use bacnet_types::enums::{ErrorClass, ErrorCode, PropertyIdentifier};
use bacnet_types::primitives::ObjectIdentifier;
use bytes::BytesMut;
use sim_core::Simulation;

use crate::sim::registry::DeviceEntry;

use super::properties::{encode_property_value_bytes, resolve_property_read, PropertyRead};

// BACnet/IP single-segment APDU ceiling. We don't implement segmentation, so cap encoded
// RPM ACKs to a safe size and return None (which routes to Error PDU) rather than truncate.
const RPM_ACK_MAX_BYTES: usize = 1400;

pub fn handle_read_property_multiple(
    service_data: &[u8],
    devices: &[DeviceEntry],
    simulation: &Simulation,
) -> Option<Vec<u8>> {
    let request = ReadPropertyMultipleRequest::decode(service_data).ok()?;
    let mut list_of_read_access_results = Vec::new();

    for spec in request.list_of_read_access_specs {
        let mut list_of_results = Vec::new();
        for reference in spec.list_of_property_references {
            let read = PropertyRead {
                object_identifier: types_to_rs_object_id(spec.object_identifier),
                property_identifier: types_to_rs_property(reference.property_identifier),
                property_array_index: reference.property_array_index,
            };
            let element = match resolve_property_read(&read, devices, simulation) {
                Some(value) => ReadResultElement {
                    property_identifier: reference.property_identifier,
                    property_array_index: reference.property_array_index,
                    property_value: encode_property_value_bytes(&value),
                    error: None,
                },
                None => ReadResultElement {
                    property_identifier: reference.property_identifier,
                    property_array_index: reference.property_array_index,
                    property_value: None,
                    error: Some((ErrorClass::OBJECT, ErrorCode::UNKNOWN_PROPERTY)),
                },
            };
            list_of_results.push(element);
        }
        list_of_read_access_results.push(ReadAccessResult {
            object_identifier: spec.object_identifier,
            list_of_results,
        });
    }

    let ack = ReadPropertyMultipleACK {
        list_of_read_access_results,
    };
    let mut buf = BytesMut::new();
    ack.encode(&mut buf);
    if buf.len() > RPM_ACK_MAX_BYTES {
        log::warn!(
            "RPM ACK size {} exceeds {} bytes; returning error (segmentation not supported)",
            buf.len(),
            RPM_ACK_MAX_BYTES
        );
        return None;
    }
    Some(buf.to_vec())
}

fn types_to_rs_object_id(identifier: ObjectIdentifier) -> bacnet_rs::object::ObjectIdentifier {
    bacnet_rs::object::ObjectIdentifier::new(
        bacnet_rs::object::ObjectType::from(identifier.object_type().to_raw()),
        identifier.instance_number(),
    )
}

fn types_to_rs_property(identifier: PropertyIdentifier) -> bacnet_rs::object::PropertyIdentifier {
    bacnet_rs::object::PropertyIdentifier::from(identifier.to_raw())
}
