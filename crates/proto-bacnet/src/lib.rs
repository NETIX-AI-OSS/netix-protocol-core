//! BACnet/IP protocol adapter — the reference implementation of the generic
//! simulator/republisher protocol traits.
//!
//! - With the `sim` feature it provides [`register_sim`], a BACnet/IP server that
//!   exposes a `sim-core` simulation.
//! - With the `republish` feature it provides the discovery/browse/poll client
//!   used by the republisher (added in a later step).

use proto_api::{BrowseKind, Capabilities, DiscoveryKind, FieldSpec};

/// Registry id used in simulator config (`protocol = "bacnet"`) and the
/// republisher protocol picker.
pub const ID: &str = "bacnet";

/// BACnet object types accepted as point addressing.
pub const OBJECT_TYPES: &[&str] = &[
    "analog_input",
    "analog_output",
    "analog_value",
    "binary_input",
    "binary_output",
    "binary_value",
    "multi_state_input",
    "multi_state_output",
    "multi_state_value",
];

/// The protocol's declarative capabilities, shared by the simulator and
/// republisher sides so the UI can render BACnet controls without hard-coding.
pub fn capabilities() -> Capabilities {
    Capabilities {
        id: "bacnet",
        display_name: "BACnet/IP",
        discovery: DiscoveryKind::Broadcast,
        browse: BrowseKind::ObjectList,
        connection_fields: vec![
            FieldSpec::text("interface", "Network interface (IPv4)")
                .with_help("blank = all interfaces"),
            FieldSpec::u32("port", "Local UDP port", 0).with_help("0 = ephemeral"),
            FieldSpec::text("broadcast_address", "Broadcast address").with_help("255.255.255.255"),
        ],
        addressing_fields: vec![
            FieldSpec::u32("device_instance", "Device instance", 0),
            FieldSpec::enumeration("object_type", "Object type", OBJECT_TYPES, "analog_input"),
            FieldSpec::u32("object_instance", "Object instance", 0),
            FieldSpec::text("property", "Property").with_help("present_value"),
        ],
        default_port: 47808,
    }
}

#[cfg(feature = "sim")]
mod sim;

/// Register the BACnet simulator adapter with a [`sim_core::SimRegistry`].
#[cfg(feature = "sim")]
pub fn register_sim(registry: &mut sim_core::SimRegistry) {
    registry.register(ID, sim::sim_factory);
}

#[cfg(feature = "republish")]
mod republish;

/// Register the BACnet republisher adapter with a [`republish_core::RepublishRegistry`].
#[cfg(feature = "republish")]
pub fn register_republish(registry: &mut republish_core::RepublishRegistry) {
    registry.register(ID, republish::republish_factory);
}
