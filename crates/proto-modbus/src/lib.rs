//! Modbus TCP protocol adapter for the generic simulator and republisher.
//!
//! - With the `sim` feature it provides [`register_sim`], a read-only Modbus TCP
//!   server that exposes a `sim-core` simulation as holding/input registers and
//!   coils/discrete inputs.
//! - With the `republish` feature it provides the Modbus TCP client used by the
//!   republisher for discovery, browse, and polling.

use proto_api::{BrowseKind, Capabilities, DiscoveryKind, FieldSpec};

/// Registry id used in simulator config (`protocol = "modbus"`) and the
/// republisher protocol picker.
pub const ID: &str = "modbus";

/// Modbus register tables a republished point can live in.
pub const TABLES: &[&str] = &["holding", "input", "coil", "discrete"];

/// Per-point data types the republisher can decode from registers.
pub const DATATYPES: &[&str] = &["u16", "i16", "u32", "i32", "f32"];

/// The protocol's declarative capabilities. Modbus TCP has no native discovery,
/// so the republisher uses a subnet sweep (falling back to manual entry) and a
/// register scan to browse.
pub fn capabilities() -> Capabilities {
    Capabilities {
        id: "modbus",
        display_name: "Modbus TCP",
        discovery: DiscoveryKind::SubnetScan,
        browse: BrowseKind::RegisterScan,
        connection_fields: vec![
            FieldSpec::text("host", "Host or CIDR").with_help("192.168.1.10 or 192.168.1.0/24"),
            FieldSpec::u32("port", "Port", 502),
            FieldSpec::u32("unit_id", "Unit ID", 1),
            FieldSpec::u32("timeout_ms", "Timeout (ms)", 1000),
            FieldSpec::u32("scan_concurrency", "Scan concurrency", 32),
            FieldSpec::u32("max_hosts", "Max hosts per scan", 256),
            FieldSpec::u32("browse_start", "Browse start address", 0),
            FieldSpec::u32("browse_count", "Browse count", 32),
        ],
        addressing_fields: vec![
            FieldSpec::enumeration("table", "Register table", TABLES, "holding"),
            FieldSpec::u32("address", "Address", 0),
            FieldSpec::enumeration("datatype", "Data type", DATATYPES, "u16"),
            FieldSpec::enumeration("word_order", "Word order", &["big", "little"], "big"),
            FieldSpec::text("scale", "Scale").with_help("1.0"),
        ],
        default_port: 502,
    }
}

#[cfg(feature = "sim")]
mod sim;

/// Register the Modbus simulator adapter with a [`sim_core::SimRegistry`].
#[cfg(feature = "sim")]
pub fn register_sim(registry: &mut sim_core::SimRegistry) {
    registry.register(ID, sim::sim_factory);
}

#[cfg(feature = "republish")]
mod republish;

/// Register the Modbus republisher adapter with a [`republish_core::RepublishRegistry`].
#[cfg(feature = "republish")]
pub fn register_republish(registry: &mut republish_core::RepublishRegistry) {
    registry.register(ID, republish::republish_factory);
}
