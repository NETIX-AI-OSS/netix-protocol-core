//! OPC UA protocol adapter for the generic simulator and republisher.
//!
//! Uses the `async-opcua` crate (MPL-2.0; confined to this crate, recorded in the
//! workspace NOTICE).
//!
//! - With the `sim` feature it provides [`register_sim`], an OPC UA server that
//!   exposes a `sim-core` simulation as an address space of Variable nodes.
//! - With the `republish` feature it provides [`register_republish`], the OPC UA
//!   client used by the republisher: it discovers endpoints, walks the address
//!   space recursively, and reads node values.

use proto_api::{BrowseKind, Capabilities, DiscoveryKind, FieldSpec};

/// Registry id used in simulator config (`protocol = "opcua"`) and the
/// republisher protocol picker.
pub const ID: &str = "opcua";

/// The protocol's declarative capabilities. OPC UA discovers servers/endpoints by
/// querying an endpoint URL and browses the server's address space.
pub fn capabilities() -> Capabilities {
    Capabilities {
        id: "opcua",
        display_name: "OPC UA",
        discovery: DiscoveryKind::EndpointQuery,
        browse: BrowseKind::AddressSpace,
        connection_fields: vec![
            FieldSpec::text("endpoint_url", "Endpoint URL").with_help("opc.tcp://host:4840"),
            FieldSpec::enumeration(
                "security_policy",
                "Security policy",
                &["none", "basic256sha256"],
                "none",
            ),
            FieldSpec::enumeration(
                "security_mode",
                "Security mode",
                &["none", "sign", "sign_encrypt"],
                "none",
            ),
            FieldSpec::text("username", "Username"),
            FieldSpec::secret("password", "Password"),
        ],
        addressing_fields: vec![FieldSpec::text("node_id", "Node ID").with_help("ns=2;s=...")],
        default_port: 4840,
    }
}

#[cfg(feature = "sim")]
mod sim;

/// Register the OPC UA simulator adapter with a [`sim_core::SimRegistry`].
#[cfg(feature = "sim")]
pub fn register_sim(registry: &mut sim_core::SimRegistry) {
    registry.register(ID, sim::sim_factory);
}

#[cfg(feature = "republish")]
mod republish;

/// Register the OPC UA republisher adapter with a [`republish_core::RepublishRegistry`].
#[cfg(feature = "republish")]
pub fn register_republish(registry: &mut republish_core::RepublishRegistry) {
    registry.register(ID, republish::republish_factory);
}
