//! OPC UA protocol adapter for the generic simulator and republisher; uses the `async-opcua` crate (MPL-2.0, confined to this crate, recorded in the workspace NOTICE).

use proto_api::{BrowseKind, Capabilities, DiscoveryKind, FieldSpec};

/// Registry id used in simulator config (`protocol = "opcua"`) and the republisher protocol picker.
pub const ID: &str = "opcua";

/// The protocol's declarative capabilities; OPC UA discovers servers/endpoints by querying an endpoint URL and browses the server's address space.
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

#[cfg(test)]
mod tests {
    use super::*;
    use proto_api::{CapabilitiesDto, FieldKind};

    #[test]
    fn capabilities_round_trip_through_wire_dto() {
        let caps = capabilities();
        let json = serde_json::to_value(&caps).unwrap();
        assert_eq!(json["id"], ID);
        assert_eq!(json["discovery"], "endpoint_query");
        assert_eq!(json["browse"], "address_space");
        assert_eq!(json["default_port"], 4840);
        let back: CapabilitiesDto = serde_json::from_value(json).unwrap();
        assert_eq!(back, caps.to_dto());
        // The security pick-list and the secret field must survive the wire so a UI can render them.
        let policy = back
            .connection_fields
            .iter()
            .find(|f| f.key == "security_policy")
            .unwrap();
        assert_eq!(
            policy.kind,
            FieldKind::Enum(vec!["none".into(), "basic256sha256".into()])
        );
        let password = back
            .connection_fields
            .iter()
            .find(|f| f.key == "password")
            .unwrap();
        assert_eq!(password.kind, FieldKind::Secret);
    }
}
