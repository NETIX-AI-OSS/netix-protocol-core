//! Protocol-neutral types shared by the generic sim/republisher cores and every protocol adapter, keyed on the [`PointKind`]/[`PointValue`] model and [`Capabilities`].

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A protocol-native address for a single point, carried opaquely by the cores and interpreted only by the owning adapter.
pub type Addressing = BTreeMap<String, serde_json::Value>;

/// Strips a device name's `-NNN` instance suffix; sim-core emit and BACnet discovery must apply this identically or seeded demo tags won't match discovered ones.
pub fn base_key(name: &str) -> &str {
    if let Some(idx) = name.rfind('-') {
        let suffix = &name[idx + 1..];
        if suffix.len() == 3 && suffix.bytes().all(|b| b.is_ascii_digit()) {
            return &name[..idx];
        }
    }
    name
}

/// The neutral category of a simulated/published point; adapters map this to their protocol-native notion (object type, table, data type, …).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PointKind {
    /// Continuous numeric value (temperature, flow, kW, …).
    Analog,
    /// Two-state value (on/off, alarm/normal).
    Binary,
    /// Enumerated state (1..N).
    MultiState,
    /// Free-form string value.
    Text,
}

/// A neutral point value, replacing protocol-specific value types at the core boundary; on the wire it is externally tagged (`{"float": 1.5}`, `{"uint": 7}`, …).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PointValue {
    Float(f64),
    Bool(bool),
    // Explicit tag: snake_case would otherwise render the variant as `u_int`.
    #[serde(rename = "uint")]
    UInt(u64),
    Int(i64),
    Text(String),
}

impl PointValue {
    /// Best-effort numeric view, used by adapters that encode everything as a
    /// number (Modbus registers) and by the TUI.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            PointValue::Float(v) => Some(*v),
            PointValue::UInt(v) => Some(*v as f64),
            PointValue::Int(v) => Some(*v as f64),
            PointValue::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
            PointValue::Text(_) => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            PointValue::Bool(v) => Some(*v),
            PointValue::UInt(v) => Some(*v != 0),
            PointValue::Int(v) => Some(*v != 0),
            PointValue::Float(v) => Some(*v != 0.0),
            PointValue::Text(_) => None,
        }
    }

    /// Display string used in TUIs/logs.
    pub fn display(&self) -> String {
        match self {
            PointValue::Float(v) => format!("{v:.3}"),
            PointValue::Bool(v) => v.to_string(),
            PointValue::UInt(v) => v.to_string(),
            PointValue::Int(v) => v.to_string(),
            PointValue::Text(v) => v.clone(),
        }
    }
}

/// How a protocol finds devices/servers to publish from; drives which discovery controls the republisher UI shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryKind {
    /// Broadcast announcement (BACnet Who-Is/I-Am).
    Broadcast,
    /// Query a known endpoint for servers/endpoints (OPC UA FindServers/GetEndpoints).
    EndpointQuery,
    /// Probe a CIDR range for a well-known port (Modbus port 502 sweep).
    SubnetScan,
    /// No automatic discovery — the user enters endpoints by hand.
    ManualOnly,
}

/// How a protocol enumerates a device's points. Drives the browse UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowseKind {
    /// Read a device object list (BACnet).
    ObjectList,
    /// Walk an address space via Browse references (OPC UA).
    AddressSpace,
    /// Scan register ranges (Modbus).
    RegisterScan,
    /// No browse — points are entered by hand.
    None,
}

/// The widget type the UI should render for a [`FieldSpec`]; on the wire every variant is `{"kind": "<name>"}` and the pick-list carries `{"kind": "enum", "options": [...]}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "options", rename_all = "snake_case")]
pub enum FieldKind {
    Text,
    U32,
    Bool,
    /// A pick-list of allowed string values.
    Enum(Vec<String>),
    /// Masked text input (passwords, passphrases).
    Secret,
}

/// One protocol-specific configuration field, rendered dynamically so adding a protocol never requires touching the GUI/TUI code.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSpec {
    /// Stable key used in the [`Addressing`]/connection map.
    pub key: String,
    /// Human label shown in the UI.
    pub label: String,
    pub kind: FieldKind,
    /// Default value when the field is unset.
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    /// Optional one-line help/placeholder.
    #[serde(default)]
    pub help: Option<String>,
}

impl FieldSpec {
    pub fn text(key: &str, label: &str) -> Self {
        Self {
            key: key.to_string(),
            label: label.to_string(),
            kind: FieldKind::Text,
            default: None,
            help: None,
        }
    }

    pub fn u32(key: &str, label: &str, default: u32) -> Self {
        Self {
            key: key.to_string(),
            label: label.to_string(),
            kind: FieldKind::U32,
            default: Some(serde_json::json!(default)),
            help: None,
        }
    }

    pub fn bool(key: &str, label: &str, default: bool) -> Self {
        Self {
            key: key.to_string(),
            label: label.to_string(),
            kind: FieldKind::Bool,
            default: Some(serde_json::json!(default)),
            help: None,
        }
    }

    pub fn enumeration(key: &str, label: &str, options: &[&str], default: &str) -> Self {
        Self {
            key: key.to_string(),
            label: label.to_string(),
            kind: FieldKind::Enum(options.iter().map(|s| s.to_string()).collect()),
            default: Some(serde_json::json!(default)),
            help: None,
        }
    }

    pub fn secret(key: &str, label: &str) -> Self {
        Self {
            key: key.to_string(),
            label: label.to_string(),
            kind: FieldKind::Secret,
            default: None,
            help: None,
        }
    }

    pub fn with_help(mut self, help: &str) -> Self {
        self.help = Some(help.to_string());
        self
    }
}

/// Declarative description of what a protocol adapter can do, read by the republisher UI to decide which controls and fields to render; serialises directly, and deserialises through [`CapabilitiesDto`] because the ids are `&'static str`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Capabilities {
    /// Stable id used in config (`protocol = "modbus"`).
    pub id: &'static str,
    /// Human-readable name shown in the protocol picker ("Modbus TCP").
    pub display_name: &'static str,
    pub discovery: DiscoveryKind,
    pub browse: BrowseKind,
    /// Fields describing how to connect (host/port/unit-id, endpoint URL, …).
    pub connection_fields: Vec<FieldSpec>,
    /// Per-point addressing fields (register/datatype, node id, object type, …).
    pub addressing_fields: Vec<FieldSpec>,
    /// Default listen/connect port for this protocol.
    pub default_port: u16,
}

impl Capabilities {
    /// Owned-string copy for consumers that read capabilities off the wire.
    pub fn to_dto(&self) -> CapabilitiesDto {
        CapabilitiesDto::from(self)
    }
}

/// Owned-string, wire-shaped twin of [`Capabilities`] (identical JSON) so discovery agents and UIs can deserialise what an adapter published.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapabilitiesDto {
    pub id: String,
    pub display_name: String,
    pub discovery: DiscoveryKind,
    pub browse: BrowseKind,
    #[serde(default)]
    pub connection_fields: Vec<FieldSpec>,
    #[serde(default)]
    pub addressing_fields: Vec<FieldSpec>,
    pub default_port: u16,
}

impl From<&Capabilities> for CapabilitiesDto {
    fn from(caps: &Capabilities) -> Self {
        Self {
            id: caps.id.to_string(),
            display_name: caps.display_name.to_string(),
            discovery: caps.discovery,
            browse: caps.browse,
            connection_fields: caps.connection_fields.clone(),
            addressing_fields: caps.addressing_fields.clone(),
            default_port: caps.default_port,
        }
    }
}

impl From<Capabilities> for CapabilitiesDto {
    fn from(caps: Capabilities) -> Self {
        Self::from(&caps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_value_numeric_coercions() {
        assert_eq!(PointValue::Float(1.5).as_f64(), Some(1.5));
        assert_eq!(PointValue::UInt(7).as_f64(), Some(7.0));
        assert_eq!(PointValue::Bool(true).as_f64(), Some(1.0));
        assert_eq!(PointValue::Text("x".into()).as_f64(), None);
        assert_eq!(PointValue::UInt(0).as_bool(), Some(false));
    }

    #[test]
    fn base_key_strips_instance_suffix() {
        assert_eq!(base_key("ahu-12-001"), "ahu-12");
        assert_eq!(base_key("201-001"), "201");
        assert_eq!(
            base_key("demo-weather-station-01-001"),
            "demo-weather-station-01"
        );
        assert_eq!(base_key("plain"), "plain");
    }

    #[test]
    fn field_spec_builders_set_defaults() {
        let f = FieldSpec::u32("port", "Port", 502);
        assert_eq!(f.default, Some(serde_json::json!(502)));
        let e = FieldSpec::enumeration("datatype", "Data type", &["u16", "f32"], "u16");
        assert!(matches!(e.kind, FieldKind::Enum(ref v) if v.len() == 2));
    }

    #[test]
    fn point_value_is_externally_tagged_snake_case() {
        let cases = [
            (PointValue::Float(1.5), serde_json::json!({"float": 1.5})),
            (PointValue::Bool(true), serde_json::json!({"bool": true})),
            (PointValue::UInt(7), serde_json::json!({"uint": 7})),
            (PointValue::Int(-3), serde_json::json!({"int": -3})),
            (
                PointValue::Text("x".into()),
                serde_json::json!({"text": "x"}),
            ),
        ];
        for (value, json) in cases {
            assert_eq!(serde_json::to_value(&value).unwrap(), json);
            let back: PointValue = serde_json::from_value(json).unwrap();
            assert_eq!(back, value);
        }
    }

    #[test]
    fn discovery_and_browse_kinds_are_snake_case_strings() {
        let discovery = [
            (DiscoveryKind::Broadcast, "broadcast"),
            (DiscoveryKind::EndpointQuery, "endpoint_query"),
            (DiscoveryKind::SubnetScan, "subnet_scan"),
            (DiscoveryKind::ManualOnly, "manual_only"),
        ];
        for (kind, name) in discovery {
            assert_eq!(serde_json::to_value(kind).unwrap(), serde_json::json!(name));
            let back: DiscoveryKind = serde_json::from_value(serde_json::json!(name)).unwrap();
            assert_eq!(back, kind);
        }
        let browse = [
            (BrowseKind::ObjectList, "object_list"),
            (BrowseKind::AddressSpace, "address_space"),
            (BrowseKind::RegisterScan, "register_scan"),
            (BrowseKind::None, "none"),
        ];
        for (kind, name) in browse {
            assert_eq!(serde_json::to_value(kind).unwrap(), serde_json::json!(name));
            let back: BrowseKind = serde_json::from_value(serde_json::json!(name)).unwrap();
            assert_eq!(back, kind);
        }
    }

    #[test]
    fn field_kind_is_adjacently_tagged_with_enum_options() {
        let cases = [
            (FieldKind::Text, serde_json::json!({"kind": "text"})),
            (FieldKind::U32, serde_json::json!({"kind": "u32"})),
            (FieldKind::Bool, serde_json::json!({"kind": "bool"})),
            (FieldKind::Secret, serde_json::json!({"kind": "secret"})),
            (
                FieldKind::Enum(vec!["u16".into(), "f32".into()]),
                serde_json::json!({"kind": "enum", "options": ["u16", "f32"]}),
            ),
        ];
        for (kind, json) in cases {
            assert_eq!(serde_json::to_value(&kind).unwrap(), json);
            let back: FieldKind = serde_json::from_value(json).unwrap();
            assert_eq!(back, kind);
        }
    }

    #[test]
    fn field_spec_round_trips_and_tolerates_missing_optionals() {
        let spec = FieldSpec::enumeration("datatype", "Data type", &["u16", "f32"], "u16")
            .with_help("Register data type");
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "key": "datatype",
                "label": "Data type",
                "kind": {"kind": "enum", "options": ["u16", "f32"]},
                "default": "u16",
                "help": "Register data type",
            })
        );
        let back: FieldSpec = serde_json::from_value(json).unwrap();
        assert_eq!(back, spec);

        // `default` and `help` may be omitted entirely by a hand-written payload.
        let minimal: FieldSpec = serde_json::from_value(
            serde_json::json!({"key": "host", "label": "Host", "kind": {"kind": "text"}}),
        )
        .unwrap();
        assert_eq!(minimal, FieldSpec::text("host", "Host"));
    }

    fn sample_capabilities() -> Capabilities {
        Capabilities {
            id: "modbus",
            display_name: "Modbus TCP",
            discovery: DiscoveryKind::SubnetScan,
            browse: BrowseKind::RegisterScan,
            connection_fields: vec![
                FieldSpec::text("host", "Host").with_help("192.168.1.10"),
                FieldSpec::u32("port", "Port", 502),
            ],
            addressing_fields: vec![
                FieldSpec::enumeration("table", "Table", &["holding", "input"], "holding"),
                FieldSpec::bool("signed", "Signed", false),
                FieldSpec::secret("token", "Token"),
            ],
            default_port: 502,
        }
    }

    #[test]
    fn capabilities_serialise_and_round_trip_through_dto() {
        let caps = sample_capabilities();
        let json = serde_json::to_value(&caps).unwrap();
        assert_eq!(json["id"], "modbus");
        assert_eq!(json["display_name"], "Modbus TCP");
        assert_eq!(json["discovery"], "subnet_scan");
        assert_eq!(json["browse"], "register_scan");
        assert_eq!(json["default_port"], 502);
        assert_eq!(json["connection_fields"][0]["key"], "host");
        assert_eq!(json["addressing_fields"][0]["kind"]["kind"], "enum");
        assert_eq!(json["addressing_fields"][0]["kind"]["options"][1], "input");

        // The DTO is byte-for-byte the same wire shape as the borrowed struct.
        let dto = caps.to_dto();
        assert_eq!(serde_json::to_value(&dto).unwrap(), json);
        assert_eq!(CapabilitiesDto::from(caps.clone()), dto);

        let back: CapabilitiesDto = serde_json::from_value(json).unwrap();
        assert_eq!(back, dto);
        assert_eq!(back.id, caps.id);
        assert_eq!(back.display_name, caps.display_name);
        assert_eq!(back.connection_fields, caps.connection_fields);
        assert_eq!(back.addressing_fields, caps.addressing_fields);
    }

    #[test]
    fn capabilities_dto_defaults_missing_field_lists() {
        let dto: CapabilitiesDto = serde_json::from_value(serde_json::json!({
            "id": "manual",
            "display_name": "Manual",
            "discovery": "manual_only",
            "browse": "none",
            "default_port": 0,
        }))
        .unwrap();
        assert!(dto.connection_fields.is_empty());
        assert!(dto.addressing_fields.is_empty());
        assert_eq!(dto.discovery, DiscoveryKind::ManualOnly);
        assert_eq!(dto.browse, BrowseKind::None);
    }
}
