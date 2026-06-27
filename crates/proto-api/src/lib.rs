//! Protocol-neutral types shared by the generic simulator (`sim-core`) and
//! republisher (`republish-core`) cores and by every protocol adapter
//! (`proto-bacnet`, `proto-modbus`, `proto-opcua`, …).
//!
//! The cores never name a concrete protocol; they speak in terms of the neutral
//! [`PointKind`] / [`PointValue`] value model and an opaque [`Addressing`] map.
//! Each protocol adapter declares its [`Capabilities`] so the user interface can
//! render the right controls without hard-coding protocol knowledge.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A protocol-native address for a single point, carried opaquely by the cores
/// and interpreted only by the owning adapter.
///
/// Examples: `{"register": 40001, "datatype": "f32"}` (Modbus),
/// `{"node_id": "ns=2;s=AHU-1.SupplyTemp"}` (OPC UA),
/// `{"object_type": "analog_input", "object_instance": 1, "property": "present_value"}` (BACnet).
pub type Addressing = BTreeMap<String, serde_json::Value>;

/// The neutral category of a simulated/published point. Adapters map this to and
/// from their protocol-native notion (BACnet object type, Modbus table, OPC UA
/// data type, …).
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

/// A neutral point value. Replaces protocol-specific value types (e.g.
/// `bacnet_rs::property::PropertyValue`) at the core boundary.
#[derive(Debug, Clone, PartialEq)]
pub enum PointValue {
    Float(f64),
    Bool(bool),
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

/// How a protocol finds devices/servers to publish from. Drives which discovery
/// controls the republisher UI shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// The widget type the UI should render for a [`FieldSpec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldKind {
    Text,
    U32,
    Bool,
    /// A pick-list of allowed string values.
    Enum(Vec<String>),
    /// Masked text input (passwords, passphrases).
    Secret,
}

/// One protocol-specific configuration field, rendered dynamically by the UI so
/// adding a protocol never requires touching the GUI/TUI code.
#[derive(Debug, Clone)]
pub struct FieldSpec {
    /// Stable key used in the [`Addressing`]/connection map.
    pub key: String,
    /// Human label shown in the UI.
    pub label: String,
    pub kind: FieldKind,
    /// Default value when the field is unset.
    pub default: Option<serde_json::Value>,
    /// Optional one-line help/placeholder.
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

/// Declarative description of what a protocol adapter can do. The republisher UI
/// reads this to decide which discovery/browse controls and which connection and
/// per-point fields to render.
#[derive(Debug, Clone)]
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
    fn field_spec_builders_set_defaults() {
        let f = FieldSpec::u32("port", "Port", 502);
        assert_eq!(f.default, Some(serde_json::json!(502)));
        let e = FieldSpec::enumeration("datatype", "Data type", &["u16", "f32"], "u16");
        assert!(matches!(e.kind, FieldKind::Enum(ref v) if v.len() == 2));
    }
}
