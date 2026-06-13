//! OPC UA republisher adapter: connect to an endpoint, browse the address space
//! for Variable nodes, and read node values.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use opcua::client::{ClientBuilder, IdentityToken, Session};
use opcua::types::{
    BrowseDescription, BrowseDirection, DataValue, MessageSecurityMode, NodeClass, NodeId,
    ObjectId, QualifiedName, ReadValueId, ReferenceDescription, ReferenceTypeId,
    TimestampsToReturn, Variant,
};
use proto_api::{Addressing, Capabilities};
use republish_core::model::{
    json_scalar, now_millis, DiscoverOutcome, DiscoveredDevice, DiscoveredPoint, PointConfig,
    PointFailure, PointSample, PollOutcome, TelemetryValue,
};
use republish_core::RepublishProtocol;
use tokio::task::JoinHandle;

const SECURITY_NONE: &str = "http://opcfoundation.org/UA/SecurityPolicy#None";
/// OPC UA `Value` attribute id (Part 6).
const ATTR_VALUE: u32 = 13;

pub struct OpcuaRepublishProtocol {
    caps: Capabilities,
}

pub fn republish_factory() -> Box<dyn RepublishProtocol> {
    Box::new(OpcuaRepublishProtocol {
        caps: crate::capabilities(),
    })
}

fn conn_str(conn: &Addressing, key: &str) -> Option<String> {
    conn.get(key)
        .map(json_scalar)
        .filter(|s| !s.trim().is_empty())
}

struct Connection {
    session: Arc<Session>,
    handle: JoinHandle<opcua::types::StatusCode>,
}

impl Connection {
    async fn close(self) {
        let _ = self.session.disconnect().await;
        self.handle.abort();
    }
}

async fn connect(conn: &Addressing) -> Result<Connection> {
    let url =
        conn_str(conn, "endpoint_url").ok_or_else(|| anyhow!("OPC UA endpoint_url is required"))?;
    let mut client = ClientBuilder::new()
        .application_name("NETIX Republisher")
        .application_uri("urn:netix:republisher")
        .product_uri("urn:netix:republisher")
        .create_sample_keypair(false)
        .trust_server_certs(true)
        .session_retry_limit(1)
        .client()
        .map_err(|errors| anyhow!("OPC UA client build failed: {errors:?}"))?;

    // Username/password is accepted via capabilities but the default flow is
    // anonymous + None security (matching the simulator's default endpoint).
    let identity = IdentityToken::Anonymous;

    let (session, event_loop) = client
        .connect_to_matching_endpoint(
            (url.as_str(), SECURITY_NONE, MessageSecurityMode::None),
            identity,
        )
        .await
        .map_err(|e| anyhow!("connect to {url} failed: {e}"))?;
    let handle = event_loop.spawn();
    session.wait_for_connection().await;
    Ok(Connection { session, handle })
}

fn variant_to_value(variant: &Variant) -> Option<TelemetryValue> {
    Some(match variant {
        Variant::Boolean(b) => TelemetryValue::Number(if *b { 1.0 } else { 0.0 }),
        Variant::SByte(v) => TelemetryValue::Number(*v as f64),
        Variant::Byte(v) => TelemetryValue::Number(*v as f64),
        Variant::Int16(v) => TelemetryValue::Number(*v as f64),
        Variant::UInt16(v) => TelemetryValue::Number(*v as f64),
        Variant::Int32(v) => TelemetryValue::Number(*v as f64),
        Variant::UInt32(v) => TelemetryValue::Number(*v as f64),
        Variant::Int64(v) => TelemetryValue::Number(*v as f64),
        Variant::UInt64(v) => TelemetryValue::Number(*v as f64),
        Variant::Float(v) => TelemetryValue::Number(*v as f64),
        Variant::Double(v) => TelemetryValue::Number(*v),
        Variant::String(s) => TelemetryValue::Text(s.to_string()),
        Variant::Empty => return None,
        other => TelemetryValue::Text(format!("{other:?}")),
    })
}

fn data_value(dv: &DataValue) -> Option<TelemetryValue> {
    dv.value.as_ref().and_then(variant_to_value)
}

fn read_value_id(node_id: NodeId) -> ReadValueId {
    ReadValueId {
        node_id,
        attribute_id: ATTR_VALUE,
        index_range: Default::default(),
        data_encoding: QualifiedName::null(),
    }
}

#[async_trait::async_trait]
impl RepublishProtocol for OpcuaRepublishProtocol {
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn discover(&self, conn: &Addressing) -> Result<DiscoverOutcome> {
        let url = conn_str(conn, "endpoint_url").unwrap_or_default();
        let connection = connect(conn).await?;
        connection.close().await;
        Ok(DiscoverOutcome {
            devices: vec![DiscoveredDevice {
                key: "opcua-server".into(),
                address: url,
                detail: "OPC UA server".into(),
            }],
            warnings: vec![],
        })
    }

    async fn browse(
        &self,
        conn: &Addressing,
        device: &DiscoveredDevice,
    ) -> Result<Vec<DiscoveredPoint>> {
        let connection = connect(conn).await?;
        let result = browse_variables(&connection.session, device).await;
        connection.close().await;
        result
    }

    async fn poll(&self, conn: &Addressing, points: &[PointConfig]) -> Result<PollOutcome> {
        let enabled: Vec<&PointConfig> = points.iter().filter(|p| p.enabled).collect();
        if enabled.is_empty() {
            return Ok(PollOutcome::default());
        }
        let connection = connect(conn).await?;
        let mut outcome = PollOutcome::default();

        let mut to_read = Vec::with_capacity(enabled.len());
        let mut valid = Vec::with_capacity(enabled.len());
        for point in &enabled {
            match conn_str(&point.addressing, "node_id").and_then(|s| s.parse::<NodeId>().ok()) {
                Some(node_id) => {
                    to_read.push(read_value_id(node_id));
                    valid.push(*point);
                }
                None => outcome.failures.push(PointFailure {
                    point: (*point).clone(),
                    error: "invalid or missing node_id".into(),
                }),
            }
        }

        match connection
            .session
            .read(&to_read, TimestampsToReturn::Neither, 0.0)
            .await
        {
            Ok(values) => {
                for (point, dv) in valid.iter().zip(values.iter()) {
                    match data_value(dv) {
                        Some(value) => outcome.samples.push(PointSample {
                            point: (*point).clone(),
                            value,
                            topic: String::new(), // filled in by the worker
                            timestamp_ms: now_millis(),
                        }),
                        None => outcome.failures.push(PointFailure {
                            point: (*point).clone(),
                            error: dv
                                .status
                                .map(|s| format!("bad status: {s}"))
                                .unwrap_or_else(|| "no value".into()),
                        }),
                    }
                }
            }
            Err(status) => {
                connection.close().await;
                return Err(anyhow!("OPC UA read failed: {status}"));
            }
        }

        connection.close().await;
        Ok(outcome)
    }
}

/// Maximum hierarchy depth descended from `Objects` while browsing.
const MAX_BROWSE_DEPTH: usize = 12;
/// Safety cap on the number of nodes visited during a browse.
const MAX_BROWSE_NODES: usize = 20_000;

/// Recursively walk the address space from `Objects`, collecting every Variable
/// node as a point. Objects/folders are descended (following hierarchical
/// references and `BrowseNext` continuations); the standard OPC UA core
/// hierarchy (namespace 0, e.g. the `Server` diagnostics tree) is skipped so the
/// result is the server's user data, organized by its folder path.
async fn browse_variables(
    session: &Arc<Session>,
    device: &DiscoveredDevice,
) -> Result<Vec<DiscoveredPoint>> {
    let mut points: Vec<DiscoveredPoint> = Vec::new();
    let mut visited: HashSet<NodeId> = HashSet::new();
    let mut frontier: VecDeque<(NodeId, Vec<String>, usize)> = VecDeque::new();

    let root: NodeId = ObjectId::ObjectsFolder.into();
    visited.insert(root.clone());
    frontier.push_back((root, Vec::new(), 0));

    while let Some((node_id, path, depth)) = frontier.pop_front() {
        if visited.len() > MAX_BROWSE_NODES {
            break;
        }
        for reference in browse_children(session, &node_id).await? {
            if !reference.is_forward {
                continue;
            }
            let child_id = reference.node_id.node_id.clone();
            let name = reference_name(&reference, &child_id);
            match reference.node_class {
                NodeClass::Variable => {
                    let mut addressing = Addressing::new();
                    addressing.insert("node_id".into(), serde_json::json!(child_id.to_string()));
                    let mut tag = path.clone();
                    tag.push(name.clone());
                    points.push(DiscoveredPoint {
                        device_key: device.key.clone(),
                        name: Some(name),
                        description: None,
                        units: None,
                        value: None, // filled by a single batched read below
                        addressing,
                        suggested_tag_path: format!("{}/{}", device.key, tag.join("/")),
                    });
                }
                NodeClass::Object => {
                    // The OPC UA core hierarchy (ns 0) is server plumbing, not
                    // user data — don't descend into it.
                    if child_id.namespace == 0 || depth + 1 > MAX_BROWSE_DEPTH {
                        continue;
                    }
                    if visited.insert(child_id.clone()) {
                        let mut child_path = path.clone();
                        child_path.push(name);
                        frontier.push_back((child_id, child_path, depth + 1));
                    }
                }
                _ => {}
            }
        }
    }

    // Read all discovered values in one request for a useful browse-time preview.
    let to_read: Vec<ReadValueId> = points
        .iter()
        .filter_map(|p| conn_str(&p.addressing, "node_id"))
        .filter_map(|s| s.parse::<NodeId>().ok())
        .map(read_value_id)
        .collect();
    if to_read.len() == points.len() {
        if let Ok(values) = session
            .read(&to_read, TimestampsToReturn::Neither, 0.0)
            .await
        {
            for (point, dv) in points.iter_mut().zip(values.iter()) {
                point.value = data_value(dv);
            }
        }
    }

    Ok(points)
}

/// Browse the forward hierarchical references of one node, following any
/// `BrowseNext` continuation points.
async fn browse_children(
    session: &Arc<Session>,
    node_id: &NodeId,
) -> Result<Vec<ReferenceDescription>> {
    let to_browse = BrowseDescription {
        node_id: node_id.clone(),
        browse_direction: BrowseDirection::Forward,
        reference_type_id: ReferenceTypeId::HierarchicalReferences.into(),
        include_subtypes: true,
        node_class_mask: 0, // all classes; filtered by the caller
        result_mask: 0x3F,  // all fields
    };
    let results = session
        .browse(&[to_browse], 0, None)
        .await
        .map_err(|status| anyhow!("browse failed: {status}"))?;
    let Some(mut result) = results.into_iter().next() else {
        return Ok(Vec::new());
    };

    let mut out = result.references.take().unwrap_or_default();
    let mut continuation = result.continuation_point;
    while !continuation.is_null_or_empty() {
        let mut more = session
            .browse_next(false, std::slice::from_ref(&continuation))
            .await
            .map_err(|status| anyhow!("browse_next failed: {status}"))?;
        let Some(mut next) = more.drain(..).next() else {
            break;
        };
        if let Some(refs) = next.references.take() {
            out.extend(refs);
        }
        continuation = next.continuation_point;
    }
    Ok(out)
}

/// The best human-readable name for a browsed reference: display name, else
/// browse name, else the node id.
fn reference_name(reference: &ReferenceDescription, node_id: &NodeId) -> String {
    let display = reference.display_name.text.to_string();
    if !display.is_empty() {
        return display;
    }
    let browse = reference.browse_name.name.to_string();
    if !browse.is_empty() {
        return browse;
    }
    node_id.to_string()
}
