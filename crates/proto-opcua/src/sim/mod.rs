//! OPC UA simulator adapter: exposes the shared simulation as an OPC UA server
//! address space (one Variable node per simulated point), with live values fed
//! through a value cache refreshed once per second from the simulation.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use opcua::server::address_space::Variable;
use opcua::server::diagnostics::NamespaceMetadata;
use opcua::server::node_manager::memory::{simple_node_manager, SimpleNodeManager};
use opcua::server::ServerBuilder;
use opcua::types::{DataValue, NodeId, ObjectId, QualifiedName, StatusCode, UAString, Variant};
use proto_api::{Addressing, Capabilities, PointValue};
use sim_core::{SimProtocol, SimServeContext};

const DEFAULT_NAMESPACE: &str = "urn:netix:simulator";

/// The server's *application* URI. This MUST differ from the node namespace
/// (below): async-opcua's built-in diagnostics node manager registers the
/// application URI as a namespace and owns it, so if the simulation's nodes
/// shared that namespace index every value read would be routed to diagnostics
/// and answered with `BadNodeIdUnknown` (browse still works via cross-manager
/// reference resolution, which is why the collision is easy to miss).
const APPLICATION_URI: &str = "urn:netix:netix-protocol-tools:simulator";

/// Identifies a simulated point so the updater can fetch its live value.
#[derive(Clone)]
struct PointRef {
    node_id: NodeId,
    device_id: u32,
    object_type: String,
    instance: u32,
}

fn neutral_to_variant(value: PointValue) -> Variant {
    match value {
        PointValue::Float(f) => Variant::from(f),
        PointValue::Bool(b) => Variant::from(b),
        PointValue::UInt(u) => Variant::from(u as u32),
        PointValue::Int(i) => Variant::from(i as i32),
        PointValue::Text(s) => Variant::from(UAString::from(s)),
    }
}

/// Simulator-side OPC UA adapter.
pub struct OpcuaSimProtocol {
    caps: Capabilities,
}

/// Factory used by the registry to construct the adapter from config options.
pub fn sim_factory(_options: &Addressing) -> anyhow::Result<Box<dyn SimProtocol>> {
    Ok(Box::new(OpcuaSimProtocol {
        caps: crate::capabilities(),
    }))
}

#[async_trait::async_trait]
impl SimProtocol for OpcuaSimProtocol {
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn serve(self: Box<Self>, ctx: SimServeContext) -> anyhow::Result<()> {
        let ns_uri = ctx
            .options
            .get("namespace")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_NAMESPACE)
            .to_string();
        if ns_uri == APPLICATION_URI {
            anyhow::bail!(
                "OPC UA `namespace` must differ from the server application URI ({APPLICATION_URI})"
            );
        }

        // `host` is dual-purpose in async-opcua: it is both the TCP bind address
        // and the host advertised in endpoint URLs. The default `0.0.0.0` binds
        // every interface but advertises an unconnectable URL to external clients
        // (e.g. UaExpert), so allow overriding it with a reachable hostname/IP.
        let host = ctx
            .options
            .get("host")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or("0.0.0.0")
            .to_string();

        let (server, handle) = ServerBuilder::new_anonymous("NETIX Simulator")
            .application_uri(APPLICATION_URI)
            .product_uri(APPLICATION_URI)
            .host(host)
            .port(ctx.port)
            .with_node_manager(simple_node_manager(
                NamespaceMetadata {
                    namespace_uri: ns_uri.clone(),
                    ..Default::default()
                },
                "simulator",
            ))
            .build()
            .map_err(|e| anyhow::anyhow!("OPC UA server build failed: {e}"))?;

        let node_manager = handle
            .node_managers()
            .get_of_type::<SimpleNodeManager>()
            .ok_or_else(|| anyhow::anyhow!("simple node manager missing"))?;
        let ns = handle
            .get_namespace_index(&ns_uri)
            .ok_or_else(|| anyhow::anyhow!("namespace '{ns_uri}' not registered"))?;

        // Build one folder per device (organized under the standard Objects
        // folder) with a Variable node per point inside it, and seed the value
        // cache. The folder tree mirrors what a real OPC UA server exposes, so
        // browsing the simulator in UaExpert (or the republisher) shows devices
        // rather than a flat list of variables.
        let cache: Arc<RwLock<HashMap<NodeId, DataValue>>> = Arc::new(RwLock::new(HashMap::new()));
        let mut points: Vec<PointRef> = Vec::new();
        let objects_folder: NodeId = ObjectId::ObjectsFolder.into();
        let mut count = 0usize;
        {
            let sim = ctx.sim.lock().await;
            let mut space = node_manager.address_space().write();
            for device in &sim.devices {
                let folder_id = NodeId::new(ns, format!("device.{}", device.device_id));
                space.add_folder(
                    &folder_id,
                    QualifiedName::new(ns, device.name.as_str()),
                    device.name.as_str(),
                    &objects_folder,
                );

                let mut variables = Vec::with_capacity(device.points.len());
                for point in &device.points {
                    let node_id = NodeId::new(
                        ns,
                        format!(
                            "{}.{}.{}",
                            device.device_id, point.object_type, point.instance
                        ),
                    );
                    let display = point.label.replace('_', " ");
                    let variant = neutral_to_variant(point.neutral_value());
                    variables.push(Variable::new(
                        &node_id,
                        QualifiedName::new(ns, point.label.as_str()),
                        display.as_str(),
                        variant.clone(),
                    ));
                    cache
                        .write()
                        .unwrap()
                        .insert(node_id.clone(), DataValue::new_now(variant));
                    points.push(PointRef {
                        node_id,
                        device_id: device.device_id,
                        object_type: point.object_type.clone(),
                        instance: point.instance,
                    });
                    count += 1;
                }
                space.add_variables(variables, &folder_id);
            }
        }

        // Read callbacks return the cached value (sync), decoupled from the async
        // simulation mutex.
        for point in &points {
            let cache = Arc::clone(&cache);
            let node_id = point.node_id.clone();
            node_manager
                .inner()
                .add_read_callback(point.node_id.clone(), move |_, _, _| {
                    cache
                        .read()
                        .unwrap()
                        .get(&node_id)
                        .cloned()
                        .ok_or(StatusCode::BadNodeIdUnknown)
                });
        }

        ctx.log_line(format!(
            "OPC UA listening on opc.tcp://0.0.0.0:{} (ns={ns_uri}, {count} nodes)",
            ctx.port
        ));

        // Refresh the value cache from the simulation once per second.
        let updater_cache = Arc::clone(&cache);
        let sim = Arc::clone(&ctx.sim);
        let cancel = ctx.cancel.clone();
        let metrics = Arc::clone(&ctx.metrics);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        let mut updates = Vec::with_capacity(points.len());
                        {
                            let sim = sim.lock().await;
                            for p in &points {
                                if let Some(v) = sim.neutral_value(p.device_id, &p.object_type, p.instance) {
                                    updates.push((p.node_id.clone(), DataValue::new_now(neutral_to_variant(v))));
                                }
                            }
                        }
                        let mut guard = updater_cache.write().unwrap();
                        for (node_id, dv) in updates {
                            guard.insert(node_id, dv);
                        }
                    }
                }
            }
            let _ = metrics; // reserved for future per-read metrics
        });

        tokio::select! {
            res = server.run() => res.map_err(|e| anyhow::anyhow!("OPC UA server error: {e}"))?,
            _ = ctx.cancel.cancelled() => handle.cancel(),
        }
        Ok(())
    }
}
