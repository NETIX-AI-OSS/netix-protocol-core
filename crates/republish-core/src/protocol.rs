//! The republisher-side protocol extension point.
//!
//! A protocol adapter implements [`RepublishProtocol`] to discover devices,
//! browse their points, and poll values. The core's worker drives these methods
//! and publishes the results to MQTT. Adapters are resolved by id from a
//! [`RepublishRegistry`] the binary populates with whichever protocols are
//! compiled in.

use std::collections::HashMap;

use proto_api::{Addressing, Capabilities};

use crate::model::{
    BrowseOutcome, DiscoverOutcome, DiscoveredDevice, PointConfig, PollOutcome, RefreshOutcome,
};

#[async_trait::async_trait]
pub trait RepublishProtocol: Send + Sync {
    /// Declarative capabilities (discovery/browse style + connection/addressing
    /// fields) the UI renders without protocol knowledge.
    fn capabilities(&self) -> &Capabilities;

    /// Find devices/servers reachable with the given connection settings.
    async fn discover(&self, conn: &Addressing) -> anyhow::Result<DiscoverOutcome>;

    /// Enumerate a device's points (object list / address space / register scan).
    async fn browse(
        &self,
        conn: &Addressing,
        device: &DiscoveredDevice,
    ) -> anyhow::Result<BrowseOutcome>;

    /// Read current values for the configured points.
    async fn poll(&self, conn: &Addressing, points: &[PointConfig]) -> anyhow::Result<PollOutcome>;

    /// Re-resolve device addresses (Who-Is / I-Am). Default: no-op, all resolved.
    async fn refresh_devices(
        &self,
        conn: &Addressing,
        device_instances: &[u32],
    ) -> anyhow::Result<RefreshOutcome> {
        let _ = (conn, device_instances);
        Ok(RefreshOutcome::default())
    }
}

/// Constructs a protocol adapter instance.
pub type RepublishFactory = fn() -> Box<dyn RepublishProtocol>;

/// Maps protocol id → adapter factory.
#[derive(Default)]
pub struct RepublishRegistry {
    factories: HashMap<String, RepublishFactory>,
}

impl RepublishRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, id: &str, factory: RepublishFactory) {
        self.factories.insert(id.to_string(), factory);
    }

    pub fn get(&self, id: &str) -> Option<RepublishFactory> {
        self.factories.get(id).copied()
    }

    pub fn build(&self, id: &str) -> Option<Box<dyn RepublishProtocol>> {
        self.get(id).map(|factory| factory())
    }

    /// Sorted list of registered protocol ids.
    pub fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.factories.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Capabilities for every registered protocol, sorted by id.
    pub fn capabilities(&self) -> Vec<Capabilities> {
        self.ids()
            .into_iter()
            .filter_map(|id| self.build(&id).map(|p| p.capabilities().clone()))
            .collect()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use proto_api::{BrowseKind, DiscoveryKind};

    /// Minimal in-test adapter. Its `capabilities().id` doubles as an identity
    /// marker so tests can assert which factory produced a given instance.
    struct FakeProtocol {
        caps: Capabilities,
    }

    impl FakeProtocol {
        fn caps(id: &'static str) -> Capabilities {
            Capabilities {
                id,
                display_name: id,
                discovery: DiscoveryKind::ManualOnly,
                browse: BrowseKind::None,
                connection_fields: vec![],
                addressing_fields: vec![],
                default_port: 0,
            }
        }
    }

    #[async_trait::async_trait]
    impl RepublishProtocol for FakeProtocol {
        fn capabilities(&self) -> &Capabilities {
            &self.caps
        }

        async fn discover(&self, _conn: &Addressing) -> anyhow::Result<DiscoverOutcome> {
            Ok(DiscoverOutcome::default())
        }

        async fn browse(
            &self,
            _conn: &Addressing,
            _device: &DiscoveredDevice,
        ) -> anyhow::Result<BrowseOutcome> {
            Ok(BrowseOutcome::default())
        }

        async fn poll(
            &self,
            _conn: &Addressing,
            _points: &[PointConfig],
        ) -> anyhow::Result<PollOutcome> {
            Ok(PollOutcome::default())
        }
    }

    fn make_alpha() -> Box<dyn RepublishProtocol> {
        Box::new(FakeProtocol {
            caps: FakeProtocol::caps("alpha"),
        })
    }

    fn make_zeta() -> Box<dyn RepublishProtocol> {
        Box::new(FakeProtocol {
            caps: FakeProtocol::caps("zeta"),
        })
    }

    #[test]
    fn new_registry_is_empty() {
        let reg = RepublishRegistry::new();
        assert!(reg.ids().is_empty());
        assert!(reg.capabilities().is_empty());
        assert!(reg.get("alpha").is_none());
        assert!(reg.build("alpha").is_none());
    }

    #[test]
    fn get_returns_factory_on_hit_and_none_on_miss() {
        let mut reg = RepublishRegistry::new();
        reg.register("alpha", make_alpha);

        let factory = reg.get("alpha").expect("registered factory should resolve");
        assert_eq!(factory().capabilities().id, "alpha");

        assert!(reg.get("nope").is_none());
    }

    #[test]
    fn build_constructs_instance_on_hit_and_none_on_miss() {
        let mut reg = RepublishRegistry::new();
        reg.register("alpha", make_alpha);

        let built = reg.build("alpha").expect("hit should build an adapter");
        assert_eq!(built.capabilities().id, "alpha");

        assert!(reg.build("unknown").is_none());
    }

    #[test]
    fn register_overwrites_existing_id() {
        let mut reg = RepublishRegistry::new();
        reg.register("alpha", make_alpha);
        // Re-register the same id with a factory that yields a different identity.
        reg.register("alpha", make_zeta);

        assert_eq!(reg.ids(), vec!["alpha".to_string()]);
        assert_eq!(reg.build("alpha").unwrap().capabilities().id, "zeta");
    }

    #[test]
    fn ids_are_sorted_regardless_of_insertion_order() {
        let mut reg = RepublishRegistry::new();
        reg.register("zeta", make_zeta);
        reg.register("alpha", make_alpha);

        assert_eq!(reg.ids(), vec!["alpha".to_string(), "zeta".to_string()]);
    }

    #[test]
    fn capabilities_are_sorted_by_id_and_cover_every_registration() {
        let mut reg = RepublishRegistry::new();
        reg.register("zeta", make_zeta);
        reg.register("alpha", make_alpha);

        let caps = reg.capabilities();
        let ids: Vec<&str> = caps.iter().map(|c| c.id).collect();
        assert_eq!(ids, vec!["alpha", "zeta"]);
    }

    #[tokio::test]
    async fn adapter_async_methods_dispatch_through_trait_object() {
        // Dynamic dispatch exercises the trait's signatures and stub bodies.
        let mut reg = RepublishRegistry::new();
        reg.register("alpha", make_alpha);
        let proto = reg.build("alpha").expect("adapter should build");

        let conn = Addressing::new();
        let device = DiscoveredDevice {
            key: "d".into(),
            instance: Some(1),
            address: "127.0.0.1".into(),
            detail: String::new(),
        };
        let point = PointConfig::default();

        assert_eq!(
            proto.discover(&conn).await.unwrap(),
            DiscoverOutcome::default()
        );
        assert_eq!(
            proto.browse(&conn, &device).await.unwrap(),
            BrowseOutcome::default()
        );
        assert_eq!(
            proto
                .poll(&conn, std::slice::from_ref(&point))
                .await
                .unwrap(),
            PollOutcome::default()
        );
    }

    #[tokio::test]
    async fn refresh_devices_default_is_noop_all_resolved() {
        let proto = FakeProtocol {
            caps: FakeProtocol::caps("alpha"),
        };
        let outcome = proto
            .refresh_devices(&Addressing::new(), &[1, 2, 3])
            .await
            .expect("default refresh should succeed");
        assert_eq!(outcome, RefreshOutcome::default());
    }
}
