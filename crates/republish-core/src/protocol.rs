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
    DiscoverOutcome, DiscoveredDevice, DiscoveredPoint, PointConfig, PollOutcome, RefreshOutcome,
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
    ) -> anyhow::Result<Vec<DiscoveredPoint>>;

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
