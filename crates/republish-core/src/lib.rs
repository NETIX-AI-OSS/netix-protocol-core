//! Protocol-agnostic core of the generic republisher (MQTT/TLS publishing, config model, worker, and the `gui`-feature iced GUI); protocols plug in via [`RepublishProtocol`]/[`RepublishRegistry`].
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[cfg(feature = "gui")]
pub mod app;
pub mod checksum;
pub mod config;
pub mod defaults;
pub mod import;
pub mod log;
pub mod model;
pub mod mqtt;
pub mod network;
pub mod protocol;
pub mod topic;
#[cfg(feature = "gui")]
pub mod ui;
pub mod worker;

pub use config::{AppConfig, MqttConfig, PayloadFormat, UiPreferences, UiTheme};
pub use model::{
    DiscoverOutcome, DiscoveredDevice, DiscoveredPoint, PointConfig, PointFailure, PointSample,
    PointStatus, PollOutcome, PublishStats, TelemetryValue,
};
pub use protocol::{RepublishFactory, RepublishProtocol, RepublishRegistry};

/// Launch the republisher GUI, building the protocol registry via `build_registry`.
#[cfg(feature = "gui")]
pub fn run(build_registry: fn() -> RepublishRegistry) -> iced::Result {
    app::run(build_registry)
}
