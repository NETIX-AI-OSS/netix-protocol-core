//! Protocol-agnostic core of the generic republisher: MQTT/TLS publishing, the
//! configuration model, the background worker, and the capability-driven iced
//! GUI. Concrete protocols plug in through the [`RepublishProtocol`] trait and
//! are resolved at runtime via a [`RepublishRegistry`].

pub mod app;
pub mod config;
pub mod import;
pub mod log;
pub mod model;
pub mod mqtt;
pub mod network;
pub mod protocol;
pub mod topic;
pub mod ui;
pub mod worker;

pub use config::{AppConfig, MqttConfig, UiPreferences, UiTheme};
pub use model::{
    DiscoverOutcome, DiscoveredDevice, DiscoveredPoint, PointConfig, PointFailure, PointSample,
    PointStatus, PollOutcome, PublishStats, TelemetryValue,
};
pub use protocol::{RepublishFactory, RepublishProtocol, RepublishRegistry};

/// Launch the republisher GUI, building the protocol registry via `build_registry`.
pub fn run(build_registry: fn() -> RepublishRegistry) -> iced::Result {
    app::run(build_registry)
}
