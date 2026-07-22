//! Protocol-agnostic core of the generic republisher: MQTT/TLS publishing, the
//! configuration model, the background worker, and (behind the `gui` feature)
//! the capability-driven iced GUI. Concrete protocols plug in through the
//! [`RepublishProtocol`] trait and are resolved at runtime via a
//! [`RepublishRegistry`].
//!
//! Coverage note: under `cargo +nightly llvm-cov` (which sets `cfg(coverage_nightly)`)
//! the `coverage_attribute` feature is enabled so that `#[cfg_attr(coverage_nightly,
//! coverage(off))]` can exclude test modules and genuinely-unreachable defensive
//! branches (OS-fault error arms) from the production-coverage figure. The attribute
//! is inert on stable, so normal builds and CI are unaffected.
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
