//! Loopback: the OPC UA republish adapter connects to the OPC UA simulator
//! adapter, browses its address space, and reads live values end-to-end.
//!
//! Mirrors the Modbus loopback (a real TCP server/client in-process). Value reads
//! work against the bundled simulator: the server gives the simulation's nodes a
//! namespace distinct from the application URI, so reads route to the simulator's
//! node manager rather than the built-in diagnostics manager.
#![cfg(all(feature = "sim", feature = "republish"))]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use proto_api::Addressing;
use sim_core::config::{
    AssetInstanceSpec, AssetTemplate, BuildingConfig, IdPolicy, SeasonalityConfig, SimulatorConfig,
    TemplatePointSpec, WeeklySchedule,
};
use sim_core::simulation::profiles::ProfileSpec;
use sim_core::{AppMetrics, SimRegistry, SimServeContext, Simulation};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use republish_core::model::PointConfig;
use republish_core::RepublishRegistry;

fn sim_config() -> SimulatorConfig {
    let mut templates = HashMap::new();
    templates.insert(
        "tpl".to_string(),
        AssetTemplate {
            description: String::new(),
            points: vec![TemplatePointSpec {
                label: "temp".into(),
                object_type: "analog_input".into(),
                units: Some("degrees_celsius".into()),
                profile: ProfileSpec::Constant { value: 42.5 },
            }],
        },
    );
    SimulatorConfig {
        building: BuildingConfig {
            name: "T".into(),
            location: None,
            timezone: None,
        },
        seasonality: SeasonalityConfig {
            weekly_schedule: WeeklySchedule {
                weekday_occupancy: vec![],
                weekend_occupancy: vec![],
            },
        },
        id_policy: IdPolicy {
            device_id_base: 1000,
            per_template_block: 100,
        },
        templates,
        instances: vec![AssetInstanceSpec {
            template: "tpl".into(),
            name_prefix: "DEV".into(),
            zone: None,
            count: 1,
        }],
        protocols: vec![],
    }
}

#[tokio::test]
async fn republisher_browses_and_reads_simulator_over_opcua() {
    let port = 14840u16;

    let sim = Arc::new(Mutex::new(Simulation::new(&sim_config()).unwrap()));
    let cancel = CancellationToken::new();
    let mut sim_registry = SimRegistry::new();
    proto_opcua::register_sim(&mut sim_registry);
    let sim_factory = sim_registry.get("opcua").unwrap();
    let adapter = sim_factory(&Addressing::new()).unwrap();
    let ctx = SimServeContext {
        sim: Arc::clone(&sim),
        metrics: Arc::new(AppMetrics::new()),
        log: None,
        port,
        options: Addressing::new(),
        cancel: cancel.clone(),
    };
    let serve = tokio::spawn(async move {
        let _ = adapter.serve(ctx).await;
    });
    // The simulator seeds the value cache once per second; allow the server to
    // bind and the first refresh to land.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let mut conn = Addressing::new();
    conn.insert(
        "endpoint_url".into(),
        serde_json::json!(format!("opc.tcp://127.0.0.1:{port}/")),
    );

    let mut rep_registry = RepublishRegistry::new();
    proto_opcua::register_republish(&mut rep_registry);
    let proto = rep_registry.build("opcua").unwrap();

    let device = proto.discover(&conn).await.unwrap().devices.remove(0);
    let browsed = proto.browse(&conn, &device).await.unwrap();
    // The variable lives in a per-device folder, so the recursive browse should
    // surface it as `temp` with the device folder in its tag path.
    let point = browsed
        .iter()
        .find(|p| p.name.as_deref() == Some("temp") && p.suggested_tag_path.contains("DEV-001"))
        .unwrap_or_else(|| panic!("browse should surface the simulated variable, got {browsed:?}"));

    // Poll the browsed node and confirm the live value round-trips.
    let cfg = PointConfig {
        enabled: true,
        device_key: device.key.clone(),
        addressing: point.addressing.clone(),
        tag_path: point.suggested_tag_path.clone(),
        ..PointConfig::default()
    };
    let outcome = proto.poll(&conn, std::slice::from_ref(&cfg)).await.unwrap();
    assert_eq!(
        outcome.failures.len(),
        0,
        "no read failures expected, got {:?}",
        outcome.failures
    );
    assert_eq!(outcome.samples.len(), 1);
    match &outcome.samples[0].value {
        republish_core::TelemetryValue::Number(n) => {
            assert!((n - 42.5).abs() < 1e-3, "got {n}");
        }
        other => panic!("expected number, got {other:?}"),
    }

    cancel.cancel();
    let _ = serve.await;
}
