//! Loopback: the Modbus republish adapter polls the Modbus simulator adapter and decodes live values end-to-end.
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
async fn republisher_polls_simulator_over_modbus() {
    let port = 15510u16;

    // Start the Modbus simulator adapter.
    let sim = Arc::new(Mutex::new(Simulation::new(&sim_config()).unwrap()));
    let cancel = CancellationToken::new();
    let mut sim_registry = SimRegistry::new();
    proto_modbus::register_sim(&mut sim_registry);
    let sim_factory = sim_registry.get("modbus").unwrap();
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
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Build the republish adapter and a point addressing the analog as f32.
    let mut rep_registry = RepublishRegistry::new();
    proto_modbus::register_republish(&mut rep_registry);
    let proto = rep_registry.build("modbus").unwrap();

    let mut conn = Addressing::new();
    conn.insert("host".into(), serde_json::json!("127.0.0.1"));
    conn.insert("port".into(), serde_json::json!(port));
    conn.insert("unit_id".into(), serde_json::json!(1));

    // Analog point => input registers 0..2 as big-endian f32 (per the sim layout).
    let mut point = PointConfig {
        device_key: "sim".into(),
        ..PointConfig::default()
    };
    point
        .addressing
        .insert("table".into(), serde_json::json!("input"));
    point
        .addressing
        .insert("address".into(), serde_json::json!(0));
    point
        .addressing
        .insert("datatype".into(), serde_json::json!("f32"));
    point
        .addressing
        .insert("word_order".into(), serde_json::json!("big"));

    let outcome = proto
        .poll(&conn, std::slice::from_ref(&point))
        .await
        .unwrap();
    assert_eq!(outcome.failures.len(), 0, "no read failures expected");
    assert_eq!(outcome.samples.len(), 1);
    match &outcome.samples[0].value {
        republish_core::TelemetryValue::Number(n) => {
            assert!((n - 42.5).abs() < 1e-3, "got {n}");
        }
        other => panic!("expected number, got {other:?}"),
    }

    // Browse should surface registers too.
    let device = proto.discover(&conn).await.unwrap().devices.remove(0);
    let browsed = proto.browse(&conn, &device).await.unwrap();
    assert!(!browsed.points.is_empty(), "browse should find registers");

    cancel.cancel();
    let _ = serve.await;
}

/// Serves the simulator on `port` with these protocol options; returns the cancel token and the serve task.
async fn serve_sim(
    port: u16,
    options: Addressing,
) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let sim = Arc::new(Mutex::new(Simulation::new(&sim_config()).unwrap()));
    let cancel = CancellationToken::new();
    let mut sim_registry = SimRegistry::new();
    proto_modbus::register_sim(&mut sim_registry);
    let adapter = sim_registry.get("modbus").unwrap()(&Addressing::new()).unwrap();
    let ctx = SimServeContext {
        sim,
        metrics: Arc::new(AppMetrics::new()),
        log: None,
        port,
        options,
        cancel: cancel.clone(),
    };
    let serve = tokio::spawn(async move {
        let _ = adapter.serve(ctx).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    (cancel, serve)
}

fn loopback_conn(port: u16) -> Addressing {
    let mut conn = Addressing::new();
    conn.insert("host".into(), serde_json::json!("127.0.0.1"));
    conn.insert("port".into(), serde_json::json!(port));
    conn.insert("unit_id".into(), serde_json::json!(1));
    conn
}

#[tokio::test]
async fn discover_reports_the_meter_s_read_device_identification() {
    let port = 15511u16;
    let mut options = Addressing::new();
    options.insert(
        "vendor_name".into(),
        serde_json::json!("Schneider Electric"),
    );
    options.insert("product_code".into(), serde_json::json!("METSEPM5320"));
    options.insert("model_name".into(), serde_json::json!("PM5320"));
    let (cancel, serve) = serve_sim(port, options).await;

    let mut registry = RepublishRegistry::new();
    proto_modbus::register_republish(&mut registry);
    let proto = registry.build("modbus").unwrap();
    let device = proto
        .discover(&loopback_conn(port))
        .await
        .unwrap()
        .devices
        .remove(0);

    assert_eq!(device.vendor_name.as_deref(), Some("Schneider Electric"));
    // ModelName wins over the ProductCode when the meter serves the regular category.
    assert_eq!(device.model_name.as_deref(), Some("PM5320"));
    assert_eq!(device.vendor_id, None);
    let wire = serde_json::to_value(&device).unwrap();
    assert_eq!(wire["vendor_name"], "Schneider Electric");
    assert_eq!(wire["model_name"], "PM5320");
    assert!(
        wire.get("vendor_id").is_none(),
        "absent fields stay off the wire"
    );

    cancel.cancel();
    let _ = serve.await;
}

#[tokio::test]
async fn discover_still_finds_a_meter_that_does_not_identify_itself() {
    let port = 15512u16;
    let (cancel, serve) = serve_sim(port, Addressing::new()).await;

    let mut registry = RepublishRegistry::new();
    proto_modbus::register_republish(&mut registry);
    let proto = registry.build("modbus").unwrap();
    let device = proto
        .discover(&loopback_conn(port))
        .await
        .unwrap()
        .devices
        .remove(0);

    assert_eq!(device.key, format!("modbus-127-0-0-1-{port}"));
    assert_eq!(device.vendor_name, None);
    assert_eq!(device.model_name, None);

    cancel.cancel();
    let _ = serve.await;
}
