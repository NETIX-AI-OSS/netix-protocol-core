//! Loopback: the BACnet republish adapter discovers, browses, and polls the
//! BACnet simulator adapter and decodes live values end-to-end.
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

/// Device instance assigned by [`SimulatorConfig`] with `device_id_base: 1000`,
/// one template block, and a single instance (`1100 + 0`).
const DEVICE_INSTANCE: u32 = 1100;

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
async fn republisher_discovers_browses_and_polls_simulator_over_bacnet() {
    // Port 0 binds the simulator on the protocol default (UDP/47808), matching
    // sim-core's protocol listener behavior. The republish client uses an
    // ephemeral local bind (connection port 0).
    let sim_port = 0u16;

    let sim = Arc::new(Mutex::new(Simulation::new(&sim_config()).unwrap()));
    let cancel = CancellationToken::new();
    let mut sim_registry = SimRegistry::new();
    proto_bacnet::register_sim(&mut sim_registry);
    let sim_factory = sim_registry.get("bacnet").unwrap();
    let adapter = sim_factory(&Addressing::new()).unwrap();
    let ctx = SimServeContext {
        sim: Arc::clone(&sim),
        metrics: Arc::new(AppMetrics::new()),
        log: None,
        port: sim_port,
        options: Addressing::new(),
        cancel: cancel.clone(),
    };
    let serve = tokio::spawn(async move {
        let _ = adapter.serve(ctx).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut conn = Addressing::new();
    conn.insert("interface".into(), serde_json::json!("127.0.0.1"));
    conn.insert("port".into(), serde_json::json!(0));
    conn.insert("broadcast_address".into(), serde_json::json!("127.0.0.1"));
    conn.insert("discovery_window_ms".into(), serde_json::json!(300));

    let mut rep_registry = RepublishRegistry::new();
    proto_bacnet::register_republish(&mut rep_registry);
    let proto = rep_registry.build("bacnet").unwrap();

    let discover = proto.discover(&conn).await.unwrap();
    assert_eq!(
        discover.warnings.len(),
        0,
        "unexpected discovery warnings: {:?}",
        discover.warnings
    );
    assert_eq!(discover.devices.len(), 1, "expected one simulated device");
    let device = discover.devices.into_iter().next().unwrap();
    assert_eq!(device.key, format!("device_{DEVICE_INSTANCE}"));

    let browsed = proto.browse(&conn, &device).await.unwrap();
    assert!(
        browsed.warnings.is_empty(),
        "unexpected browse warnings: {:?}",
        browsed.warnings
    );
    assert!(!browsed.points.is_empty(), "browse should find BACnet objects");
    let point = browsed
        .points
        .iter()
        .find(|p| {
            p.addressing
                .get("object_type")
                .and_then(|v| v.as_str())
                == Some("analog_input")
        })
        .unwrap_or_else(|| panic!("browse should surface analog_input, got {browsed:?}"));

    let cfg = PointConfig {
        enabled: true,
        device_key: device.key.clone(),
        addressing: point.addressing.clone(),
        tag_path: point.suggested_tag_path.clone(),
        ..PointConfig::default()
    };
    let refresh = proto
        .refresh_devices(&conn, std::slice::from_ref(&DEVICE_INSTANCE))
        .await
        .unwrap();
    assert!(
        refresh.unresolved.is_empty(),
        "device should resolve before poll: {:?}",
        refresh.unresolved
    );
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
