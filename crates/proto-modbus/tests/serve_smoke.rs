//! End-to-end test of the Modbus simulator adapter: boot `serve()` on a local
//! port and read it back with the tokio-modbus client.
#![cfg(feature = "sim")]

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
use tokio_modbus::prelude::Reader;
use tokio_util::sync::CancellationToken;

fn test_config() -> SimulatorConfig {
    let mut templates = HashMap::new();
    templates.insert(
        "tpl".to_string(),
        AssetTemplate {
            description: String::new(),
            points: vec![
                TemplatePointSpec {
                    label: "temp".into(),
                    object_type: "analog_input".into(),
                    units: Some("degrees_celsius".into()),
                    profile: ProfileSpec::Constant { value: 42.5 },
                },
                TemplatePointSpec {
                    label: "occupied".into(),
                    object_type: "binary_value".into(),
                    units: None,
                    profile: ProfileSpec::ConstantBool { value: true },
                },
            ],
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
async fn serves_modbus_registers_and_coils_from_simulation() {
    let sim = Arc::new(Mutex::new(Simulation::new(&test_config()).unwrap()));
    let metrics = Arc::new(AppMetrics::new());
    let cancel = CancellationToken::new();

    let mut registry = SimRegistry::new();
    proto_modbus::register_sim(&mut registry);
    let factory = registry.get("modbus").expect("modbus registered");
    let adapter = factory(&Addressing::new()).expect("build adapter");

    // A high, unprivileged port unlikely to collide.
    let port = 15502u16;
    let ctx = SimServeContext {
        sim: Arc::clone(&sim),
        metrics: Arc::clone(&metrics),
        log: None,
        port,
        options: Addressing::new(),
        cancel: cancel.clone(),
    };
    let serve = tokio::spawn(async move {
        let _ = adapter.serve(ctx).await;
    });

    // Give the listener a moment to bind.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let socket_addr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut client = tokio_modbus::client::tcp::connect(socket_addr)
        .await
        .expect("connect");

    // Analog point => f32 across two big-endian input registers.
    let regs = client
        .read_input_registers(0, 2)
        .await
        .expect("io")
        .expect("modbus");
    let bits = ((regs[0] as u32) << 16) | regs[1] as u32;
    assert_eq!(f32::from_bits(bits), 42.5);

    // Holding registers mirror input registers.
    let holding = client
        .read_holding_registers(0, 2)
        .await
        .expect("io")
        .expect("modbus");
    assert_eq!(holding, regs);

    // Binary point => single discrete input bit (and mirrored coil).
    let discrete = client
        .read_discrete_inputs(0, 1)
        .await
        .expect("io")
        .expect("modbus");
    assert_eq!(discrete, vec![true]);

    // Reading past the mapped range is an IllegalDataAddress exception (inner Err).
    let oob = client.read_input_registers(0, 9999).await.expect("io");
    assert!(
        oob.is_err(),
        "out-of-range read should be a modbus exception"
    );

    assert!(metrics.request_count() >= 3);

    cancel.cancel();
    let _ = serve.await;
}
