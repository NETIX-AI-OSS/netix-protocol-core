//! Verifies the OPC UA simulator exposes a secured Basic256Sha256 endpoint.
#![cfg(all(feature = "sim", feature = "republish"))]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use proto_api::Addressing;
use republish_core::RepublishRegistry;
use sim_core::config::{
    AssetInstanceSpec, AssetTemplate, BuildingConfig, IdPolicy, SeasonalityConfig, SimulatorConfig,
    TemplatePointSpec, WeeklySchedule,
};
use sim_core::simulation::profiles::ProfileSpec;
use sim_core::{AppMetrics, SimRegistry, SimServeContext, Simulation};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

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
async fn secured_simulator_browses_over_basic256sha256_endpoint() {
    let port = 14841u16;
    let server_pki = tempfile::tempdir().unwrap();
    let client_pki = tempfile::tempdir().unwrap();

    // Secure endpoints require the server to trust the client's application certificate.
    // Provision that trust before the simulator opens its certificate store.
    let _client = opcua::client::ClientBuilder::new()
        .application_name("NETIX Republisher")
        .application_uri("urn:netix:republisher")
        .product_uri("urn:netix:republisher")
        .pki_dir(client_pki.path())
        .create_sample_keypair(true)
        .client()
        .unwrap();
    let client_cert_path = client_pki.path().join("own/cert.der");
    let client_cert = opcua::crypto::CertificateStore::read_cert(&client_cert_path).unwrap();
    let server_trusted = server_pki.path().join("trusted");
    std::fs::create_dir_all(&server_trusted).unwrap();
    std::fs::copy(
        &client_cert_path,
        server_trusted.join(opcua::crypto::CertificateStore::cert_file_name(
            &client_cert,
        )),
    )
    .unwrap();

    let sim = Arc::new(Mutex::new(Simulation::new(&sim_config()).unwrap()));
    let cancel = CancellationToken::new();
    let mut sim_registry = SimRegistry::new();
    proto_opcua::register_sim(&mut sim_registry);
    let mut options = Addressing::new();
    options.insert("secured".into(), serde_json::json!(true));
    options.insert("host".into(), serde_json::json!("127.0.0.1"));
    options.insert(
        "pki_dir".into(),
        serde_json::json!(server_pki.path().to_string_lossy()),
    );
    let adapter = sim_registry.get("opcua").unwrap()(&options).unwrap();
    let ctx = SimServeContext {
        sim: Arc::clone(&sim),
        metrics: Arc::new(AppMetrics::new()),
        log: None,
        port,
        options,
        cancel: cancel.clone(),
    };
    let serve = tokio::spawn(async move {
        let _ = adapter.serve(ctx).await;
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let client = opcua::client::ClientBuilder::new()
        .application_name("NETIX Republisher")
        .application_uri("urn:netix:republisher")
        .product_uri("urn:netix:republisher")
        .create_sample_keypair(false)
        .trust_server_certs(true)
        .session_retry_limit(1)
        .client()
        .unwrap();
    let url = format!("opc.tcp://127.0.0.1:{port}/");
    let endpoints = tokio::time::timeout(
        Duration::from_secs(10),
        client.get_server_endpoints_from_url(url.as_str()),
    )
    .await
    .expect("GetEndpoints timed out")
    .expect("GetEndpoints failed");
    assert!(
        endpoints.iter().any(|endpoint| {
            endpoint
                .security_policy_uri
                .as_ref()
                .contains("Basic256Sha256")
        }),
        "expected Basic256Sha256 endpoint, got {endpoints:?}"
    );

    let mut conn = Addressing::new();
    conn.insert("endpoint_url".into(), serde_json::json!(url));
    conn.insert(
        "security_policy".into(),
        serde_json::json!("basic256sha256"),
    );
    conn.insert("security_mode".into(), serde_json::json!("sign_encrypt"));
    conn.insert(
        "pki_dir".into(),
        serde_json::json!(client_pki.path().to_string_lossy()),
    );
    let mut rep_registry = RepublishRegistry::new();
    proto_opcua::register_republish(&mut rep_registry);
    let proto = rep_registry.build("opcua").unwrap();
    let device = proto.discover(&conn).await.unwrap().devices.remove(0);
    let browsed = tokio::time::timeout(Duration::from_secs(20), proto.browse(&conn, &device))
        .await
        .expect("secure Browse timed out")
        .expect("secure Browse failed");
    assert!(browsed
        .points
        .iter()
        .any(|point| point.name.as_deref() == Some("temp")));

    cancel.cancel();
    let _ = serve.await;
}
