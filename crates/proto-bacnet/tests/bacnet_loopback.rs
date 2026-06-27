//! In-process BACnet/IP loopback tests for the republish adapter.
//!
//! Ports the four integration tests from the original `bacnet-republisher`
//! crate: I-Am discovery, object scan, RPM poll, and RPM-timeout fallback.

use bacnet_client::client::BACnetClient;
use bacnet_encoding::apdu::{
    self, encode_apdu, Apdu, ComplexAck, UnconfirmedRequest as UnconfirmedRequestPdu,
};
use bacnet_network::layer::{NetworkLayer, ReceivedApdu};
use bacnet_services::read_property::{ReadPropertyACK, ReadPropertyRequest};
use bacnet_services::rpm::{
    ReadAccessResult, ReadPropertyMultipleACK, ReadPropertyMultipleRequest, ReadResultElement,
};
use bacnet_services::who_is::IAmRequest;
use bacnet_transport::bip::BipTransport;
use bacnet_types::enums::{
    ConfirmedServiceChoice, NetworkPriority, ObjectType, PropertyIdentifier, Segmentation,
    UnconfirmedServiceChoice,
};
use bacnet_types::primitives::ObjectIdentifier;
use bytes::{Bytes, BytesMut};
use proto_api::Addressing;
use proto_bacnet::test_support::{
    collect_discovered_devices, poll_points_once_with_client, scan_device_objects_with_client,
};
use republish_core::model::{PointConfig, TelemetryValue};
use std::net::Ipv4Addr;
use tokio::time::{sleep, timeout, Duration};

const DEVICE_INSTANCE: u32 = 5678;

#[tokio::test]
async fn collects_discovered_device_from_iam() {
    let (mut client, mut server_net, _server_rx) = start_pair(500).await;

    send_i_am(&mut server_net, client.local_mac(), DEVICE_INSTANCE).await;
    sleep(Duration::from_millis(100)).await;

    let devices = collect_discovered_devices(&client).await;

    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].instance, DEVICE_INSTANCE);
    assert_eq!(devices[0].vendor_id, 42);

    server_net.stop().await.unwrap();
    client.stop().await.unwrap();
}

#[tokio::test]
async fn scans_object_metadata_best_effort() {
    let (mut client, mut server_net, mut server_rx) = start_pair(500).await;
    send_i_am(&mut server_net, client.local_mac(), DEVICE_INSTANCE).await;
    sleep(Duration::from_millis(100)).await;

    let server = tokio::spawn(async move {
        for _ in 0..6 {
            let received = timeout(Duration::from_secs(2), server_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let Apdu::ConfirmedRequest(request) = apdu::decode_apdu(received.apdu.clone()).unwrap()
            else {
                panic!("expected confirmed request");
            };
            assert_eq!(
                request.service_choice,
                ConfirmedServiceChoice::READ_PROPERTY
            );
            let rp = ReadPropertyRequest::decode(&request.service_request).unwrap();
            let value = read_property_value(&rp);
            send_read_property_ack(
                &mut server_net,
                &received.source_mac,
                request.invoke_id,
                rp,
                value,
            )
            .await;
        }
        server_net.stop().await.unwrap();
    });

    let objects = scan_device_objects_with_client(&client, DEVICE_INSTANCE, 8)
        .await
        .unwrap();

    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].object_type, "analog_input");
    assert_eq!(objects[0].object_instance, 1);
    assert_eq!(objects[0].object_name.as_deref(), Some("Supply Temp"));
    assert_eq!(
        objects[0].description.as_deref(),
        Some("AHU supply temperature")
    );
    assert_eq!(objects[0].units.as_deref(), Some("62"));
    assert_eq!(objects[0].present_value, Some(TelemetryValue::Number(72.0)));

    server.await.unwrap();
    client.stop().await.unwrap();
}

#[tokio::test]
async fn polls_present_value_with_rpm() {
    let (mut client, mut server_net, mut server_rx) = start_pair(500).await;
    send_i_am(&mut server_net, client.local_mac(), DEVICE_INSTANCE).await;
    sleep(Duration::from_millis(100)).await;

    let server = tokio::spawn(async move {
        let received = timeout(Duration::from_secs(2), server_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let Apdu::ConfirmedRequest(request) = apdu::decode_apdu(received.apdu.clone()).unwrap()
        else {
            panic!("expected confirmed request");
        };
        assert_eq!(
            request.service_choice,
            ConfirmedServiceChoice::READ_PROPERTY_MULTIPLE
        );
        let rpm = ReadPropertyMultipleRequest::decode(&request.service_request).unwrap();
        assert_eq!(rpm.list_of_read_access_specs.len(), 1);
        send_rpm_ack(&mut server_net, &received.source_mac, request.invoke_id).await;
        server_net.stop().await.unwrap();
    });

    let outcome = poll_points_once_with_client(&client, &[sample_point()], 4, None)
        .await
        .unwrap();

    assert_eq!(outcome.failures.len(), 0);
    assert_eq!(outcome.warnings.len(), 0);
    assert_eq!(outcome.samples.len(), 1);
    assert_eq!(outcome.samples[0].value, TelemetryValue::Number(72.0));

    server.await.unwrap();
    client.stop().await.unwrap();
}

#[tokio::test]
async fn falls_back_to_single_read_after_rpm_timeout() {
    let (mut client, mut server_net, mut server_rx) = start_pair(150).await;
    send_i_am(&mut server_net, client.local_mac(), DEVICE_INSTANCE).await;
    sleep(Duration::from_millis(100)).await;

    let server = tokio::spawn(async move {
        let rpm = timeout(Duration::from_secs(2), server_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let Apdu::ConfirmedRequest(request) = apdu::decode_apdu(rpm.apdu.clone()).unwrap() else {
            panic!("expected confirmed request");
        };
        assert_eq!(
            request.service_choice,
            ConfirmedServiceChoice::READ_PROPERTY_MULTIPLE
        );

        loop {
            let read = timeout(Duration::from_secs(2), server_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let Apdu::ConfirmedRequest(request) = apdu::decode_apdu(read.apdu.clone()).unwrap()
            else {
                panic!("expected confirmed request");
            };
            if request.service_choice == ConfirmedServiceChoice::READ_PROPERTY_MULTIPLE {
                continue;
            }
            assert_eq!(
                request.service_choice,
                ConfirmedServiceChoice::READ_PROPERTY
            );
            let rp = ReadPropertyRequest::decode(&request.service_request).unwrap();
            send_read_property_ack(
                &mut server_net,
                &read.source_mac,
                request.invoke_id,
                rp,
                real(72.0),
            )
            .await;
            break;
        }
        server_net.stop().await.unwrap();
    });

    let outcome = poll_points_once_with_client(&client, &[sample_point()], 4, None)
        .await
        .unwrap();

    assert_eq!(outcome.failures.len(), 0);
    assert_eq!(outcome.warnings.len(), 1);
    assert_eq!(outcome.samples.len(), 1);
    assert_eq!(outcome.samples[0].value, TelemetryValue::Number(72.0));

    server.await.unwrap();
    client.stop().await.unwrap();
}

async fn start_pair(
    apdu_timeout_ms: u64,
) -> (
    BACnetClient<BipTransport>,
    NetworkLayer<BipTransport>,
    tokio::sync::mpsc::Receiver<ReceivedApdu>,
) {
    let client = BACnetClient::bip_builder()
        .interface(Ipv4Addr::LOCALHOST)
        .port(0)
        .apdu_timeout_ms(apdu_timeout_ms)
        .build()
        .await
        .unwrap();

    let transport = BipTransport::new(Ipv4Addr::LOCALHOST, 0, Ipv4Addr::BROADCAST);
    let mut server_net = NetworkLayer::new(transport);
    let server_rx = server_net.start().await.unwrap();

    (client, server_net, server_rx)
}

async fn send_i_am(
    server_net: &mut NetworkLayer<BipTransport>,
    destination_mac: &[u8],
    device_instance: u32,
) {
    let i_am = IAmRequest {
        object_identifier: ObjectIdentifier::new(ObjectType::DEVICE, device_instance).unwrap(),
        max_apdu_length: 1476,
        segmentation_supported: Segmentation::NONE,
        vendor_id: 42,
    };
    let mut service_buf = BytesMut::new();
    i_am.encode(&mut service_buf);

    let pdu = Apdu::UnconfirmedRequest(UnconfirmedRequestPdu {
        service_choice: UnconfirmedServiceChoice::I_AM,
        service_request: Bytes::from(service_buf.to_vec()),
    });
    let mut buf = BytesMut::new();
    encode_apdu(&mut buf, &pdu).unwrap();

    server_net
        .send_apdu(&buf, destination_mac, false, NetworkPriority::NORMAL)
        .await
        .unwrap();
}

async fn send_read_property_ack(
    server_net: &mut NetworkLayer<BipTransport>,
    destination_mac: &[u8],
    invoke_id: u8,
    request: ReadPropertyRequest,
    value: Vec<u8>,
) {
    let ack = ReadPropertyACK {
        object_identifier: request.object_identifier,
        property_identifier: request.property_identifier,
        property_array_index: request.property_array_index,
        property_value: value,
    };
    let mut ack_buf = BytesMut::new();
    ack.encode(&mut ack_buf);
    send_complex_ack(
        server_net,
        destination_mac,
        invoke_id,
        ConfirmedServiceChoice::READ_PROPERTY,
        ack_buf,
    )
    .await;
}

async fn send_rpm_ack(
    server_net: &mut NetworkLayer<BipTransport>,
    destination_mac: &[u8],
    invoke_id: u8,
) {
    let ack = ReadPropertyMultipleACK {
        list_of_read_access_results: vec![ReadAccessResult {
            object_identifier: ObjectIdentifier::new(ObjectType::ANALOG_INPUT, 1).unwrap(),
            list_of_results: vec![ReadResultElement {
                property_identifier: PropertyIdentifier::PRESENT_VALUE,
                property_array_index: None,
                property_value: Some(real(72.0)),
                error: None,
            }],
        }],
    };
    let mut ack_buf = BytesMut::new();
    ack.encode(&mut ack_buf);
    send_complex_ack(
        server_net,
        destination_mac,
        invoke_id,
        ConfirmedServiceChoice::READ_PROPERTY_MULTIPLE,
        ack_buf,
    )
    .await;
}

async fn send_complex_ack(
    server_net: &mut NetworkLayer<BipTransport>,
    destination_mac: &[u8],
    invoke_id: u8,
    service_choice: ConfirmedServiceChoice,
    service_ack: BytesMut,
) {
    let pdu = Apdu::ComplexAck(ComplexAck {
        segmented: false,
        more_follows: false,
        invoke_id,
        sequence_number: None,
        proposed_window_size: None,
        service_choice,
        service_ack: Bytes::from(service_ack.to_vec()),
    });
    let mut buf = BytesMut::new();
    encode_apdu(&mut buf, &pdu).unwrap();
    server_net
        .send_apdu(&buf, destination_mac, false, NetworkPriority::NORMAL)
        .await
        .unwrap();
}

fn read_property_value(request: &ReadPropertyRequest) -> Vec<u8> {
    match (
        request.object_identifier.object_type(),
        request.property_identifier,
        request.property_array_index,
    ) {
        (ObjectType::DEVICE, PropertyIdentifier::OBJECT_LIST, Some(0)) => unsigned(1),
        (ObjectType::DEVICE, PropertyIdentifier::OBJECT_LIST, Some(1)) => {
            object_id(ObjectType::ANALOG_INPUT, 1)
        }
        (ObjectType::ANALOG_INPUT, PropertyIdentifier::OBJECT_NAME, None) => {
            character_string("Supply Temp")
        }
        (ObjectType::ANALOG_INPUT, PropertyIdentifier::DESCRIPTION, None) => {
            character_string("AHU supply temperature")
        }
        (ObjectType::ANALOG_INPUT, PropertyIdentifier::UNITS, None) => enumerated(62),
        (ObjectType::ANALOG_INPUT, PropertyIdentifier::PRESENT_VALUE, None) => real(72.0),
        other => panic!("unexpected read-property request: {other:?}"),
    }
}

fn sample_point() -> PointConfig {
    let mut addressing = Addressing::new();
    addressing.insert("device_instance".into(), serde_json::json!(DEVICE_INSTANCE));
    addressing.insert("object_type".into(), serde_json::json!("analog_input"));
    addressing.insert("object_instance".into(), serde_json::json!(1));
    addressing.insert("property".into(), serde_json::json!("present_value"));
    PointConfig {
        enabled: true,
        device_key: "device_5678".to_string(),
        addressing,
        ..PointConfig::default()
    }
}

fn unsigned(value: u8) -> Vec<u8> {
    vec![0x21, value]
}

fn enumerated(value: u8) -> Vec<u8> {
    vec![0x91, value]
}

fn real(value: f32) -> Vec<u8> {
    let mut bytes = vec![0x44];
    bytes.extend_from_slice(&value.to_bits().to_be_bytes());
    bytes
}

fn character_string(value: &str) -> Vec<u8> {
    let mut bytes = vec![0x75, (value.len() + 1) as u8, 0x00];
    bytes.extend_from_slice(value.as_bytes());
    bytes
}

fn object_id(object_type: ObjectType, instance: u32) -> Vec<u8> {
    let raw = ((object_type.to_raw() & 0x3ff) << 22) | (instance & 0x3f_ffff);
    let mut bytes = vec![0xC4];
    bytes.extend_from_slice(&raw.to_be_bytes());
    bytes
}
