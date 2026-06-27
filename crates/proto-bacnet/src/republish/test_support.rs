//! Test-facing helpers for BACnet loopback integration tests.
//!
//! These mirror the public API surface of the original `bacnet-republisher`
//! crate so integration tests can exercise discovery, browse, and poll paths
//! without going through the full GUI/worker stack.

use std::sync::atomic::AtomicBool;

use anyhow::Result;
use bacnet_client::client::BACnetClient;
use bacnet_transport::bip::BipTransport;
use bacnet_types::enums::{ObjectType, PropertyIdentifier};
use bacnet_types::primitives::ObjectIdentifier;

use republish_core::model::{PointConfig, PollOutcome, TelemetryValue};

use super::value::{decode_object_id, decode_unsigned, object_type_name};
use super::{poll_with_client, read_scalar, MAX_BROWSE_OBJECTS};

type BacnetIpClient = BACnetClient<BipTransport>;

/// A device discovered via I-Am, exposed for test assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestDiscoveredDevice {
    pub instance: u32,
    pub vendor_id: u16,
}

/// Object metadata from a device scan, exposed for test assertions.
#[derive(Debug, Clone, PartialEq)]
pub struct DeviceObject {
    pub device_instance: u32,
    pub object_type: String,
    pub object_instance: u32,
    pub object_name: Option<String>,
    pub description: Option<String>,
    pub units: Option<String>,
    pub present_value: Option<TelemetryValue>,
}

/// Collect devices currently in the client's I-Am cache.
pub async fn collect_discovered_devices(client: &BacnetIpClient) -> Vec<TestDiscoveredDevice> {
    let mut devices = client
        .discovered_devices()
        .await
        .into_iter()
        .map(|device| TestDiscoveredDevice {
            instance: device.object_identifier.instance_number(),
            vendor_id: device.vendor_id,
        })
        .collect::<Vec<_>>();
    devices.sort_by_key(|device| device.instance);
    devices
}

/// Scan a device's object list and read metadata for each non-device object.
pub async fn scan_device_objects_with_client(
    client: &BacnetIpClient,
    device_instance: u32,
    max_objects: usize,
) -> Result<Vec<DeviceObject>> {
    let device_oid = ObjectIdentifier::new(ObjectType::DEVICE, device_instance)?;
    let count_ack = client
        .read_property_from_device(
            device_instance,
            device_oid,
            PropertyIdentifier::OBJECT_LIST,
            Some(0),
        )
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let count = decode_unsigned(&count_ack.property_value)
        .map_err(|e| anyhow::anyhow!(e.to_string()))? as usize;

    let limit = count.min(max_objects.min(MAX_BROWSE_OBJECTS));
    let mut objects = Vec::new();
    for index in 1..=limit {
        let Ok(ack) = client
            .read_property_from_device(
                device_instance,
                device_oid,
                PropertyIdentifier::OBJECT_LIST,
                Some(index as u32),
            )
            .await
        else {
            continue;
        };
        let Ok((object_type, object_instance)) = decode_object_id(&ack.property_value) else {
            continue;
        };
        if object_type == ObjectType::DEVICE {
            continue;
        }
        let object_identifier = ObjectIdentifier::new(object_type, object_instance)?;
        let object_name = read_scalar(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::OBJECT_NAME,
        )
        .await
        .map(|v| v.to_string())
        .filter(|v| !v.trim().is_empty());
        let description = read_scalar(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::DESCRIPTION,
        )
        .await
        .map(|v| v.to_string())
        .filter(|v| !v.trim().is_empty());
        let units = read_scalar(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::UNITS,
        )
        .await
        .map(|v| v.to_string())
        .filter(|v| !v.trim().is_empty());
        let present_value = read_scalar(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::PRESENT_VALUE,
        )
        .await;

        objects.push(DeviceObject {
            device_instance,
            object_type: object_type_name(object_type),
            object_instance,
            object_name,
            description,
            units,
            present_value,
        });
    }
    Ok(objects)
}

/// Poll configured points once using the given client (no MQTT topic assignment).
pub async fn poll_points_once_with_client(
    client: &BacnetIpClient,
    points: &[PointConfig],
    concurrency: usize,
    _cancel: Option<&AtomicBool>,
) -> Result<PollOutcome> {
    poll_with_client(client, points, concurrency).await
}
