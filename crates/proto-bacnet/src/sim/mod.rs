//! BACnet/IP simulator adapter: serves the shared simulation over UDP/47808,
//! answering Who-Is, ReadProperty, and ReadPropertyMultiple.

mod apdu;
mod properties;
pub mod registry;
mod rpm;
mod whois_compat;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bacnet_rs::app::Apdu;
use bacnet_rs::network::Npdu;
use bacnet_rs::object::{Device, Segmentation};
use bacnet_rs::service::{ConfirmedServiceChoice, IAmRequest, UnconfirmedServiceChoice};
use proto_api::{Addressing, Capabilities};
use sim_core::{AppLog, AppMetrics, SimProtocol, SimServeContext, Simulation};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use registry::DeviceEntry;

/// BACnet vendor identifier assigned to this simulator.
pub const VENDOR_ID: u32 = 260;

/// Maximum APDU length advertised in I-Am and device property responses.
/// BACnet/IP single-segment ceiling (1476 bytes = 1500 MTU − 14 Ethernet − 20 IP − 8 UDP − BVLC/NPDU overhead).
pub const MAX_APDU_LENGTH: u32 = 1476;

/// Simulator-side BACnet adapter.
pub struct BacnetSimProtocol {
    caps: Capabilities,
}

/// Factory used by the registry to construct the adapter from config options.
pub fn sim_factory(_options: &Addressing) -> anyhow::Result<Box<dyn SimProtocol>> {
    Ok(Box::new(BacnetSimProtocol {
        caps: crate::capabilities(),
    }))
}

#[async_trait::async_trait]
impl SimProtocol for BacnetSimProtocol {
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn serve(self: Box<Self>, ctx: SimServeContext) -> anyhow::Result<()> {
        // Build the BACnet device/point index once from the current simulation.
        let devices = {
            let sim = ctx.sim.lock().await;
            registry::build_device_registry(&sim.devices)
        };

        let addr = format!("0.0.0.0:{}", ctx.port);
        let socket = Arc::new(UdpSocket::bind(&addr).await?);
        if let Err(e) = socket.set_broadcast(true) {
            ctx.log_line(format!("WARN: failed to set broadcast on socket: {e}"));
        }
        ctx.log_line(format!("BACnet/IP listening on {addr}"));

        let mut buf = [0u8; 4096];
        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => break,
                res = socket.recv_from(&mut buf) => match res {
                    Ok((len, src)) => {
                        handle_datagram(&socket, &buf[..len], src, &ctx.sim, &devices, &ctx.metrics, &ctx.log).await;
                    }
                    Err(e) => {
                        // On Windows, an ICMP Port Unreachable for a datagram we previously
                        // sent (e.g. an I-Am to a client port that has since closed) surfaces
                        // as WSAECONNRESET on the next recv. The socket is fine; skip noise.
                        if e.kind() == std::io::ErrorKind::ConnectionReset {
                            continue;
                        }
                        ctx.log_line(format!("WARN: error receiving from socket: {e}"));
                    }
                }
            }
        }
        Ok(())
    }
}

fn log_line(app_log: &Option<Arc<AppLog>>, msg: impl Into<String>) {
    let msg = msg.into();
    match app_log {
        Some(log) => log.push(msg),
        None => log::info!("{msg}"),
    }
}

async fn handle_datagram(
    socket: &UdpSocket,
    data: &[u8],
    src: SocketAddr,
    simulation: &Arc<Mutex<Simulation>>,
    devices: &[DeviceEntry],
    metrics: &Arc<AppMetrics>,
    app_log: &Option<Arc<AppLog>>,
) {
    let Some(apdu_bytes) = extract_apdu(data) else {
        return;
    };

    let apdu = match Apdu::decode(&apdu_bytes) {
        Ok(apdu) => apdu,
        Err(_) => return,
    };

    if apdu::is_unconfirmed_whois(&apdu) {
        if let Apdu::UnconfirmedRequest { service_data, .. } = apdu {
            handle_whois(service_data, socket, src, simulation, metrics, app_log).await;
        }
        return;
    }

    if let Apdu::ConfirmedRequest {
        invoke_id,
        service_choice,
        service_data,
        ..
    } = apdu
    {
        handle_confirmed_request(
            socket,
            src,
            invoke_id,
            service_choice,
            &service_data,
            simulation,
            devices,
            metrics,
            app_log,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_confirmed_request(
    socket: &UdpSocket,
    src: SocketAddr,
    invoke_id: u8,
    service_choice: ConfirmedServiceChoice,
    service_data: &[u8],
    simulation: &Arc<Mutex<Simulation>>,
    devices: &[DeviceEntry],
    metrics: &Arc<AppMetrics>,
    app_log: &Option<Arc<AppLog>>,
) {
    let client = src.to_string();
    let sim = simulation.lock().await;
    let response = match service_choice {
        ConfirmedServiceChoice::ReadProperty => {
            metrics.record_request("read_property", Some(&client));
            properties::handle_read_property(service_data, devices, &sim)
                .map(|ack| apdu::build_complex_ack(invoke_id, service_choice, ack))
        }
        ConfirmedServiceChoice::ReadPropertyMultiple => {
            metrics.record_request("read_property_multiple", Some(&client));
            rpm::handle_read_property_multiple(service_data, devices, &sim)
                .map(|ack| apdu::build_complex_ack(invoke_id, service_choice, ack))
        }
        _ => {
            log_line(
                app_log,
                format!("Unsupported confirmed service {service_choice:?} from {src}"),
            );
            None
        }
    };
    drop(sim);

    let Some(apdu_bytes) = response else {
        let error = apdu::build_error_pdu(invoke_id, service_choice, 0, 31);
        // Resolve to an Option here so the non-Send `Box<dyn Error>` is dropped
        // before the await — keeps the async_trait future `Send`.
        let packet = wrap_unicast_npdu(&error).ok();
        if let Some(packet) = packet {
            let _ = socket.send_to(&packet, src).await;
        }
        return;
    };

    let packet = wrap_unicast_npdu(&apdu_bytes).ok();
    if let Some(packet) = packet {
        if let Err(e) = socket.send_to(&packet, src).await {
            log_line(
                app_log,
                format!("ERROR: failed to send confirmed ack to {src}: {e}"),
            );
        }
    }
}

async fn handle_whois(
    service_data: Vec<u8>,
    socket: &UdpSocket,
    src: SocketAddr,
    simulation: &Arc<Mutex<Simulation>>,
    metrics: &Arc<AppMetrics>,
    app_log: &Option<Arc<AppLog>>,
) {
    metrics.record_request("who_is", Some(&src.to_string()));
    let whois = whois_compat::decode_whois(&service_data);

    let responses: Vec<(u32, Vec<u8>)> = {
        let sim = simulation.lock().await;
        sim.devices
            .iter()
            .filter(|device| whois.matches(device.device_id))
            .filter_map(|device| {
                create_iam_response(device.device_id, &device.name)
                    .ok()
                    .map(|bytes| (device.device_id, bytes))
            })
            .collect()
    };

    log_line(
        app_log,
        format!(
            "Responding to Who-Is from {src}: {} device(s)",
            responses.len()
        ),
    );

    for (device_id, response) in responses {
        if let Err(e) = socket.send_to(&response, src).await {
            log_line(
                app_log,
                format!("ERROR: failed to send I-Am for device {device_id}: {e}"),
            );
        }
        tokio::time::sleep(Duration::from_micros(500)).await;
    }
}

fn extract_apdu(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < 4 || data[0] != 0x81 {
        return None;
    }

    let bvlc_function = data[1];
    let bvlc_length = ((data[2] as u16) << 8) | (data[3] as u16);
    if data.len() != bvlc_length as usize {
        return None;
    }

    let npdu_start = match bvlc_function {
        0x0A | 0x0B => 4,
        0x04 => 10,
        _ => return None,
    };

    if data.len() <= npdu_start {
        return None;
    }

    let (_npdu, npdu_len) = Npdu::decode(&data[npdu_start..]).ok()?;
    let apdu_start = npdu_start + npdu_len;
    if data.len() <= apdu_start {
        return None;
    }

    Some(data[apdu_start..].to_vec())
}

fn create_iam_response(device_id: u32, name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut device = Device::new(device_id, name.to_string());
    device.set_vendor_by_id(VENDOR_ID as u16)?;

    let iam = IAmRequest::new(
        device.identifier,
        MAX_APDU_LENGTH,
        Segmentation::Both,
        device.vendor_identifier,
    );

    let mut iam_buffer = Vec::new();
    iam.encode(&mut iam_buffer)?;

    let mut apdu = vec![0x10];
    apdu.push(UnconfirmedServiceChoice::IAm as u8);
    apdu.extend_from_slice(&iam_buffer);

    wrap_unicast_npdu(&apdu)
}

fn wrap_unicast_npdu(apdu: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut npdu = Npdu::new();
    npdu.control.priority = 0;
    let mut message = npdu.encode();
    message.extend_from_slice(apdu);

    let mut bvlc_message = vec![0x81, 0x0A, 0x00, 0x00];
    bvlc_message.extend_from_slice(&message);
    let total_len = bvlc_message.len() as u16;
    bvlc_message[2] = (total_len >> 8) as u8;
    bvlc_message[3] = (total_len & 0xFF) as u8;
    Ok(bvlc_message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bvlc_frame(function: u8, npdu_apdu: &[u8]) -> Vec<u8> {
        let total = 4 + npdu_apdu.len();
        let mut frame = vec![0x81, function, (total >> 8) as u8, (total & 0xFF) as u8];
        frame.extend_from_slice(npdu_apdu);
        frame
    }

    fn minimal_npdu_apdu(apdu: &[u8]) -> Vec<u8> {
        let mut v = vec![0x01, 0x00]; // version=1, control=0
        v.extend_from_slice(apdu);
        v
    }

    #[test]
    fn extract_apdu_rejects_bad_frames() {
        assert!(extract_apdu(&[]).is_none());
        assert!(extract_apdu(&[0x81, 0x0A, 0x00]).is_none());
        let mut frame = bvlc_frame(0x0A, &minimal_npdu_apdu(&[0x10, 0x08]));
        frame[0] = 0x82;
        assert!(extract_apdu(&frame).is_none());
        let frame = bvlc_frame(0xFF, &minimal_npdu_apdu(&[0x10, 0x08]));
        assert!(extract_apdu(&frame).is_none());
    }

    #[test]
    fn extract_apdu_valid_frames() {
        let apdu_bytes = &[0x10u8, 0x08];
        let frame = bvlc_frame(0x0A, &minimal_npdu_apdu(apdu_bytes));
        assert_eq!(extract_apdu(&frame).unwrap(), apdu_bytes);
        let frame = bvlc_frame(0x0B, &minimal_npdu_apdu(apdu_bytes));
        assert_eq!(extract_apdu(&frame).unwrap(), apdu_bytes);
        // 0x04 forwarded: 6-byte address before NPDU.
        let mut inner = vec![0u8; 6];
        inner.extend_from_slice(&minimal_npdu_apdu(apdu_bytes));
        let frame = bvlc_frame(0x04, &inner);
        assert_eq!(extract_apdu(&frame).unwrap(), apdu_bytes);
    }

    #[test]
    fn wrap_unicast_npdu_produces_valid_bvlc_frame() {
        let frame = wrap_unicast_npdu(&[0x10, 0x08]).expect("wrap");
        assert_eq!(frame[0], 0x81);
        assert_eq!(frame[1], 0x0A);
        let declared_len = ((frame[2] as u16) << 8) | frame[3] as u16;
        assert_eq!(declared_len as usize, frame.len());
    }

    #[test]
    fn constants_have_expected_values() {
        assert_eq!(VENDOR_ID, 260);
        assert_eq!(MAX_APDU_LENGTH, 1476);
    }
}
