use bacnet_rs::app::Apdu;
use bacnet_rs::service::ConfirmedServiceChoice;

pub fn build_complex_ack(
    invoke_id: u8,
    service_choice: ConfirmedServiceChoice,
    service_ack: Vec<u8>,
) -> Vec<u8> {
    Apdu::ComplexAck {
        segmented: false,
        more_follows: false,
        invoke_id,
        sequence_number: None,
        proposed_window_size: None,
        service_choice,
        service_data: service_ack,
    }
    .encode()
}

pub fn build_error_pdu(
    invoke_id: u8,
    service_choice: ConfirmedServiceChoice,
    error_class: u32,
    error_code: u32,
) -> Vec<u8> {
    Apdu::Error {
        invoke_id,
        service_choice,
        error_class: error_class as u8,
        error_code: error_code as u8,
    }
    .encode()
}

pub fn is_unconfirmed_whois(apdu: &Apdu) -> bool {
    matches!(
        apdu,
        Apdu::UnconfirmedRequest {
            service_choice: bacnet_rs::service::UnconfirmedServiceChoice::WhoIs,
            ..
        }
    )
}
