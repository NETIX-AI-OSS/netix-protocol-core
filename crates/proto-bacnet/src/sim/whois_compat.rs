use bacnet_rs::service::WhoIsRequest;

/// Decode a Who-Is request from either bacnet-rs or bacnet-services/bacnet-encoding layout.
pub fn decode_whois(service_data: &[u8]) -> WhoIsRequest {
    if service_data.is_empty() {
        return WhoIsRequest::new();
    }
    if let Ok(request) = WhoIsRequest::decode(service_data) {
        return request;
    }
    WhoIsRequest::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_ranged_whois_from_bacnet_services_layout() {
        let request = WhoIsRequest::for_device(1001);
        let mut encoded = Vec::new();
        request.encode(&mut encoded).unwrap();
        let decoded = decode_whois(&encoded);
        assert_eq!(decoded.device_instance_range_low_limit, Some(1001));
        assert_eq!(decoded.device_instance_range_high_limit, Some(1001));
        assert!(decoded.matches(1001));
        assert!(!decoded.matches(1002));
    }
}
