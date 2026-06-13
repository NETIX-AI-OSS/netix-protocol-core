use bacnet_types::enums::{ObjectType, PropertyIdentifier};
use republish_core::TelemetryValue;
use std::fmt::Write;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    Empty,
    UnsupportedTag(u8),
    Truncated,
    InvalidUtf8,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => formatter.write_str("empty BACnet value"),
            Self::UnsupportedTag(tag) => {
                write!(formatter, "unsupported BACnet application tag {tag}")
            }
            Self::Truncated => formatter.write_str("truncated BACnet value"),
            Self::InvalidUtf8 => formatter.write_str("invalid BACnet character string"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub fn decode_scalar_value(bytes: &[u8]) -> Result<TelemetryValue, DecodeError> {
    let Some(first) = bytes.first().copied() else {
        return Err(DecodeError::Empty);
    };

    let tag = first >> 4;
    let length_code = first & 0x07;

    match tag {
        0 => Ok(TelemetryValue::Text("null".to_string())),
        1 => Ok(TelemetryValue::Text((length_code != 0).to_string())),
        2 => decode_unsigned(bytes).map(|value| TelemetryValue::Number(value as f64)),
        3 => decode_signed(bytes).map(|value| TelemetryValue::Number(value as f64)),
        4 => decode_real(bytes),
        5 => decode_double(bytes),
        7 => decode_character_string(bytes),
        9 => decode_unsigned(bytes).map(|value| TelemetryValue::Text(value.to_string())),
        12 => decode_object_id(bytes).map(|(object_type, instance)| {
            TelemetryValue::Text(format!("{},{}", object_type_name(object_type), instance))
        }),
        other => Err(DecodeError::UnsupportedTag(other)),
    }
}

pub fn property_identifier_from_text(value: &str) -> Option<PropertyIdentifier> {
    let normalized = normalize_identifier(value);
    if let Ok(raw) = normalized.parse::<u32>() {
        return Some(PropertyIdentifier::from_raw(raw));
    }
    PropertyIdentifier::ALL_NAMED
        .iter()
        .find(|(name, _)| normalize_identifier(name) == normalized)
        .map(|(_, value)| *value)
}

pub fn object_type_from_text(value: &str) -> Option<ObjectType> {
    let normalized = normalize_identifier(value);
    if let Ok(raw) = normalized.parse::<u32>() {
        return Some(ObjectType::from_raw(raw));
    }
    ObjectType::ALL_NAMED
        .iter()
        .find(|(name, _)| normalize_identifier(name) == normalized)
        .map(|(_, value)| *value)
}

#[cfg(test)]
pub fn property_name(property: PropertyIdentifier) -> String {
    PropertyIdentifier::ALL_NAMED
        .iter()
        .find(|(_, value)| *value == property)
        .map(|(name, _)| name.to_ascii_lowercase())
        .unwrap_or_else(|| property.to_raw().to_string())
}

pub fn object_type_name(object_type: ObjectType) -> String {
    ObjectType::ALL_NAMED
        .iter()
        .find(|(_, value)| *value == object_type)
        .map(|(name, _)| name.to_ascii_lowercase())
        .unwrap_or_else(|| object_type.to_raw().to_string())
}

fn hex(bytes: &[u8]) -> String {
    let mut value = String::new();
    for byte in bytes {
        let _ = write!(&mut value, "{byte:02X}");
    }
    value
}

pub(crate) fn decode_unsigned(bytes: &[u8]) -> Result<u64, DecodeError> {
    let payload = application_payload(bytes)?;
    let mut value = 0u64;
    for byte in payload {
        value = (value << 8) | u64::from(*byte);
    }
    Ok(value)
}

fn decode_signed(bytes: &[u8]) -> Result<i64, DecodeError> {
    let payload = application_payload(bytes)?;
    if payload.is_empty() {
        return Ok(0);
    }
    let negative = payload[0] & 0x80 != 0;
    let mut value = if negative { -1i64 } else { 0i64 };
    for byte in payload {
        value = (value << 8) | i64::from(*byte);
    }
    Ok(value)
}

fn decode_real(bytes: &[u8]) -> Result<TelemetryValue, DecodeError> {
    let payload = application_payload(bytes)?;
    if payload.len() != 4 {
        return Err(DecodeError::Truncated);
    }
    let raw = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    Ok(TelemetryValue::Number(f32::from_bits(raw) as f64))
}

fn decode_double(bytes: &[u8]) -> Result<TelemetryValue, DecodeError> {
    let payload = application_payload(bytes)?;
    if payload.len() != 8 {
        return Err(DecodeError::Truncated);
    }
    let raw = u64::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    Ok(TelemetryValue::Number(f64::from_bits(raw)))
}

fn decode_character_string(bytes: &[u8]) -> Result<TelemetryValue, DecodeError> {
    let payload = application_payload(bytes)?;
    if payload.is_empty() {
        return Ok(TelemetryValue::Text(String::new()));
    }
    let encoding = payload[0];
    let data = &payload[1..];
    match encoding {
        0 | 3 | 4 | 5 => std::str::from_utf8(data)
            .map(|value| TelemetryValue::Text(value.to_string()))
            .map_err(|_| DecodeError::InvalidUtf8),
        _ => Ok(TelemetryValue::Text(hex(data))),
    }
}

pub(crate) fn decode_object_id(bytes: &[u8]) -> Result<(ObjectType, u32), DecodeError> {
    let payload = application_payload(bytes)?;
    if payload.len() != 4 {
        return Err(DecodeError::Truncated);
    }
    let raw = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let object_type = ObjectType::from_raw((raw >> 22) & 0x3ff);
    let instance = raw & 0x3f_ffff;
    Ok((object_type, instance))
}

fn application_payload(bytes: &[u8]) -> Result<&[u8], DecodeError> {
    let Some(first) = bytes.first().copied() else {
        return Err(DecodeError::Empty);
    };
    let length_code = first & 0x07;
    let (offset, length) = match length_code {
        0..=4 => (1, length_code as usize),
        5 => {
            let Some(length) = bytes.get(1).copied() else {
                return Err(DecodeError::Truncated);
            };
            (2, length as usize)
        }
        _ => return Err(DecodeError::UnsupportedTag(first >> 4)),
    };
    let end = offset + length;
    if end > bytes.len() {
        return Err(DecodeError::Truncated);
    }
    Ok(&bytes[offset..end])
}

fn normalize_identifier(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|character| !matches!(character, '_' | '-' | ' '))
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_real_value() {
        let value = decode_scalar_value(&[0x44, 0x42, 0x90, 0x00, 0x00]).unwrap();
        assert_eq!(value, TelemetryValue::Number(72.0));
    }

    #[test]
    fn decodes_boolean_as_text() {
        assert_eq!(
            decode_scalar_value(&[0x11]).unwrap(),
            TelemetryValue::Text("true".to_string())
        );
    }

    #[test]
    fn maps_named_identifiers_from_text() {
        assert_eq!(
            property_identifier_from_text("present value"),
            Some(PropertyIdentifier::PRESENT_VALUE)
        );
        assert_eq!(
            object_type_from_text("analog_input"),
            Some(ObjectType::ANALOG_INPUT)
        );
    }

    // --- decode_double (tag 5) ---

    #[test]
    fn decodes_double_value() {
        // Tag 5 (double), length 8
        let raw: f64 = 1234.5678;
        let bits = raw.to_bits();
        let bytes = bits.to_be_bytes();
        let mut packet = vec![0x55u8, 0x08]; // tag=5, extended-length indicator (length_code=5 means next byte is length)
                                             // Actually tag=5 uses length_code in lower 3 bits. 8 > 4 so length_code=5 (extended).
                                             // Format: first byte = (tag<<4)|(5), second byte = actual length (8)
                                             // first byte: (5 << 4) | 5 = 0x55, second byte: 8
        packet.push(0x08);
        packet.extend_from_slice(&bytes);
        // Correct: first byte 0x55 = tag 5, length_code 5 = extended; second byte = 8
        let packet = {
            let mut v = Vec::new();
            v.push((5u8 << 4) | 5u8); // tag=5, length_code=5 (extended)
            v.push(8u8); // actual length
            v.extend_from_slice(&bytes);
            v
        };
        let result = decode_scalar_value(&packet).unwrap();
        assert_eq!(result, TelemetryValue::Number(raw));
    }

    #[test]
    fn decodes_double_truncated_returns_error() {
        // tag=5, extended-length=8, but only 4 payload bytes
        let mut packet = vec![(5u8 << 4) | 5u8, 8u8];
        packet.extend_from_slice(&[0u8; 4]);
        assert_eq!(decode_scalar_value(&packet), Err(DecodeError::Truncated));
    }

    // --- decode_signed negative path ---

    #[test]
    fn decodes_signed_negative_value() {
        // Tag 3 (signed), value -1 encoded as 0xFF (1 byte)
        let packet = vec![(3u8 << 4) | 1u8, 0xFFu8]; // tag=3, length=1, payload=0xFF
        let result = decode_scalar_value(&packet).unwrap();
        assert_eq!(result, TelemetryValue::Number(-1.0));
    }

    #[test]
    fn decodes_signed_negative_two_bytes() {
        // -256 in two's complement big-endian: 0xFF 0x00
        let packet = vec![(3u8 << 4) | 2u8, 0xFF, 0x00];
        let result = decode_scalar_value(&packet).unwrap();
        assert_eq!(result, TelemetryValue::Number(-256.0));
    }

    #[test]
    fn decodes_signed_zero_length_is_zero() {
        // Tag 3, length 0 → value 0
        let packet = vec![(3u8 << 4)];
        let result = decode_scalar_value(&packet).unwrap();
        assert_eq!(result, TelemetryValue::Number(0.0));
    }

    // --- decode_character_string non-UTF-8 fallback ---

    #[test]
    fn decodes_character_string_utf8() {
        // Tag 7, encoding 0 (UTF-8), "hi"
        let payload = b"hi";
        let mut packet = vec![(7u8 << 4) | (1 + payload.len()) as u8, 0u8]; // encoding=0
        packet.extend_from_slice(payload);
        let result = decode_scalar_value(&packet).unwrap();
        assert_eq!(result, TelemetryValue::Text("hi".to_string()));
    }

    #[test]
    fn decodes_character_string_non_utf8_encoding_returns_hex() {
        // Tag 7, encoding 1 (UCS-2), two bytes 0xAB 0xCD
        let data: &[u8] = &[0xAB, 0xCD];
        let length = 1 + data.len(); // encoding byte + data
        let mut packet = vec![(7u8 << 4) | length as u8, 0x01u8]; // encoding=1 (UCS-2)
        packet.extend_from_slice(data);
        let result = decode_scalar_value(&packet).unwrap();
        // Should fall back to hex representation
        assert_eq!(result, TelemetryValue::Text("ABCD".to_string()));
    }

    #[test]
    fn decodes_empty_character_string() {
        // Tag 7, length 0
        let packet = vec![(7u8 << 4)];
        let result = decode_scalar_value(&packet).unwrap();
        assert_eq!(result, TelemetryValue::Text(String::new()));
    }

    // --- object_type_from_text with raw numeric string ---

    #[test]
    fn object_type_from_text_raw_zero() {
        // ObjectType 0 = ANALOG_INPUT
        let result = object_type_from_text("0");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), ObjectType::ANALOG_INPUT);
    }

    #[test]
    fn object_type_from_text_raw_numeric() {
        // ObjectType 1 = ANALOG_OUTPUT
        let result = object_type_from_text("1");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), ObjectType::ANALOG_OUTPUT);
    }

    #[test]
    fn property_identifier_from_text_raw_numeric() {
        // PropertyIdentifier 85 = PRESENT_VALUE
        let result = property_identifier_from_text("85");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), PropertyIdentifier::PRESENT_VALUE);
    }
}
