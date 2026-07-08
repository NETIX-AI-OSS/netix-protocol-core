#![no_main]

use libfuzzer_sys::fuzz_target;
use proto_bacnet::decode_scalar_value;

fuzz_target!(|data: &[u8]| {
    let _ = decode_scalar_value(data);
});
