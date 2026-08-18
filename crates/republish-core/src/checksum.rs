//! Provenance checksum for an emitted republisher config, stamped as `sim_config_checksum` so a stale, un-regenerated config becomes detectable drift.

use sha2::{Digest, Sha256};

/// Lowercase hex SHA-256 of `bytes`; used to stamp and later verify the `sim_config_checksum` provenance marker.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_known_vector() {
        // NIST/RFC-6234 test vector for the empty input and "abc".
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn distinct_inputs_hash_differently() {
        assert_ne!(sha256_hex(b"config-a"), sha256_hex(b"config-b"));
    }
}
