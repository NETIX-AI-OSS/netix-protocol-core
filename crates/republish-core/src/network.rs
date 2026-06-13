//! IPv4 interface enumeration, used by adapters that bind to a local NIC and by
//! the UI's interface picker.

use get_if_addrs::{get_if_addrs, IfAddr};
use std::fmt;
use std::net::Ipv4Addr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkInterface {
    pub name: String,
    pub addr: Ipv4Addr,
}

impl fmt::Display for NetworkInterface {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} ({})", self.name, self.addr)
    }
}

/// Whether an interface is a plausible target for protocol discovery binding
/// (excludes loopback, link-local, and common tunnel/virtual NICs).
pub fn is_discovery_interface(name: &str, addr: Ipv4Addr) -> bool {
    if addr.is_loopback() || addr.is_unspecified() || addr.is_link_local() {
        return false;
    }
    let lower = name.to_ascii_lowercase();
    const EXCLUDED_PREFIXES: &[&str] = &[
        "utun", "ppp", "ipsec", "gif", "stf", "awdl", "llw", "lo", "ap",
    ];
    !EXCLUDED_PREFIXES
        .iter()
        .any(|prefix| lower == *prefix || lower.starts_with(prefix))
}

pub fn ipv4_interfaces() -> Vec<NetworkInterface> {
    let mut interfaces = get_if_addrs()
        .map(|interfaces| {
            interfaces
                .into_iter()
                .filter_map(|interface| match interface.addr {
                    IfAddr::V4(v4) if is_discovery_interface(&interface.name, v4.ip) => {
                        Some(NetworkInterface {
                            name: interface.name,
                            addr: v4.ip,
                        })
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    interfaces.sort_by(|left, right| left.name.cmp(&right.name).then(left.addr.cmp(&right.addr)));
    interfaces.dedup_by(|left, right| left.name == right.name && left.addr == right.addr);
    interfaces
}

pub fn interface_choices(interfaces: &[NetworkInterface]) -> Vec<Ipv4Addr> {
    let mut choices = interfaces
        .iter()
        .map(|interface| interface.addr)
        .collect::<Vec<_>>();
    choices.sort();
    choices.dedup();
    choices
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excludes_tunnel_and_link_local() {
        assert!(!is_discovery_interface(
            "utun10",
            Ipv4Addr::new(10, 7, 0, 2)
        ));
        assert!(!is_discovery_interface(
            "en0",
            Ipv4Addr::new(169, 254, 1, 1)
        ));
        assert!(is_discovery_interface("en0", Ipv4Addr::new(172, 20, 10, 3)));
    }
}
