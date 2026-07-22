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
#[cfg_attr(coverage_nightly, coverage(off))]
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

    #[test]
    fn display_formats_name_and_addr() {
        let interface = NetworkInterface {
            name: "en0".into(),
            addr: Ipv4Addr::new(192, 168, 1, 5),
        };
        assert_eq!(interface.to_string(), "en0 (192.168.1.5)");
    }

    #[test]
    fn interface_choices_sorts_and_dedups_addrs() {
        let interfaces = vec![
            NetworkInterface {
                name: "en1".into(),
                addr: Ipv4Addr::new(10, 0, 0, 2),
            },
            NetworkInterface {
                name: "en0".into(),
                addr: Ipv4Addr::new(10, 0, 0, 1),
            },
            // Same addr behind a different interface name collapses to one choice.
            NetworkInterface {
                name: "en2".into(),
                addr: Ipv4Addr::new(10, 0, 0, 1),
            },
        ];
        let choices = interface_choices(&interfaces);
        assert_eq!(
            choices,
            vec![Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(10, 0, 0, 2)]
        );
    }

    #[test]
    fn interface_choices_empty_input_yields_no_choices() {
        assert!(interface_choices(&[]).is_empty());
    }

    #[test]
    fn ipv4_interfaces_enumerates_host_sorted_and_filtered() {
        // Exercises the real get_if_addrs() enumeration plus the filter/sort/dedup
        // pipeline. The set depends on host NICs (a minimal container may yield an
        // empty result once loopback is filtered out), but the enumeration path runs
        // deterministically and every returned entry must satisfy the invariants.
        let interfaces = ipv4_interfaces();

        // Sorted by (name, addr): no adjacent pair is out of order.
        assert!(interfaces.windows(2).all(|pair| {
            pair[0]
                .name
                .cmp(&pair[1].name)
                .then(pair[0].addr.cmp(&pair[1].addr))
                != std::cmp::Ordering::Greater
        }));
        // Deduped: no exact (name, addr) duplicate survives.
        assert!(interfaces
            .windows(2)
            .all(|pair| !(pair[0].name == pair[1].name && pair[0].addr == pair[1].addr)));
        // Every retained interface is a valid discovery target (the Some-arm invariant).
        assert!(interfaces
            .iter()
            .all(|interface| is_discovery_interface(&interface.name, interface.addr)));

        // Choices derived from the live enumeration are strictly ascending (sorted+deduped).
        let choices = interface_choices(&interfaces);
        assert!(choices.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
