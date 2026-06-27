//! Subnet scanning helpers for Modbus TCP discovery.

use std::net::{Ipv4Addr, ToSocketAddrs};

use anyhow::{anyhow, Context, Result};

/// Maximum hosts scanned when no explicit cap is configured (/24 default cap).
pub const DEFAULT_MAX_HOSTS: u32 = 256;

pub fn parse_host_targets(host: &str, max_hosts: u32) -> Result<Vec<Ipv4Addr>> {
    let host = host.trim();
    if host.is_empty() {
        return Err(anyhow!("Modbus host is required"));
    }
    if let Some((network, prefix)) = host.split_once('/') {
        expand_cidr(network, prefix.parse()?, max_hosts)
    } else {
        resolve_host_ipv4(host)
    }
}

fn resolve_host_ipv4(host: &str) -> Result<Vec<Ipv4Addr>> {
    if let Ok(addr) = host.parse::<Ipv4Addr>() {
        return Ok(vec![addr]);
    }
    let addrs: Vec<Ipv4Addr> = (host, 502)
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve Modbus host '{host}'"))?
        .filter_map(|addr| match addr.ip() {
            std::net::IpAddr::V4(v4) => Some(v4),
            std::net::IpAddr::V6(_) => None,
        })
        .collect();
    if addrs.is_empty() {
        return Err(anyhow!("no IPv4 address found for Modbus host '{host}'"));
    }
    Ok(addrs)
}

fn expand_cidr(network: &str, prefix: u8, max_hosts: u32) -> Result<Vec<Ipv4Addr>> {
    if prefix > 32 {
        return Err(anyhow!("invalid CIDR prefix {prefix}"));
    }
    let base: u32 = u32::from(network.parse::<Ipv4Addr>()?);
    let host_bits = 32 - prefix;
    let total = 1u32.checked_shl(host_bits as u32).unwrap_or(u32::MAX);
    let cap = max_hosts.max(1).min(total);
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << host_bits
    };
    let network_addr = base & mask;
    let mut addrs = Vec::with_capacity(cap as usize);
    for offset in 0..cap {
        addrs.push(Ipv4Addr::from(network_addr.wrapping_add(offset)));
    }
    Ok(addrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_ip_returns_one_address() {
        let addrs = parse_host_targets("192.168.1.10", 256).unwrap();
        assert_eq!(addrs, vec![Ipv4Addr::new(192, 168, 1, 10)]);
    }

    #[test]
    fn localhost_resolves_via_dns() {
        let addrs = parse_host_targets("localhost", 256).unwrap();
        assert!(!addrs.is_empty());
        assert!(addrs.iter().all(|addr| addr.is_loopback()));
    }

    #[test]
    fn cidr_expands_with_cap() {
        let addrs = parse_host_targets("192.168.1.0/30", 256).unwrap();
        assert_eq!(addrs.len(), 4);
        assert_eq!(addrs[0], Ipv4Addr::new(192, 168, 1, 0));
        assert_eq!(addrs[3], Ipv4Addr::new(192, 168, 1, 3));
    }
}
