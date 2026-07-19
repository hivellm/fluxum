//! IP and CIDR matching (SPEC-026 §4): the shared parser/matcher behind
//! `server.trusted_proxies` and the connection blocklist/allowlist.
//!
//! Entries are plain addresses (`10.1.2.3`, `2001:db8::1`) or CIDR blocks
//! (`10.0.0.0/8`, `2001:db8::/32`), IPv4 and IPv6. Matching normalizes
//! IPv4-mapped IPv6 addresses (`::ffff:10.1.2.3`) to their IPv4 form first,
//! so a dual-stack listener cannot be talked past a v4 rule by dressing the
//! peer address in a v6 coat.

use std::net::IpAddr;
use std::str::FromStr;

use crate::error::{FluxumError, Result};

/// One address or CIDR block. `10.1.2.3` parses as `10.1.2.3/32`;
/// `2001:db8::1` as `/128`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpNet {
    addr: IpAddr,
    prefix: u8,
}

impl IpNet {
    /// Whether `ip` falls inside this block. Families never cross-match
    /// (after IPv4-mapped normalization).
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip.to_canonical()) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = prefix_mask_v4(self.prefix);
                u32::from(net) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = prefix_mask_v6(self.prefix);
                u128::from(net) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }
}

fn prefix_mask_v4(prefix: u8) -> u32 {
    u32::MAX.checked_shl(u32::from(32 - prefix)).unwrap_or(0)
}

fn prefix_mask_v6(prefix: u8) -> u128 {
    u128::MAX.checked_shl(u32::from(128 - prefix)).unwrap_or(0)
}

impl FromStr for IpNet {
    type Err = FluxumError;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let (addr_str, prefix_str) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let addr: IpAddr = addr_str
            .parse()
            .map_err(|_| FluxumError::config(format!("'{s}': not an IP address or CIDR block")))?;
        // Normalize the network address too, so `::ffff:10.0.0.0/104`-style
        // entries and plain v4 entries agree on family.
        let addr = addr.to_canonical();
        let max_prefix = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix_str {
            None => max_prefix,
            Some(p) => {
                let p: u8 = p.parse().map_err(|_| {
                    FluxumError::config(format!("'{s}': prefix length is not a number"))
                })?;
                if p > max_prefix {
                    return Err(FluxumError::config(format!(
                        "'{s}': prefix length {p} exceeds /{max_prefix}"
                    )));
                }
                p
            }
        };
        Ok(Self { addr, prefix })
    }
}

/// A set of [`IpNet`] entries, matched linearly. The lists this backs are
/// operator-sized (a handful of proxies, a ban list), so a scan beats the
/// constant factors of anything cleverer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IpSet {
    nets: Vec<IpNet>,
}

impl IpSet {
    /// Parse every entry, or fail naming the first bad one.
    ///
    /// # Errors
    /// The first entry that is not an IP address or CIDR block.
    pub fn parse(entries: &[String]) -> Result<Self> {
        let nets = entries
            .iter()
            .map(|e| e.parse())
            .collect::<Result<Vec<IpNet>>>()?;
        Ok(Self { nets })
    }

    /// Whether `ip` matches any entry.
    pub fn contains(&self, ip: IpAddr) -> bool {
        self.nets.iter().any(|net| net.contains(ip))
    }

    /// Whether the set has no entries (a feature keyed on it is off).
    pub fn is_empty(&self) -> bool {
        self.nets.is_empty()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.nets.len()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn plain_addresses_match_exactly() {
        let set = IpSet::parse(&["10.1.2.3".into(), "2001:db8::1".into()]).unwrap();
        assert!(set.contains(ip("10.1.2.3")));
        assert!(!set.contains(ip("10.1.2.4")));
        assert!(set.contains(ip("2001:db8::1")));
        assert!(!set.contains(ip("2001:db8::2")));
    }

    #[test]
    fn cidr_blocks_match_their_range() {
        let set = IpSet::parse(&["10.0.0.0/8".into(), "2001:db8::/32".into()]).unwrap();
        assert!(set.contains(ip("10.255.255.255")));
        assert!(!set.contains(ip("11.0.0.0")));
        assert!(set.contains(ip("2001:db8:ffff::1")));
        assert!(!set.contains(ip("2001:db9::1")));
    }

    #[test]
    fn zero_prefix_matches_everything_in_family() {
        let set = IpSet::parse(&["0.0.0.0/0".into()]).unwrap();
        assert!(set.contains(ip("203.0.113.7")));
        // But never the other family.
        assert!(!set.contains(ip("2001:db8::1")));
    }

    #[test]
    fn ipv4_mapped_ipv6_peers_match_v4_rules() {
        let set = IpSet::parse(&["10.0.0.0/8".into()]).unwrap();
        assert!(set.contains(ip("::ffff:10.1.2.3")));
        assert!(!set.contains(ip("::ffff:11.1.2.3")));
    }

    #[test]
    fn bad_entries_are_rejected_with_the_offender_named() {
        for bad in ["not-an-ip", "10.0.0.0/33", "2001:db8::/129", "10.0.0.0/x"] {
            let err = IpSet::parse(&[bad.to_owned()]).unwrap_err();
            assert!(err.to_string().contains(bad), "error names '{bad}': {err}");
        }
    }

    #[test]
    fn an_empty_set_matches_nothing_and_reports_empty() {
        let set = IpSet::default();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert!(!set.contains(ip("127.0.0.1")));
    }
}
