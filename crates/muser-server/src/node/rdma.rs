//! The MelonDMA RDMA bulk lane, as `muser node add --transport rdma` enrolls it.
//!
//! Everything the lane needs is read off the node at every enrollment rather
//! than remembered: GID tables on the paired GX10 have already moved once
//! under a bonded interface, and the one fact this cannot get wrong is which
//! index is the RoCE v2 entry for the cabled address. On Linux any index
//! answers a query, and index 0 is the link-local RoCE v1 GID, so a stale or
//! guessed index is not an error — it is the wrong RoCE version on one end.
//!
//! The Mac's half is two values the provider cannot learn for itself, because
//! the DriverKit extension owns the port and there is no system ARP for that
//! link: this Mac's address on the link, and the node's MAC on it. The MAC
//! comes from the node's sysfs; the address is the other host of the node's
//! point-to-point subnet unless given explicitly.

use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

use super::ssh::Ssh;
use super::Result;

/// MelonDMA exposes the one card it drives under this verbs name.
pub const MAC_DEVICE: &str = "mlx5_0";

/// What `--transport` asks of this run.
#[derive(Debug, Clone, Default)]
pub struct RdmaRequest {
    /// `Some(true)` enrolls the lane, `Some(false)` removes it, `None` keeps
    /// whatever the node already had.
    pub want: Option<bool>,
    pub node_address: Option<String>,
    pub mac_address: Option<String>,
}

/// The enrolled lane, recorded in the registry and written into both configs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RdmaLane {
    pub node_device: String,
    pub node_gid_index: u32,
    pub node_netdev: String,
    pub node_address: String,
    pub node_mac: String,
    pub mac_address: String,
}

impl RdmaLane {
    /// The node's `handoff.json` `rdma` section.
    pub fn handoff_section(&self) -> serde_json::Value {
        serde_json::json!({
            "device": self.node_device,
            "gid_index": self.node_gid_index,
        })
    }

    /// This Mac's `cluster.json` `rdma` section. The GID index is left to the
    /// provider (-1): MelonDMA hands every client its own slot.
    pub fn cluster_section(&self) -> serde_json::Value {
        serde_json::json!({
            "device": MAC_DEVICE,
            "gid_index": -1,
            "local_address": self.mac_address,
            "peer_mac": self.node_mac,
        })
    }
}

/// One RoCE v2 IPv4 GID on the node whose port is up. Tab-separated:
/// device, GID index, netdev, address/prefix, MAC.
const DISCOVER: &str = r#"set -u
for port in /sys/class/infiniband/*/ports/1; do
    [ -d "$port" ] || continue
    dev=$(basename "$(dirname "$(dirname "$port")")")
    case "$(cat "$port/state" 2>/dev/null)" in *ACTIVE*) ;; *) continue ;; esac
    for gid_path in "$port"/gids/*; do
        index=$(basename "$gid_path")
        gid=$(cat "$gid_path" 2>/dev/null) || continue
        case "$gid" in 0000:0000:0000:0000:0000:ffff:*) ;; *) continue ;; esac
        [ "$(cat "$port/gid_attrs/types/$index" 2>/dev/null)" = "RoCE v2" ] || continue
        netdev=$(cat "$port/gid_attrs/ndevs/$index" 2>/dev/null) || continue
        [ -n "$netdev" ] || continue
        mac=$(cat "/sys/class/net/$netdev/address" 2>/dev/null) || continue
        hex=$(printf '%s' "$gid" | tr -d ':' | cut -c25-32)
        address=$(printf '%d.%d.%d.%d' 0x$(printf '%s' "$hex" | cut -c1-2) 0x$(printf '%s' "$hex" | cut -c3-4) 0x$(printf '%s' "$hex" | cut -c5-6) 0x$(printf '%s' "$hex" | cut -c7-8))
        cidr=$(ip -o -4 addr show dev "$netdev" 2>/dev/null | awk -v a="$address" '{split($4, p, "/"); if (p[1] == a) print $4}' | head -n 1)
        [ -n "$cidr" ] || continue
        printf '%s\t%s\t%s\t%s\t%s\n' "$dev" "$index" "$netdev" "$cidr" "$mac"
    done
done
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Candidate {
    device: String,
    gid_index: u32,
    netdev: String,
    address: Ipv4Addr,
    prefix: u8,
    mac: String,
}

fn parse(output: &str) -> Vec<Candidate> {
    output
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split('\t').collect();
            let [device, index, netdev, cidr, mac] = fields.as_slice() else {
                return None;
            };
            let (address, prefix) = cidr.split_once('/')?;
            Some(Candidate {
                device: device.to_string(),
                gid_index: index.parse().ok()?,
                netdev: netdev.to_string(),
                address: address.parse().ok()?,
                prefix: prefix.parse().ok()?,
                mac: mac.to_ascii_lowercase(),
            })
        })
        .collect()
}

/// The other end of a point-to-point subnet: /31 has two addresses, /30 two
/// usable hosts. Anything wider is a real network and the Mac's address has
/// to be named.
fn point_to_point_peer(address: Ipv4Addr, prefix: u8) -> Option<Ipv4Addr> {
    let value = u32::from(address);
    match prefix {
        31 => Some(Ipv4Addr::from(value ^ 1)),
        30 => {
            let host = value & 3;
            let base = value & !3;
            match host {
                1 => Some(Ipv4Addr::from(base + 2)),
                2 => Some(Ipv4Addr::from(base + 1)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn choose(candidates: Vec<Candidate>, request: &RdmaRequest) -> Result<RdmaLane> {
    let listing = |candidates: &[Candidate]| {
        candidates
            .iter()
            .map(|c| {
                format!(
                    "{} gid {} on {} ({}/{})",
                    c.device, c.gid_index, c.netdev, c.address, c.prefix
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    };
    if candidates.is_empty() {
        return Err("the node has no active RoCE v2 IPv4 address on any RDMA port — cable \
                    the link, give its interface an IPv4 address, and retry"
            .into());
    }
    let chosen = match &request.node_address {
        Some(wanted) => candidates
            .iter()
            .find(|c| c.address.to_string() == *wanted)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "--rdma-node-address {wanted} is not an active RoCE v2 address on the node \
                     (found: {})",
                    listing(&candidates)
                )
            })?,
        None => {
            // A cable straight to this Mac is a /30 or /31; anything else is a
            // LAN the lane should not be guessing its way onto.
            let direct: Vec<Candidate> = candidates
                .iter()
                .filter(|c| point_to_point_peer(c.address, c.prefix).is_some())
                .cloned()
                .collect();
            match direct.as_slice() {
                [only] => only.clone(),
                [] => {
                    return Err(format!(
                        "no point-to-point RoCE v2 address on the node; name the one cabled \
                         to this Mac with --rdma-node-address (found: {})",
                        listing(&candidates)
                    ))
                }
                several => {
                    return Err(format!(
                        "several point-to-point RoCE v2 addresses on the node; pick one with \
                         --rdma-node-address (found: {})",
                        listing(several)
                    ))
                }
            }
        }
    };
    let mac_address = match &request.mac_address {
        Some(address) => address
            .parse::<Ipv4Addr>()
            .map_err(|_| format!("--rdma-mac-address {address:?} is not an IPv4 address"))?,
        None => point_to_point_peer(chosen.address, chosen.prefix).ok_or_else(|| {
            format!(
                "{}/{} is not point-to-point, so this Mac's address on the link cannot be \
                 derived; pass --rdma-mac-address",
                chosen.address, chosen.prefix
            )
        })?,
    };
    if mac_address == chosen.address {
        return Err("the Mac and the node cannot share one RDMA address".into());
    }
    Ok(RdmaLane {
        node_device: chosen.device,
        node_gid_index: chosen.gid_index,
        node_netdev: chosen.netdev,
        node_address: chosen.address.to_string(),
        node_mac: chosen.mac,
        mac_address: mac_address.to_string(),
    })
}

/// Reads the node's RoCE v2 GID table and picks the lane.
pub fn discover(ssh: &Ssh, request: &RdmaRequest) -> Result<RdmaLane> {
    choose(parse(&ssh.run(DISCOVER, &[])?), request)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPARK: &str = "rocep1s0f1\t3\tmac-rdma-bond\t192.168.200.2/30\t4c:bb:47:7d:a1:a5\n\
                         roceP2p1s0f1\t5\tenP2p1s0f1np1\t10.0.0.7/24\tAA:BB:CC:DD:EE:01\n";

    #[test]
    fn the_cabled_link_is_the_point_to_point_one() {
        let lane = choose(parse(SPARK), &RdmaRequest::default()).unwrap();
        assert_eq!(
            lane,
            RdmaLane {
                node_device: "rocep1s0f1".into(),
                node_gid_index: 3,
                node_netdev: "mac-rdma-bond".into(),
                node_address: "192.168.200.2".into(),
                node_mac: "4c:bb:47:7d:a1:a5".into(),
                mac_address: "192.168.200.1".into(),
            }
        );
        assert_eq!(lane.handoff_section()["gid_index"], 3);
        assert_eq!(lane.cluster_section()["gid_index"], -1);
        assert_eq!(lane.cluster_section()["peer_mac"], "4c:bb:47:7d:a1:a5");
    }

    #[test]
    fn point_to_point_peers() {
        let ip = |text: &str| text.parse::<Ipv4Addr>().unwrap();
        assert_eq!(point_to_point_peer(ip("192.168.200.2"), 30), Some(ip("192.168.200.1")));
        assert_eq!(point_to_point_peer(ip("192.168.200.1"), 30), Some(ip("192.168.200.2")));
        assert_eq!(point_to_point_peer(ip("192.168.200.0"), 30), None);
        assert_eq!(point_to_point_peer(ip("10.0.0.4"), 31), Some(ip("10.0.0.5")));
        assert_eq!(point_to_point_peer(ip("10.0.0.7"), 24), None);
    }

    #[test]
    fn a_lan_address_needs_both_ends_named() {
        let only_lan = "roceP2p1s0f1\t5\tenP2p1s0f1np1\t10.0.0.7/24\taa:bb:cc:dd:ee:01\n";
        let error = choose(parse(only_lan), &RdmaRequest::default()).unwrap_err();
        assert!(error.contains("--rdma-node-address"), "{error}");
        let error = choose(
            parse(only_lan),
            &RdmaRequest {
                want: Some(true),
                node_address: Some("10.0.0.7".into()),
                mac_address: None,
            },
        )
        .unwrap_err();
        assert!(error.contains("--rdma-mac-address"), "{error}");
        let lane = choose(
            parse(only_lan),
            &RdmaRequest {
                want: Some(true),
                node_address: Some("10.0.0.7".into()),
                mac_address: Some("10.0.0.9".into()),
            },
        )
        .unwrap();
        assert_eq!(lane.mac_address, "10.0.0.9");
    }

    #[test]
    fn a_node_without_roce_v2_says_what_to_fix() {
        let error = choose(parse(""), &RdmaRequest::default()).unwrap_err();
        assert!(error.contains("no active RoCE v2"), "{error}");
    }
}
