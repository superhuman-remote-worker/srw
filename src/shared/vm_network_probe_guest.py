#!/usr/bin/env python3
"""Unprivileged, read-only network evidence for the explicit NoCloud profile.

This file is installed as guest user-data only for opted-in disks. The legacy
cloud-init Secret remains byte-for-byte unchanged when the profile is absent.
"""

import configparser
import glob
import hashlib
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys


def command(*argv):
    try:
        result = subprocess.run(argv, check=False, capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def safe_text(path):
    try:
        return path.read_text(errors="replace").strip()
    except OSError:
        return None


def readable_hashes(pattern):
    result = {}
    for raw in sorted(glob.glob(pattern)):
        path = Path(raw)
        if not path.is_file():
            continue
        try:
            result[raw] = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            # Agent-host does not have root's access to cloud-init/netplan.
            # An unreadable file is never presented as verified evidence.
            continue
    return result


def parse_networkd_rule(status, content):
    """Accept only networkd's selected single interface-name DHCP rule."""
    if not isinstance(status, str) or not isinstance(content, str):
        return None
    found = re.search(r"(?m)^\s*Network File:\s*(\S+)\s*$", status)
    if found is None:
        return None
    # A drop-in can add static settings after the named file was generated.
    if re.search(r"(?m)^\s*Network File Drop-Ins:\s*", status):
        return None
    raw_path = found.group(1)
    path = PurePosixPath(raw_path)
    if (
        ".." in path.parts
        or not (
            raw_path.startswith("/run/systemd/network/")
            or raw_path.startswith("/etc/systemd/network/")
        )
        or path.suffix != ".network"
    ):
        return None
    config = configparser.ConfigParser(interpolation=None, strict=True)
    try:
        config.read_string(content)
    except configparser.Error:
        return None
    match = config["Match"] if config.has_section("Match") else {}
    network = config["Network"] if config.has_section("Network") else {}
    # A DHCP client can coexist with static settings in the same selected
    # rule. A name match and DHCP=yes alone do not prove a reusable address.
    if set(config.sections()) - {"Match", "Network", "DHCP"}:
        return None
    if set(network) - {"dhcp", "linklocaladdressing", "ipv6acceptra"}:
        return None
    dhcp = config["DHCP"] if config.has_section("DHCP") else {}
    if set(dhcp) - {"routemetric", "usemtu"}:
        return None
    if (
        match.get("Name") != "enp1s0"
        or set(match) != {"name"}
        or network.get("DHCP", "").lower() != "yes"
    ):
        return None
    return raw_path


def dhcp4_lease(root, ifindex):
    """Read networkd's bounded per-link DHCP lease; unknown is not proof."""
    if type(ifindex) is not int or not 0 < ifindex < 2**31:
        return None
    path = root / "run/systemd/netif/leases" / str(ifindex)
    try:
        with path.open("rb") as source:
            raw = source.read(16385)
    except OSError:
        return None
    if len(raw) > 16384:
        return None
    try:
        content = raw.decode("ascii")
    except UnicodeDecodeError:
        return None
    fields = {}
    for line in content.splitlines():
        if not line or line.startswith("#"):
            continue
        key, marker, value = line.partition("=")
        if not marker or key in fields:
            return None
        fields[key] = value
    address = fields.get("ADDRESS")
    routers = (fields.get("ROUTER") or "").split()
    try:
        ipaddress.IPv4Address(address)
        for router in routers:
            ipaddress.IPv4Address(router)
    except (ipaddress.AddressValueError, TypeError):
        return None
    if not routers:
        return None
    return address, set(routers), hashlib.sha256(raw).hexdigest()


def collect(challenge, *, root=Path("/"), run=command):
    if not isinstance(challenge, str) or not challenge:
        return None
    try:
        raw_interfaces = json.loads(run("ip", "-j", "address") or "null")
        routes = json.loads(run("ip", "-j", "route") or "null")
    except (TypeError, ValueError):
        return None
    if not isinstance(raw_interfaces, list) or not isinstance(routes, list):
        return None
    interfaces = []
    selected_address = None
    selected_route = None
    lease_proof = None
    for item in raw_interfaces:
        if not isinstance(item, dict):
            continue
        lease = dhcp4_lease(root, item.get("ifindex")) if item.get("ifname") == "enp1s0" else None
        address_info = item.get("addr_info", [])
        if not isinstance(address_info, list):
            continue
        ipv4_addresses = [entry.get("local") for entry in address_info
                          if isinstance(entry, dict) and entry.get("family") == "inet"]
        local = (
            lease[0] if lease and ipv4_addresses == [lease[0]] else None
        )
        if local is None:
            local = next(
                (entry.get("local") for entry in address_info
                 if isinstance(entry, dict) and entry.get("local")), None
            )
        if item.get("ifname") != "lo" and local and item.get("address"):
            interfaces.append({
                "ifname": item["ifname"], "address": local, "mac": item["address"]
            })
        if lease and local == lease[0] and ipv4_addresses == [lease[0]]:
            default_ipv4_routes = [route for route in routes if isinstance(route, dict)
                                   and route.get("dst") in ("default", "0.0.0.0/0")]
            if (len(default_ipv4_routes) == 1
                    and default_ipv4_routes[0].get("dev") == "enp1s0"
                    and default_ipv4_routes[0].get("protocol") == "dhcp"
                    and default_ipv4_routes[0].get("gateway") in lease[1]):
                selected_address = local
                selected_route = default_ipv4_routes[0]
                lease_proof = (item["ifindex"], lease[2])
    default_route = selected_route or next(
        (route for route in routes if isinstance(route, dict) and
         route.get("dst") in ("default", "0.0.0.0/0", "::/0")), None
    )
    status = run("networkctl", "status", "enp1s0", "--no-pager", "--full")
    selected = re.search(r"(?m)^\s*Network File:\s*(\S+)\s*$", status or "")
    rule_path = selected.group(1) if selected else None
    rule_text = None
    if rule_path and (
        rule_path.startswith("/run/systemd/network/")
        or rule_path.startswith("/etc/systemd/network/")
    ) and ".." not in PurePosixPath(rule_path).parts:
        rule_text = safe_text(root / rule_path.lstrip("/"))
    selected_rule = parse_networkd_rule(status, rule_text)
    cache_link = root / "var/lib/cloud/instance"
    try:
        cache_target = os.readlink(cache_link)
    except OSError:
        cache_target = None
    cached_id = PurePosixPath(cache_target).name if cache_target else None
    instance_id = safe_text(root / "var/lib/cloud/data/instance-id")
    if not instance_id:
        instance_id = run("cloud-init", "query", "instance_id")
    netplan = readable_hashes(str(root / "etc/netplan/*"))
    networkd = {
        **readable_hashes(str(root / "etc/systemd/network/*")),
        **readable_hashes(str(root / "run/systemd/network/*.network")),
    }
    rule_hash = networkd.get(str(root / selected_rule.lstrip("/"))) if selected_rule else None
    return {
        "challenge": challenge,
        "boot_id": safe_text(root / "proc/sys/kernel/random/boot_id"),
        "machine_id": safe_text(root / "etc/machine-id"),
        "interfaces": interfaces,
        "address": selected_address or (interfaces[0]["address"] if interfaces else None),
        "routes": routes,
        "default_route": default_route,
        "dns": safe_text(root / "etc/resolv.conf"),
        "netplan_sha256": netplan,
        "networkd_sha256": networkd,
        "cloud_init_instance_id": instance_id,
        "cloud_init_cached_instance_id": cached_id,
        "cloud_init_cache_identity": (
            hashlib.sha256(cache_target.encode()).hexdigest() if cache_target else None
        ),
        "cloud_init_cache_cleaned": False,
        "network_profile_rule": {
            "kind": "networkd-name-dhcp-v1",
            "interface": "enp1s0",
            "name_only_dhcp": rule_hash is not None and lease_proof is not None,
            "network_file_sha256": rule_hash,
            "dhcp4_address": selected_address,
            "dhcp4_gateway": selected_route.get("gateway") if selected_route else None,
            "dhcp4_lease_sha256": lease_proof[1] if lease_proof else None,
            "dhcp4_ifindex": lease_proof[0] if lease_proof else None,
        },
    }


def main():
    evidence = collect(sys.argv[1]) if len(sys.argv) == 2 else None
    if evidence is None:
        raise SystemExit(2)
    print(json.dumps(evidence, sort_keys=True))


if __name__ == "__main__":
    main()
