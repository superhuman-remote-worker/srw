#!/usr/bin/env python3
"""Unprivileged, read-only network evidence for the explicit NoCloud profile.

This file is installed as guest user-data only for opted-in disks. The legacy
cloud-init Secret remains byte-for-byte unchanged when the profile is absent.
"""

import configparser
import glob
import hashlib
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
    config = configparser.ConfigParser(interpolation=None, strict=False)
    try:
        config.read_string(content)
    except configparser.Error:
        return None
    match = config["Match"] if config.has_section("Match") else {}
    network = config["Network"] if config.has_section("Network") else {}
    if (
        match.get("Name") != "enp1s0"
        or set(match) != {"name"}
        or network.get("DHCP", "").lower() != "yes"
    ):
        return None
    return raw_path


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
    for item in raw_interfaces:
        if not isinstance(item, dict):
            continue
        local = next(
            (entry.get("local") for entry in item.get("addr_info", [])
             if isinstance(entry, dict) and entry.get("local")), None
        )
        if item.get("ifname") != "lo" and local and item.get("address"):
            interfaces.append({
                "ifname": item["ifname"], "address": local, "mac": item["address"]
            })
    default_route = next(
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
        "address": interfaces[0]["address"] if interfaces else None,
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
            "name_only_dhcp": rule_hash is not None,
            "network_file_sha256": rule_hash,
        },
    }


def main():
    evidence = collect(sys.argv[1]) if len(sys.argv) == 2 else None
    if evidence is None:
        raise SystemExit(2)
    print(json.dumps(evidence, sort_keys=True))


if __name__ == "__main__":
    main()
