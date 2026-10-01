#!/usr/bin/env python3
"""Reuse a recent VM base only when its recorded build inputs match this tree."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
INPUTS = (
    "docker/agent-vm-base/scripts/provision-stage1.sh",
    "docker/agent-vm-base/scripts/cleanup-stage1.sh",
    "docker/agent-vm-base/stage1.pkr.hcl",
    "docker/agent-vm-base/Dockerfile.containerDisk-stage1",
    "docker/agent-vm-base/cloud-init",
    "docker/assert-browser-stack.sh",
    ".playwright-version",
    "scripts/vm_stage1_image.py",
    # Build invocations can change inputs too. Conservatively invalidate for
    # edits to any workflow that produces this base.
    ".github/workflows/main.yml",
    ".github/workflows/develop.yml",
    ".github/workflows/stage1-rebuild.yml",
)
LABEL = "io.srw.stage1-inputs"


def input_digest(root=ROOT):
    digest = hashlib.sha256(b"srw-stage1-inputs-v1\0")
    files = []
    for name in INPUTS:
        path = root / name
        if not path.exists():
            raise ValueError(f"Missing stage1 input: {name}")
        if path.is_dir():
            files.extend(p for p in path.rglob("*") if p.is_file())
        else:
            files.append(path)
    for path in sorted(files):
        data = path.read_bytes()
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(len(data).to_bytes(8, "big") + data)
    return digest.hexdigest()


def inspect_json(image, field):
    result = subprocess.run(
        [
            "docker",
            "buildx",
            "imagetools",
            "inspect",
            image,
            "--format",
            "{{json ." + field + "}}",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=90,
    )
    return json.loads(result.stdout)


def reusable_image(image, expected, *, now=None, inspect=inspect_json):
    """Resolve the tag once, then inspect that immutable image's small config."""
    now = now or datetime.now(timezone.utc)
    try:
        manifest = inspect(image, "Manifest")
        digest = manifest.get("digest", "")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            return None
        repository = image.split("@")[0].rsplit(":", 1)[0]
        pinned = repository + "@" + digest
        config = inspect(pinned, "Image")
        labels = config.get("config", {}).get("Labels", {})
        created = datetime.fromisoformat(config["created"].replace("Z", "+00:00"))
        if (
            labels.get(LABEL) != expected
            or labels.get("io.srw.component") != "workspace-stage1"
            or config.get("architecture") != "amd64"
            or config.get("os") != "linux"
            or not timedelta(0) <= now - created <= timedelta(days=8)
        ):
            return None
        return pinned
    except (
        subprocess.SubprocessError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
    ):
        # Missing, inaccessible, legacy, malformed or old images trigger a build.
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", help="Channel or commit tag to inspect")
    parser.add_argument("--require-match", action="store_true")
    args = parser.parse_args()
    expected = input_digest()
    if not args.image:
        print(expected)
        return 0
    pinned = reusable_image(args.image, expected)
    print(f"inputs={expected}")
    print(f"reuse={'true' if pinned else 'false'}")
    if pinned:
        print(f"image={pinned}")
    return 1 if args.require_match and not pinned else 0


if __name__ == "__main__":
    raise SystemExit(main())
