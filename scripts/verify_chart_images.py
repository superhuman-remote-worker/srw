#!/usr/bin/env python3
"""Refuse chart publication until every stamped image is available and verified.

NEEDS_JSON is the workflow's ``toJSON(needs)`` value. Develop supplies component
identities and rebuild decisions from its changes job. Main uses --release-sha
and requires all component builds. Neither mode substitutes the run SHA for a
missing component identity or treats a failed rebuild as reusable old output.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Callable


COMPONENTS = (
    "orchestrator",
    "agent",
    "cockpit",
    "mcp",
    "workspace",
    "vm-controller",
    "vm-preparer",
    "driver-shim",
    "driver-mcp-front",
    "driver-git-swap",
    "fuse-opener",
)
# Images that another component's job builds from the same inputs. They share
# that component's identity and rebuild decision and have no job of their own.
COBUILT = {"workspace-minimal": "workspace", "cloud-mount": "fuse-opener"}
_SHA = re.compile(r"[0-9a-f]{40}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


def expected_images(
    needs: dict[str, Any], repository: str, release_sha: str | None = None
) -> dict[str, str]:
    """Validate the build graph first, then return the exact refs to inspect."""
    if needs.get("architecture", {}).get("result") != "success":
        raise ValueError("architecture validation did not succeed")
    if release_sha is None:
        changes = needs.get("changes", {})
        if changes.get("result") != "success":
            raise ValueError("component identity detection did not succeed")
        outputs = changes.get("outputs", {})
    else:
        outputs = {
            key: value
            for component in COMPONENTS
            for key, value in ((component, "true"), (component + "-sha", release_sha))
        }

    refs = {}
    for component in COMPONENTS:
        changed = outputs.get(component)
        sha = outputs.get(component + "-sha", "")
        result = needs.get("build-" + component, {}).get("result")
        if changed not in {"true", "false"}:
            raise ValueError(f"{component}: missing rebuild decision")
        if not isinstance(sha, str) or not _SHA.fullmatch(sha):
            raise ValueError(f"{component}: missing or invalid source revision")
        if result not in {"success", "skipped"} or (
            changed == "true" and result != "success"
        ):
            raise ValueError(
                f"{component}: rebuild={changed}, build result={result!r}; "
                "refusing publication"
            )
        refs[component] = f"{repository}-{component}:sha-{sha[:7]}"

    for image, component in COBUILT.items():
        refs[image] = refs[component].replace(
            f"{repository}-{component}:", f"{repository}-{image}:", 1
        )
    return refs


def inspect_digest(ref: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "buildx",
            "imagetools",
            "inspect",
            ref,
            "--format",
            "{{.Manifest.Digest}}",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result.stdout.strip()


def verify_images(
    refs: dict[str, str], inspect: Callable[[str], str] = inspect_digest
) -> dict[str, dict[str, str]]:
    verified = {}
    for component, ref in refs.items():
        digest = inspect(ref)
        if not _DIGEST.fullmatch(digest):
            raise ValueError(
                f"{component}: registry returned no valid digest for {ref}"
            )
        verified[component] = {"ref": ref, "digest": digest}
    return verified


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--release-sha")
    parser.add_argument("--inventory", type=Path)
    args = parser.parse_args(argv)
    try:
        needs = json.loads(os.environ["NEEDS_JSON"])
        refs = expected_images(needs, args.repository, args.release_sha)
        verified = verify_images(refs)
    except (KeyError, ValueError, subprocess.SubprocessError, OSError) as error:
        print(f"ERROR: chart image verification failed: {error}", file=sys.stderr)
        return 1

    # Write outputs only after the complete set passed, never partial success.
    if github_env := os.environ.get("GITHUB_ENV"):
        with Path(github_env).open("a") as output:
            for component, item in verified.items():
                name = component.replace("-", "_").upper() + "_DIGEST"
                output.write(f"{name}={item['digest']}\n")
    rendered = json.dumps(verified, indent=2, sort_keys=True) + "\n"
    if args.inventory:
        args.inventory.parent.mkdir(parents=True, exist_ok=True)
        args.inventory.write_text(rendered)
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
