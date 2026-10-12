"""Exact VM SSH binding shared by admission and live-channel renewal."""

from __future__ import annotations

import hashlib
import json
from typing import Any
from uuid import UUID


def vm_binding_digest(proof: Any) -> str:
    fields = (
        str(UUID(str(proof.workspace_generation))),
        str(UUID(str(proof.vm_uid))),
        str(UUID(str(proof.vmi_uid))),
        str(UUID(str(proof.launcher_pod_uid))),
        str(UUID(str(proof.rootdisk_pvc_uid))),
        str(proof.ssh_host_key_fingerprint),
        str(proof.host),
        str(proof.port),
    )
    if (
        not fields[5].startswith("SHA256:")
        or not fields[6]
        or not 1 <= len(fields[6]) <= 255
        or not isinstance(proof.port, int)
        or not 1 <= proof.port <= 65535
    ):
        raise ValueError("invalid VM SSH binding")
    return hashlib.sha256(
        json.dumps(fields, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()
