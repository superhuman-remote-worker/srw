"""Decode only reviewed ctr TaskExit producers, retaining raw evidence.

ctr uses Go encoding/json on a typed TaskExit; exit_status is uint32 with
omitempty. Missing means zero for these exact qualified producers only.
The caller supplies the actual binary digest and matching client/server
version/revision from a receipt collected before subscribing to events.
These installed builds report empty revision strings: source-tag commits
are separate references, not invented installed-build revisions. Each
binary below has actual controlled zero AND nonzero native-exit receipts.
"""

import json
import re


REVISIONS = {
    "v2.1.4-k3s1": "54cce777dc9690f51ddc0b7c740553318894b961",
    "v2.1.5-k3s1.33": "d2967760a43f57e0be2fe8143aff6f6ef2e230d7",
}
BINARIES = {
    "v2.1.4-k3s1": "b32fa7d0c65c358bdb541d246ca133a6c8fbcae842a047f9a3aa0ca379413279",
    "v2.1.5-k3s1.33": "ce4df0dae6b2b9f3d0515f9e33130d854f62d87e93256fcdc32f1a83de2dfa05",
}


def need(value, reason):
    if not value:
        raise ValueError("native_task_exit:" + reason)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        need(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def decode_task_exit(
    line, *, container_id, pid, window_start_ns, window_end_ns, producer
):
    need(type(producer) is dict, "producer_shape")
    version = producer.get("client_version")
    need(type(version) is str and version in REVISIONS, "unqualified_version")
    revision = REVISIONS[version]
    need(
        producer.get("server_version") == version
        and producer.get("source_revision") == revision
        and producer.get("client_revision") == producer.get("server_revision") == ""
        and producer.get("serializer")
        == "containerd-TaskExit-encoding/json-omitempty-v1"
        and producer.get("ctr_sha256") == BINARIES[version],
        "unqualified_producer",
    )
    need(
        type(container_id) is str
        and re.fullmatch("[0-9a-f]{64}", container_id) is not None
        and type(pid) is int
        and 0 < pid < 2**32
        and type(window_start_ns) is int
        and type(window_end_ns) is int
        and 0 < window_start_ns < window_end_ns
        and window_end_ns - window_start_ns <= 1200 * 10**9,
        "expected_identity_or_window",
    )
    need(
        type(line) is str
        and len(line.encode()) <= 16384
        and "\n" not in line
        and "\r" not in line,
        "bounded_single_line",
    )
    prefix = re.match(r"^\d{4}-\d{2}-\d{2} [^{}\r\n]+ k8s\.io /tasks/exit ", line)
    need(prefix is not None, "typed_envelope")
    try:
        event = json.loads(line[prefix.end() :], object_pairs_hook=unique_object)
    except (ValueError, TypeError) as error:
        raise ValueError("native_task_exit:invalid_json") from error
    need(
        type(event) is dict
        and set(event) <= {"container_id", "id", "pid", "exit_status", "exited_at"}
        and event.get("container_id") == container_id
        and event.get("id") == container_id
        and type(event.get("pid")) is int
        and event["pid"] == pid,
        "exact_init_identity",
    )
    timestamp = event.get("exited_at")
    need(
        type(timestamp) is dict
        and set(timestamp) <= {"seconds", "nanos"}
        and type(timestamp.get("seconds")) is int
        and type(timestamp.get("nanos", 0)) is int
        and 0 <= timestamp.get("nanos", 0) < 10**9,
        "timestamp_shape",
    )
    exited = timestamp["seconds"] * 10**9 + timestamp.get("nanos", 0)
    need(window_start_ns <= exited <= window_end_ns, "original_observation_window")
    status = event.get("exit_status", 0)
    need(type(status) is int and 0 <= status < 2**32, "uint32_status")
    return {
        "container_id": container_id,
        "pid": pid,
        "exited_at_ns": exited,
        "exit_status": status,
        "positive_init_exit": status == 0,
        "status_source": "explicit" if "exit_status" in event else "go-json-omitempty",
        "raw_event": event,
        "raw_line": line,
    }
