import copy
import json
import unittest

from tests.e2e.native_task_exit import decode_task_exit


CID = "a" * 64
START = 1_792_016_000_000_000_000
END = START + 60_000_000_000
PRODUCER = {
    "client_version": "v2.1.4-k3s1",
    "server_version": "v2.1.4-k3s1",
    "client_revision": "",
    "server_revision": "",
    "ctr_sha256": "b32fa7d0c65c358bdb541d246ca133a6c8fbcae842a047f9a3aa0ca379413279",
    "source_revision": "54cce777dc9690f51ddc0b7c740553318894b961",
    "serializer": "containerd-TaskExit-encoding/json-omitempty-v1",
}


def event():
    return {
        "container_id": CID,
        "id": CID,
        "pid": 123,
        "exited_at": {"seconds": START // 1_000_000_000 + 1, "nanos": 25},
    }


def line(value=None, *, topic="/tasks/exit", namespace="k8s.io"):
    if value is None:
        value = event()
    return (
        "2026-10-15 00:00:00.000 +0000 UTC "
        + namespace
        + " "
        + topic
        + " "
        + json.dumps(value)
    )


def decode(raw=None, producer=None):
    return decode_task_exit(
        line() if raw is None else raw,
        container_id=CID,
        pid=123,
        window_start_ns=START,
        window_end_ns=END,
        producer=PRODUCER if producer is None else producer,
    )


class TaskExitTests(unittest.TestCase):
    def test_actual_blank_revisions_are_bound_to_the_controlled_binary(self):
        try:
            result = decode()
        except ValueError as error:
            self.fail("Exact controlled binary was rejected: " + str(error))
        self.assertEqual(result["exit_status"], 0)

    def test_other_observed_binary_version_is_separately_bound(self):
        profile = dict(
            PRODUCER,
            client_version="v2.1.5-k3s1.33",
            server_version="v2.1.5-k3s1.33",
            ctr_sha256="ce4df0dae6b2b9f3d0515f9e33130d854f62d87e93256fcdc32f1a83de2dfa05",
            source_revision="d2967760a43f57e0be2fe8143aff6f6ef2e230d7",
        )
        self.assertEqual(decode(producer=profile)["exit_status"], 0)
        profile["ctr_sha256"] = PRODUCER["ctr_sha256"]
        with self.assertRaises(ValueError):
            decode(producer=profile)

    def test_unknown_binary_digest_and_source_tag_cannot_qualify(self):
        for key, value in [("ctr_sha256", "b" * 64), ("source_revision", "f" * 40)]:
            profile = dict(PRODUCER)
            profile[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                decode(producer=profile)

    def test_qualified_omitted_uint32_is_zero_without_rewriting_event(self):
        raw = line()
        result = decode(raw)
        self.assertEqual(result["exit_status"], 0)
        self.assertEqual(result["status_source"], "go-json-omitempty")
        self.assertNotIn("exit_status", result["raw_event"])
        self.assertEqual(result["raw_line"], raw)

    def test_explicit_zero_and_nonzero_stay_distinct(self):
        for status in [0, 1, 23, 137, 4294967295]:
            with self.subTest(status=status):
                raw = event() | {"exit_status": status}
                result = decode(line(raw))
                self.assertEqual(result["exit_status"], status)
                self.assertEqual(result["status_source"], "explicit")

    def test_bad_status_never_becomes_success(self):
        for status in [None, True, False, "0", 0.0, -1, 4294967296, {}, []]:
            with self.subTest(status=status), self.assertRaises(ValueError):
                decode(line(event() | {"exit_status": status}))

    def test_foreign_or_missing_init_identity_rejected(self):
        for key, bad in [
            ("container_id", "c" * 64),
            ("id", "exec-other"),
            ("pid", 124),
            ("pid", 0),
            ("pid", True),
        ]:
            with self.subTest(key=key, bad=bad), self.assertRaises(ValueError):
                decode(line(event() | {key: bad}))
        for key in ["container_id", "id", "pid", "exited_at"]:
            value = event()
            del value[key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                decode(line(value))

    def test_timestamp_requires_valid_protobuf_shape_and_original_window(self):
        for value in [
            None,
            {},
            {"seconds": 0},
            {"seconds": "1", "nanos": 0},
            {"seconds": START // 10**9, "nanos": -1},
            {"seconds": START // 10**9, "nanos": 10**9},
            {"seconds": START // 10**9 - 1, "nanos": 0},
            {"seconds": END // 10**9 + 1, "nanos": 0},
        ]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                decode(line(event() | {"exited_at": value}))
        # Go's encoding/json also omits zero-valued timestamp nanos.
        self.assertEqual(
            decode(line(event() | {"exited_at": {"seconds": START // 10**9 + 1}}))[
                "exit_status"
            ],
            0,
        )

    def test_topic_namespace_and_json_type_are_required(self):
        for raw in [
            line(topic="/tasks/delete"),
            line(namespace="other"),
            "garbage " + json.dumps(event()),
            line([]),
            line("string"),
            line() + " {}",
            line() + "\n" + line(),
            "x" * 16385,
            line().replace('"pid": 123', '"pid": 123, "pid": 123'),
        ]:
            with self.subTest(raw=raw[:60]), self.assertRaises(ValueError):
                decode(raw)

    def test_unknown_or_unqualified_producer_rejected_even_for_explicit_zero(self):
        for key, value in [
            ("client_version", "v2.1.99"),
            ("server_version", "v2.1.5-k3s1.33"),
            ("client_revision", "foreign"),
            ("server_revision", "foreign"),
            ("ctr_sha256", ""),
            ("serializer", "unknown"),
        ]:
            producer = copy.deepcopy(PRODUCER)
            producer[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                decode(line(event() | {"exit_status": 0}), producer)


if __name__ == "__main__":
    unittest.main()
