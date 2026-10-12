"""Tests for the rclone-subprocess ObjectStore (production virtual-tier transport).

rclone is invoked as a subprocess against a single configured remote. These
tests mock subprocess.run entirely (no rclone binary, no network) and pin the
two things that matter: the exact command + credential-env we hand rclone, and
how we parse its output (lsjson --stat for head, lsjson -R for list) including
the file-vs-prefix distinction and missing-object signalling. Mirrors the
paramiko-mock approach in test_workspace_backends.py.
"""

import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from unittest.mock import patch

import pytest

from shared.runtime.core.backends.object_store import (  # noqa: E402
    InMemoryObjectStore,
    ObjectStoreError,
)
from shared.runtime.core.backends.rclone import (  # noqa: E402
    RcloneObjectStore,
    RcloneSizeLimitExceeded,
    object_store_from_spec,
)


def _cp(returncode=0, stdout=b"", stderr=b"") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr
    )


@pytest.fixture
def store() -> RcloneObjectStore:
    return RcloneObjectStore(
        remote_type="s3",
        config={
            "provider": "Minio",
            "access_key_id": "AKIA",
            "secret_access_key": "shh",
            "endpoint": "http://minio.minio.svc:9000",
        },
        root="my-bucket",
    )


@pytest.fixture
def run_mock():
    with patch("shared.runtime.core.backends.rclone.subprocess.run") as m:
        yield m


class TestConstruction:
    def test_requires_type(self):
        with pytest.raises(ValueError, match="remote_type"):
            RcloneObjectStore(remote_type="")

    def test_env_overlay_has_type_and_config(self, store):
        env = store._env_overlay
        assert env["RCLONE_CONFIG_SRW_TYPE"] == "s3"
        assert env["RCLONE_CONFIG_SRW_ACCESS_KEY_ID"] == "AKIA"
        assert env["RCLONE_CONFIG_SRW_SECRET_ACCESS_KEY"] == "shh"
        assert env["RCLONE_CONFIG_SRW_ENDPOINT"] == "http://minio.minio.svc:9000"

    def test_remote_path_maps_key(self, store):
        assert store._remote_path("jobs/1/a.txt") == "srw:my-bucket/jobs/1/a.txt"

    def test_remote_path_without_root(self):
        s = RcloneObjectStore(remote_type="s3", root="")
        assert s._remote_path("k") == "srw:k"


class TestGet:
    def test_get_returns_stdout_bytes(self, store, run_mock):
        run_mock.side_effect = [
            _cp(stdout=b'{"IsDir":false,"Size":10}'),
            _cp(stdout=b"file bytes"),
        ]
        assert store.get("jobs/1/a.txt") == b"file bytes"
        argv = run_mock.call_args_list[1].args[0]
        assert argv == ["rclone", "cat", "srw:my-bucket/jobs/1/a.txt"]

    def test_get_passes_credential_env(self, store, run_mock):
        run_mock.side_effect = [
            _cp(stdout=b'{"IsDir":false,"Size":1}'),
            _cp(stdout=b"x"),
        ]
        store.get("k")
        env = run_mock.call_args.kwargs["env"]
        assert env["RCLONE_CONFIG_SRW_TYPE"] == "s3"
        assert env["RCLONE_CONFIG_SRW_ACCESS_KEY_ID"] == "AKIA"

    def test_get_valid_empty_ignores_noisy_success_stderr(self, store, run_mock):
        run_mock.side_effect = [
            _cp(stdout=b'{"IsDir":false,"Size":0}', stderr=b"not found in cache"),
            _cp(stdout=b"", stderr=b"not found in cache"),
            _cp(stdout=b'{"IsDir":false,"Size":0}', stderr=b"not found in cache"),
        ]
        assert store.get("empty") == b""

    def test_get_missing_raises_file_not_found(self, store, run_mock):
        run_mock.return_value = _cp(returncode=4, stderr=b"file not found")
        with pytest.raises(FileNotFoundError):
            store.get("ghost")

    def test_get_generic_error_with_missing_words_is_transport(self, store, run_mock):
        run_mock.side_effect = [
            _cp(stdout=b'{"IsDir":false,"Size":1}'),
            _cp(returncode=1, stderr=b"remote not found"),
        ]
        with pytest.raises(ObjectStoreError):
            store.get("ghost")

    def test_get_other_error_raises_object_store_error(self, store, run_mock):
        run_mock.side_effect = [
            _cp(stdout=b'{"IsDir":false,"Size":1}'),
            _cp(returncode=1, stderr=b"AccessDenied"),
        ]
        with pytest.raises(ObjectStoreError):
            store.get("k")


def _write_exact_read_rclone(directory: Path) -> Path:
    executable = directory / "fake-rclone"
    executable.write_text(
        """#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

command = sys.argv[1]
key = sys.argv[-1].split(':', 1)[-1].rsplit('/', 1)[-1]
if command == 'lsjson':
    if key == 'disappears':
        counter = Path(os.environ['RCLONE_CONFIG_SRW_COUNTER'])
        count = int(counter.read_text()) if counter.exists() else 0
        counter.write_text(str(count + 1))
        print(json.dumps({'IsDir': count > 0, 'Size': -1 if count > 0 else 0}))
    elif key == 'oversized-stat':
        sys.stdout.write('x' * 70000)
    elif key in {'missing', 'directory'}:
        print(json.dumps({'IsDir': True, 'Size': -1}))
    else:
        print(json.dumps({'IsDir': False, 'Size': 0 if key == 'empty' else 4}))
elif command == 'cat':
    if key == 'cat-missing':
        raise SystemExit(4)
    marker = os.environ.get('RCLONE_CONFIG_SRW_CAT_MARKER')
    if marker:
        Path(marker).write_text(key)
    sys.stdout.buffer.write(
        {'missing': b'', 'directory': b'child', 'empty': b'',
         'payload': b'data', 'disappears': b''}[key]
    )
else:
    raise SystemExit(2)
"""
    )
    executable.chmod(0o755)
    return executable


@pytest.mark.parametrize("reader", ["get", "get_bounded"])
@pytest.mark.parametrize(
    ("key", "expected"),
    [("missing", None), ("directory", None), ("empty", b""), ("payload", b"data")],
)
def test_exact_read_distinguishes_missing_directory_and_empty(
    tmp_path: Path, reader: str, key: str, expected: bytes | None
) -> None:
    marker = tmp_path / "cat-called"
    store = RcloneObjectStore(
        remote_type="local",
        config={"cat_marker": str(marker)},
        root="bucket",
        rclone_bin=str(_write_exact_read_rclone(tmp_path)),
        transfer_timeout=2,
        meta_timeout=2,
    )
    read = getattr(store, reader)
    if expected is None:
        with pytest.raises(FileNotFoundError):
            read(key, 16) if reader == "get_bounded" else read(key)
        assert not marker.exists()
    else:
        assert (read(key, 16) if reader == "get_bounded" else read(key)) == expected
        assert marker.read_text() == key


@pytest.mark.parametrize("reader", ["get", "get_bounded"])
def test_empty_cat_after_object_disappears_is_not_a_valid_empty_file(
    tmp_path: Path, reader: str
) -> None:
    store = RcloneObjectStore(
        remote_type="local",
        config={"counter": str(tmp_path / "stat-count")},
        rclone_bin=str(_write_exact_read_rclone(tmp_path)),
    )
    read = getattr(store, reader)
    with pytest.raises(FileNotFoundError):
        read("disappears", 16) if reader == "get_bounded" else read("disappears")
    assert (tmp_path / "stat-count").read_text() == "2"


@pytest.mark.parametrize("reader", ["get", "get_bounded"])
def test_cat_not_found_exit_after_valid_stat_is_missing(
    tmp_path: Path, reader: str
) -> None:
    store = RcloneObjectStore(
        remote_type="local", rclone_bin=str(_write_exact_read_rclone(tmp_path))
    )
    read = getattr(store, reader)
    with pytest.raises(FileNotFoundError):
        read("cat-missing", 16) if reader == "get_bounded" else read("cat-missing")


def test_bounded_stat_output_is_capped_and_zero_limit_keeps_empty(
    tmp_path: Path,
) -> None:
    store = RcloneObjectStore(
        remote_type="local", rclone_bin=str(_write_exact_read_rclone(tmp_path))
    )
    with pytest.raises(ObjectStoreError, match="stat output too large"):
        store.get_bounded("oversized-stat", 16)
    assert store.get_bounded("empty", 0) == b""
    with pytest.raises(RcloneSizeLimitExceeded):
        store.get_bounded("payload", 0)


def _write_hanging_stat_rclone(directory: Path) -> Path:
    executable = directory / "hanging-rclone"
    executable.write_text(
        """#!/usr/bin/env python3
import os
import signal
import sys
import time

started = os.environ['RCLONE_CONFIG_SRW_STARTED']
stopped = os.environ['RCLONE_CONFIG_SRW_STOPPED']
def stop(_signal, _frame):
    with open(stopped, 'w', encoding='utf-8') as handle:
        handle.write(str(os.getpid()))
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
with open(started, 'w', encoding='utf-8') as handle:
    handle.write(str(os.getpid()))
while True:
    time.sleep(1)
"""
    )
    executable.chmod(0o755)
    return executable


@pytest.mark.parametrize("cancel", [False, True])
def test_bounded_stat_timeout_or_cancellation_reaps_child(
    tmp_path: Path, cancel: bool
) -> None:
    started = tmp_path / "started"
    stopped = tmp_path / "stopped"
    store = RcloneObjectStore(
        remote_type="local",
        config={"started": str(started), "stopped": str(stopped)},
        rclone_bin=str(_write_hanging_stat_rclone(tmp_path)),
        meta_timeout=2,
        transfer_timeout=0.5,
    )
    cancellation = Event()
    began = time.monotonic()
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(
            store.get_bounded, "file", 16, cancelled=cancellation.is_set
        )
        for _ in range(200):
            if started.exists():
                break
            time.sleep(0.01)
        assert started.exists()
        if cancel:
            cancellation.set()
        with pytest.raises(
            ObjectStoreError, match="cancelled" if cancel else "timed out"
        ):
            result.result(timeout=4)
    assert time.monotonic() - began < 3
    assert stopped.exists()
    pid = int(stopped.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


class TestPut:
    def test_put_pipes_data_to_rcat(self, store, run_mock):
        run_mock.return_value = _cp()
        store.put("jobs/1/a.txt", b"payload")
        argv = run_mock.call_args.args[0]
        assert argv == ["rclone", "rcat", "srw:my-bucket/jobs/1/a.txt"]
        assert run_mock.call_args.kwargs["input"] == b"payload"

    def test_put_rejects_non_bytes(self, store):
        with pytest.raises(TypeError):
            store.put("k", "not bytes")  # type: ignore[arg-type]

    def test_put_failure_raises(self, store, run_mock):
        run_mock.return_value = _cp(returncode=1, stderr=b"quota exceeded")
        with pytest.raises(ObjectStoreError):
            store.put("k", b"x")


class TestHead:
    def test_head_file_returns_size(self, store, run_mock):
        run_mock.return_value = _cp(
            stdout=b'{"Path":"a.txt","Name":"a.txt","Size":42,"IsDir":false}'
        )
        assert store.head("jobs/1/a.txt") == 42
        argv = run_mock.call_args.args[0]
        assert argv == ["rclone", "lsjson", "--stat", "srw:my-bucket/jobs/1/a.txt"]

    def test_head_ignores_noisy_success_stderr(self, store, run_mock):
        run_mock.return_value = _cp(
            stdout=b'{"IsDir":false,"Size":0}', stderr=b"not found in cache"
        )
        assert store.head("empty") == 0

    def test_head_directory_returns_none(self, store, run_mock):
        run_mock.return_value = _cp(stdout=b'{"Path":"d","IsDir":true,"Size":-1}')
        assert store.head("d") is None

    def test_head_missing_returns_none(self, store, run_mock):
        run_mock.return_value = _cp(returncode=3, stderr=b"directory not found")
        assert store.head("ghost") is None

    def test_head_null_is_transport_error(self, store, run_mock):
        run_mock.return_value = _cp(stdout=b"null")
        with pytest.raises(ObjectStoreError):
            store.head("ghost")

    def test_head_bad_json_is_transport_error(self, store, run_mock):
        run_mock.return_value = _cp(stdout=b"not json")
        with pytest.raises(ObjectStoreError):
            store.head("k")

    @pytest.mark.parametrize("code", [1, 5, 6])
    def test_head_other_errors_are_not_missing(self, store, run_mock, code):
        run_mock.return_value = _cp(returncode=code, stderr=b"remote not found")
        with pytest.raises(ObjectStoreError):
            store.head("ghost")

    @pytest.mark.parametrize(
        "output",
        [
            b"[]",
            b"{}",
            b'{"IsDir":"false","Size":0}',
            b'{"IsDir":false}',
            b'{"IsDir":false,"Size":-1}',
            b'{"IsDir":false,"Size":true}',
        ],
    )
    def test_head_rejects_malformed_file_stat(self, store, run_mock, output):
        run_mock.return_value = _cp(stdout=output)
        with pytest.raises(ObjectStoreError):
            store.head("k")


class TestList:
    def test_list_parses_and_rebuilds_keys(self, store, run_mock):
        run_mock.return_value = _cp(
            stdout=(
                b'[{"Path":"a.txt","Size":3,"IsDir":false},'
                b'{"Path":"sub/b.txt","Size":5,"IsDir":false}]'
            )
        )
        result = store.list("jobs/1/")
        assert [(o.key, o.size) for o in result] == [
            ("jobs/1/a.txt", 3),
            ("jobs/1/sub/b.txt", 5),
        ]
        argv = run_mock.call_args.args[0]
        assert argv == [
            "rclone",
            "lsjson",
            "--recursive",
            "--files-only",
            "--no-modtime",
            "srw:my-bucket/jobs/1/",
        ]

    def test_list_filters_directories(self, store, run_mock):
        run_mock.return_value = _cp(
            stdout=(
                b'[{"Path":"d","IsDir":true,"Size":-1},'
                b'{"Path":"f.txt","IsDir":false,"Size":1}]'
            )
        )
        result = store.list("p/")
        assert [o.key for o in result] == ["p/f.txt"]

    def test_list_missing_prefix_returns_empty(self, store, run_mock):
        run_mock.return_value = _cp(returncode=1, stderr=b"directory not found")
        assert store.list("ghost/") == []

    def test_list_other_error_raises(self, store, run_mock):
        run_mock.return_value = _cp(returncode=1, stderr=b"AccessDenied")
        with pytest.raises(ObjectStoreError):
            store.list("p/")

    def test_list_sorted(self, store, run_mock):
        run_mock.return_value = _cp(
            stdout=(
                b'[{"Path":"z","Size":0,"IsDir":false},'
                b'{"Path":"a","Size":0,"IsDir":false}]'
            )
        )
        assert [o.key for o in store.list("")] == ["a", "z"]


class TestDelete:
    def test_delete_success_returns_true(self, store, run_mock):
        run_mock.return_value = _cp()
        assert store.delete("jobs/1/a.txt") is True
        argv = run_mock.call_args.args[0]
        assert argv == ["rclone", "deletefile", "srw:my-bucket/jobs/1/a.txt"]

    def test_delete_missing_returns_false(self, store, run_mock):
        run_mock.return_value = _cp(returncode=1, stderr=b"object not found")
        assert store.delete("ghost") is False

    def test_delete_other_error_raises(self, store, run_mock):
        run_mock.return_value = _cp(returncode=1, stderr=b"AccessDenied")
        with pytest.raises(ObjectStoreError):
            store.delete("k")


class TestCopy:
    def test_copy_uses_copyto(self, store, run_mock):
        run_mock.return_value = _cp()
        store.copy("a.txt", "b.txt")
        argv = run_mock.call_args.args[0]
        assert argv == [
            "rclone",
            "copyto",
            "srw:my-bucket/a.txt",
            "srw:my-bucket/b.txt",
        ]

    def test_copy_missing_source_raises_file_not_found(self, store, run_mock):
        run_mock.return_value = _cp(returncode=1, stderr=b"source object not found")
        with pytest.raises(FileNotFoundError):
            store.copy("ghost", "dst")


class TestRunErrors:
    def test_binary_missing_raises_object_store_error(self, store, run_mock):
        run_mock.side_effect = FileNotFoundError()
        with pytest.raises(ObjectStoreError, match="not found on PATH"):
            store.get("k")

    def test_timeout_raises_object_store_error(self, store, run_mock):
        run_mock.side_effect = subprocess.TimeoutExpired(cmd="rclone", timeout=1)
        with pytest.raises(ObjectStoreError, match="timed out"):
            store.get("k")


class TestConnect:
    def test_connect_ok_when_binary_present(self, store):
        with patch(
            "shared.runtime.core.backends.rclone.shutil.which",
            return_value="/usr/bin/rclone",
        ):
            store.connect()  # must not raise

    def test_connect_raises_when_binary_missing(self, store):
        with patch(
            "shared.runtime.core.backends.rclone.shutil.which", return_value=None
        ):
            with pytest.raises(ObjectStoreError, match="not found on PATH"):
                store.connect()


class TestObjectStoreFromSpec:
    def test_memory_type_returns_in_memory_store(self):
        s = object_store_from_spec({"type": "memory"})
        assert isinstance(s, InMemoryObjectStore)

    def test_nested_rclone_spec(self):
        s = object_store_from_spec(
            {
                "name": "workspace",
                "prefix": "jobs/1/",
                "rclone_spec": {
                    "type": "s3",
                    "config": {"access_key_id": "K"},
                    "root": "bucket-x",
                },
            }
        )
        assert isinstance(s, RcloneObjectStore)
        assert s._type == "s3"
        assert s._root == "bucket-x"
        assert s._env_overlay["RCLONE_CONFIG_SRW_ACCESS_KEY_ID"] == "K"

    def test_bare_spec(self):
        s = object_store_from_spec({"type": "webdav", "config": {"url": "http://x"}})
        assert isinstance(s, RcloneObjectStore)
        assert s._type == "webdav"

    def test_root_from_outer_bucket_field(self):
        s = object_store_from_spec(
            {"rclone_spec": {"type": "s3"}, "bucket": "outer-bucket"}
        )
        assert s._root == "outer-bucket"

    def test_missing_type_raises(self):
        with pytest.raises(ValueError, match="type"):
            object_store_from_spec({"config": {}})
