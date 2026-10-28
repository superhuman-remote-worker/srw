"""The optional ObjectStore operations and their defaults.

``stat``, ``list_dir`` and ``put_if_match`` exist for remotes that are a
user's cloud folder (features/cloud_folders_on_virtual_sessions.md § Agent).
Every store gets them: the defaults are built from the required surface, and
``InMemoryObjectStore`` adds a version counter so the conditional write can
be exercised without a remote.
"""

from typing import List, Optional

import pytest

from shared.runtime.core.backends.object_store import (
    InMemoryObjectStore,
    ObjectInfo,
    ObjectStat,
    ObjectStore,
    ObjectStoreError,
    ObjectVersionMismatch,
)


class BareStore(ObjectStore):
    """The required surface only: what an S3 prefix looks like to the seam."""

    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}
        self.puts: list[str] = []

    def get(self, key: str) -> bytes:
        try:
            return self.data[key]
        except KeyError:
            raise FileNotFoundError(key) from None

    def put(self, key: str, data: bytes) -> None:
        self.data[key] = bytes(data)
        self.puts.append(key)

    def head(self, key: str) -> Optional[int]:
        return len(self.data[key]) if key in self.data else None

    def list(self, prefix: str) -> List[ObjectInfo]:
        return sorted(
            (
                ObjectInfo(k, len(v))
                for k, v in self.data.items()
                if k.startswith(prefix)
            ),
            key=lambda i: i.key,
        )

    def delete(self, key: str) -> bool:
        return self.data.pop(key, None) is not None


class TestDefaults:
    def test_stat_knows_files_only_and_has_no_version(self):
        store = BareStore()
        store.put("p/a.txt", b"abc")
        assert store.stat("p/a.txt") == ObjectStat(key="p/a.txt", size=3)
        assert store.stat("p/a.txt").version is None
        assert store.stat("p") is None
        assert store.stat("p/ghost") is None

    def test_list_dir_folds_the_recursive_listing_to_one_level(self):
        store = BareStore()
        for key in (
            "p/b.txt",
            "p/a.txt",
            "p/sub/one",
            "p/sub/deep/two",
            "p/zed/x",
            "q/n",
        ):
            store.put(key, b"1")
        assert store.list_dir("p/") == [
            ObjectStat(key="p/a.txt", size=1),
            ObjectStat(key="p/b.txt", size=1),
            ObjectStat(key="p/sub", size=0, is_dir=True),
            ObjectStat(key="p/zed", size=0, is_dir=True),
        ]
        assert store.list_dir("p/sub/") == [
            ObjectStat(key="p/sub/deep", size=0, is_dir=True),
            ObjectStat(key="p/sub/one", size=1),
        ]
        assert store.list_dir("") == [
            ObjectStat(key="p", size=0, is_dir=True),
            ObjectStat(key="q", size=0, is_dir=True),
        ]
        assert store.list_dir("nothing/") == []

    def test_put_if_match_without_versions_checks_existence_only(self):
        store = BareStore()
        after = store.put_if_match("p/a.txt", b"new", None)
        assert after == ObjectStat(key="p/a.txt", size=3) and store.puts == ["p/a.txt"]
        # The caller read no version (the store has none): the write goes through.
        store.put_if_match("p/a.txt", b"newer", None)
        assert store.data["p/a.txt"] == b"newer"
        # A caller holding a version for an object that is gone is refused.
        with pytest.raises(ObjectVersionMismatch) as excinfo:
            store.put_if_match("p/gone", b"x", "v1")
        assert (excinfo.value.key, excinfo.value.expected, excinfo.value.actual) == (
            "p/gone",
            "v1",
            None,
        )
        assert "p/gone" not in store.data

    def test_put_if_match_reports_a_store_that_loses_its_write(self):
        class Amnesiac(BareStore):
            def stat(self, key):  # the write never becomes visible
                return None

        with pytest.raises(ObjectStoreError):
            Amnesiac().put_if_match("k", b"x", None)


class TestInMemoryVersions:
    def test_every_write_bumps_the_version(self):
        store = InMemoryObjectStore()
        store.put("a", b"1")
        first = store.stat("a")
        assert first == ObjectStat(key="a", size=1, version="1")
        store.put("a", b"22")
        assert store.stat("a") == ObjectStat(key="a", size=2, version="2")
        store.delete("a")
        assert store.stat("a") is None
        store.put("a", b"3")
        assert store.stat("a").version == "1"

    def test_conditional_write_matches_the_read_version(self):
        store = InMemoryObjectStore()
        store.put("doc.md", b"draft")
        read = store.stat("doc.md")
        after = store.put_if_match("doc.md", b"draft, edited", read.version)
        assert after.version == "2" and store.get("doc.md") == b"draft, edited"
        # Someone else wrote in between: the stale version is refused.
        store.put("doc.md", b"human edit")
        with pytest.raises(ObjectVersionMismatch) as excinfo:
            store.put_if_match("doc.md", b"agent edit", after.version)
        assert excinfo.value.expected == "2" and excinfo.value.actual == "3"
        assert store.get("doc.md") == b"human edit"

    def test_list_dir_entries_carry_versions(self):
        store = InMemoryObjectStore()
        store.put("p/a", b"x")
        store.put("p/a", b"xy")
        store.put("p/d/b", b"z")
        # The default list_dir builds on ``list`` and so reports no version;
        # ``stat`` does. A store with a native listing reports both.
        assert store.list_dir("p/") == [
            ObjectStat(key="p/a", size=2),
            ObjectStat(key="p/d", size=0, is_dir=True),
        ]
        assert store.stat("p/a").version == "2"

    def test_object_stat_is_an_object_info(self):
        info = ObjectStat(key="k", size=1, version="v")
        assert isinstance(info, ObjectInfo) and (info.key, info.size) == ("k", 1)
