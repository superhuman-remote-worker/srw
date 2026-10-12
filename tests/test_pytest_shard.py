"""Sharding preserves coverage and file-local fixtures, including new files."""

from scripts.pytest_shard import partition_files
from scripts.pytest_file_timings import file_timings

import pytest
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET


def test_shards_cover_every_file_once_and_balance_long_files():
    files = ["slow-a.py", "slow-b.py", "new.py", "fast-a.py", "fast-b.py"]
    groups = partition_files(files, {"slow-a.py": 100, "slow-b.py": 90}, 2)
    assert sorted(path for group in groups for path in group) == sorted(files)
    assert not set(groups[0]) & set(groups[1])
    assert next(i for i, g in enumerate(groups) if "slow-a.py" in g) != next(
        i for i, g in enumerate(groups) if "slow-b.py" in g
    )
    assert groups == partition_files(
        reversed(files), {"slow-a.py": 100, "slow-b.py": 90}, 2
    )


def test_more_shards_than_files_does_not_duplicate_work():
    groups = partition_files(["only.py", "only.py"], {}, 3)
    assert groups == [["only.py"], [], []]


def test_stale_weights_never_select_a_removed_file():
    assert partition_files(["new.py"], {"removed.py": 1000}, 2) == [["new.py"], []]


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_unusable_durations_do_not_drop_files(value):
    assert partition_files(["one.py"], {"one.py": value}, 1) == [["one.py"]]


def test_zero_shards_is_rejected():
    with pytest.raises(ValueError):
        partition_files(["one.py"], {}, 0)


def test_junit_weights_sum_classes_and_ignore_missing_modules(tmp_path):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test_one.py").touch()
    report = tmp_path / "report.xml"
    report.write_text("""<testsuite>
      <testcase classname="tests.test_one.First" time="2.5"/>
      <testcase classname="tests.test_one.Second" time="1.5"/>
      <testcase classname="tests.removed" time="999"/>
    </testsuite>""")
    assert file_timings([report], tmp_path) == {"tests/test_one.py": 4.0}


def test_cli_executes_each_test_once_and_preserves_failure_exit(tmp_path):
    root = Path(__file__).resolve().parents[1]
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in ("pytest_shard.py", "pytest-fast.sh"):
        shutil.copy2(root / "scripts" / name, scripts / name)
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_good.py").write_text("def test_good(): assert True\n")
    (tests / "test_bad.py").write_text("def test_bad(): assert False\n")
    (tests / "test_skipped.py").write_text(
        "import pytest\npytest.skip('optional dependency', allow_module_level=True)\n"
    )
    outcomes = []
    manifests = []
    for index in range(4):
        report = tmp_path / f"reports-{index}"
        result = subprocess.run(
            [
                sys.executable,
                str(scripts / "pytest_shard.py"),
                "--index",
                str(index),
                "--count",
                "4",
                "--report-dir",
                str(report),
                "tests/",
            ],
            env=dict(os.environ, SRW_PYTEST_WORKERS="1"),
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode in (0, 1), result.stdout + result.stderr
        outcomes.append(result.returncode)
        manifests.append(json.loads((report / "selection.json").read_text()))
    assert sorted(outcomes) == [0, 0, 0, 1]
    assert sorted(n for m in manifests for n in m["nodeids"]) == [
        "tests/test_bad.py::test_bad",
        "tests/test_good.py::test_good",
    ]
    assert [f for m in manifests for f in m["collection_skips"]] == [
        "tests/test_skipped.py"
    ]
    assert (
        sum(
            len(ET.parse(report).findall(".//skipped"))
            for report in tmp_path.glob("reports-*/junit.xml")
        )
        == 1
    )
    assert not (tmp_path / "reports-3/junit.xml").exists()


def test_collection_error_cannot_be_reported_as_an_empty_successful_shard(tmp_path):
    root = Path(__file__).resolve().parents[1]
    (tmp_path / "scripts").mkdir()
    shutil.copy2(root / "scripts/pytest_shard.py", tmp_path / "scripts/pytest_shard.py")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test_broken.py").write_text("this is not valid Python!\n")
    result = subprocess.run(
        [
            sys.executable,
            str(tmp_path / "scripts/pytest_shard.py"),
            "--index",
            "0",
            "--count",
            "2",
            "--report-dir",
            str(tmp_path / "reports"),
            "tests/",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert "SyntaxError" in result.stderr
    assert not (tmp_path / "reports/selection.json").exists()
