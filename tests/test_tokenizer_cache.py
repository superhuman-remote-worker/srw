"""Build-cache regression fixtures use real tiktoken I/O without public downloads."""

import ast
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shlex

import pytest
import tiktoken
import tiktoken.load
import tiktoken.model
import tiktoken.registry


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "docker" / "prepare-tokenizer-cache.py"
SPEC = importlib.util.spec_from_file_location("tokenizer_cache_build", SCRIPT)
cache_build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cache_build)


@pytest.fixture
def prepared_cache(tmp_path, monkeypatch):
    """Real cache writes/hash checks/encoding with a tiny synthetic byte vocab."""
    data = b"".join(
        base64.b64encode(bytes([value])) + f" {value}\n".encode()
        for value in range(256)
    )
    url = "https://fixture.invalid/test-tokenizer.tiktoken"
    expected_hash = hashlib.sha256(data).hexdigest()
    cache = tmp_path / "tokenizer-cache"
    monkeypatch.setenv("TIKTOKEN_CACHE_DIR", str(cache))
    monkeypatch.setattr(cache_build, "ENCODINGS", ("fixture_encoding",))
    monkeypatch.setattr(
        tiktoken.model, "MODEL_TO_ENCODING", {"fixture": "fixture_encoding"}
    )
    monkeypatch.setattr(tiktoken.model, "MODEL_PREFIX_TO_ENCODING", {})
    monkeypatch.setattr(tiktoken.registry, "ENCODINGS", {})

    def constructor():
        return {
            "name": "fixture_encoding",
            "pat_str": r"[\s\S]",
            "mergeable_ranks": tiktoken.load.load_tiktoken_bpe(url, expected_hash),
            "special_tokens": {},
        }

    monkeypatch.setattr(
        tiktoken.registry, "ENCODING_CONSTRUCTORS", {"fixture_encoding": constructor}
    )
    downloads = []

    def synthetic_download(path):
        assert path == url
        downloads.append(path)
        return data

    monkeypatch.setattr(tiktoken.load, "read_file", synthetic_download)
    manifest = cache_build.prepare(cache)
    assert downloads == [url]
    asset = cache / hashlib.sha1(url.encode()).hexdigest()
    assert asset.read_bytes() == data
    return cache, asset, manifest, downloads


def test_prepare_then_cold_offline_verify_uses_disk_without_writes(prepared_cache):
    cache, asset, manifest, downloads = prepared_cache
    before = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in cache.iterdir()
    }
    assert cache.stat().st_mode & 0o777 == 0o755
    assert all(path.stat().st_mode & 0o777 == 0o644 for path in cache.iterdir())
    assert manifest["assets"][asset.name]["bytes"] == asset.stat().st_size
    assert cache_build.verify_assets(cache) == manifest
    assert len(downloads) == 1
    assert before == {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in cache.iterdir()
    }


def test_missing_asset_fails_even_with_an_initialized_encoding(prepared_cache):
    cache, asset, _, _ = prepared_cache
    assert "fixture_encoding" in tiktoken.registry.ENCODINGS
    asset.unlink()
    with pytest.raises(RuntimeError, match="no assets"):
        cache_build.verify_assets(cache)


def test_cold_load_checks_required_asset_beyond_manifest_inventory(prepared_cache):
    cache, asset, manifest, downloads = prepared_cache
    # A stale manifest may be self-consistent but omit a required file. A warm
    # in-memory encoder must not bypass the actual constructor's disk lookup.
    assert "fixture_encoding" in tiktoken.registry.ENCODINGS
    asset.rename(cache / ("f" * 40))
    manifest["assets"] = cache_build.asset_inventory(cache)
    (cache / cache_build.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match="tried to download"):
        cache_build.verify_assets(cache)
    assert len(downloads) == 1


def test_corrupt_asset_is_rejected_before_retrying_download(prepared_cache):
    cache, asset, _, downloads = prepared_cache
    asset.write_bytes(b"damaged tokenizer file")
    with pytest.raises(RuntimeError, match="asset content drift"):
        cache_build.verify_assets(cache)
    assert len(downloads) == 1


def test_tiktoken_hash_is_authoritative_even_if_manifest_is_regenerated(prepared_cache):
    cache, asset, manifest, downloads = prepared_cache
    asset.write_bytes(b"damaged tokenizer file")
    manifest["assets"] = cache_build.asset_inventory(cache)
    (cache / cache_build.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match="tried to download"):
        cache_build.verify_assets(cache)
    assert len(downloads) == 1


def test_download_error_cannot_be_hidden_by_positive_approximate_count():
    with pytest.raises(RuntimeError, match="tried to download"):
        with cache_build.forbid_downloads():
            try:
                tiktoken.load.read_file("https://fixture.invalid/missing")
            except RuntimeError:
                result = 42
            assert result > 0


def test_new_library_model_encoding_requires_explicit_review(monkeypatch):
    monkeypatch.setattr(
        tiktoken.model,
        "MODEL_TO_ENCODING",
        {"future-model": "uncached_future_encoding"},
    )
    with pytest.raises(RuntimeError, match="Review new tiktoken model encodings"):
        cache_build.check_encoding_contract()


@pytest.mark.parametrize("value", [None, "", "relative/cache"])
def test_implicit_disabled_and_relative_cache_paths_are_rejected(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("TIKTOKEN_CACHE_DIR", raising=False)
    else:
        monkeypatch.setenv("TIKTOKEN_CACHE_DIR", value)
    with pytest.raises(RuntimeError, match="absolute TIKTOKEN_CACHE_DIR"):
        cache_build.cache_directory()


def test_unreadable_cache_fails_instead_of_using_fallback(prepared_cache, monkeypatch):
    cache, asset, _, _ = prepared_cache
    read_bytes = Path.read_bytes

    def denied(path):
        if path == asset:
            raise PermissionError("synthetic unreadable tokenizer asset")
        return read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", denied)
    with pytest.raises(PermissionError):
        cache_build.verify_assets(cache)


@pytest.mark.parametrize("change", ["version", "encodings"])
def test_manifest_cannot_silently_certify_a_changed_dependency(prepared_cache, change):
    cache, _, manifest, _ = prepared_cache
    if change == "version":
        manifest["tiktoken_version"] = "fixture-old-version"
    else:
        manifest["encodings"] = []
    (cache / cache_build.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match="drift"):
        cache_build.verify_assets(cache)


@pytest.mark.parametrize("recipe", ["Dockerfile.agent", "Dockerfile.agent.dev"])
def test_agent_recipes_prepare_final_cache_and_verify_as_nonroot_offline(recipe):
    text = (ROOT / "docker" / recipe).read_text(encoding="utf-8")
    instructions = text.replace("\\\n", "").splitlines()
    script = SCRIPT.relative_to(ROOT).as_posix()
    copies = [
        shlex.split(line)
        for line in instructions
        if line.startswith("COPY ") and script in line
    ]
    assert len(copies) == 1
    assert copies[0][-2] == script
    target = copies[0][-1]
    preparation = instructions.index(f"RUN python {target} prepare")
    verification = instructions.index(f"RUN --network=none python {target} verify")
    runtime_user = instructions.index("USER srw")
    assert (
        instructions.index("ENV TIKTOKEN_CACHE_DIR=/opt/tiktoken-cache") < preparation
    )
    assert preparation < instructions.index(
        "COPY --chown=srw:srw src/agent/ ./src/agent/"
    )
    assert preparation < runtime_user < verification
    assert "PYTHONSAFEPATH=1" in text


def test_tokenizer_script_change_triggers_tilt_and_develop_agent_rebuilds():
    script = SCRIPT.relative_to(ROOT).as_posix()
    tree = ast.parse((ROOT / "Tiltfile").read_text(encoding="utf-8"))
    builds = [
        call
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "docker_build"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and call.args[0].value == "srw-agent"
    ]
    assert len(builds) == 1
    inputs = next(item.value for item in builds[0].keywords if item.arg == "only")
    assert script in ast.literal_eval(inputs)

    workflow = (ROOT / ".github" / "workflows" / "develop.yml").read_text(
        encoding="utf-8"
    )
    paths = re.search(r"^\s*AGENT_PATHS=\((.*?)\)", workflow, re.MULTILINE | re.DOTALL)
    assert paths is not None
    assert script in shlex.split(paths.group(1))
    quality_paths = re.findall(
        r'if git diff --name-only "\$BASE" HEAD -- (.*?)\|\s*grep -q \.',
        workflow.replace("\\\n", ""),
        re.DOTALL,
    )
    python_checks = [
        shlex.split(paths)
        for paths in quality_paths
        if "'src/'" in paths and "'tests/'" in paths
    ]
    assert len(python_checks) == 1
    assert script in python_checks[0]


def test_image_verification_rejects_root_even_if_cache_is_present(
    prepared_cache, monkeypatch
):
    cache, _, _, _ = prepared_cache
    monkeypatch.setattr(cache_build.os, "getuid", lambda: 0)
    with pytest.raises(RuntimeError, match="non-root"):
        cache_build.verify(cache)


def _recipe_steps(recipe):
    text = (ROOT / "docker" / recipe).read_text(encoding="utf-8")
    instructions = text.replace("\\\n", "").splitlines()
    script = SCRIPT.relative_to(ROOT).as_posix()
    copies = [
        shlex.split(line)
        for line in instructions
        if line.startswith("COPY ") and script in line
    ]
    assert len(copies) == 1
    assert copies[0][-2] == script
    return text, instructions, copies[0][-1]


def test_orchestrator_recipe_prepares_cache_and_verifies_as_nonroot_offline():
    # KB chunking runs in the orchestrator: without a baked vocab its first
    # token count is a synchronous download (the 2026-08-07 liveness kill).
    text, instructions, target = _recipe_steps("Dockerfile.orchestrator")
    preparation = instructions.index(f"RUN python {target} prepare")
    verification = instructions.index(
        f"RUN --network=none python {target} verify --component orchestrator"
    )
    runtime_user = instructions.index("USER srw")
    assert (
        instructions.index("ENV TIKTOKEN_CACHE_DIR=/opt/tiktoken-cache") < preparation
    )
    assert preparation < instructions.index(
        "COPY --chown=srw:srw src/orchestrator/ ./src/orchestrator/"
    )
    assert preparation < runtime_user < verification
    assert "PYTHONSAFEPATH=1" in text


def test_orchestrator_dev_recipe_bakes_the_same_cache():
    # The dev image runs as root, so it cannot host the non-root verification;
    # it must still ship the assets, because k3d is where the vocab host fails.
    _, instructions, target = _recipe_steps("Dockerfile.orchestrator.dev")
    preparation = instructions.index(f"RUN python {target} prepare")
    assert (
        instructions.index("ENV TIKTOKEN_CACHE_DIR=/opt/tiktoken-cache") < preparation
    )
    assert preparation < instructions.index(
        "COPY src/orchestrator/ /app/src/orchestrator/"
    )


def test_tokenizer_script_change_rebuilds_the_tilt_orchestrator_image():
    script = SCRIPT.relative_to(ROOT).as_posix()
    tree = ast.parse((ROOT / "Tiltfile").read_text(encoding="utf-8"))
    builds = [
        call
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "docker_build"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and call.args[0].value == "srw-orchestrator"
    ]
    assert len(builds) == 1
    inputs = next(item.value for item in builds[0].keywords if item.arg == "only")
    assert script in ast.literal_eval(inputs)
    # A script edit must rebuild the image, not live-sync into a running Pod.
    fallbacks = [
        call
        for call in ast.walk(builds[0])
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "fall_back_on"
    ]
    assert len(fallbacks) == 1
    assert script in ast.literal_eval(fallbacks[0].args[0])


def test_unknown_component_is_rejected_before_any_import():
    with pytest.raises(RuntimeError, match="Unknown image component"):
        cache_build.verify(Path("/nonexistent"), "workspace")
