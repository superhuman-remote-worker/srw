"""The bundled NATS server must accept the chart's JetStream configuration."""

from pathlib import Path
import shutil

import docker
import pytest
from testcontainers.core.container import DockerContainer

from tests.test_manifest_hosting_helm import render


@pytest.fixture(scope="module")
def nats_container_engine():
    if shutil.which("helm") is None:
        pytest.skip("Helm is not installed")
    try:
        engine = docker.from_env()
        try:
            engine.ping()
        finally:
            engine.close()
    except Exception as exc:
        pytest.skip(f"local container engine unavailable: {exc}")


@pytest.mark.parametrize("storage_size", ["2Gi", "128Mi"])
def test_bundled_nats_accepts_rendered_jetstream_config(
    tmp_path: Path, nats_container_engine, storage_size: str
):
    documents = render(
        "nats.internal=true",
        "nats.url=",
        f"nats.storageSize={storage_size}",
    )
    config = next(
        item["data"]["nats.conf"]
        for item in documents
        if item["kind"] == "ConfigMap" and "nats.conf" in item.get("data", {})
    )
    statefulset = next(
        item
        for item in documents
        if item["kind"] == "StatefulSet" and item["metadata"]["name"].endswith("-nats")
    )
    image = statefulset["spec"]["template"]["spec"]["containers"][0]["image"]
    tmp_path.chmod(0o755)
    config_file = tmp_path / "nats.conf"
    config_file.write_text(config)
    config_file.chmod(0o644)
    with (
        DockerContainer(image)
        .with_volume_mapping(str(tmp_path), "/config", mode="ro,Z")
        .with_command("-t -c /config/nats.conf") as container
    ):
        result = container.get_wrapped_container().wait(timeout=30)
        stdout, stderr = container.get_logs()
        assert result["StatusCode"] == 0, (stdout + stderr).decode()
