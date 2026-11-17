"""How the srw command-line client is tested, built and released by CI.

The client (``cli/``) is a Go module beside a Python and TypeScript tree, so
none of the other pipeline tests notice when its jobs lose a step. These
cases pin what must stay true:

* both pipelines run the client's checks, and its wire test finds a Go
  toolchain in the Python shards;
* a broken client stops the chart, the tag and the release, like every other
  component;
* a release carries the six archives, their checksums and the installer, and
  the checksums are attested only when something is published;
* the names of the assets, which are public contract.
"""

from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
PUBLISH = "needs.resolve-version.result == 'success'"
ASSETS = {
    "srw_linux_amd64.tar.gz",
    "srw_linux_arm64.tar.gz",
    "srw_darwin_amd64.tar.gz",
    "srw_darwin_arm64.tar.gz",
    "srw_windows_amd64.zip",
    "srw_windows_arm64.zip",
    "checksums.txt",
    "install.sh",
}


def workflow(name: str) -> dict:
    return yaml.safe_load((ROOT / f".github/workflows/{name}.yml").read_text())


def jobs(name: str) -> dict:
    return workflow(name)["jobs"]


def script(job: dict) -> str:
    return "\n".join(step.get("run", "") for step in job["steps"])


def step_using(job: dict, action: str) -> dict:
    return next(s for s in job["steps"] if s.get("uses", "").startswith(action + "@"))


@pytest.mark.parametrize("branch", ["main", "develop"])
def test_both_pipelines_check_the_client(branch):
    job = jobs(branch)["test-cli"]
    assert job["permissions"] == {"contents": "read"}
    assert step_using(job, "actions/setup-go")["with"]["go-version"] == "1.26.8"
    run = script(job)
    for needed in (
        "go mod verify",
        "gofmt -l .",
        "go vet ./...",
        "GOOS=windows go vet ./...",
        "GOOS=darwin go vet ./...",
        "go test -count=1 -race",
        # Pinned: an audit tool that changes under the pipeline is a
        # different check every week.
        "golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...",
        "scripts/release/build_cli.sh",
        "sha256sum -c checksums.txt",
        "sh scripts/install-cli.sh",
    ):
        assert needed in run, needed


def test_main_checks_the_client_on_every_run_and_develop_when_it_changed():
    assert "needs" not in jobs("main")["test-cli"]
    develop = jobs("develop")
    assert develop["test-cli"]["needs"] == ["changes"]
    assert (
        "needs.changes.outputs.cli-test-changed == 'true'" in develop["test-cli"]["if"]
    )
    assert (
        develop["changes"]["outputs"]["cli-test-changed"]
        == "${{ steps.test-paths.outputs.cli }}"
    )
    detect = script(develop["changes"])
    # The route inventory is an input: the client's route table is checked
    # against it, so a server change can break the client.
    assert "'cli/' 'policy/endpoint_inventory.txt'" in detect
    assert 'echo "cli=true" >> "$GITHUB_OUTPUT"' in detect
    assert 'echo "cli=false" >> "$GITHUB_OUTPUT"' in detect


@pytest.mark.parametrize("branch", ["main", "develop"])
def test_the_python_shards_can_build_the_client_for_the_wire_test(branch):
    shards = jobs(branch)["test-python-shards"]
    assert step_using(shards, "actions/setup-go")["with"]["go-version"] == "1.26.8"


def test_a_client_change_runs_the_python_suite_on_develop():
    # tests/test_srw_cli_wire.py runs the built binary against real routes.
    detect = script(jobs("develop")["changes"])
    python_paths = detect.split("'src/' 'tests/'", 1)[1].split("python=true", 1)[0]
    assert "'cli/'" in python_paths


def test_a_broken_client_stops_the_release():
    main = jobs("main")
    build = main["build-cli"]
    assert "test-cli" in build["needs"]
    assert "needs.test-cli.result == 'success'" in build["if"]
    assert "build-cli" in main["release-chart"]["needs"]
    assert "needs.build-cli.result == 'success'" in main["release-chart"]["if"]


def test_provenance_is_attested_only_for_a_release():
    build = jobs("main")["build-cli"]
    assert build["permissions"] == {
        "contents": "read",
        "packages": "write",
        "id-token": "write",
        "attestations": "write",
    }
    attest = step_using(build, "actions/attest")
    assert attest["if"] == PUBLISH
    assert attest["with"] == {"subject-checksums": "dist/cli/checksums.txt"}
    # No other job in either pipeline asks for an OIDC token.
    for branch in ("main", "develop"):
        for name, job in jobs(branch).items():
            if name != "build-cli":
                assert "id-token" not in (job.get("permissions") or {}), name


def test_the_archives_reach_the_github_release():
    main = jobs("main")
    upload = step_using(main["build-cli"], "actions/upload-artifact")["with"]
    assert upload["name"] == "srw-cli"
    assert upload["path"] == "dist/cli/"
    assert upload["if-no-files-found"] == "error"
    release = main["github-release"]
    downloads = [
        s["with"]
        for s in release["steps"]
        if s.get("uses", "").startswith("actions/download-artifact@")
    ]
    assert {"name": "srw-cli", "path": "dist/cli"} in downloads
    run = release["steps"][-1]["run"]
    assert (
        "assets=(dist/cli/srw_*.tar.gz dist/cli/srw_*.zip "
        "dist/cli/checksums.txt dist/cli/install.sh)"
    ) in run
    # Created with its files, so a published release has every asset.
    assert '--notes-file release-notes.md "${assets[@]}"' in run
    # A published release is never changed, and one without its assets fails.
    assert "exists without" in run and "exit 1" in run


def test_the_image_follows_the_publication_gate():
    build = jobs("main")["build-cli"]
    image = step_using(build, "docker/build-push-action")["with"]
    assert image["file"] == "./docker/Dockerfile.cli"
    assert image["platforms"] == "linux/amd64,linux/arm64"
    assert image["push"] == "${{ " + PUBLISH + " }}"
    assert "org.opencontainers.image.licenses=FSL-1.1-ALv2" in image["labels"]
    meta = step_using(build, "docker/metadata-action")["with"]
    assert meta["images"] == "${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}-cli"
    # A second architecture without emulation: nothing may run in the last
    # stage of the image.
    dockerfile = (ROOT / "docker/Dockerfile.cli").read_text()
    last_stage = dockerfile.rsplit("\nFROM ", 1)[1]
    assert "\nRUN " not in last_stage
    assert 'ENTRYPOINT ["srw"]' in last_stage


def test_the_docs_site_serves_the_installer():
    docs = workflow("docs")
    # PyYAML reads the key `on` as the boolean True.
    paths = docs[True]["push"]["paths"]
    assert "scripts/install-cli.sh" in paths
    package = (ROOT / "docs-site/package.json").read_text()
    assert "scripts/copy-install-script.mjs" in package


def test_the_asset_names_are_the_public_contract(tmp_path):
    """Built for real: the names are what install.sh, the docs and every
    generic installer ask for, and they carry no version."""
    import shutil

    if shutil.which("go") is None:
        pytest.skip("the Go toolchain is not installed")
    subprocess.run(
        [str(ROOT / "scripts/release/build_cli.sh"), "9.9.9", "abc1234", str(tmp_path)],
        check=True,
        capture_output=True,
    )
    assert {path.name for path in tmp_path.iterdir()} == ASSETS
    listed = (tmp_path / "checksums.txt").read_text().split()
    assert set(listed[1::2]) == ASSETS - {"checksums.txt", "install.sh"}
    # Every archive carries the program, the licence and the notices of what
    # is linked into it.
    import tarfile
    import zipfile

    with tarfile.open(tmp_path / "srw_linux_amd64.tar.gz") as archive:
        assert sorted(archive.getnames()) == [
            "LICENSE",
            "THIRD_PARTY_NOTICES.md",
            "srw",
        ]
    with zipfile.ZipFile(tmp_path / "srw_windows_amd64.zip") as archive:
        assert sorted(archive.namelist()) == [
            "LICENSE",
            "THIRD_PARTY_NOTICES.md",
            "srw.exe",
        ]
    installer = (ROOT / "scripts/install-cli.sh").read_text()
    assert 'archive="srw_${os}_${arch}.tar.gz"' in installer
    # The whole installer is one function called on the last line, so a
    # download that breaks off halfway runs nothing.
    assert installer.rstrip().endswith('main "$@"')
