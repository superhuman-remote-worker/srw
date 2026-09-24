"""The guest image must leave IDE dormant but allow unprivileged on-demand start."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_vm_image_installs_disabled_user_code_server_unit():
    stage2 = (ROOT / "docker/agent-vm-base/scripts/provision-stage2.sh").read_text()
    packer = (ROOT / "docker/agent-vm-base/stage2.pkr.hcl").read_text()
    unit = (ROOT / "docker/agent-vm-base/files/srw-code-server-user.service").read_text()
    assert '"files/srw-code-server-user.service"' in packer
    assert "loginctl enable-linger agent-host" in stage2
    assert "srw-code-server-user.service" in stage2
    assert "systemctl --user enable" not in stage2
    assert "ExecStart=/usr/bin/code-server --config /etc/code-server/config.yaml" in unit
    assert "WantedBy=default.target" not in unit
    config = (ROOT / "docker/agent-vm-base/files/code-server-config.yaml").read_text()
    assert "bind-addr: 127.0.0.1:8080" in config
    assert "auth: none" in config
