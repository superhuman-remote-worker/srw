# =============================================================================
# Agent VM Base Image — Stage 2 Packer Template
# =============================================================================
#
# Builds the final agent VM image by booting the stage1 qcow2 and layering
# per-commit config on top: user setup, SSH server config, management daemon,
# sudo approval gate, tmux/git config. Stage 2 finishes in ~5 minutes
# because all the heavy installs already happened in stage1.
#
# Prerequisites:
#   - stage1 qcow2 at input/agent-vm-base-stage1.qcow2 (CI extracts it from
#     the published GHCR containerDisk via `docker create + docker cp`).
#
# Build:
#   packer init stage2.pkr.hcl
#   packer build stage2.pkr.hcl
#
# Output: output/agent-vm-base.qcow2
# =============================================================================

packer {
  required_plugins {
    qemu = {
      version = ">= 1.1.0"
      source  = "github.com/hashicorp/qemu"
    }
  }
}

variable "stage1_image" {
  type        = string
  default     = "input/agent-vm-base-stage1.qcow2"
  description = "Path to the stage1 qcow2 image (extracted from GHCR containerDisk)"
}

variable "ssh_username" {
  type    = string
  default = "packer"
}

variable "ssh_password" {
  type    = string
  default = "packer"
}

variable "disk_size" {
  type    = string
  default = "20G"
}

variable "memory" {
  type    = number
  default = 4096
}

variable "cpus" {
  type    = number
  default = 4
}

source "qemu" "agent-vm-base" {
  iso_url      = var.stage1_image
  iso_checksum = "none"
  disk_image   = true

  output_directory = "output"
  vm_name          = "agent-vm-base.qcow2"
  format           = "qcow2"
  disk_size        = var.disk_size

  headless    = true
  accelerator = "kvm"
  memory      = var.memory
  cpus        = var.cpus

  ssh_username           = var.ssh_username
  ssh_password           = var.ssh_password
  ssh_timeout            = "10m"
  ssh_handshake_attempts = 200

  # Shutdown is handled inside cleanup.sh
  shutdown_command = ""
  shutdown_timeout = "2m"

  # Reuse the same cloud-init seed as stage1 — instance-id matches, so
  # cloud-init detects "already ran" and skips. Packer user already exists
  # from stage1's first boot.
  cd_files = ["cloud-init/*"]
  cd_label = "cidata"

  net_device = "virtio-net"
}

build {
  sources = ["source.qemu.agent-vm-base"]

  # Daemon + sudo-gate files. Packer's `sources` uploads multiple files in
  # one SCP session, saving SSH-handshake overhead.
  #
  # browser-exec, check-browser-stream.py, and assert-browser-stack.sh are
  # deliberately sourced from ../ (docker/) rather than copied into files/:
  # they are the exact same files the container workspace ships
  # (docker/Dockerfile.workspace). A copy under files/ would reintroduce the
  # hand-maintained duplication that let the VM image ship for ~5 weeks without
  # browser-exec at all — a working Chromium no agent could reach. See
  # knowledge-base/knowledge/issues/vm_workspace_missing_browser_exec.md.
  provisioner "file" {
    sources = [
      "files/management-daemon.py",
      "files/management-daemon.service",
      "files/code-server-config.yaml",
      "files/code-server.service",
      "files/srw-code-server-user.service",
      "files/sudo-gated.service",
      "files/sudo-gated.socket",
      "files/sudo-gated-config.yaml",
      "files/sudo-gate.conf",
      "files/sudo_gate.so",
      "../browser-exec",
      "../check-browser-stream.py",
      "../assert-browser-stack.sh",
      # VM-only, unlike its browser sibling: the sandbox tier cannot run
      # containers at all, so there is no twin image to keep in step.
      "../assert-container-stack.sh",
    ]
    destination = "/tmp/"
  }

  # sudo-gated-bin needs a rename to /tmp/sudo-gated, so it stays separate.
  # CI requires this file and the plugin artifact to be non-empty.
  provisioner "file" {
    source      = "files/sudo-gated-bin"
    destination = "/tmp/sudo-gated"
  }

  # Stage 2: light per-commit bits — user setup, SSH config, daemon, sudo gate.
  provisioner "shell" {
    script = "scripts/provision-stage2.sh"
    environment_vars = [
      "DEBIAN_FRONTEND=noninteractive",
    ]
  }

  # Final cleanup: removes packer user, runs cloud-init clean, truncates
  # machine-id, disables sshd PasswordAuthentication, then shuts down.
  #
  # cleanup.sh deletes the packer user + sudoers and ends in `shutdown -P now`
  # inside one sudo block (shutdown_command is empty above), so shutdown MUST
  # stay in-script — Packer can't SSH back as the deleted packer user. These
  # two flags stop Packer racing the poweroff to `rm` its temp script, which
  # intermittently fails with "Error removing temporary script: connection
  # refused".
  #   expect_disconnect: the SSH drop on shutdown is expected, not an error.
  #   skip_clean:        don't reconnect to delete the temp script.
  provisioner "shell" {
    script            = "scripts/cleanup.sh"
    expect_disconnect = true
    skip_clean        = true
  }
}
