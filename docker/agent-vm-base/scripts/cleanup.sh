#!/usr/bin/env bash
# =============================================================================
# Agent VM Base Image — Cleanup Script
# =============================================================================
#
# Minimize image size and remove build artifacts.
# Run as the last Packer provisioner.
#
# Runs everything in a single sudo bash block so that sudo is only
# checked once — cloud-init clean removes the sudoers entry that
# grants the packer user NOPASSWD access.
# =============================================================================

set -euo pipefail

echo "=== Cleaning up image ==="

sudo bash <<'CLEANUP'
set -euo pipefail

# Disable password auth (was enabled for Packer SSH)
sed -i 's/^PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config 2>/dev/null || true

# Clean apt cache
apt-get autoremove -y
apt-get clean
rm -rf /var/lib/apt/lists/*

# Clean pip cache
rm -rf /root/.cache/pip /home/*/.cache/pip

# Clean root's Playwright cache (the browsers live in /opt/playwright). Not
# agent-host's: its ~/.cache/ms-playwright holds the links to them (stage2).
rm -rf /root/.cache/ms-playwright

# Clean npm cache
npm cache clean --force 2>/dev/null || true

# Remove cloud-init artifacts (will re-run on first real boot)
cloud-init clean --logs --seed

# Clear tmp
rm -rf /tmp/* /var/tmp/*

# Clear machine-id (regenerated on first boot)
truncate -s 0 /etc/machine-id
rm -f /var/lib/dbus/machine-id

# Never publish a VM image with reusable SSH host private keys. cloud-init and
# the per-instance runcmd both regenerate these on the clone's first boot.
rm -f /etc/ssh/ssh_host_*

# Clear shell history
rm -f /root/.bash_history /home/*/.bash_history

# Remove packer build user and sudoers, then shut down
rm -f /etc/sudoers.d/90-cloud-init-users
userdel -r packer 2>/dev/null || true

# Verify critical files exist before shutdown
ls -la /opt/srw/management-daemon.py /etc/systemd/system/management-daemon.service

# Seal the sudo approval plugin fail-closed as the final image mutation. The
# temporary fail-open setting is needed only while Packer runs sudo commands.
chattr -i /etc/sudo.conf
sed -i '/sudo_gate_approval/s/fail_mode=open/fail_mode=deny/' /etc/sudo.conf
chattr +i /etc/sudo.conf

# Final stage2 contract gate. Packer invokes cleanup from a private directory,
# so run guest-facing checks as agent-host from / and use absolute paths.
cd /
/usr/bin/grep -Eq 'sudo_gate_approval.*fail_mode=deny$' /etc/sudo.conf
/usr/sbin/runuser -u agent-host -- /usr/bin/env PYTHONPYCACHEPREFIX=/dev/shm/srw-stage2-pycache \
    /usr/bin/python3 -m py_compile /opt/srw/management-daemon.py
/usr/sbin/runuser -u agent-host -- /usr/local/bin/sudo-gated --version

echo "=== Cleanup complete ==="
sync
shutdown -P now
CLEANUP
