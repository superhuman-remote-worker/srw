# Prepare a shared Linux host for SRW on K3s

Use this guide **before starting K3s** when a server also runs Podman Quadlets,
has several network interfaces, or must keep application volumes on a separate
disk. The generator's `install.sh` starts after `kubectl` can reach a ready
cluster; it does not configure the host's disks, firewall or routing.

This procedure is for a fresh single-node installation. Moving an existing
cluster needs a separate backup and migration procedure. Do not mount over an
existing kubelet directory containing workload data.

## Operator and initial checks

K3s runs as a root system service with its own containerd. Use the existing
administrator account and sudo for host setup, then run Helm and kubectl as
that account. A new Linux service user or additional wheel membership is not
needed. Kubernetes ServiceAccounts are separate identities for pods; the chart
creates those it needs. No host network login credentials belong in them.

Record the existing services, storage and networking:

```bash
systemctl status k3s k3s-agent --no-pager
systemctl --user list-units --type=service --state=running
podman ps
lsblk -o NAME,SIZE,MODEL,SERIAL,WWN,FSTYPE,UUID,MOUNTPOINTS
findmnt /data
df -h / /data
ip -br address
ip rule show
ip route show table all
sudo ss -lntup
```

An absent K3s service is expected. Check for other Kubernetes installations too;
do not run the installer over an existing cluster. Record Quadlet container IDs
and health so you can compare them after installation. Check K3s port conflicts
against the [K3s requirements](https://docs.k3s.io/installation/requirements#local-ports).

## Decide where data goes

The examples use an **already mounted HDD filesystem at `/data`**. Substitute
your verified mount point throughout, including the systemd dependencies and
SELinux mappings. Mount it persistently by UUID or another stable identifier;
these instructions do not partition or format disks.

| Data | Path in this example | Disk |
| --- | --- | --- |
| K3s binary | `/usr/local/bin/k3s` | OS/SSD |
| Control database, container images and writable container layers | `/var/lib/rancher/k3s` | OS/SSD |
| Application PVCs from `local-path` | `/data/srw/volumes` | HDD |
| Kubelet disk-backed temporary volumes, including `emptyDir` | `/data/srw/kubelet`, bind-mounted at `/var/lib/kubelet` | HDD |
| Container logs | `/var/log/pods` | OS/SSD, bounded below |

A StorageClass selects storage for **new PVCs**. It neither moves existing PVs
nor relocates `emptyDir`. `workspace.ephemeralStorageClass: local-path` alone
does not enable per-session PVCs: `workspace.pvcEnabled` defaults to false.
The kubelet bind mount covers disk-backed temporary workspace volumes without
changing that application behavior. Memory-backed `emptyDir` remains in RAM.
Large container images/layers still consume SSD space; monitor both filesystems.

Splitting `/var/lib/kubelet` from container logs and writable layers is outside
Kubernetes' documented local ephemeral-storage layouts. Kubelet accounting and
`ephemeral-storage` limit enforcement may be incomplete: use host monitoring
and free-space alerts for both SSD and HDD instead of relying on pod limits to
protect them. See [local ephemeral-storage configurations](https://kubernetes.io/docs/concepts/storage/ephemeral-storage/#configurations-for-local-ephemeral-storage).

Verify the mount first, then create dedicated directories:

```bash
mountpoint -q /data || { echo '/data must be mounted first'; exit 1; }
sudo install -d -m 755 /data/srw/volumes /data/srw/kubelet /var/lib/kubelet
```

## Configure K3s before its first start

Create `/etc/rancher/k3s/config.yaml` with sudo. Merge deliberately if it already
exists. This example selects the generator's single-origin HTTPS mode:

```yaml
write-kubeconfig-mode: "0600"
default-local-storage-path: /data/srw/volumes
disable:
  - traefik
  - servicelb
selinux: true
kubelet-arg:
  - container-log-max-size=10Mi
  - container-log-max-files=3
  - image-gc-high-threshold=80
  - image-gc-low-threshold=75
```

For the multi-host ingress preset, omit `disable` so the bundled Traefik and
ServiceLB can serve ports 80/443. Keep the K3s network-policy controller enabled.
`selinux: true` is for SELinux hosts with the K3s policy installed. Image GC
thresholds are percentages, not a fixed SSD budget. If the host uses swap,
decide its Kubernetes swap policy explicitly before starting; do not disable
host swap underneath unrelated workloads.

On a multi-interface host, add the chosen node address and interface (these
documentation addresses are placeholders):

```yaml
node-ip: 192.0.2.10
advertise-address: 192.0.2.10
flannel-iface: ens192
```

Check for conflicts with the default pod/service ranges `10.42.0.0/16` and
`10.43.0.0/16` in university, VPN and Podman networks. Choose different
`cluster-cidr`, `service-cidr` and `cluster-dns` values before installation if
necessary; use those ranges in every firewall/routing check below. See the
[K3s server options](https://docs.k3s.io/cli/server) for these settings.

Download the installer and select an explicit version. The following versions
were used in SRW's September 2026 installation validation; select a supported
version for a later deployment instead of silently following the stable channel:

```bash
curl -fsSL https://get.k3s.io -o k3s-install.sh
sudo env INSTALL_K3S_VERSION="${K3S_VERSION:-v1.36.4+k3s1}" \
  INSTALL_K3S_SKIP_START=true sh k3s-install.sh server
```

The installer writes the system service but does not start it. This leaves time
to prepare its mount dependencies and networking.
[K3s configuration reference](https://docs.k3s.io/installation/configuration).

## Require the HDD before K3s starts

Create `/etc/systemd/system/var-lib-kubelet.mount`:

```ini
[Unit]
Description=K3s kubelet volumes on the data disk
RequiresMountsFor=/data
Before=k3s.service

[Mount]
What=/data/srw/kubelet
Where=/var/lib/kubelet
Type=none
Options=bind

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/k3s.service.d/storage.conf` (create its parent
directory first):

```ini
[Unit]
RequiresMountsFor=/data
Requires=var-lib-kubelet.mount
After=var-lib-kubelet.mount

[Service]
ExecStartPre=/usr/bin/mountpoint -q /data
ExecStartPre=/usr/bin/mountpoint -q /var/lib/kubelet
```

The explicit mount checks make K3s fail to start if the HDD is absent, instead
of provisioning into an ordinary `/data` directory on SSD. The underlying HDD
mount must also be managed by systemd, normally through `/etc/fstab`.

On Fedora with SELinux enforcing, verify that the installer installed
`k3s-selinux`. Install `policycoreutils-python-utils` for `semanage`, then apply
persistent path equivalences before starting workloads:

```bash
rpm -q k3s-selinux
sudo dnf install -y policycoreutils-python-utils
sudo semanage fcontext -a -e /var/lib/rancher/k3s/storage /data/srw/volumes
sudo semanage fcontext -a -e /var/lib/kubelet /data/srw/kubelet
sudo restorecon -RF /data/srw/volumes /data/srw/kubelet
sudo systemctl daemon-reload
sudo systemctl enable --now var-lib-kubelet.mount
findmnt -T /var/lib/kubelet
```

If mappings already exist, inspect them with `semanage fcontext -l` and update
them rather than adding duplicates. Investigate AVC denials instead of turning
off SELinux. See [K3s SELinux support](https://docs.k3s.io/advanced#selinux-support).

## Fedora networking without disrupting Quadlets

Keep firewalld enabled. Inspect `sudo firewall-cmd --get-active-zones` and choose
the zone attached to the intended client interface. Add only the required
application port and pod/service sources, to both runtime and permanent config:

```bash
SRW_FIREWALL_ZONE=FedoraServer # replace with the actual interface zone
for scope in runtime permanent; do
  flags=()
  [ "$scope" = runtime ] || flags+=(--permanent)
  sudo firewall-cmd "${flags[@]}" --zone="$SRW_FIREWALL_ZONE" --add-port=30443/tcp
  sudo firewall-cmd "${flags[@]}" --zone=trusted --add-source=10.42.0.0/16
  sudo firewall-cmd "${flags[@]}" --zone=trusted --add-source=10.43.0.0/16
done
```

This avoids a firewall reload that could replace unrelated runtime rules. Use
80/443 for the ingress preset. Direct single-origin node access uses 30443;
another public port needs an explicit proxy/NAT mapping to NodePort 30443. Open
6443 only to administrative clients/additional nodes that need it. Restrict
Flannel ports to cluster nodes when adding nodes; do not expose VXLAN publicly.
These rules adapt the [upstream firewalld requirements](https://docs.k3s.io/installation/requirements#firewalld).

If NetworkManager manages the host, create
`/etc/NetworkManager/conf.d/90-k3s-unmanaged.conf`:

```ini
[keyfile]
unmanaged-devices=interface-name:cni0;interface-name:flannel*
```

Merge with any existing unmanaged-device configuration. Load it with
`sudo nmcli general reload conf` and verify with `nmcli device status`; do not
restart the host's network connections or NetworkManager during remote setup.

## Start and verify the host

```bash
sudo systemctl start k3s
sudo systemctl status k3s --no-pager
install -d -m 700 "$HOME/.kube"
sudo install -m 600 -o "$(id -u)" -g "$(id -g)" \
  /etc/rancher/k3s/k3s.yaml "$HOME/.kube/srw-k3s.yaml"
export KUBECONFIG="$HOME/.kube/srw-k3s.yaml"
kubectl wait --for=condition=Ready node --all --timeout=180s
kubectl -n kube-system get pods -o wide
kubectl -n kube-system get configmap local-path-config -o jsonpath='{.data.config\.json}'
kubectl get storageclass
```

The kubeconfig grants cluster administration. Keep both copies mode 0600 and
refresh the operator copy when K3s renews the embedded client certificate. If
the K3s kubectl wrapper warns that it cannot read a root-only `config.yaml`, use
a standalone kubectl binary; never make credential-bearing host config public.

Before running SRW, test a disposable PVC and consuming pod on `local-path`.
Check the resulting PV's `spec.local.path` (or `spec.hostPath.path`) lies under
`/data/srw/volumes`, and `findmnt -T <pv-path>` resolves to the HDD. Test a
disk-backed `emptyDir` write and inspect its kubelet path too. Delete only the
disposable test resources after inspection. A PVC with WaitForFirstConsumer
will remain Pending until a pod consumes it.

### Multiple interfaces and policy routing

An API timeout from CoreDNS, metrics-server or the provisioner can be a return
route problem even when firewalld accepts the packets. For an actual pod IP:

```bash
ip rule show
ip route get 10.42.0.3 from 192.0.2.10 # substitute actual pod/node addresses
sudo tcpdump -ni any 'tcp port 6443'
```

Replies to a local pod must use the pod interface/route, not an external
gateway selected by a host source-address rule. If that is the cause, add
destination rules for the pod/service CIDRs to the main table **ahead of** the
conflicting source rule, using unused priorities. Persist these with the host's
network configuration or a service required before K3s, and verify them after
restart. Priority numbers and routing tables depend on the existing host.

Test the SRW URL from another machine as well as locally. NodePort replies can
select a route before reverse NAT restores the node's source IP. If a capture
shows SYN arriving on one interface and SYN-ACK leaving another, a scoped
connection mark and reply policy rule may be needed for that ingress
interface/address/port. Do not route all pod-source traffic through the public
interface: that can break pod DNS and ordinary egress. Choose an unused mark
bit and rule priority after inspecting the host's nftables/routing configuration.
The generator cannot safely infer those host-specific rules.

Verify pod-to-node API transport, in-cluster service DNS, external DNS/HTTPS,
and external client access separately. Recheck existing Quadlet health. Once
these checks pass, install Helm at the explicit version in the generated
runbook and continue with `./install.sh` using the private kubeconfig.

Keep `chart-version.txt` and `installation-versions.txt` alongside the generated
files for retries. Save image digests from running pods for release evidence;
version pins alone do not guarantee immutable image tags. Back up both
`APP_ENCRYPTION_KEY` and the workspace SSH Secret privately as the runbook shows.
