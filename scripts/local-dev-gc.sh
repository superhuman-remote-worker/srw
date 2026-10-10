#!/usr/bin/env bash
# =============================================================================
# local-dev-gc.sh — reclaim the disk that the local k3d + Tilt loop leaks.
#
# Every Tilt build pushes fresh tags to the k3d registry and the k3d node's
# containerd, and nothing collects them: the registry never garbage-collects,
# and the node's kubelet only prunes images once the host disk is 85% full.
# Left alone, the registry grows past 100 GB within weeks.
#
# What it removes, keeping anything a k3d workload still references:
#   1. Registry: every tag except the newest KEEP_TAGS per repository, tags
#      younger than KEEP_HOURS, and tags any k3d cluster references; then
#      `registry garbage-collect --delete-untagged`. The registry is stopped
#      during the collection (usually under a minute). Skipped while Tilt is
#      building.
#   2. Node: registry images in the k3d nodes that no container uses and step 1
#      kept nothing of. Images imported by hand (pause, busybox) are untouched.
#   3. Host: older Tilt-built tags in the host Docker image store, dangling
#      images, and BuildKit cache above BUILD_CACHE_MAX.
#
# Fails safe: if any running k3d cluster cannot report its workloads, steps 1
# and 2 are skipped rather than run blind.
#
# Usage: scripts/local-dev-gc.sh [--dry-run | --install-timer | --remove-timer]
#   --install-timer  run daily via a systemd user timer; the unit records this
#                    checkout's path, your PATH, and the settings below
#   --remove-timer   disable and delete that timer
#
# Settings (environment): CLUSTER_NAME (srw), REGISTRY_NAME (<cluster>-registry),
# KEEP_TAGS (2), KEEP_HOURS (24), BUILD_CACHE_MAX (20GB).
# =============================================================================
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-srw}"
REGISTRY_NAME="${REGISTRY_NAME:-${CLUSTER_NAME}-registry}"
KEEP_TAGS="${KEEP_TAGS:-2}"
KEEP_HOURS="${KEEP_HOURS:-24}"
BUILD_CACHE_MAX="${BUILD_CACHE_MAX:-20GB}"
DRY_RUN=0
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
UNIT=srw-local-dev-gc

log()  { printf '\033[1;34m[gc]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ok]\033[0m %s\n' "$*"; }
skip() { printf '\033[1;33m[skip]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

install_timer() {
  command -v systemctl >/dev/null || die "systemd not found; schedule $SELF with cron instead"
  mkdir -p "$UNIT_DIR"
  cat > "$UNIT_DIR/$UNIT.service" <<EOF
[Unit]
Description=Prune the local k3d registry, node, and Tilt image churn

[Service]
Type=oneshot
Environment="PATH=$PATH"
Environment=CLUSTER_NAME=$CLUSTER_NAME REGISTRY_NAME=$REGISTRY_NAME
Environment=KEEP_TAGS=$KEEP_TAGS KEEP_HOURS=$KEEP_HOURS BUILD_CACHE_MAX=$BUILD_CACHE_MAX
ExecStart="$SELF"
Nice=10
IOSchedulingClass=idle
EOF
  cat > "$UNIT_DIR/$UNIT.timer" <<EOF
[Unit]
Description=Daily local k3d/Tilt image pruning

[Timer]
OnCalendar=*-*-* 04:30
RandomizedDelaySec=30min
Persistent=true

[Install]
WantedBy=timers.target
EOF
  systemctl --user daemon-reload
  systemctl --user --quiet enable --now "$UNIT.timer"
  ok "daily timer installed for $SELF"
  systemctl --user list-timers "$UNIT.timer" --no-pager | awk 'NR <= 2'
  echo "Logs: journalctl --user -u $UNIT"
}

remove_timer() {
  systemctl --user --quiet disable --now "$UNIT.timer" 2>/dev/null || true
  rm -f "$UNIT_DIR/$UNIT.service" "$UNIT_DIR/$UNIT.timer"
  systemctl --user daemon-reload
  ok "daily timer removed"
}

case "${1:-}" in
  "") ;;
  --dry-run) DRY_RUN=1 ;;
  --install-timer) install_timer; exit 0 ;;
  --remove-timer) remove_timer; exit 0 ;;
  -h|--help) awk 'NR > 2 && /^# ====/ {exit} NR > 2 {sub(/^# ?/, ""); print}' "$0"; exit 0 ;;
  *) die "unknown argument: $1 (try --help)" ;;
esac

command -v docker >/dev/null || die "docker not found"
command -v python3 >/dev/null || die "python3 not found"

exec 9>"${XDG_RUNTIME_DIR:-/tmp}/srw-local-dev-gc.lock"
flock -n 9 || { skip "another local-dev-gc run is in progress"; exit 0; }

WORK=$(mktemp -d)
REGISTRY_STOPPED=0
cleanup() {
  if [ "$REGISTRY_STOPPED" = 1 ]; then
    docker start "$REGISTRY_NAME" >/dev/null || printf '[error] restart %s by hand\n' "$REGISTRY_NAME" >&2
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

[ "$DRY_RUN" = 1 ] && log "dry run: nothing will be deleted"
avail_before=$(df -B1 --output=avail /var/lib | tail -1)

# --- References held by k3d workloads ----------------------------------------
# Normalised to `repo:tag` or `repo@sha256:...`. Every running k3d cluster is
# asked, since any of them may pull from the shared registry.
refs_ok=1
: > "$WORK/refs"
for node in $(docker ps --filter label=app=k3d --filter label=k3d.role=server --format '{{.Names}}'); do
  if ! docker exec "$node" kubectl get pods,deployments,statefulsets,daemonsets,jobs,cronjobs,configmaps \
      --all-namespaces -o yaml > "$WORK/objects" 2>/dev/null; then
    skip "$node did not list its workloads; registry and node pruning are off this run"
    refs_ok=0
    continue
  fi
  grep -oE "(${REGISTRY_NAME}|localhost|127\.0\.0\.1):[0-9]+/[A-Za-z0-9._/-]+(:[A-Za-z0-9._-]+|@sha256:[0-9a-f]{64})" \
    "$WORK/objects" | sed -E 's#^[^/]+/##' >> "$WORK/refs" || true
done
sort -u -o "$WORK/refs" "$WORK/refs"
[ "$refs_ok" = 1 ] && log "$(wc -l < "$WORK/refs") registry image references held by k3d workloads"

# --- 1. Registry ---------------------------------------------------------------
registry_listed=0
if [ "$refs_ok" != 1 ]; then
  :
elif [ "$(docker inspect -f '{{.State.Running}}' "$REGISTRY_NAME" 2>/dev/null)" != true ]; then
  skip "registry $REGISTRY_NAME is not running"
else
  # One line per tag: <mtime> <repo> <tag> <digest>
  docker exec "$REGISTRY_NAME" sh -c '
    cd /var/lib/registry/docker/registry/v2/repositories 2>/dev/null || exit 0
    find . -path "*/_manifests/tags/*/current/link" | while read -r f; do
      t=${f%/current/link}; r=${t%/_manifests/tags/*}
      echo "$(stat -c %Y "$f") ${r#./} ${t##*/} $(cat "$f")"
    done' > "$WORK/tags"
  registry_listed=1

  # Writes $WORK/keep (repo:tag and repo@digest lines) and $WORK/drop (repo tag lines); exits 3
  # if a referenced digest has no tag, since --delete-untagged would remove it.
  set +e
  python3 - "$WORK" "$KEEP_TAGS" "$KEEP_HOURS" <<'PY'
import collections, os, sys, time
work, keep_tags, keep_hours = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
refs = set(open(os.path.join(work, "refs")).read().split())
by_repo = collections.defaultdict(list)
for line in open(os.path.join(work, "tags")):
    mtime, repo, tag, digest = line.split()
    by_repo[repo].append((int(mtime), tag, digest))
cutoff = time.time() - keep_hours * 3600
keep, drop = [], []
for repo, tags in by_repo.items():
    tags.sort(reverse=True)
    for i, (mtime, tag, digest) in enumerate(tags):
        held = f"{repo}:{tag}" in refs or f"{repo}@{digest}" in refs
        (keep if i < keep_tags or mtime > cutoff or held else drop).append((repo, tag, digest))
tagged = {f"{repo}@{digest}" for repo, tags in by_repo.items() for _, _, digest in tags}
orphans = sorted(r for r in refs if "@sha256:" in r and r not in tagged and r.split("@")[0] in by_repo)
with open(os.path.join(work, "keep"), "w") as f:
    f.writelines(f"{r}:{t}\n{r}@{d}\n" for r, t, d in keep)
with open(os.path.join(work, "drop"), "w") as f:
    f.writelines(f"{r} {t}\n" for r, t, _ in drop)
print(f"registry: keeping {len(keep)} tags, dropping {len(drop)} across {len(by_repo)} repositories")
for r in orphans:
    print(f"referenced digest has no tag, refusing to collect: {r}")
sys.exit(3 if orphans else 0)
PY
  select_rc=$?
  set -e
  [ "$select_rc" = 0 ] || [ "$select_rc" = 3 ] || die "registry tag selection failed"

  building=""
  if command -v tilt >/dev/null; then
    building=$(tilt get uiresources -o json 2>/dev/null | python3 -c '
import json, sys
items = json.load(sys.stdin)["items"]
print(" ".join(i["metadata"]["name"] for i in items
               if ((i.get("status") or {}).get("currentBuild") or {}).get("startTime")))' 2>/dev/null || true)
  fi

  if [ "$select_rc" = 3 ]; then
    skip "registry collection skipped (see above)"
  elif [ ! -s "$WORK/drop" ]; then
    ok "registry has nothing to drop"
  elif [ -n "$building" ]; then
    skip "Tilt is building ($building); registry collection deferred"
  elif [ "$DRY_RUN" = 1 ]; then
    log "registry: would drop these tags"
    sed 's/ /:/; s/^/  /' "$WORK/drop" | sort | awk 'NR <= 40'
    if [ "$(wc -l < "$WORK/drop")" -gt 40 ]; then
      echo "  ... and $(( $(wc -l < "$WORK/drop") - 40 )) more"
    fi
  else
    image=$(docker inspect -f '{{.Config.Image}}' "$REGISTRY_NAME")
    before=$(docker exec "$REGISTRY_NAME" du -sk /var/lib/registry | cut -f1)
    log "stopping $REGISTRY_NAME to collect $(wc -l < "$WORK/drop") tags"
    docker stop "$REGISTRY_NAME" >/dev/null
    REGISTRY_STOPPED=1
    docker run --rm -i --volumes-from "$REGISTRY_NAME" --entrypoint sh "$image" -c '
      set -e
      root=/var/lib/registry/docker/registry/v2/repositories
      while read -r repo tag; do rm -rf "$root/$repo/_manifests/tags/$tag"; done
      registry garbage-collect --delete-untagged /etc/docker/registry/config.yml > /dev/null 2>&1
    ' < "$WORK/drop"
    docker start "$REGISTRY_NAME" >/dev/null
    REGISTRY_STOPPED=0
    after=$(docker exec "$REGISTRY_NAME" du -sk /var/lib/registry | cut -f1)
    ok "registry: $(( before / 1048576 )) GiB -> $(( after / 1048576 )) GiB"
  fi
fi

# --- 2. k3d node containerd ----------------------------------------------------
if [ "$registry_listed" != 1 ]; then
  skip "node image pruning needs the registry listing"
else
  cat "$WORK/refs" "$WORK/keep" > "$WORK/held"
  nodes=$(docker ps --filter "label=k3d.cluster=${CLUSTER_NAME}" --format '{{.Names}} {{.Label "k3d.role"}}' \
           | awk '$2 == "server" || $2 == "agent" {print $1}')
  for node in $nodes; do
    docker exec "$node" crictl images -o json > "$WORK/images.json"
    docker exec "$node" crictl ps -a -o json > "$WORK/containers.json"
    python3 - "$WORK" "$REGISTRY_NAME" > "$WORK/node-drop" <<'PY'
import json, os, sys
work, registry = sys.argv[1], sys.argv[2]
held = set(open(os.path.join(work, "held")).read().split())
used = set()
for c in json.load(open(os.path.join(work, "containers.json")))["containers"]:
    used.update(filter(None, [c.get("imageRef"), (c.get("image") or {}).get("image")]))
for img in json.load(open(os.path.join(work, "images.json")))["images"]:
    names = [n.split("/", 1)[1] for n in (img.get("repoTags") or []) + (img.get("repoDigests") or [])
             if n.startswith(registry + ":")]
    if not names or img.get("pinned") or img["id"] in used or any(n in held for n in names):
        continue
    print(img["id"], int(img.get("size") or 0), names[0])
PY
    count=$(wc -l < "$WORK/node-drop")
    gib=$(awk '{s += $2} END {printf "%.1f", s / 1073741824}' "$WORK/node-drop")
    if [ "$count" = 0 ]; then
      ok "$node: no unused registry images"
    elif [ "$DRY_RUN" = 1 ]; then
      log "$node: would remove $count unused images (~$gib GiB before layer sharing)"
      awk '{print "  " $3}' "$WORK/node-drop" | sort | awk 'NR <= 20'
    else
      if awk '{print $1}' "$WORK/node-drop" | xargs docker exec "$node" crictl --timeout 120s rmi > /dev/null; then
        ok "$node: removed $count unused images (~$gib GiB before layer sharing)"
      else
        skip "$node: some images could not be removed (started in use?); the rest are gone"
      fi
    fi
  done
fi

# --- 3. Host Docker ------------------------------------------------------------
port=$(docker inspect -f '{{index .Config.Labels "k3s.registry.port.external"}}' "$REGISTRY_NAME" 2>/dev/null || true)
docker images --no-trunc --format '{{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.CreatedAt}}' > "$WORK/host-images"
# A container can vanish between ps and inspect (testcontainers' reaper); inspect
# still prints the rest. A missed one is safe: rmi refuses an image a container uses.
docker ps -aq | xargs -r docker inspect -f '{{.Image}}' > "$WORK/host-used" 2>/dev/null || true
python3 - "$WORK" "localhost:${port:-5005}/" "$KEEP_TAGS" "$KEEP_HOURS" > "$WORK/host-drop" <<'PY'
import collections, datetime, os, sys, time
work, prefix, keep_tags, keep_hours = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
used = set(open(os.path.join(work, "host-used")).read().split())
by_repo = collections.defaultdict(list)
for line in open(os.path.join(work, "host-images")):
    repo, tag, image_id, created = line.rstrip("\n").split("\t")
    if repo.startswith(prefix) and tag.startswith("tilt-"):
        ts = datetime.datetime.strptime(created[:25], "%Y-%m-%d %H:%M:%S %z").timestamp()
        by_repo[repo].append((ts, tag, image_id))
cutoff = time.time() - keep_hours * 3600
for repo, tags in by_repo.items():
    tags.sort(reverse=True)
    for i, (ts, tag, image_id) in enumerate(tags):
        if i >= keep_tags and ts < cutoff and image_id not in used:
            print(f"{repo}:{tag}")
PY
if [ "$DRY_RUN" = 1 ]; then
  log "host: would remove $(wc -l < "$WORK/host-drop") older Tilt image tags, dangling images, and build cache above $BUILD_CACHE_MAX"
  sed 's/^/  /' "$WORK/host-drop"
  docker buildx du 2>/dev/null | tail -1 | sed 's/^/  build cache /'
else
  if [ -s "$WORK/host-drop" ]; then
    xargs docker rmi < "$WORK/host-drop" > /dev/null || skip "host: some Tilt tags could not be removed"
  fi
  docker image prune -f > /dev/null
  docker builder prune -f --max-used-space "$BUILD_CACHE_MAX" > /dev/null
  ok "host: removed $(wc -l < "$WORK/host-drop") older Tilt image tags; build cache capped at $BUILD_CACHE_MAX"
fi

avail_after=$(df -B1 --output=avail /var/lib | tail -1)
[ "$DRY_RUN" = 1 ] || ok "freed $(( (avail_after - avail_before) / 1073741824 )) GiB (btrfs may release more over the next minute)"
