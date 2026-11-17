#!/usr/bin/env bash
# Build the srw command-line client for every supported target.
#
#   scripts/release/build_cli.sh VERSION COMMIT OUT_DIR
#
# VERSION is the platform release without a leading "v" (0.3.0), COMMIT the
# source revision. OUT_DIR receives:
#
#   srw_<os>_<arch>.tar.gz   linux and darwin, amd64 and arm64
#   srw_windows_<arch>.zip   amd64 and arm64
#   checksums.txt            plain `sha256sum` output over the six archives
#   install.sh               a copy of scripts/install-cli.sh
#
# Each archive holds one executable (srw, or srw.exe) and LICENSE. The names
# carry no version, so .../releases/latest/download/<name> and
# .../releases/download/vX.Y.Z/<name> both work without an API call. These
# names are public contract; do not change them.
#
# The archives are reproducible: the same sources, version, commit and Go
# toolchain give the same bytes. The darwin/arm64 binary is ad-hoc signed by
# the Go linker, also when cross-compiled here; do not strip or repack it.
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: $0 VERSION COMMIT OUT_DIR" >&2
    exit 2
fi
version=$1
commit=$2
out=$3

root=$(cd "$(dirname "$0")/../.." && pwd)
package=github.com/superhuman-remote-worker/srw/cli/internal/buildinfo
# One fixed timestamp for every file in every archive.
epoch=${SOURCE_DATE_EPOCH:-0}

mkdir -p "$out"
out=$(cd "$out" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

names=()
for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64; do
    os=${target%/*}
    arch=${target#*/}
    stage="$work/$os-$arch"
    mkdir -p "$stage"
    binary=srw
    if [ "$os" = windows ]; then
        binary=srw.exe
    fi
    (
        cd "$root/cli"
        CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -buildvcs=false \
            -ldflags "-s -w -X $package.Version=$version -X $package.Commit=$commit" \
            -o "$stage/$binary" .
    )
    cp "$root/LICENSE" "$stage/LICENSE"
    if [ "$os" = windows ]; then
        name="srw_${os}_${arch}.zip"
        python3 - "$stage" "$out/$name" "$epoch" <<'PY'
import sys, time, zipfile
from pathlib import Path

stage, target, epoch = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
# The zip format cannot hold a date before 1980.
stamp = time.gmtime(max(epoch, 315532800))[:6]
with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
    for path in sorted(stage.iterdir()):
        info = zipfile.ZipInfo(path.name, date_time=stamp)
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = (0o755 if path.suffix == ".exe" else 0o644) << 16
        archive.writestr(info, path.read_bytes())
PY
    else
        name="srw_${os}_${arch}.tar.gz"
        tar --sort=name --owner=0 --group=0 --numeric-owner --mtime="@$epoch" \
            -C "$stage" -cf - LICENSE srw | gzip -n -9 > "$out/$name"
    fi
    names+=("$name")
done

cp "$root/scripts/install-cli.sh" "$out/install.sh"
(cd "$out" && sha256sum "${names[@]}" > checksums.txt)
echo "built ${#names[@]} archives, checksums.txt and install.sh in $out"
