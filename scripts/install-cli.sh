#!/bin/sh
# Install srw, the command-line client of Superhuman Remote Worker.
#
#   curl -fsSL https://docs.superhuman-remote-worker.com/install.sh | sh
#   curl -fsSL https://docs.superhuman-remote-worker.com/install.sh | sh -s -- --version 0.3.0
#
# It downloads the archive for this machine from the GitHub release, checks it
# against the release's checksums.txt and puts `srw` into ~/.local/bin (or
# --dir). It never calls sudo and never edits a shell profile.
#
# The whole script is one function that is called on the last line, so a
# download that breaks off halfway runs nothing.
set -eu

main() {
    repository=${SRW_INSTALL_REPOSITORY:-https://github.com/superhuman-remote-worker/srw}
    version=""
    directory="${HOME:-}/.local/bin"

    while [ "$#" -gt 0 ]; do
        case "$1" in
            --version)
                [ "$#" -ge 2 ] || fail "--version needs a value, for example --version 0.3.0"
                version=${2#v}
                shift 2
                ;;
            --dir)
                [ "$#" -ge 2 ] || fail "--dir needs a directory"
                directory=$2
                shift 2
                ;;
            -h | --help)
                echo "usage: install.sh [--version X.Y.Z] [--dir DIRECTORY]"
                echo "Installs srw into ~/.local/bin, or into DIRECTORY."
                return 0
                ;;
            *)
                fail "unknown option $1 (see --help)"
                ;;
        esac
    done
    [ -n "$directory" ] && [ "$directory" != "/.local/bin" ] || fail "cannot tell where to install; pass --dir DIRECTORY"

    case "$(uname -s)" in
        Linux) os=linux ;;
        Darwin) os=darwin ;;
        MINGW* | MSYS* | CYGWIN* | Windows_NT)
            fail "on Windows, download srw_windows_amd64.zip from $repository/releases/latest and unpack srw.exe into a directory on your PATH"
            ;;
        *) fail "srw has no build for $(uname -s)" ;;
    esac
    case "$(uname -m)" in
        x86_64 | amd64) arch=amd64 ;;
        aarch64 | arm64) arch=arm64 ;;
        *) fail "srw has no build for $(uname -m)" ;;
    esac

    archive="srw_${os}_${arch}.tar.gz"
    if [ -n "$version" ]; then
        base="$repository/releases/download/v$version"
    else
        base="$repository/releases/latest/download"
    fi

    if command -v sha256sum >/dev/null 2>&1; then
        sum="sha256sum"
    elif command -v shasum >/dev/null 2>&1; then
        sum="shasum -a 256"
    else
        fail "need sha256sum or shasum to check the download; install one of them"
    fi

    mkdir -p "$directory"
    # The temporary directory sits beside the target, so the last step is a
    # rename on one file system: srw is either the old file or the new one.
    scratch=$(mktemp -d "$directory/.srw-install.XXXXXX")
    trap 'rm -rf "$scratch"' EXIT INT TERM

    echo "Downloading $base/$archive"
    download "$base/$archive" "$scratch/$archive"
    download "$base/checksums.txt" "$scratch/checksums.txt"

    expected=$(awk -v name="$archive" '$2 == name || $2 == "*" name { print $1 }' "$scratch/checksums.txt")
    [ -n "$expected" ] || fail "checksums.txt does not list $archive"
    actual=$(cd "$scratch" && $sum "$archive" | awk '{ print $1 }')
    [ "$expected" = "$actual" ] || fail "the checksum of $archive does not match checksums.txt; nothing was installed"

    tar -xzf "$scratch/$archive" -C "$scratch" srw
    chmod 755 "$scratch/srw"
    mv -f "$scratch/srw" "$directory/srw"

    echo "Installed $("$directory/srw" version --client 2>/dev/null | head -n 1 || echo srw) to $directory/srw"
    case ":${PATH:-}:" in
        *":$directory:"*) ;;
        *) echo "$directory is not on your PATH. Add it, for example: export PATH=\"$directory:\$PATH\"" ;;
    esac
    echo "Next: srw login"
}

download() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --proto '=https,file' --tlsv1.2 -o "$2" "$1" || fail "cannot download $1"
    elif command -v wget >/dev/null 2>&1; then
        wget -q -O "$2" "$1" || fail "cannot download $1"
    else
        fail "need curl or wget to download srw"
    fi
}

fail() {
    echo "install.sh: $*" >&2
    exit 1
}

main "$@"
