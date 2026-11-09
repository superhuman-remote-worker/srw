#!/usr/bin/env bash
# First visit to Nextcloud on a single-origin install, from a browser that
# already carries another cookie on the origin (the Cockpit's session cookie
# rides on every /cloud request). Nextcloud then never sends its same-site
# marker cookies, its login hop fails the lax-cookie check, and the redirect it
# answers with is mangled by getRequestUri() under OVERWRITEWEBROOT:
# /cloud/index.phpirect_url=... ("Page not found"). The chart hands out the
# markers (apache-samesite-markers.conf); this check proves the first visit
# lands on the login page.
#
# A cookie-less client cannot catch this: Nextcloud gives it the markers on
# its first response.
#
# Usage: scripts/single-origin-first-visit-check.sh https://localhost:8443 [folder]
set -euo pipefail

origin="${1:?usage: $0 <public origin, e.g. https://localhost:8443> [folder]}"
folder="${2:-Demo%20Folder}"
host="${origin#*://}"
host="${host%%/*}"
host="${host%%:*}"

jar="$(mktemp)"
trap 'rm -f "$jar"' EXIT
# The foreign cookie, as the Cockpit's BFF sets it: host-only, Path=/, Secure.
printf '%s\tFALSE\t/\tTRUE\t0\tsrw_session\tfirst-visit-check\n' "$host" >"$jar"

url="${origin}/cloud/apps/files/?dir=/${folder}"
status=""
for hop in 1 2 3 4 5; do
  headers="$(curl -sk -o /dev/null -D - -H 'Accept: text/html' -b "$jar" -c "$jar" "$url")"
  status="$(printf '%s\n' "$headers" | head -1 | awk '{print $2}')"
  location="$(printf '%s\n' "$headers" | grep -i '^location:' | tr -d '\r' | cut -d' ' -f2- || true)"
  echo "hop ${hop}: ${status} ${url#"$origin"}${location:+ -> ${location}}"
  case "$location" in
    *index.php[!/]*)
      echo "FAIL: Nextcloud redirected to a mangled URL (${location})" >&2
      exit 1
      ;;
  esac
  [ -z "$location" ] && break
  case "$location" in
    http*) url="$location" ;;
    *) url="${origin}${location}" ;;
  esac
done

if [ "$status" != "200" ] || [[ "$url" != "${origin}/cloud/login"* ]]; then
  echo "FAIL: the first visit did not land on the Nextcloud login (last: ${status} ${url})" >&2
  exit 1
fi
echo "OK: the first visit lands on the Nextcloud login"
