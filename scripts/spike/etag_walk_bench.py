# scripts/spike/etag_walk_bench.py — Phase 0 spike (protected cloud mode), Task 5.
#
# Times the mount-time etag baseline (design §3.4): a full-tree
# ``list_project_folder`` walk through the orchestrator's main-cloud backend,
# exactly as the overlay mount path would run it. On Nextcloud/OpenCloud this
# is a breadth-first series of ``Depth: 1`` PROPFINDs (one per directory);
# the script also counts those HTTP requests by wrapping the backend's
# per-directory helper.
#
# ``--depth-infinity`` mode instead sends ONE ``Depth: infinity`` PROPFIND for
# the whole tree through the same backend client + agent auth. Nextcloud's
# groupfolders DAV endpoint accepts this (measured Task 5); OpenCloud rejects
# it with HTTP 400 per the code comment at
# ``orchestrator/services/cloud/opencloud.py:763`` (not re-tested live — the
# local in-chart OpenCloud was down during the spike), so the mode is
# Nextcloud-only. Count reconciliation: the raw multistatus contains one
# ``<d:response>`` per tree node PLUS the requested collection's own
# self-href; ``parse_propfind_entries`` drops that root self-entry, so
# ``entries == raw_responses - 1``. Unlike the BFS walk, this path reports
# each subdirectory exactly once (the BFS's duplicate-subdir quirk — parent
# listing + own self-entry — cannot occur with a single request).
#
# Run IN the orchestrator pod with its installed application packages:
#   kubectl -n srw exec -i deploy/srw-orchestrator -- python3 -u - \
#     < scripts/spike/etag_walk_bench.py <backend_id> <handle_db> [runs] [--depth-infinity]
# The image does NOT contain scripts/ — to run by path instead of stdin you
# must ``kubectl cp`` the script into the pod first:
#   kubectl -n srw cp scripts/spike/etag_walk_bench.py <pod>:/tmp/etag_walk_bench.py
#   kubectl -n srw exec <pod> -- python3 -u /tmp/etag_walk_bench.py <backend_id> <handle_db>
#
# ``handle_db`` is the projects.main_cloud_folder_handle column value, e.g.
#   '{"backend": "nextcloud", "native_id": "1", "vendor_meta": {"mountpoint": "my-project"}}'
#
# NOTE: ``build_application_resources()`` constructs one application's resources
# (no connection, no background loop; those hang off the lifespan handler);
# router state still needs an explicit ``ensure_initialized()``.
import asyncio
import sys
import time

from orchestrator.application import build_application_resources
from orchestrator.services.cloud import ProjectFolderHandle
from orchestrator.services.cloud._propfind import parse_propfind_entries


async def _init_backend(backend_id: str, handle_db: str):
    router = build_application_resources().main_cloud_router
    ok = await router.ensure_initialized()
    if not ok:
        print("FATAL: main-cloud backend failed ensure_initialized()", flush=True)
        raise SystemExit(1)
    backend = router.for_backend(backend_id)
    handle = ProjectFolderHandle.from_db(handle_db, backend=backend_id)
    return backend, handle


async def bench_walk(backend_id: str, handle_db: str, runs: int) -> None:
    """Product path: ``list_project_folder`` (sequential depth-1 BFS)."""
    backend, handle = await _init_backend(backend_id, handle_db)

    # Count per-directory PROPFINDs without touching product code. Both the
    # Nextcloud and OpenCloud adapters walk the tree by calling a single
    # depth-1 helper once per directory (``_propfind_depth_one``).
    counter = {"n": 0}
    helper = getattr(backend, "_propfind_depth_one", None)
    if helper is not None:

        async def counting(*a, **kw):
            counter["n"] += 1
            return await helper(*a, **kw)

        backend._propfind_depth_one = counting

    for run in range(1, runs + 1):
        counter["n"] = 0
        t0 = time.monotonic()
        entries = await backend.list_project_folder(handle)
        dt = time.monotonic() - t0
        files = [e for e in entries if not e.is_dir]
        dirs = [e for e in entries if e.is_dir]
        with_etag = sum(1 for e in files if e.etag)
        # NB: ``dirs`` double-counts subdirectories (parent listing + own
        # self-entry — see Task 5 findings), so derive the request count from
        # the wrapped helper, falling back to unique dir paths + root.
        reqs = counter["n"] if helper is not None else 1 + len({d.path for d in dirs})
        print(
            f"[run {run}] backend={backend.backend_id} files={len(files)} "
            f"dirs={len(dirs)} entries={len(entries)} etags={with_etag} "
            f"requests={reqs} walk={dt:.3f}s "
            f"({len(files) / dt:.1f} files/s, {dt / max(reqs, 1) * 1000:.1f} ms/req)",
            flush=True,
        )


async def bench_depth_infinity(backend_id: str, handle_db: str, runs: int) -> None:
    """One ``Depth: infinity`` PROPFIND for the full tree (Nextcloud only)."""
    backend, handle = await _init_backend(backend_id, handle_db)
    if not hasattr(backend, "_groupfolder_dav_base"):
        print(
            f"FATAL: --depth-infinity is Nextcloud-only (backend is "
            f"{backend.backend_id}; OpenCloud rejects Depth: infinity with "
            f"400 per opencloud.py:763)",
            flush=True,
        )
        raise SystemExit(1)

    base_path = backend._groupfolder_dav_base(handle)
    href_prefix = f"{base_path}/"
    for run in range(1, runs + 1):
        t0 = time.monotonic()
        resp = await backend._client.request(
            "PROPFIND",
            base_path,
            headers={"Depth": "infinity"},
            auth=(backend._agent_user, backend._agent_password),
        )
        dt = time.monotonic() - t0
        if resp.status_code != 207:
            print(
                f"[inf run {run}] NOT SUPPORTED: status={resp.status_code} "
                f"({resp.text[:200]!r})",
                flush=True,
            )
            continue
        raw_responses = resp.text.count("<d:response>")
        entries = parse_propfind_entries(resp.text, href_prefix=href_prefix)
        files = [e for e in entries if not e.is_dir]
        dirs = [e for e in entries if e.is_dir]
        with_etag = sum(1 for e in files if e.etag)
        print(
            f"[inf run {run}] backend={backend.backend_id} status=207 "
            f"raw_responses={raw_responses} entries={len(entries)} "
            f"(= raw - 1 root self-href) files={len(files)} dirs={len(dirs)} "
            f"etags={with_etag} requests=1 walk={dt:.3f}s "
            f"({len(files) / dt:.1f} files/s)",
            flush=True,
        )


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--depth-infinity"]
    infinity = "--depth-infinity" in sys.argv[1:]
    if len(args) < 2:
        print(
            "usage: etag_walk_bench.py <backend_id> <handle_db> [runs] "
            "[--depth-infinity]"
        )
        raise SystemExit(2)
    runs = int(args[2]) if len(args) > 2 else 2
    fn = bench_depth_infinity if infinity else bench_walk
    asyncio.run(fn(args[0], args[1], runs))
