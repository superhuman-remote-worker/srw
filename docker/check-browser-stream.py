#!/usr/bin/env python3
"""Conformance check for browser-exec's shared-browser stream mode.

Runs inside the workspace image against the real daemon and Chromium.
Exit zero proves stream identity/authentication, live screencast frames,
baton refusal, viewer control navigation, input dispatch, and baton release.
"""

import asyncio
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

os.environ.setdefault("BROWSER_EXEC_SOCKET", "/tmp/bx-check.sock")
os.environ.setdefault("BROWSER_EXEC_PROFILE", "/tmp/bx-check-profile")
os.environ.setdefault("BROWSER_EXEC_LOG", "/tmp/bx-check.log")
os.environ.setdefault("BROWSER_EXEC_STREAM_PORT", "38801")

BROWSER_EXEC = os.environ.get("BROWSER_EXEC_BIN", "/usr/local/bin/browser-exec")

loader = importlib.machinery.SourceFileLoader("browser_exec_mod", BROWSER_EXEC)
spec = importlib.util.spec_from_loader("browser_exec_mod", loader)
BE = importlib.util.module_from_spec(spec)
loader.exec_module(BE)

PAGE = Path("/tmp/bx-check-page.html")
POPUP_PAGE = Path("/tmp/bx-check-popup.html")
SOCKET = Path(os.environ["BROWSER_EXEC_SOCKET"])
PROFILE = Path(os.environ["BROWSER_EXEC_PROFILE"])
LOG = Path(os.environ["BROWSER_EXEC_LOG"])


def unix_action(action: str, args: dict, timeout: float = 120.0) -> dict:
    request = json.dumps({"action": action, "args": args}).encode() + b"\n"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(os.environ["BROWSER_EXEC_SOCKET"])
        client.sendall(request)
        client.shutdown(socket.SHUT_WR)
        response = b""
        while not response.endswith(b"\n"):
            chunk = client.recv(65536)
            if not chunk:
                break
            response += chunk
    return json.loads(response.decode())


def check(label: str, ok: bool, detail: str = "") -> None:
    status = "ok      " if ok else "FAILED  "
    suffix = f"  {detail}" if detail else ""
    print(f"  {status} {label}{suffix}", flush=True)
    if not ok:
        raise RuntimeError(label)


async def expect_frame(reader, wanted_type: int, timeout: float = 20.0):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"no frame of type {wanted_type}")
        frame_type, payload = await asyncio.wait_for(
            BE.read_stream_frame(reader), timeout=remaining
        )
        if frame_type == wanted_type:
            return payload


async def send_json(writer, frame_type: int, value: dict) -> None:
    writer.write(BE.encode_stream_frame(frame_type, json.dumps(value).encode()))
    await writer.drain()


async def observe_navigation(reader, expected_name: str, timeout: float = 30.0):
    """Require a main-frame loading true→false transition at one URL."""
    deadline = time.monotonic() + timeout
    states = []
    saw_loading = False
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"navigation did not settle at {expected_name}")
        frame_type, payload = await asyncio.wait_for(
            BE.read_stream_frame(reader), timeout=remaining
        )
        if frame_type == BE.T_ERROR:
            raise RuntimeError(f"stream error: {json.loads(payload.decode())}")
        if frame_type != BE.T_STATE:
            continue
        state = json.loads(payload.decode())
        states.append(state)
        if state.get("loading") is True:
            saw_loading = True
        if (
            saw_loading
            and state.get("loading") is False
            and expected_name in (state.get("url") or "")
        ):
            return states


async def expect_state(reader, predicate, timeout: float = 30.0):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("expected browser state did not arrive")
        frame_type, payload = await asyncio.wait_for(
            BE.read_stream_frame(reader), timeout=remaining
        )
        if frame_type == BE.T_ERROR:
            raise RuntimeError(f"stream error: {json.loads(payload.decode())}")
        if frame_type == BE.T_STATE:
            state = json.loads(payload.decode())
            if predicate(state):
                return state


async def expect_frame_size(reader, width: int, height: int, timeout: float = 20.0):
    """The next FRAME whose CSS size is width x height: (header, jpeg)."""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"no {width}x{height} frame")
        header, jpeg = BE.decode_frame_payload(
            await expect_frame(reader, BE.T_FRAME, timeout=remaining)
        )
        if (round(header.get("w") or 0), round(header.get("h") or 0)) == (
            width,
            height,
        ):
            return header, jpeg


async def collect_states(reader, seconds: float) -> list:
    """Every STATE for ``seconds``; stops only between whole frames."""
    deadline = time.monotonic() + seconds
    states = []
    while time.monotonic() < deadline:
        frame_type, payload = await asyncio.wait_for(
            BE.read_stream_frame(reader), timeout=20
        )
        if frame_type == BE.T_STATE:
            states.append(json.loads(payload.decode()))
    return states


async def close_writer(writer) -> None:
    if writer is None:
        return
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


def remove_artifacts() -> None:
    for path in (SOCKET, PAGE, POPUP_PAGE, LOG):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    shutil.rmtree(PROFILE, ignore_errors=True)


async def main() -> int:
    print("Shared-browser stream conformance:", flush=True)
    remove_artifacts()
    PAGE.write_text(
        """<!doctype html>
<html><head><title>Main stream check</title></head><body style="margin:0">
<a id="popup" href="bx-check-popup.html" target="_blank"
   style="position:absolute;left:20px;top:20px;width:240px;height:50px;
          line-height:50px;background:#acf">Open popup target</a>
<h1 id="t" style="padding-top:100px">main stream check</h1>
<script>
let n = 0;
setInterval(() => { document.getElementById("t").textContent =
  `main stream check ${n++}`; }, 100);
</script></body></html>
"""
    )
    POPUP_PAGE.write_text(
        """<!doctype html>
<html><head><title>Popup stream check</title></head><body>
<h1 id="t">popup stream check</h1>
<script>
let n = 0;
setInterval(() => { document.getElementById("t").textContent =
  `popup stream check ${n++}`; }, 100);
</script></body></html>
"""
    )

    log_handle = None
    daemon = None
    writers = []
    try:
        log_handle = LOG.open("ab")
        daemon = subprocess.Popen(
            [sys.executable, BROWSER_EXEC, "serve"],
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        for _ in range(120):
            if SOCKET.exists():
                break
            if daemon.poll() is not None:
                raise RuntimeError("browser-exec daemon exited during startup")
            await asyncio.sleep(0.25)
        check("daemon socket ready", SOCKET.exists())

        info = unix_action("stream_info", {"initial_baton": "user"})
        check(
            "stream_info returns identity",
            "generation" in info and "token" in info,
            str({key: info.get(key) for key in ("generation", "port", "baton")}),
        )
        check("initial baton honoured", info.get("baton") == "user")
        port = int(info["port"])

        # Wrong token: ERROR then close.
        bad_reader, bad_writer = await asyncio.open_connection("127.0.0.1", port)
        writers.append(bad_writer)
        await send_json(
            bad_writer,
            BE.T_HELLO,
            {"token": "nope", "min_protocol": 1},
        )
        frame_type, payload = await asyncio.wait_for(
            BE.read_stream_frame(bad_reader), timeout=5
        )
        check(
            "bad token rejected",
            frame_type == BE.T_ERROR and json.loads(payload)["code"] == "unauthorized",
        )
        check("bad-token connection closed", await bad_reader.read(1) == b"")
        await close_writer(bad_writer)

        # Authenticated viewer establishes a daemon-wide one-viewer cap.
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writers.append(writer)
        await send_json(
            writer,
            BE.T_HELLO,
            {"token": info["token"], "min_protocol": 1, "max_viewers": 1},
        )
        state = json.loads(await expect_frame(reader, BE.T_STATE, timeout=5))
        check(
            "STATE on connect",
            state["generation"] == info["generation"] and state["baton"] == "user",
        )

        limit_reader, limit_writer = await asyncio.open_connection("127.0.0.1", port)
        writers.append(limit_writer)
        await send_json(
            limit_writer,
            BE.T_HELLO,
            {"token": info["token"], "min_protocol": 1, "max_viewers": 16},
        )
        frame_type, payload = await asyncio.wait_for(
            BE.read_stream_frame(limit_reader), timeout=5
        )
        check(
            "daemon-wide viewer cap",
            frame_type == BE.T_ERROR
            and json.loads(payload.decode()).get("code") == "viewer_limit",
        )
        check("limited connection closed", await limit_reader.read(1) == b"")
        await close_writer(limit_writer)

        # Navigate to the repainting main page and require real load state.
        await send_json(
            writer,
            BE.T_CONTROL,
            {"op": "navigate", "url": PAGE.as_uri()},
        )
        navigation_states = await observe_navigation(reader, PAGE.name)
        check(
            "main navigation loading transition",
            any(item.get("loading") is True for item in navigation_states)
            and navigation_states[-1].get("loading") is False,
            navigation_states[-1].get("url") or "",
        )

        frame_payload = await expect_frame(reader, BE.T_FRAME)
        header, jpeg = BE.decode_frame_payload(frame_payload)
        check(
            "screencast frame arrives",
            jpeg[:2] == b"\xff\xd8",
            f"{len(jpeg)} bytes",
        )
        check(
            "frame carries generation",
            header["generation"] == info["generation"],
        )

        # Mutating agent actions are refused; reads remain available.
        response = unix_action("navigate", {"url": PAGE.as_uri()})
        check(
            "agent navigate refused while user drives",
            response.get("error") == "user_is_driving",
        )
        response = unix_action("screenshot", {})
        check(
            "agent screenshot allowed while user drives",
            "screenshot" in response and response.get("baton") == "user",
        )

        # Viewer input dispatch must leave the stream alive.
        await send_json(
            writer,
            BE.T_INPUT,
            {
                "kind": "mouse",
                "params": {
                    "type": "mouseMoved",
                    "x": 5,
                    "y": 5,
                },
            },
        )
        await asyncio.sleep(0.5)

        # The viewer sizes the page: opt in, then each size reaches STATE and
        # the frames 1:1 (the window is resized; a phone width is emulated
        # inside Chromium's narrowest window).
        await send_json(
            writer,
            BE.T_CONTROL,
            {"op": "client_features", "features": ["viewport_control"]},
        )
        state = await expect_state(reader, lambda item: "viewport_control" in item)
        check(
            "viewport control advertised after opt-in",
            state.get("viewport_control") is True,
        )
        for width, height, label in (
            (1024, 700, "pane-sized"),
            (390, 844, "phone width (emulated)"),
            (768, 1024, "tablet portrait"),
            (1440, 900, "desktop"),
        ):
            await send_json(
                writer,
                BE.T_CONTROL,
                {"op": "set_viewport", "width": width, "height": height},
            )
            wanted = {"width": width, "height": height}
            state = await expect_state(
                reader, lambda item, wanted=wanted: item.get("viewport") == wanted
            )
            header, jpeg = await expect_frame_size(reader, width, height)
            check(
                f"set_viewport {label}",
                state["viewport"] == wanted,
                f"{width}x{height}, frame {len(jpeg)} bytes",
            )
        await send_json(
            writer,
            BE.T_CONTROL,
            {"op": "set_viewport", "width": 5000, "height": 5000},
        )
        state = await expect_state(
            reader, lambda item: item["viewport"]["width"] > 1440
        )
        check(
            "set_viewport stays inside the screen",
            state["viewport"]["width"] <= BE.SCREEN_WIDTH
            and state["viewport"]["height"] < BE.SCREEN_HEIGHT,
            str(state["viewport"]),
        )
        await send_json(
            writer,
            BE.T_CONTROL,
            {"op": "set_viewport", "width": 1280, "height": 800},
        )
        await expect_state(
            reader,
            lambda item: item["viewport"] == {"width": 1280, "height": 800},
        )

        # Release to browser-use and click its indexed target=_blank link. The
        # ClickElementEvent path owns agent focus; raw CDP clicks intentionally
        # leave a newly opened background tab out of focus.
        await send_json(writer, BE.T_CONTROL, {"op": "release_baton"})
        state = await expect_state(reader, lambda item: item.get("baton") == "agent")
        check("baton released", state["baton"] == "agent")
        await send_json(
            writer,
            BE.T_CONTROL,
            {"op": "set_viewport", "width": 800, "height": 600},
        )
        states = await collect_states(reader, 1.5)
        check(
            "set_viewport ignored while the agent drives",
            all(item["viewport"] == {"width": 1280, "height": 800} for item in states),
            str([item["viewport"] for item in states]),
        )
        snapshot = unix_action("snapshot", {})
        check(
            "target-blank page snapshot",
            "error" not in snapshot,
            str(snapshot.get("error", "")),
        )
        match = re.search(
            r"\[(\d+)\]<a\b.{0,2000}?Open popup target",
            snapshot.get("dom") or "",
            re.IGNORECASE | re.DOTALL,
        )
        check(
            "target-blank link indexed",
            match is not None,
            (snapshot.get("dom") or "")[:500].replace("\n", " "),
        )
        click_response = unix_action("click", {"ref": int(match.group(1))})
        check(
            "target-blank link clicked",
            "error" not in click_response,
            str(click_response.get("error", "")),
        )
        popup_state = await expect_state(
            reader,
            lambda item: POPUP_PAGE.name in (item.get("url") or ""),
        )
        check(
            "target-blank STATE follows active target",
            POPUP_PAGE.name in (popup_state.get("url") or ""),
            popup_state.get("url") or "",
        )
        popup_payload = await expect_frame(reader, BE.T_FRAME)
        _, popup_jpeg = BE.decode_frame_payload(popup_payload)
        check(
            "target-blank JPEG follows active target",
            popup_jpeg[:2] == b"\xff\xd8",
            f"{len(popup_jpeg)} bytes",
        )

        # Take and release once more across the switched target, then prove
        # browser-use mutations resume on that active page.
        await send_json(writer, BE.T_CONTROL, {"op": "take_baton"})
        await expect_state(reader, lambda item: item.get("baton") == "user")
        await send_json(writer, BE.T_CONTROL, {"op": "release_baton"})
        await expect_state(reader, lambda item: item.get("baton") == "agent")
        response = unix_action("navigate", {"url": PAGE.as_uri()})
        check(
            "agent navigate allowed after release",
            "error" not in response,
            str(response.get("error", "")),
        )

        # Zero viewers stops (but does not discard) the adapter. Reconnect,
        # navigate again, and detect callback multiplication via duplicate
        # states/JPEG payloads.
        await close_writer(writer)
        await asyncio.sleep(0.5)
        reconnect_reader, reconnect_writer = await asyncio.open_connection(
            "127.0.0.1", port
        )
        writers.append(reconnect_writer)
        await send_json(
            reconnect_writer,
            BE.T_HELLO,
            {"token": info["token"], "min_protocol": 1, "max_viewers": 1},
        )
        reconnect_state = json.loads(
            await expect_frame(reconnect_reader, BE.T_STATE, timeout=10)
        )
        check(
            "reconnect preserves generation",
            reconnect_state.get("generation") == info["generation"],
        )
        await expect_frame(reconnect_reader, BE.T_FRAME)
        await send_json(
            reconnect_writer,
            BE.T_CONTROL,
            {"op": "take_baton"},
        )
        await expect_state(
            reconnect_reader,
            lambda item: item.get("baton") == "user",
        )
        await send_json(
            reconnect_writer,
            BE.T_CONTROL,
            {"op": "navigate", "url": POPUP_PAGE.as_uri()},
        )
        reconnect_states = await observe_navigation(reconnect_reader, POPUP_PAGE.name)
        state_keys = [
            json.dumps(item, sort_keys=True, separators=(",", ":"))
            for item in reconnect_states
        ]
        check(
            "reconnect does not duplicate STATE callbacks",
            all(left != right for left, right in zip(state_keys, state_keys[1:])),
        )
        reconnect_jpegs = []
        for _ in range(4):
            payload = await expect_frame(reconnect_reader, BE.T_FRAME)
            _, jpeg = BE.decode_frame_payload(payload)
            reconnect_jpegs.append(jpeg)
        check(
            "reconnect does not duplicate FRAME callbacks",
            all(
                left != right
                for left, right in zip(reconnect_jpegs, reconnect_jpegs[1:])
            ),
        )
        await close_writer(reconnect_writer)

        print("Shared-browser stream conformance OK.", flush=True)
        return 0
    finally:
        for open_writer in writers:
            await close_writer(open_writer)
        if daemon is not None and daemon.poll() is None:
            try:
                unix_action("shutdown", {}, timeout=15)
            except Exception:
                pass
            try:
                daemon.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(daemon.pid, signal.SIGTERM)
                try:
                    daemon.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(daemon.pid, signal.SIGKILL)
                    daemon.wait(timeout=10)
        if daemon is not None:
            try:
                os.killpg(daemon.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            else:
                await asyncio.sleep(0.5)
                try:
                    os.killpg(daemon.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        if log_handle is not None:
            log_handle.close()
        remove_artifacts()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
