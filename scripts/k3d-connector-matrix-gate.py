#!/usr/bin/env python3
"""Local k3d gate for connector drivers D2: the generated capability matrix.

Design: knowledge-base/knowledge/features/connector_drivers.md, Track D, D2.
Gate: "the page lists every installed driver with its access levels and
'enforced by' lines, and the picker hides levels a driver doesn't offer."

Read-only: it creates, changes and deletes nothing, so it needs no
confirmation and no cleanup. Same envelope as the D1a gate otherwise: the
exact k3d-srw/srw context, the password only on ``kubectl exec -i`` stdin and
scrubbed from every printed line.

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight  the orchestrator pod serves this checkout's matrix module, the
             datasource router and the built-in specs, byte for byte
  api        GET /api/datasources/drivers as the test account answers 200
             with one entry per built-in spec of this checkout, in order;
             every datasource driver has access levels, each with an
             enforced_by line; built-ins are built-in and trusted; the
             enforced and installation egress columns say "not applicable";
             no credential slot schema carries a default or an example
  page       Playwright: Settings -> Connector drivers lists every driver the
             API returns, each access level with its "Enforced by" line, and
             the Connectors page links to it
  picker     Playwright: Connectors -> New connector -> Public, then for every
             publishable type: the access choices shown are exactly the levels
             the driver's spec offers (MCP: read-write only; KB: read-only
             only; Postgres: both), and the line under them is the bound
             level's enforced_by. The form is closed without saving. An
             account without the public_datasources grant gets the public
             choice shown in the gate's own browser only (a NOTE says so).

Run with the repository venv (Playwright and its Chromium installed):

  .venv/bin/python scripts/k3d-connector-matrix-gate.py          # plan
  .venv/bin/python scripts/k3d-connector-matrix-gate.py --run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LOCAL_CONTEXT = "k3d-srw"
LOCAL_NAMESPACE = "srw"
ORCHESTRATOR = "deploy/srw-orchestrator"
ORCHESTRATOR_CONTAINER = "orchestrator"
POD_ROOT = "/app"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
SERVED = (
    "src/orchestrator/services/connector_drivers/matrix.py",
    "src/orchestrator/services/connector_drivers/registry.py",
    "src/orchestrator/routers/datasources.py",
    "src/shared/connectors/builtin.py",
    "src/shared/connectors/contract.py",
)
#: Types the connector form never publishes, so it shows them no access choice.
UNPUBLISHED_IN_FORM = frozenset({"email", "credentials"})
_SECRETS: list[str] = []

PLAN = [
    "preflight: the orchestrator pod serves this checkout's matrix module, "
    "datasource router and built-in specs",
    "api: GET /api/datasources/drivers lists every built-in spec in order, "
    "access levels with enforced_by, built-in trust, egress not applicable, "
    "no value in a credential slot schema",
    "page: Playwright finds every driver and each level's 'Enforced by' line "
    "on Settings -> Connector drivers; the Connectors page links to it",
    "picker: Playwright opens New connector, makes it public and, per "
    "publishable type, sees exactly the access levels the driver offers and "
    "the bound level's enforced_by; closes without saving",
]


class GateError(RuntimeError):
    pass


class SafetyError(RuntimeError):
    """The requested run is outside the local boundary."""


def _scrub(text: str) -> str:
    for secret in _SECRETS:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def kubectl(args: list[str], *, data: str | None = None, timeout: int = 120) -> str:
    argv = ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", LOCAL_NAMESPACE, *args]
    try:
        result = subprocess.run(
            argv, input=data, text=True, capture_output=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise GateError(f"kubectl {args[0]} timed out after {timeout}s") from None
    if result.returncode:
        raise GateError(
            f"kubectl {args[0]} failed (exit {result.returncode}): "
            f"{_scrub(result.stderr.strip())[-400:]}"
        )
    return _scrub(result.stdout.strip())


def in_orchestrator(program: str, payload: dict[str, Any]) -> Any:
    out = kubectl(
        ["exec", "-i", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
        + ["python", "-c", program],
        data=json.dumps(payload) + "\n",
    )
    return json.loads(out.splitlines()[-1])


_API_PROGRAM = r"""
import json, sys, urllib.error, urllib.parse, urllib.request
envelope = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
form = urllib.parse.urlencode({
    "grant_type": "password", "client_id": "admin-cli", "scope": "openid",
    "username": envelope["username"], "password": envelope["password"],
}).encode()
with opener.open(envelope["token_url"], data=form, timeout=30) as response:
    token = json.load(response)["id_token"]
request = urllib.request.Request(
    "http://localhost:8085" + envelope["path"],
    headers={"Authorization": "Bearer " + token},
)
try:
    with opener.open(request, timeout=60) as response:
        status, text = response.status, response.read().decode("utf-8", "replace")
except urllib.error.HTTPError as error:
    status, text = error.code, error.read().decode("utf-8", "replace")
print(json.dumps({"status": status, "body": text}))
"""

_HASH_PROGRAM = r"""
import hashlib, json, sys
from pathlib import Path
request = json.loads(sys.stdin.readline())
root = Path(request["root"])
print(json.dumps({
    path: hashlib.sha256((root / path).read_bytes()).hexdigest()
    if (root / path).is_file() else None
    for path in request["paths"]
}))
"""


# ---------------------------------------------------------------------------
# What the matrix should say (pure, unit-tested)
# ---------------------------------------------------------------------------


def offered(driver: dict[str, Any]) -> tuple[dict | None, dict | None]:
    """(read-only level, read-write level) the cockpit offers for a driver.

    Mirrors ``offeredAccess`` in cockpit/src/app/core/models/
    connector-driver.model.ts: read-only floors at the lowest level and needs
    a lower one; read-write is gone when the driver is forced read-only.
    """
    levels = sorted(driver.get("access_levels") or [], key=lambda lv: lv["rank"])
    if not levels:
        return None, None
    if driver.get("forced_read_only"):
        return levels[0], None
    if len(levels) == 1:
        return None, levels[0]
    return levels[0], levels[-1]


def picker_expectation(driver: dict[str, Any]) -> tuple[list[str], str | None]:
    """The access choices the form shows, and the enforced_by line under them.

    A new public connector starts read-only, so the line is the read-only
    level's when the driver offers one.
    """
    read_only, read_write = offered(driver)
    choices = [
        name
        for name, level in (("read_only", read_only), ("read_write", read_write))
        if level
    ]
    bound = read_only or read_write
    return choices, bound["enforced_by"] if bound else None


def matrix_problems(matrix: dict[str, Any], spec_names: list[str]) -> list[str]:
    """Everything the API matrix gets wrong against this checkout's specs."""
    problems: list[str] = []
    drivers = matrix.get("drivers") or []
    names = [driver.get("name") for driver in drivers]
    if names != spec_names:
        problems.append(f"drivers {names} are not the built-in specs {spec_names}")
    for driver in drivers:
        name = driver.get("name")
        levels = driver.get("access_levels") or []
        if driver.get("legacy_type") and not levels:
            problems.append(f"{name} has no access levels")
        problems += [
            f"{name} level {level.get('id')} has no enforced_by line"
            for level in levels
            if not (level.get("enforced_by") or "").strip()
        ]
        if (driver.get("trust") or {}).get("tier") != "builtin":
            problems.append(f"{name} is not built-in and trusted")
        egress = driver.get("egress") or {}
        for column in ("enforced", "installation"):
            if (egress.get(column) or {}).get("status") != "not_applicable":
                problems.append(f"{name} egress {column} is not 'not applicable'")
        for slot in driver.get("credential_slots") or []:
            text = json.dumps(slot.get("schema"))
            if '"default"' in text or '"examples"' in text:
                problems.append(f"{name} slot {slot.get('name')} carries a value")
    return problems


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


@dataclass
class Report:
    results: list[tuple[str, bool, str]] = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        detail = _scrub(detail)
        self.results.append((name, bool(ok), detail))
        print(
            f"{'PASS' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}",
            flush=True,
        )
        return bool(ok)

    def note(self, text: str) -> None:
        print(f"NOTE {_scrub(text)}", flush=True)

    @property
    def passed(self) -> bool:
        return bool(self.results) and all(ok for _, ok, _ in self.results)


class MatrixGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.password = args.password
        _SECRETS.append(args.password)
        self.report = Report()
        self.matrix: dict[str, Any] = {}
        #: Set when the account lacks the publish grant (see reveal_publish).
        self.revealed = False

    def run(self) -> int:
        try:
            self.preflight()
            self.api()
            if self.args.skip_cockpit:
                self.report.note("cockpit checks skipped (--skip-cockpit)")
            elif self.matrix:
                self.cockpit()
        except GateError as exc:
            self.report.check("gate", False, str(exc))
        failed = [name for name, ok, _ in self.report.results if not ok]
        verdict = "PASS" if self.report.passed else "FAIL"
        print(
            f"{verdict} d2: {len(self.report.results)} checks, {len(failed)} failed {failed or ''}".rstrip()
        )
        return 0 if self.report.passed else 1

    def preflight(self) -> None:
        served = in_orchestrator(
            _HASH_PROGRAM, {"root": POD_ROOT, "paths": list(SERVED)}
        )
        stale = [
            path
            for path in SERVED
            if served.get(path)
            != hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
        ]
        self.report.check(
            "preflight: the orchestrator serves this checkout's matrix code",
            not stale,
            f"stale or missing: {stale}" if stale else "",
        )

    def api(self) -> None:
        result = in_orchestrator(
            _API_PROGRAM,
            {
                "username": self.args.user,
                "password": self.password,
                "token_url": KEYCLOAK_TOKEN_URL,
                "path": "/api/datasources/drivers",
            },
        )
        status = int(result["status"])
        if not self.report.check(
            "api: GET /api/datasources/drivers answers 200",
            status == 200,
            f"HTTP {status}",
        ):
            return
        self.matrix = json.loads(result["body"])
        sys.path.insert(0, str(ROOT / "src"))
        from shared.connectors.builtin import BUILTIN_SPECS

        problems = matrix_problems(self.matrix, [spec.name for spec in BUILTIN_SPECS])
        self.report.check(
            "api: every built-in driver, levels with enforced_by, built-in trust, "
            "egress not applicable, no slot values",
            not problems,
            "; ".join(problems[:5]),
        )

    def cockpit(self) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.report.check(
                "cockpit", False, "playwright is not installed in this interpreter"
            )
            return
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                context = browser.new_context(
                    ignore_https_errors=True,
                    service_workers="block",
                    viewport={"width": 1600, "height": 1000},
                )
                context.route("**/api/users/me/capabilities", self.reveal_publish)
                page = context.new_page()
                page.goto(
                    f"{self.args.base_url}/settings/connector-drivers",
                    wait_until="domcontentloaded",
                    timeout=60000,
                )
                page.wait_for_selector("#username", timeout=60000)
                page.fill("#username", self.args.user)
                page.fill("#password", self.password)
                page.click("#kc-login")
                self.check_page(page)
                self.check_picker(page)
            except Exception as exc:  # noqa: BLE001 -- the check reports it
                self.report.check(
                    "cockpit",
                    False,
                    f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}",
                )
            finally:
                browser.close()

    def check_page(self, page: Any) -> None:
        page.locator("[data-driver]").first.wait_for(timeout=60000)
        problems: list[str] = []
        shown = page.locator("[data-driver]").evaluate_all(
            "els => els.map(e => e.dataset.driver)"
        )
        expected = [driver["name"] for driver in self.matrix["drivers"]]
        if shown != expected:
            problems.append(f"page lists {shown}, the API {expected}")
        for driver in self.matrix["drivers"]:
            card = page.locator(f'[data-driver="{driver["name"]}"]')
            for level in driver["access_levels"]:
                line = card.locator(
                    f'[data-level="{level["id"]}"] .enforced-by'
                ).inner_text()
                if level["enforced_by"] not in " ".join(line.split()):
                    problems.append(
                        f"{driver['name']} {level['id']} shows {line[:80]!r}"
                    )
        self.report.check(
            "page (Playwright): every driver with each level's 'Enforced by' line",
            not problems,
            "; ".join(problems[:5]),
        )

    def reveal_publish(self, route: Any) -> None:
        """Show the public access choice to an account without the publish grant.

        The access choice sits behind the ``public_datasources`` grant, which
        is not what this gate tests. Only this browser sees the grant; the
        form is never saved, and the server would refuse a publish anyway.
        """
        response = route.fetch()
        try:
            body = response.json()
        except ValueError:
            body = None
        grants = body.get("grants") if isinstance(body, dict) else None
        if isinstance(grants, dict) and grants.get("public_datasources") is not True:
            grants["public_datasources"] = True
            self.revealed = True
            route.fulfill(response=response, body=json.dumps(body))
            return
        route.fulfill(response=response)

    def check_picker(self, page: Any) -> None:
        with page.expect_response(
            lambda r: "/api/users/me/capabilities" in r.url, timeout=60000
        ):
            page.goto(
                f"{self.args.base_url}/datasources",
                wait_until="domcontentloaded",
                timeout=60000,
            )
        if self.revealed:
            self.report.note(
                f"{self.args.user} lacks the public_datasources grant; the gate "
                "showed the public access choice in its own browser only"
            )
        page.locator(".header-actions app-button").first.wait_for(timeout=60000)
        link = page.locator(
            'a.drivers-link[href="/settings/connector-drivers"]'
        ).count()
        self.report.check(
            "picker (Playwright): the Connectors page links to the matrix", link == 1
        )
        page.locator(".header-actions app-button button").first.click()
        type_select = page.locator(".form-panel .form-body select").first
        type_select.wait_for(timeout=30000)
        public = page.locator(".visibility-toggle input[type=checkbox]")
        public.check(timeout=30000)
        problems: list[str] = []
        for driver in self.matrix["drivers"]:
            kind = driver.get("legacy_type")
            if not kind or kind in UNPUBLISHED_IN_FORM:
                continue
            type_select.select_option(kind)
            choices, line = picker_expectation(driver)
            seen = self.read_choices(page, choices)
            if seen[0] != choices:
                problems.append(f"{kind}: shows {seen[0]}, the spec offers {choices}")
            if line and line not in seen[1]:
                problems.append(f"{kind}: line {seen[1][:80]!r} is not {line[:60]!r}")
        page.locator(".form-header app-icon-button button").first.click()
        self.report.check(
            "picker (Playwright): access choices are exactly the driver's levels "
            "(MCP read-write only, KB read-only only, Postgres both)",
            not problems,
            "; ".join(problems[:5]),
        )

    @staticmethod
    def read_choices(page: Any, expected: list[str]) -> tuple[list[str], str]:
        """The rendered access choices and enforced line, once they settle."""
        deadline = time.monotonic() + 5
        while True:
            choices = page.locator(".access-radio label[data-access]").evaluate_all(
                "els => els.map(e => e.dataset.access)"
            )
            line = " ".join(
                " ".join(page.locator(".access-enforced").all_inner_texts()).split()
            )
            if choices == expected or time.monotonic() >= deadline:
                return choices, line
            time.sleep(0.2)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--context", default=LOCAL_CONTEXT)
    parser.add_argument("--namespace", default=LOCAL_NAMESPACE)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--user", default="test")
    parser.add_argument("--password", default="srw-k3d-dev-test")
    parser.add_argument("--base-url", default="https://localhost")
    parser.add_argument("--skip-cockpit", action="store_true")
    return parser


def validate(args: argparse.Namespace) -> None:
    if args.context != LOCAL_CONTEXT or args.namespace != LOCAL_NAMESPACE:
        raise SafetyError("this gate is restricted to k3d-srw/srw")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", args.user):
        raise SafetyError("user name is malformed")
    if not re.fullmatch(r"https://(localhost|127\.0\.0\.1)(:\d{1,5})?", args.base_url):
        raise SafetyError("--base-url must be the local cockpit")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate(args)
    except SafetyError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    if not args.run:
        print("plan (dry run, read-only; pass --run to execute):")
        for step in PLAN:
            print(f"  - {step}")
        return 0
    return MatrixGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
