"""Invariants for the Keycloak `srw` theme.

The dark-mode assertion is the important one: PatternFly v5 wraps every dark
token in :where(), which has zero specificity, so putting dark values under
:root or a media query loses to nothing and silently disables dark mode.
"""

import functools
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
THEME = ROOT / "helm/keycloak-theme/srw"
LOGIN_CSS = next((THEME / "login/resources/css").glob("srw.*.css"), None)


def _css_rules(text: str) -> str:
    """CSS with /* ... */ comments removed.

    The dark-mode assertions below must judge rules, not prose. Reading the raw
    file makes it impossible to document the prohibited patterns by name, which
    is exactly what the header comment needs to do.
    """
    return re.sub(r"/\*.*?\*/", "", text, flags=re.S)


def _ftl_directives(text: str) -> str:
    """.ftl with <#-- ... --> comments removed.

    template.ftl's header comment documents each version guard by name,
    including the literal guard expression itself (e.g. "-> ltr!true" in its
    prose). A raw-text assertion for that guard would keep passing even if
    the real <#assign> below it were weakened to an unguarded reference,
    because the comment alone satisfies the substring check. Strip
    FreeMarker comments before judging directives -- the FTL analogue of
    _css_rules() above.
    """
    return re.sub(r"<#--.*?-->", "", text, flags=re.S)


def _properties_directives(text: str) -> str:
    """.properties with comment (#...) and blank lines removed.

    theme.properties headers routinely restate the exact key=value they are
    explaining (e.g. email/theme.properties opens "# parent=base, NOT
    parent=keycloak: ..."). A raw-text assertion for "parent=base" is
    satisfied by that comment even if the real directive were edited to
    something else. Strip comments before judging directives -- the
    .properties analogue of _css_rules() above.
    """
    lines = (
        line
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    )
    return "\n".join(lines)


def _template_directives(text: str) -> str:
    """Helm template text with {{/* ... */}} and whole-line # comments removed.

    The SMTP guards below are documented by a long comment block that has to
    name the shapes it rejects -- including the literal `smtpServer.port=1025`
    hardcode it replaced. A raw-text assertion for that literal would then be
    satisfied by the prose explaining its removal. Strip comments before
    judging directives -- the Helm-template analogue of _css_rules() above.
    Only whole-line `#` comments are dropped, so a mid-line `#` inside a real
    shell command or URL is never mangled.
    """
    text = re.sub(r"\{\{-?\s*/\*.*?\*/\s*-?\}\}", "", text, flags=re.S)
    lines = (line for line in text.splitlines() if not line.lstrip().startswith("#"))
    return "\n".join(lines)


def test_login_theme_properties_are_correct() -> None:
    # The header comment restates "parent=keycloak.v2" verbatim while
    # explaining it, so this must judge directives, not raw text (see
    # _properties_directives).
    props = _properties_directives((THEME / "login/theme.properties").read_text())
    assert "parent=keycloak.v2" in props
    # v1 is deprecated as of KC 26.0.
    assert "parent=keycloak\n" not in props
    # The parent file must be re-listed: `styles` REPLACES, it does not append.
    assert "styles=css/styles.css css/" in props
    # Inherited from keycloak.v2; redeclaring shifts property-merge order.
    assert "import=common/keycloak" not in props


def test_css_filename_is_versioned() -> None:
    """Theme resources are served with max-age=2592000 and the /resources/<tag>/
    segment only moves on a KC version migration, so the filename is the only
    cache-busting lever."""
    assert LOGIN_CSS is not None
    assert LOGIN_CSS.name != "srw.css"


def test_dark_tokens_are_on_the_bare_class_not_root_or_media_query() -> None:
    css = LOGIN_CSS.read_text()
    rules = _css_rules(css)
    assert ".pf-v5-theme-dark" in rules
    assert "prefers-color-scheme" not in rules, (
        "PatternFly toggles a class, not a media query -- a media query desyncs."
    )
    dark_start = rules.index(".pf-v5-theme-dark")
    root_start = rules.index(":root")
    assert root_start < dark_start, ":root must come first so the class wins on tie"


def test_overrides_the_brandable_keycloak_variables() -> None:
    # Judges declarations, not raw text (see _css_rules). The header and the
    # per-block comments name these tokens while explaining them -- mutation-
    # proved: replacing the real `--keycloak-logo-url: url(...)` declaration
    # with a comment mentioning it kept this test green.
    css = _css_rules(LOGIN_CSS.read_text())
    for token in (
        "--pf-v5-global--primary-color--100",
        "--pf-v5-global--BackgroundColor--100",
        "--pf-v5-global--Color--100",
        "--pf-v5-global--BorderRadius--sm",
        "--keycloak-card-top-color",
        "--keycloak-logo-url",
    ):
        assert token in css, f"missing override: {token}"


def test_header_wrapper_colour_is_forced() -> None:
    """keycloak.v2 sets #kc-header-wrapper colour with !important, relying on a
    dark background image we remove -- without countering it the header goes
    white-on-cream.

    Scoped to the rule block. A file-wide `"!important" in css` is satisfied by
    any other rule that happens to carry one -- and one does: `.login-pf body`
    forces the page ground a few lines above. Deleting the !important from THIS
    rule (the whole subject of the test) then left it green. Routing through
    _css_rules() alone is not enough for the same reason.
    """
    rules = _css_rules(LOGIN_CSS.read_text())
    found = re.search(r"#kc-header-wrapper\s*\{[^}]*\}", rules)
    assert found, "no #kc-header-wrapper rule block -- was the selector renamed?"
    assert "!important" in found.group(0)


def test_no_external_font_dependency() -> None:
    """The login page must work when everything else is down, and must not leak
    every login attempt's IP to a third party."""
    css = LOGIN_CSS.read_text()
    rules = _css_rules(css)
    assert "fonts.googleapis.com" not in rules
    assert "@import" not in rules


def test_light_tokens_match_the_shared_brand_palette() -> None:
    """Extends the Python drift guard across the third copy of the palette.

    brand.py is checked against the SCSS by tests/test_brand_palette.py; this
    ties the Keycloak CSS to brand.py, so all three move together or a test
    fails. Without it the login page is exactly the surface that silently rots.
    """
    from orchestrator.services import brand

    css = LOGIN_CSS.read_text()
    rules = _css_rules(css)
    root = rules[rules.index(":root {") : rules.index(".pf-v5-theme-dark")]

    expected = {
        "--pf-v5-global--primary-color--100": brand.TRAVERTINE["accent-color"],
        "--pf-v5-global--BackgroundColor--100": brand.TRAVERTINE["panel-bg"],
        "--pf-v5-global--BackgroundColor--200": brand.TRAVERTINE["app-bg"],
        "--pf-v5-global--Color--100": brand.TRAVERTINE["text-primary"],
        "--pf-v5-global--Color--200": brand.TRAVERTINE["text-secondary"],
        "--pf-v5-global--BorderColor--100": brand.TRAVERTINE["border-color"],
        "--keycloak-card-top-color": brand.TRAVERTINE["accent-color"],
    }
    for token, want in expected.items():
        found = re.search(rf"{re.escape(token)}:\s*(#[0-9a-fA-F]{{3,8}})", root)
        assert found, f"{token} missing from the :root block"
        assert brand.normalize_hex(found.group(1)) == want, (
            f"{token} is {found.group(1)}, brand.py says {want}"
        )


def test_dark_tokens_match_the_shared_senate_palette() -> None:
    """The dark half of the login palette had no guard at all.

    The light block is chained SCSS -> brand.py -> CSS by the test above, but
    nothing tied .pf-v5-theme-dark to anything -- which is how Color--200 came
    to hold Senate `text-muted` while its light counterpart holds
    `text-secondary`, the same token mapped to two different roles. brand.py
    deliberately carries only Travertine (the orchestrator renders no dark
    surfaces), so this reads $senate-theme straight out of the SCSS.

    Scoped to the .pf-v5-theme-dark block: a whole-file search would match the
    :root declarations of the very same token names.
    """
    from orchestrator.services import brand

    from tests.test_brand_palette import default_accent_block

    scss = (ROOT / brand.SCSS_TOKEN_SOURCE).read_text()
    start = scss.index("$senate-theme: (")
    # Accent tokens moved to $accents on 2026-09-10; the login page wears the
    # default accent, so its Senate sub-map is appended to the base map.
    senate_scss = (
        scss[start : scss.index("\n);", start)]
        + "\n"
        + default_accent_block(scss, "senate")
    )
    senate = {
        k: brand.normalize_hex(v)
        for k, v in re.findall(
            r"'([a-z0-9-]+)':\s*(#[0-9a-fA-F]{3,8})",
            senate_scss,
        )
    }
    assert len(senate) >= 20, (
        f"only parsed {len(senate)} tokens from $senate-theme; fix the parser "
        "before trusting this test"
    )

    rules = _css_rules(LOGIN_CSS.read_text())
    block = re.search(r"\.pf-v5-theme-dark\s*\{[^}]*\}", rules)
    assert block, "no .pf-v5-theme-dark rule block -- was the selector renamed?"

    expected = {
        "--pf-v5-global--primary-color--100": senate["accent-color"],
        "--pf-v5-global--primary-color--200": senate["accent-hover"],
        "--pf-v5-global--BackgroundColor--100": senate["panel-bg"],
        "--pf-v5-global--BackgroundColor--200": senate["app-bg"],
        "--pf-v5-global--Color--100": senate["text-primary"],
        "--pf-v5-global--Color--200": senate["text-secondary"],
        "--pf-v5-global--BorderColor--100": senate["border-color"],
        "--keycloak-card-top-color": senate["accent-color"],
    }
    for token, want in expected.items():
        found = re.search(
            rf"{re.escape(token)}:\s*(#[0-9a-fA-F]{{3,8}})", block.group(0)
        )
        assert found, f"{token} missing from the .pf-v5-theme-dark block"
        assert brand.normalize_hex(found.group(1)) == want, (
            f"{token} is {found.group(1)}, $senate-theme says {want}"
        )


KC = ROOT / "helm/templates/services/keycloak.yaml"


def test_configmap_enumerates_every_theme_file() -> None:
    """ConfigMap keys cannot contain '/', so the nested layout only exists via
    items[].path. A file missing an items entry is a silent no-op -- and a
    theme with no login/ directory simply never appears."""
    kc = KC.read_text()
    for path in sorted(p.relative_to(THEME) for p in THEME.rglob("*") if p.is_file()):
        assert f"path: {path}" in kc, f"{path} has no items[].path entry"


@functools.lru_cache(maxsize=1)
def _render_theme_docs() -> tuple[dict[str, str], list[dict]]:
    """Render the real chart once and return what the API server would see:
    the `keycloak-theme` ConfigMap's `data` mapping, and the Deployment's
    `keycloak-theme` volume `items` list.

    A regex over the *template source* can only ever match the literal
    `{{ $key }}` text -- never the templated-out key it produces -- so it
    finds nothing and passes vacuously even if the `replace "/" "_"` step is
    deleted entirely. Only the rendered output proves what key the API server
    receives. Mirrors the render helper in test_vm_chart_manifest_contract.py.

    Both halves come from ONE render because the invariant that matters spans
    them: an items[].key that no ConfigMap key satisfies is not a rendering
    error, it is a pod that never starts (CreateContainerConfigError).
    """
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(ROOT / "helm"),
            "-f",
            str(ROOT / "helm/ci/test-values.yaml"),
            "--set",
            "keycloak.enabled=true",
            "--set",
            "keycloak.internal=true",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"helm template failed:\n{result.stderr}"

    data: dict[str, str] | None = None
    items: list[dict] | None = None
    for doc in yaml.safe_load_all(result.stdout):
        if not doc:
            continue
        name = doc.get("metadata", {}).get("name", "")
        if doc.get("kind") == "ConfigMap" and name.endswith("-keycloak-theme"):
            data = doc.get("data") or {}
        elif doc.get("kind") == "Deployment" and name.endswith("-keycloak"):
            for vol in doc["spec"]["template"]["spec"].get("volumes") or []:
                if vol.get("name") == "keycloak-theme":
                    items = (vol.get("configMap") or {}).get("items") or []
    assert data is not None, "no keycloak-theme ConfigMap in the render"
    assert items is not None, "no keycloak-theme volume on the keycloak Deployment"
    return data, items


def _render_theme_configmap_data() -> dict[str, str]:
    return _render_theme_docs()[0]


@functools.lru_cache(maxsize=1)
def _render_realm() -> dict:
    """The realm JSON the Keycloak container imports, as a parsed object.

    Same reasoning as _render_theme_docs: the realm is Helm-templated inside
    a ConfigMap, so only the rendered output is the artifact under test.
    """
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(ROOT / "helm"),
            "-f",
            str(ROOT / "helm/ci/test-values.yaml"),
            "--set",
            "keycloak.enabled=true",
            "--set",
            "keycloak.internal=true",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"helm template failed:\n{result.stderr}"

    for doc in yaml.safe_load_all(result.stdout):
        if not doc:
            continue
        if doc.get("kind") != "ConfigMap":
            continue
        if not doc.get("metadata", {}).get("name", "").endswith("-keycloak-realm"):
            continue
        return json.loads((doc.get("data") or {})["srw-realm.json"])
    raise AssertionError("no keycloak-realm ConfigMap in the render")


@functools.lru_cache(maxsize=2)
def _render_realm_with(*overrides: str) -> dict:
    """`_render_realm` with extra `--set` overrides, for flag-gated blocks."""
    cmd = [
        "helm",
        "template",
        "srw",
        str(ROOT / "helm"),
        "-f",
        str(ROOT / "helm/ci/test-values.yaml"),
        "--set",
        "keycloak.enabled=true",
        "--set",
        "keycloak.internal=true",
    ]
    for o in overrides:
        cmd += ["--set", o]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, f"helm template failed:\n{result.stderr}"
    for doc in yaml.safe_load_all(result.stdout):
        if not doc or doc.get("kind") != "ConfigMap":
            continue
        if not doc.get("metadata", {}).get("name", "").endswith("-keycloak-realm"):
            continue
        return json.loads((doc.get("data") or {})["srw-realm.json"])
    raise AssertionError("no keycloak-realm ConfigMap in the render")


def test_dev_users_are_off_by_default_in_values() -> None:
    """The published passwords in README only stay harmless while this is false.

    Asserted against values.yaml rather than a render so it fails even if the
    template stops consuming the flag.
    """
    values = yaml.safe_load((ROOT / "helm/values.yaml").read_text())
    assert values["keycloak"]["devUsers"]["enabled"] is False


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm binary not installed")
def test_shipped_realm_seeds_only_the_bootstrap_admin() -> None:
    """A default install must not carry the documented dev credentials."""
    users = _render_realm().get("users", [])
    assert [u["username"] for u in users] == ["test"]


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm binary not installed")
def test_dev_users_render_and_satisfy_the_password_policy() -> None:
    """Enabled, they must clear the realm's own length(16)/notUsername policy —
    otherwise Keycloak rejects them and the seeding silently half-works."""
    realm = _render_realm_with("keycloak.devUsers.enabled=true")
    users = {u["username"]: u for u in realm.get("users", [])}
    assert "test" in users
    for name in ("dev-admin-1", "dev-admin-2", "dev-user-1", "dev-user-4"):
        assert name in users, f"{name} missing from the enabled render"
        pw = users[name]["credentials"][0]["value"]
        assert len(pw) >= 16, f"{name} password is {len(pw)} chars, policy needs 16"
        assert pw != name, f"{name} password equals its username (notUsername)"
    assert "admin" in users["dev-admin-1"]["realmRoles"]
    assert "admin" not in users["dev-user-1"]["realmRoles"]


def test_local_kubernetes_dev_credentials_match_the_chart() -> None:
    """The local guide publishes these passwords; a drifted table sends
    developers to a login that fails, or understates which accounts exist."""
    values = yaml.safe_load((ROOT / "helm/values.yaml").read_text())
    chart = {
        u["username"]: u["password"] for u in values["keycloak"]["devUsers"]["users"]
    }
    guide = dict(
        re.findall(
            r"^\|\s*`(dev-[a-z0-9-]+)`\s*\|\s*`([^`]+)`",
            (ROOT / "docs/local-kubernetes.md").read_text(),
            re.M,
        )
    )
    assert guide == chart, (
        "Local Kubernetes dev-credentials table is out of sync with "
        "keycloak.devUsers in helm/values.yaml.\n"
        f"  guide: {guide}\n  chart:  {chart}"
    )


def test_local_kubernetes_dev_account_snippet_matches_the_chart() -> None:
    """The guide's snippet for adding the accounts to an existing realm repeats
    the passwords and roles; a drifted line creates an account the table lies
    about."""
    values = yaml.safe_load((ROOT / "helm/values.yaml").read_text())
    chart = {
        u["username"]: (u["password"], "admin user" if u["admin"] else "user")
        for u in values["keycloak"]["devUsers"]["users"]
    }
    snippet = {
        name: (password, roles)
        for name, password, roles in re.findall(
            r"^(dev-[a-z0-9-]+) (\S+) ([a-z ]+)$",
            (ROOT / "docs/local-kubernetes.md").read_text(),
            re.M,
        )
    }
    assert snippet == chart, (
        "Local Kubernetes dev-account snippet is out of sync with "
        "keycloak.devUsers in helm/values.yaml.\n"
        f"  snippet: {snippet}\n  chart:   {chart}"
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm binary not installed")
def test_configmap_keys_have_no_slashes() -> None:
    """A real API server rejects a ConfigMap key containing '/'. Assert on the
    rendered `data` mapping, not the template source (see
    _render_theme_configmap_data), and require at least one key so a render
    that silently produced none fails loudly instead of passing vacuously."""
    data = _render_theme_configmap_data()
    assert data, "keycloak-theme ConfigMap rendered with an EMPTY data block"
    for key in data:
        assert "/" not in key, (
            f"{key!r} still contains '/' -- the API server would reject it"
        )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm binary not installed")
def test_volume_items_reference_keys_the_configmap_actually_has() -> None:
    """Joins the two halves nothing else joined.

    test_configmap_enumerates_every_theme_file checks items[].path against the
    files on disk; test_configmap_keys_have_no_slashes checks the rendered
    ConfigMap keys. Both stay green under two mutations that break the deploy:

      * typo one items[].key -- kubelet cannot resolve it and the pod sits in
        CreateContainerConfigError, while every `path:` assertion still matches;
      * change the mangling in keycloak-theme-configmap.yaml (say `-` instead
        of `_`) -- keys stay slash-free, paths are untouched, and every
        hand-written items[].key goes stale at once.

    Set equality in both directions: a missing key breaks the mount, and a
    ConfigMap key with no items entry is a theme file that silently never
    appears at the mount point.
    """
    data, items = _render_theme_docs()
    assert data, "keycloak-theme ConfigMap rendered with an EMPTY data block"
    assert items, "keycloak-theme volume rendered with NO items"
    assert {i["key"] for i in items} == set(data), (
        "items[].key and ConfigMap keys disagree.\n"
        f"  keys with no items entry (never mounted): {sorted(set(data) - {i['key'] for i in items})}\n"
        f"  items with no such key (pod will not start): {sorted({i['key'] for i in items} - set(data))}"
    )


def test_theme_is_mounted_at_the_themes_root() -> None:
    kc = KC.read_text()
    assert "mountPath: /opt/keycloak/themes/srw" in kc


def test_pod_template_has_a_theme_checksum_annotation() -> None:
    """Three caches make theme edits invisible without a pod roll: cacheThemes,
    cacheTemplates, and the gzip cache -- which writes a .gz on first request
    and only regenerates if the file is ABSENT. Browsers send Accept-Encoding:
    gzip and get stale CSS while curl shows it fresh."""
    kc = KC.read_text()
    assert "checksum/keycloak-theme:" in kc
    assert "keycloak-theme-configmap.yaml" in kc


def test_realm_import_uses_the_srw_login_theme() -> None:
    assert '"loginTheme": "srw"' in KC.read_text()


def test_display_name_html_carries_the_logo_hook() -> None:
    """--keycloak-logo-url only renders if displayNameHtml provides
    .kc-logo-text for the stylesheet to turn into a background image."""
    assert "kc-logo-text" in KC.read_text()


def test_email_theme_parents_base_not_keycloak() -> None:
    """keycloak/email/ holds only theme.properties today, so the layer is a
    no-op that Red Hat can add files to in any patch release.

    The header comment restates "parent=base" verbatim while explaining it,
    so this must judge directives, not raw text (see _properties_directives).
    """
    props = _properties_directives((THEME / "email/theme.properties").read_text())
    assert "parent=base" in props


def test_email_wrapper_guards_version_dependent_variables() -> None:
    # The header comment restates "ltr!true" verbatim while explaining the
    # guard, so a raw-text presence check would keep passing even if the real
    # <#assign> were weakened to an unguarded ${ltr}. Judge directives only
    # (see _ftl_directives) -- and for the same reason, do it for the absence
    # checks below too: a comment mentioning a forbidden token in passing
    # must not be able to fail this test.
    ftl = _ftl_directives((THEME / "email/html/template.ftl").read_text())
    # `ltr` only exists from KC 26.2; unguarded it makes EVERY email fail to send.
    assert "ltr!true" in ftl
    # Never reference theme resources for images: those URLs embed the migration
    # tag and 404 for every already-delivered email after an upgrade.
    assert "url.resourcesUrl" not in ftl
    assert "url.resourcesCommonUrl" not in ftl
    # Per-type variables must not appear in a wrapper shared by all types.
    for per_type in ("${link}", "${event}", "${code}"):
        assert per_type not in ftl


def test_email_wrapper_sets_inline_fallbacks() -> None:
    """Message bodies are inherited <p>/<a> fragments; clients that strip
    <head> must still get sane typography from the containing <td>.

    Strips FTL comments first (see _ftl_directives), then isolates the <tr>
    immediately preceding <#nested> rather than everything since <body>: the
    fallback logo <span> above it also carries a (unrelated) font-family, so
    a looser slice would still pass after the containing <td>'s own
    font-family was deleted.
    """
    ftl = _ftl_directives((THEME / "email/html/template.ftl").read_text())
    enclosing_td = ftl.split("<#nested>")[0].rsplit("<tr>", 1)[1]
    assert "font-family" in enclosing_td


def test_email_wrapper_uses_no_unmanaged_colours() -> None:
    """Extends the drift guard across the FOURTH copy of the palette.

    SCSS -> brand.py -> login CSS are chained by tests above and by
    tests/test_brand_palette.py, but template.ftl carried six literal hexes
    under no guard at all -- and that is exactly how a `text-muted` footer
    colour (3.82:1 on panel-bg, below the 4.5:1 AA floor the rest of this
    feature exists to fix) reached the branch after the ruling that removed it
    everywhere else. Judges directives, not raw text: the header comment must
    stay free to name a colour it explains (see _ftl_directives).
    """
    from orchestrator.services import brand

    ftl = _ftl_directives((THEME / "email/html/template.ftl").read_text())
    managed = {brand.normalize_hex(v) for v in brand.TRAVERTINE.values()}
    used = {brand.normalize_hex(h) for h in re.findall(r"#[0-9a-fA-F]{3,6}\b", ftl)}
    assert used, "no hexes found in template.ftl -- the extractor is broken"
    assert used <= managed, f"unmanaged colours: {sorted(used - managed)}"


def test_email_wrapper_never_uses_text_muted() -> None:
    """The membership guard above does NOT cover this, and that is the point.

    `text-muted` is a member of brand.TRAVERTINE (the dict mirrors the SCSS
    token map, and the drift guard checks every key), so a footer painted
    #8a7b66 is perfectly "managed" -- and measures 3.82:1 on panel-bg, under
    the 4.5:1 AA floor for the 12px text it was used on. That is precisely the
    defect this branch shipped and then had to remove again. Membership
    catches drift; only an explicit ban catches a wrong-but-managed token.

    See tests/test_brand_palette.py for the contrast arithmetic and
    tests/test_email_layout.py::test_footer_note_uses_text_secondary for the
    same ban on the Python-rendered side.
    """
    from orchestrator.services import brand

    ftl = _ftl_directives((THEME / "email/html/template.ftl").read_text())
    used = {brand.normalize_hex(h) for h in re.findall(r"#[0-9a-fA-F]{3,6}\b", ftl)}
    assert brand.TRAVERTINE["text-muted"] not in used, (
        "template.ftl paints text with text-muted, which fails WCAG AA on every "
        "Travertine surface -- footer/legal copy uses text-secondary (ruled "
        "2026-08-16)"
    )


def test_realm_import_uses_the_srw_email_theme() -> None:
    assert '"emailTheme": "srw"' in KC.read_text()


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm binary not installed")
def test_rendered_realm_selects_both_themes_and_the_logo_hook() -> None:
    """The three assertions above read the TEMPLATE; this one reads what
    Keycloak actually imports.

    The realm lives inside a Helm-templated ConfigMap, so a raw substring
    match cannot tell a live JSON value from one stranded inside a
    `{{- if }}` that never renders, and cannot prove the block is still
    parseable JSON at all. Parsing the rendered `srw-realm.json` does both.
    This replaced a second copy of the realm that used to ship as
    docker/keycloak/realm-export.json for the Compose stack (retired with
    Compose itself) -- that file seeded a `test`/`test` admin user with a
    non-temporary password, so the chart's realm is now the only realm.
    """
    realm = _render_realm()
    assert realm["loginTheme"] == "srw"
    assert realm["emailTheme"] == "srw"
    assert "kc-logo-text" in realm["displayNameHtml"]


HELPERS = ROOT / "helm/templates/_helpers.tpl"


def test_smtp_port_and_tls_come_from_values() -> None:
    """values.yaml exposes email.smtp.port but the bootstrap hardcoded 1025 --
    a dev mail-catcher port -- so any real relay on 587 was unreachable.

    Judges directives, not raw text: the guards' own comment block names the
    `smtpServer.port=1025` hardcode it replaced (see _template_directives).
    """
    kc = _template_directives(KC.read_text())
    helpers = _template_directives(HELPERS.read_text())
    assert "smtpServer.port=1025" not in kc, (
        "the dev mail-catcher port is hardcoded again"
    )
    assert 'include "srw.keycloak.smtpPort"' in kc
    assert 'include "srw.keycloak.smtpStartTls"' in kc
    assert ".Values.email.smtp.port" in helpers
    assert ".Values.email.smtp.useTls" in helpers


def _render_keycloak_poststart_script(*extra_args: str) -> str:
    """Render the real chart and return the keycloak Deployment's postStart
    shell script -- the text that actually runs `kcadm update ... smtpServer`.

    A source-text assertion can only ever see the literal `{{ }}` expression,
    never what the guard inside it decides for a given input -- and that
    decision is the entire subject of these tests. It is also why every bug in
    this line's history shipped: the template source looked correct in all
    four of them. Only rendering shows what kcadm is actually handed.

    Mirrors _render_theme_configmap_data() above, but reads the Deployment
    (kind == Deployment, name endswith "-keycloak") rather than the theme
    ConfigMap, and drills into containers[].lifecycle.postStart instead of
    .data.
    """
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(ROOT / "helm"),
            "-f",
            str(ROOT / "helm/ci/test-values.yaml"),
            "--set",
            "keycloak.enabled=true",
            "--set",
            "keycloak.internal=true",
            *extra_args,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"helm template failed:\n{result.stderr}"

    for doc in yaml.safe_load_all(result.stdout):
        if (
            doc
            and doc.get("kind") == "Deployment"
            and doc.get("metadata", {}).get("name", "").endswith("-keycloak")
        ):
            containers = doc["spec"]["template"]["spec"]["containers"]
            keycloak = next(c for c in containers if c["name"] == "keycloak")
            return keycloak["lifecycle"]["postStart"]["exec"]["command"][-1]
    raise AssertionError("no keycloak Deployment in the render")


def _smtp_field(script: str, field: str) -> str:
    """The exact value kcadm receives for `-s "smtpServer.<field>=<value>"`.

    Substring assertions are the wrong shape for this guard. `"port=1025" in
    script` passes on a prefix, so a rendered `port=10250` satisfies it; and
    when the guard regressed to `starttls=` or `starttls=" "` -- the two bugs
    that cost this task four rounds -- the only signal a substring check could
    give was the absence of some other string, never the offending value
    itself. Capture up to the closing quote and compare with ==, so every
    failure message names what was actually rendered.

    A value containing a `"` truncates the capture rather than escaping it,
    which is the correct outcome: such a value would also terminate the shell
    string early, and the == assertion fails loudly instead of matching.
    """
    found = re.search(rf'-s "smtpServer\.{re.escape(field)}=([^"]*)"', script)
    assert found is not None, (
        f"no smtpServer.{field} assignment in the rendered postStart script"
    )
    return found.group(1)


def _both(flag: str, value: str) -> list[str]:
    """The same override, same shape, applied to BOTH SMTP fields.

    Requirement: the port and starttls guards stay consistent. Driving both
    from one row means a fix applied to only one of them fails the matrix.
    """
    return [flag, f"email.smtp.port={value}", flag, f"email.smtp.useTls={value}"]


# (helm args, expected rendered port, expected rendered starttls).
#
# Organised by the Go kind Helm hands the template, because kind -- not the
# spelling of the flag -- is the axis the guards are total over. Every kind
# Helm's loaders can produce for a leaf value is represented: invalid (nil),
# bool, int64, float64, string, map, slice. Within `string`, the sub-rows are
# the content classes that decide whitelist membership.
#
# The starttls expectations are the load-bearing ones. Keycloak parses that
# field with Java's Boolean.parseBoolean, which returns false for anything
# that is not literally "true" and does no trimming, so EVERY row expecting
# "true" is also asserting that a meaningless value did not silently disable
# STARTTLS and put SMTP credentials on the wire in cleartext.
#
# Read the two columns together. Where a row expects starttls "true" and the
# default is also "true", that column alone cannot tell "honoured" from
# "defaulted" -- but the same row's port column can, because the same raw
# value is a valid port for almost none of these shapes. The rows that pin
# "honoured" for starttls specifically are the four expecting "false".
_SMTP_OVERRIDE_MATRIX = [
    # --- absent / nil ---
    pytest.param([], "1025", "true", id="absent"),
    pytest.param(_both("--set", "null"), "1025", "true", id="nil-via-set-null"),
    # --- string: the empty and near-empty classes ---
    # Reachable from an ordinary CI pattern: --set email.smtp.useTls=$USE_TLS
    # with the variable unset, or set to a value that is only whitespace.
    pytest.param(_both("--set", ""), "1025", "true", id="empty-string"),
    pytest.param(_both("--set", " "), "1025", "true", id="whitespace-space"),
    pytest.param(_both("--set", "\t"), "1025", "true", id="whitespace-tab"),
    pytest.param(_both("--set", "\n"), "1025", "true", id="whitespace-newline"),
    # --- bool: what a bare --set produces ---
    pytest.param(_both("--set", "false"), "1025", "false", id="bool-false"),
    pytest.param(_both("--set", "true"), "1025", "true", id="bool-true"),
    # --- string: real boolean literals, as --set-string produces them ---
    pytest.param(_both("--set-string", "false"), "1025", "false", id="string-false"),
    pytest.param(_both("--set-string", "true"), "1025", "true", id="string-true"),
    pytest.param(
        _both("--set-string", "FALSE"), "1025", "false", id="string-false-uppercase"
    ),
    pytest.param(
        _both("--set-string", " false "), "1025", "false", id="string-false-padded"
    ),
    # --- string: shapes that are NOT boolean literals -> default, never passthrough ---
    pytest.param(_both("--set-string", "null"), "1025", "true", id="string-null"),
    pytest.param(_both("--set-string", "yes"), "1025", "true", id="string-yes"),
    pytest.param(_both("--set-string", "1.5"), "1025", "true", id="string-fractional"),
    # --- int64: what a bare --set produces for an integer literal ---
    pytest.param(_both("--set", "0"), "0", "true", id="int-zero"),
    pytest.param(_both("--set", "587"), "587", "true", id="int-587"),
    pytest.param(_both("--set", "65535"), "65535", "true", id="int-65535"),
    pytest.param(_both("--set-string", "587"), "587", "true", id="string-digits"),
    pytest.param(
        _both("--set-string", "0587"), "0587", "true", id="string-leading-zero"
    ),
    # --- float64: what a values FILE and --set-json produce for any number ---
    pytest.param(_both("--set-json", "587"), "587", "true", id="float-587"),
    pytest.param(_both("--set-json", "1.5"), "1025", "true", id="float-fractional"),
    # --- map / slice: a whole subtree pasted where a scalar belongs ---
    pytest.param(_both("--set-json", '{"a":1}'), "1025", "true", id="map"),
    pytest.param(_both("--set-json", "[1,2]"), "1025", "true", id="slice"),
    # --- hostile strings: the rendered value lands inside a double-quoted
    #     shell word in the postStart hook, so a `"` or a newline that reaches
    #     it is command injection, not just a bad config value.
    pytest.param(
        _both("--set-string", '1"; id; echo "'),
        "1025",
        "true",
        id="shell-metacharacters",
    ),
    pytest.param(
        _both("--set-string", "false\nid"), "1025", "true", id="interior-newline"
    ),
]


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm binary not installed")
@pytest.mark.parametrize(("args", "want_port", "want_starttls"), _SMTP_OVERRIDE_MATRIX)
def test_smtp_port_and_starttls_are_total_over_every_value_shape(
    args: list[str], want_port: str, want_starttls: str
) -> None:
    """The two guards emit a member of a closed set, or the default. Nothing else.

    Four previous remedies each closed the one broken case in front of them
    and opened another, because each was a blacklist -- `default`, then
    `toString | default`, then `kindIs "invalid"`, then that OR `eq (toString
    .) ""` -- and a blacklist is only ever as complete as the list of shapes
    someone thought to try. `starttls=` and `starttls=" "` both survived to
    the rendered output, and Boolean.parseBoolean reads both as false: TLS off
    without being asked.

    The guards are now whitelists, so the argument is structural rather than
    enumerative. `toString` is total (Sprig falls through to fmt "%v" for
    every type, nil included), so the comparison always has a string on the
    left; membership is exact equality against a literal set, or a fully
    anchored ASCII-digits regex (Go's `$` is end-of-TEXT without `(?m)`, so an
    embedded newline cannot slip past it); and the else-branch is the default,
    not the input. The output alphabet is therefore closed under every
    possible input, which is what makes the "never silently false" and "never
    shell-injectable" properties hold for shapes nobody has tried yet.

    This matrix does not establish that property -- it samples it. It is here
    so that a future edit which reintroduces a passthrough branch fails.
    """
    script = _render_keycloak_poststart_script(*args)

    assert _smtp_field(script, "port") == want_port, (
        f"helm {' '.join(args) or '(no override)'} rendered "
        f"smtpServer.port={_smtp_field(script, 'port')!r}, expected {want_port!r}"
    )
    assert _smtp_field(script, "starttls") == want_starttls, (
        f"helm {' '.join(args) or '(no override)'} rendered "
        f"smtpServer.starttls={_smtp_field(script, 'starttls')!r}, expected {want_starttls!r} "
        "-- Boolean.parseBoolean reads anything that is not literally 'true' as false, "
        "so a wrong value here disables STARTTLS silently"
    )
