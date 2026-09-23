"""Sanitize worker-authored text before a human or a model reads it.

Audit finding OC-05. Worker output reaches the officer and the Legate through
several doors — evidence pages, completion reports, routed message subjects and
bodies, escalation context, notification bodies — and a worker, a compromised
tool, or a prompt-injected web page it summarized can copy a credential into any
of them. Two redactors already existed and neither covered this:

* ``logging_config.redact`` has the right generic patterns but belongs to the
  log formatter. Presentation must not depend on a logging concern, and the
  formatter cannot know a caller's runtime secrets.
* ``kb_git_source.redact_git_error`` knows how to erase *known* secret values
  and URL userinfo, but evidence reads call it with no secrets, so it was only
  ever stripping userinfo.

This module is the one sanitizer for the presentation boundary, combining both:
known runtime values where the caller can supply them, generic secret shapes
where it cannot.

**It reports rather than hides.** Every call returns how many redactions were
made so the surface can tell the officer that evidence was withheld. Silently
altered evidence is worse than withheld evidence — he would judge a truncated
artifact believing it complete.

**It is not a security control on its own.** Redaction does not make worker text
trustworthy; that text is still untrusted instructions and must never be
followed. This only stops a credential riding along in something the officer was
always going to read. Nor does it change the underlying artifact: raw evidence
keeps its bytes and its checksum, and only the view is sanitized.

Deliberately conservative on false positives. A pattern broad enough to catch
"any long opaque string" would erase commit SHAs, job ids and content hashes —
the things an officer navigates by — so every pattern here is anchored to a
recognizable prefix, a key name, or a structural marker.

**Two profiles.** :func:`sanitize` is the presentation profile: text a human or
an officer reads and never edits. :func:`sanitize_tool_output` is the narrower
profile for agent tool results, which feed the model's own transcript. The
agent edits files from what it read, so a false positive there is a marker
written back into a file (the write tools refuse that, but a refused edit is
still a stalled agent). The tool profile keeps only shapes that ARE a
credential value: the agent's own known tokens, URL userinfo, query secrets and
scp passwords that look like tokens (so ``postgres:postgres@`` in a compose
file survives), prefixed provider tokens, JWTs and whole private-key blocks. It
drops the key-name heuristic, which shreds source code
(``def check(token: str)``).

**Every quantifier is bounded.** Tool output can carry a fetched web page, and
the orchestrator runs the presentation profile synchronously inside async
handlers, so a pattern that goes quadratic on hostile input holds the GIL for
seconds. Lookaheads that run at every candidate position are the trap: an
unbounded scan inside one makes each candidate cost the rest of the text.
"""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence
from urllib.parse import quote, quote_plus

REDACTED = "[REDACTED]"

# Values that are a template reference, not a secret. A compose file, a Helm
# chart, a shell script or a DSN builder is full of them — `${PGPASSWORD}`,
# `${X:-default}`, `$GITEA_TOKEN`, `$pass`, `$(cat f)`, `{{.Values.pw}}`,
# `{password}`, `{}`, `%s`, `%(password)s`, `<password>`, `***`, `YOUR_TOKEN`
# — and erasing one tells the reader nothing while corrupting a file the agent
# rewrites. Bounded, because it runs inside lookaheads at every candidate.
_PLACEHOLDER = (
    r"(?:\$\{[^}\s]{0,64}\}"
    r"|\$\([^)\s]{0,64}\)"
    r"|\$[A-Za-z_][A-Za-z0-9_]{0,63}"
    r"|\{\{[^}\s]{0,64}\}\}"
    r"|\{[^{}\s]{0,64}\}"
    r"|%(?:\([A-Za-z_][A-Za-z0-9_]{0,63}\))?s"
    r"|<[^>\s]{0,64}>"
    r"|\*{1,64}"
    r"|[A-Z]{1,32}(?:_[A-Z]{1,32}){1,8})"
)
# A userinfo password that must be left alone: a placeholder, or a known secret
# this pass already replaced (re-matching `oauth2:[REDACTED]@` would count one
# credential twice).
_KEEP_PASSWORD = r"(?!(?:" + _PLACEHOLDER + r"|\[REDACTED\])@)"

# key: <value> / key=<value> for secret-ish key names. Anchored on the NAME, so
# `password: hunter2` goes and `job_id: 4f2a91c8` stays.
#
# Two details that were wrong on the first pass and are covered by tests:
# the separator accepts single quotes (`client_secret: 'shhh'` is as common as
# the double-quoted form), and the value may carry a `Bearer ` prefix — matching
# only up to the first space would erase the word "Bearer" and leave the token
# it introduces sitting in plain view.
#
# The value never starts at our own marker, so a second pass over sanitized
# text (a tool result later archived, an escalation body later recorded as a
# notification) counts nothing: the count means values removed, not looks. Nor
# at a placeholder — which is why only the name is case-insensitive: under a
# global `(?i)` the all-caps placeholder shapes would match any lowercase word.
_SECRET_NAMES = (
    r"authorization|api[_-]?key|secret|client[_-]?secret|password|passwd"
    r"|token|access[_-]?key|private[_-]?key|refresh[_-]?token"
)
# An HTTP auth scheme word, taken with the credential it introduces
# (`Authorization: Basic <b64>`, Gitea's `Authorization: token <hex>`).
_AUTH_SCHEME = r"(?i:(?:bearer|basic|token)[ \t]{1,16})"
_KV_SECRET = re.compile(
    r"\b((?i:" + _SECRET_NAMES + r"))"
    r"([\"']?[ \t]{0,16}[:=][ \t]{0,16}[\"']?)"
    r"((?!"
    + _AUTH_SCHEME
    + r"?\[REDACTED\]|"
    + _PLACEHOLDER
    + r"(?:[\s\"',}{)&]|$))"
    + _AUTH_SCHEME
    + r"?[^\s\"',}{)]{1,4096})"
)

# A JWT, bounded on the left by a character outside its own alphabet. `\b`
# was not enough: `-` is a word boundary AND a JWT character, so `eyJ-eyJ-…`
# started a match at every repetition and each one rescanned to the end.
_JWT = re.compile(
    r"(?<![A-Za-z0-9_\-])eyJ[A-Za-z0-9_\-]{1,4096}+\.[A-Za-z0-9_\-]{1,4096}+"
    r"\.[A-Za-z0-9_\-]{1,4096}"
)

# Standalone shapes with a recognizable prefix or structure. Each is specific
# enough that a benign identifier cannot match it.
_PREFIXED: tuple[re.Pattern[str], ...] = (
    _JWT,
    # OpenAI / Anthropic / OpenRouter: `sk-`, up to four short dash segments
    # (`ant-api03-`, `proj-`, `or-v1-`), then a long dash-free run. The run is
    # what `sk-ecdsa-sha2-nistp256@openssh.com` (an SSH key TYPE) and
    # `sk-learn-regression-model-v1` lack.
    re.compile(
        r"\bsk-(?=(?:[A-Za-z0-9]{1,12}-){0,4}[A-Za-z0-9_]{20})[A-Za-z0-9_\-]{20,512}"
    ),
    re.compile(r"\b[rs]k_(?:live|test)_[A-Za-z0-9]{16,256}"),  # Stripe
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{8,512}"),  # Slack
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,255}"),  # GitHub
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,255}"),  # GitHub fine-grained
    re.compile(r"\bglpat-[A-Za-z0-9_\-]{20,255}"),  # GitLab
    re.compile(r"\bhf_[A-Za-z0-9]{30,255}"),  # Hugging Face
    re.compile(r"\bnpm_[A-Za-z0-9]{36,255}"),  # npm
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),  # AWS access key id
    re.compile(r"\bAIza[0-9A-Za-z_\-]{35}\b"),  # Google API key
)
# "Bearer <anything>" — right for a report, wrong for a README the agent is
# editing ("send a Bearer token in the header" would lose its noun).
_BEARER = re.compile(r"(?i)\bbearer[ \t]{1,16}[A-Za-z0-9._\-]{1,4096}")
# The tool profile's auth-scheme credential (`Bearer`, `Basic`, Gitea's
# `token`): it must look like one — long, and carrying a digit, which prose
# after the word does not ("a bearer token", "token budget"). The scheme word
# is kept so the line still says what kind of header it was.
_BEARER_CREDENTIAL = re.compile(
    r"(?i)\b((?:bearer|basic|token)[ \t]{1,16})(?=[A-Za-z0-9._~+/\-]{0,4096}\d)"
    r"[A-Za-z0-9._~+/\-]{20,4096}={0,2}"
)
_STANDALONE: tuple[re.Pattern[str], ...] = (_BEARER, *_PREFIXED)

# A PEM private key, header to footer. The body is base64 (plus whitespace,
# the `\n` escapes of a key embedded in JSON, and the `Proc-Type:`/`DEK-Info:`
# lines of a legacy encrypted key) and nothing else — code that merely sits
# between two marker LITERALS, as in a key-parsing module, is not a key. The
# body alphabet excludes `-`, so each BEGIN is one bounded scan.
#
# Each header line starts after a MANDATORY line break and its value is
# possessive. With an optional separator, `A: A: A: …` could be carved into
# header lines at every position and backtracked roughly quartically (50 s for
# 4.6 KB); with a mandatory one each line has exactly one reading.
_PEM_BODY = r"(?:[A-Za-z0-9+/=\s]|\\[nrt]){0,65536}+"
_PEM_BLOCK = re.compile(
    r"-----BEGIN [A-Z ]{0,40}PRIVATE KEY-----"
    r"(?:(?:\r?\n|\\n)[A-Z][A-Za-z-]{0,30}: [^\n\\]{0,200}+){0,4}"
    + _PEM_BODY
    + r"-----END [A-Z ]{0,40}PRIVATE KEY-----"
)
# A lone PEM header, for diagnostics that echo one line of a key rather than the
# whole block — the case kb_git_source handles by splitting known secrets.
_PEM_HEADER = re.compile(r"-----BEGIN [A-Z ]{0,40}PRIVATE KEY-----")

# scheme://user:password@host — erase the credential, keep the host so the
# officer can still tell WHICH remote failed. `git remote -v` against a
# token-authenticated clone prints exactly this. A username alone
# (`ssh://git@host`) is not a credential and survives. The scheme is bounded
# rather than `\b`-anchored: `a+a+a+…` otherwise started a match at every
# `a` and each one rescanned the whole run looking for `://`. The user half
# stops at the first `:` (it cannot contain one unencoded) and may be empty
# (`redis://:pw@host`). Group 2 is the password, for the tool profile's
# token test.
_URL_USERINFO = re.compile(
    r"([a-zA-Z][\w+.\-]{0,31}://)[^\s/@:\[\]]{0,256}+:"
    + _KEEP_PASSWORD
    + r"([^\s/@]{0,2048}+)@"
)
# The same URL cut off before its `@`: a live read of a pane whose line is
# still being written ends mid-password, and the full pattern above never sees
# the `@` it anchors on. Only at the very end of the text, never a bare port
# (`http://localhost:4200` ends a dev server's banner), and never across a
# quote or bracket (`url: "nats://nats:4222"`, `http://[::1]:8080`). The user
# half excludes `:` — otherwise `a:a:a:…` has a user/password split at every
# colon — and the pattern only ever runs over the last _TAIL_WINDOW characters,
# since it can only match there.
_TAIL_CLASS = r"[^\s/@\"'`<>()\[\]{},;]"
_TAIL_USER_CLASS = r"[^\s/@:\"'`<>()\[\]{},;]"
_URL_USERINFO_TAIL = re.compile(
    r"([a-zA-Z][\w+.\-]{0,31}://)" + _TAIL_USER_CLASS + r"{0,256}:"
    r"(?!(?:\d{1,5}|" + _PLACEHOLDER + r"|\[REDACTED\])\s{0,64}\Z)"
    r"(" + _TAIL_CLASS + r"{1,2048}+)(?=\s{0,64}\Z)"
)
# scheme (32) + user (256) + `:` + password (2048) + trailing space (64).
_TAIL_WINDOW = 2_401
# user:password@host for scp-style git remotes, which carry no scheme. The
# lookbehind (not `\b`) keeps `a.a.a.…` from being a match start at every dot.
# An image digest (`python:3.12-slim@sha256:…`) is not a password, nor is the
# default of a shell expansion (`${DB_PASS:-changeme}@db`).
_SCP_PASSWORD = re.compile(
    r"(?<![\w.\-])(?<!\$\{)[\w.\-]{1,256}+:"
    + _KEEP_PASSWORD
    + r"([^\s/@:]{1,2048}+)@(?!sha(?:256|384|512):)(?=[\w.\-]{1,256}+[:/])"
)
# ?access_token=… / &X-Amz-Signature=… / #id_token=… — a secret riding in a
# URL rather than its userinfo. Anchored on the query delimiter AND a name
# that ends in a credential word, so `?tokenizer=bert` and `?sort=key` stay.
# The value stops at anything that closes a URL in prose or markup, so a
# Markdown link keeps its `)`.
_QUERY_VALUE_END = r"&#;\s\"'`<>()\[\]{},"
_URL_QUERY_SECRET = re.compile(
    r"([?&;#](?i:[\w.\-]{0,40}?(?:token|secret|passw(?:or)?d|pwd|api[_-]?key"
    r"|apikey|access[_-]?key|signature|credential)s?|key|sig)=)"
    r"(?![$<{*\[]|[A-Z]{1,32}(?:_[A-Z]{1,32}){1,8}(?:[" + _QUERY_VALUE_END + r"]|$))"
    r"([^" + _QUERY_VALUE_END + r"]{1,4096}+)"
)


def _token_like(value: str, *, min_length: int = 16) -> bool:
    """Shaped like a generated credential rather than a word or a port.

    The tool profile's bar for a userinfo, scp or query value. A dev default
    (`postgres:postgres@`), a port (`8443:30443@server`) or a doc example
    (`?sig=1`) is not one; a 40-hex token, a PAT, a signature is — letters
    AND digits, and long. The agent's OWN tokens do not depend on this: they
    are matched literally first.
    """
    return (
        len(value) >= min_length
        and any(c.isdigit() for c in value)
        and any(c.isalpha() for c in value)
    )


@dataclass(frozen=True, slots=True)
class Redaction:
    """Sanitized text plus how much was removed.

    ``count`` exists so a surface can say "3 values withheld" instead of
    handing over quietly-shortened evidence. An officer who cannot tell the
    difference between "the worker wrote nothing here" and "we removed it" will
    judge the wrong artifact.
    """

    text: str
    count: int

    @property
    def redacted(self) -> bool:
        return self.count > 0


def _known_secret_variants(secrets: Iterable[str]) -> list[str]:
    """Every form a known secret plausibly appears in, longest first.

    Literal, URL-encoded, single lines of a multiline key, and base64 — the
    last because HTTP Basic auth ships `user:token` base64-encoded
    (`curl -v -u oauth2:$TOK`, `GIT_CURL_VERBOSE`), so a caller that knows a
    credential pair passes `user:token` and the header value is caught too.

    Longest-first matters: redacting a short prefix first would leave the tail
    of a longer secret behind as an orphan fragment.
    """
    variants: set[str] = set()
    for secret in secrets or ():
        if not secret or len(secret) < 8:
            # Below this a "secret" is as likely to be a common word, and
            # erasing every occurrence of it would shred the text.
            continue
        variants.add(secret)
        variants.add(quote(secret, safe=""))
        variants.add(quote_plus(secret, safe=""))
        variants.update(line for line in secret.splitlines() if len(line) >= 8)
        encoded = base64.b64encode(secret.encode("utf-8")).decode("ascii")
        variants.add(encoded)
        variants.add(encoded.rstrip("="))
    return sorted(variants, key=len, reverse=True)


def _sub(
    pattern: re.Pattern[str],
    text: str,
    replacement: Callable[[re.Match[str]], str],
    keep: Callable[[re.Match[str]], bool] | None,
) -> tuple[str, int]:
    """``pattern.subn`` that leaves a match alone when ``keep`` says so."""
    count = 0

    def _replace(match: re.Match[str]) -> str:
        nonlocal count
        if keep is not None and keep(match):
            return match.group(0)
        count += 1
        return replacement(match)

    return pattern.sub(_replace, text), count


def _redact(text: str, secrets: Sequence[str], *, presentation: bool) -> Redaction:
    result = text
    count = 0

    # Known values first: a real credential may also match a generic pattern,
    # and we would rather remove it as a known secret than depend on the shape.
    for secret in _known_secret_variants(secrets):
        if secret in result:
            count += result.count(secret)
            result = result.replace(secret, REDACTED)

    # PEM blocks before line-level patterns, so a whole key is one redaction
    # rather than a header plus a wall of surviving base64.
    result, n = _PEM_BLOCK.subn(REDACTED, result)
    count += n

    # In the tool profile a generic userinfo/scp/query value must look like a
    # token; the presentation profile withholds any of them.
    def _not_token(group: int, min_length: int = 16) -> Callable[..., bool] | None:
        if presentation:
            return None
        return lambda m: not _token_like(m.group(group), min_length=min_length)

    # URL structure before the name- and prefix-based shapes: `token:` in
    # `https://x-access-token:ghs_…@github.com/o/r` would otherwise take the
    # value up to the next space — host and path with it — and the diagnostic
    # would no longer say WHICH remote failed.
    result, n = _sub(
        _URL_USERINFO, result, lambda m: f"{m.group(1)}{REDACTED}@", _not_token(2)
    )
    count += n
    # A cut-off prefix is shorter than the token it came from: 8 is enough to
    # tell a token's head from a word, and a shorter head gives little away.
    head, tail = result[:-_TAIL_WINDOW], result[-_TAIL_WINDOW:]
    tail, n = _sub(
        _URL_USERINFO_TAIL,
        tail,
        lambda m: f"{m.group(1)}{REDACTED}",
        _not_token(2, min_length=8),
    )
    result = head + tail
    count += n
    result, n = _sub(_SCP_PASSWORD, result, lambda m: f"{REDACTED}@", _not_token(1))
    count += n
    result, n = _sub(
        _URL_QUERY_SECRET, result, lambda m: f"{m.group(1)}{REDACTED}", _not_token(2)
    )
    count += n

    if presentation:
        # A lone header gives nothing away and is a string literal in every
        # key-handling module the agent might edit; only a reader needs it gone.
        result, n = _PEM_HEADER.subn(REDACTED, result)
        count += n

        def _kv(match: re.Match[str]) -> str:
            return f"{match.group(1)}{match.group(2)}{REDACTED}"

        result, n = _KV_SECRET.subn(_kv, result)
        count += n

    if not presentation:
        result, n = _BEARER_CREDENTIAL.subn(rf"\1{REDACTED}", result)
        count += n
    for pattern in _STANDALONE if presentation else _PREFIXED:
        result, n = pattern.subn(REDACTED, result)
        count += n

    return Redaction(result, count)


def sanitize(text: str | None, *, secrets: Sequence[str] = ()) -> Redaction:
    """Redact secret-shaped and known-secret content for presentation.

    ``secrets`` are exact runtime values the caller knows (a workspace token, a
    git credential). They are matched literally, URL-encoded and base64-encoded,
    which catches values the generic patterns cannot recognize.
    """
    if not text:
        return Redaction(text or "", 0)
    return _redact(text, secrets, presentation=True)


def sanitize_tool_output(text: str | None, *, secrets: Sequence[str] = ()) -> Redaction:
    """Redact credential values from a tool result the model will read.

    The narrower profile (see the module docstring): no key-name heuristic, no
    lone PEM header, "Bearer" only before something credential-shaped, and a
    URL/scp/query value only when it looks like a generated token. The known
    ``secrets`` are matched regardless of shape.

    Both profiles catch a credential URL cut off at the end of the text, which
    a read of a still-printing pane produces. A caller that truncates must
    redact the region it keeps BEFORE it cuts: a fragment with neither scheme
    nor ``@`` matches nothing.
    """
    if not text:
        return Redaction(text or "", 0)
    return _redact(text, secrets, presentation=False)


def sanitize_text(text: str | None, *, secrets: Sequence[str] = ()) -> str:
    """:func:`sanitize` when the caller has nowhere to report the count."""
    return sanitize(text, secrets=secrets).text


__all__ = [
    "REDACTED",
    "Redaction",
    "sanitize",
    "sanitize_text",
    "sanitize_tool_output",
]
