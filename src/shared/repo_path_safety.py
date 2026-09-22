"""Shape rules for values spliced into a forge's ``/repos/{owner}/{repo}`` URL.

Two clients build forge REST URLs from values they do not choose: the
orchestrator's admin Gitea client (``orchestrator/services/gitea.py``,
security audit 2026-08-27 findings #3/#4) and the repository-connector forge
adapter (``shared/runtime/services/forge.py``). httpx normalises dot segments
before sending, so a ``..`` that reaches the path unencoded re-targets the
request at a different repository, and a raw ``?`` or ``#`` cuts the path
short. Both clients apply these rules at their URL formatters. Each raises
its own error type (the Gitea one maps to HTTP 400, the forge one is a
``ForgeError`` every caller already handles), so the checks take it as a
parameter rather than owning one.
"""

import re
from urllib.parse import unquote

#: ASCII letters, digits, ``.``, ``-``, ``_`` -- the owner/repository charset
#: Gitea (``AlphaDashDotPattern``), GitHub and GitLab share. A name in it is
#: already exactly one URL path segment and needs no further encoding; ``.``
#: and ``..`` are the only spellings in it that are still dot segments.
REPO_NAME_RE = re.compile(r"[A-Za-z0-9._-]+")
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")


def _check_shape(value: str, *, what: str, error: type[Exception]) -> None:
    if _CONTROL_CHARS_RE.search(value):
        raise error(f"{what} contains a control character")
    if "\\" in value:
        raise error(f"{what} contains a backslash")
    if value.startswith("/"):
        raise error(f"{what} must be repository-relative, not absolute")
    for segment in value.split("/"):
        if segment == "":
            raise error(f"{what} contains an empty segment")
        if segment in (".", ".."):
            raise error(f"{what} contains a dot segment")


def check_repo_path_shape(
    value: str, *, what: str, error: type[Exception] = ValueError
) -> None:
    """Refuse every shape that lets a repo-relative path leave its repository.

    Rejects NUL and other control characters, backslashes, absolute paths,
    empty segments (``a//b``) and ``.``/``..`` segments -- on the raw value
    and again on its percent-decoded form, because ``..%2F`` is one decode
    away from the same traversal. ``what`` labels the ``error`` message.
    """
    _check_shape(value, what=what, error=error)
    _check_shape(unquote(value), what=what, error=error)


__all__ = ["REPO_NAME_RE", "check_repo_path_shape"]
