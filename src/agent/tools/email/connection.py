"""Email (IMAP/SMTP) datasource connection layer.

``EmailConnection`` bundles the mailbox credentials with the resolved,
non-secret scoping config (access tier, folder allowlist, drafts folder,
recipient allowlist, unattended-send flag). It is created by the email
connection factory (``agent.connectors.connections``) and reached by the tools
via ``ToolContext.connection_for("email")``.

Connections are strictly per-operation: every tool call opens a fresh IMAP
(or SMTP) connection with an explicit socket timeout and closes it when the
operation finishes. Nothing is cached across tool calls, so there is no
keepalive/reconnect machinery and a dead mail host fails fast instead of
wedging the tool node.

Correctness rules enforced here (see knowledge-base/knowledge/features/email_datasource.md,
"IMAP correctness caveats"):

- Read paths open folders with EXAMINE (``readonly=True``) and fetch with
  ``mark_seen=False`` so the read tier cannot flip ``\\Seen``.
- Folder allowlist matching uses the server-reported hierarchy delimiter
  (never a hardcoded ``/``) with subtree semantics and case-insensitive INBOX.
- Special folders (Drafts/Trash/Archive/...) are resolved via RFC 6154
  SPECIAL-USE attributes from LIST, with the configured name as fallback.
- ``move_uids`` never issues a bare EXPUNGE: server-side UID MOVE when the
  capability exists, else UID COPY + UID STORE ``\\Deleted`` + UID EXPUNGE
  scoped to exactly the moved UIDs (RFC 4315 UIDPLUS), else fail closed.
"""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# Explicit socket timeout for every IMAP/SMTP connection — a dead socket must
# fail fast, not wedge the tool node (cf. the search_files SSH wedge).
DEFAULT_SOCKET_TIMEOUT = 30.0

# IMAP system flags (plain strings — identical to imap_tools.MailMessageFlags,
# defined here so the module tops stay free of imap_tools imports).
SEEN_FLAG = "\\Seen"
FLAGGED_FLAG = "\\Flagged"
DELETED_FLAG = "\\Deleted"
DRAFT_FLAG = "\\Draft"

ACCESS_TIERS = ("read", "read_write", "draft", "send")

# RFC 6154 SPECIAL-USE mailbox attributes that mark well-known folders.
SPECIAL_USE_FLAGS = frozenset(
    {"\\Drafts", "\\Sent", "\\Trash", "\\Junk", "\\Archive", "\\All"}
)

# Friendly destination tokens accepted by email_move ("archive"/"trash" are
# just move destinations) mapped to the SPECIAL-USE attribute to resolve.
SPECIAL_DESTINATION_TOKENS = {
    "trash": "\\Trash",
    "archive": "\\Archive",
    "junk": "\\Junk",
    "spam": "\\Junk",
    "sent": "\\Sent",
    "drafts": "\\Drafts",
}


class EmailToolError(Exception):
    """Policy or capability refusal with an agent-readable message.

    Tools catch this and return ``f"Error: {e}"`` — never raise to the graph.
    """


def _segment_eq(a: str, b: str, first: bool) -> bool:
    """Compare one hierarchy segment; INBOX (first segment) is case-insensitive."""
    if first and a.upper() == "INBOX" and b.upper() == "INBOX":
        return True
    return a == b


def folder_name_eq(a: str, b: str) -> bool:
    """Exact folder-name equality with case-insensitive INBOX."""
    if a.upper() == "INBOX" and b.upper() == "INBOX":
        return True
    return a == b


def folder_allowed(name: str, allowlist: Sequence[str], delim: str) -> bool:
    """Check a folder name against the allowlist with subtree semantics.

    - Empty allowlist = the whole mailbox is in scope.
    - An entry allows itself and its subtree: ``AI`` allows ``AI`` and
      ``AI<delim>Processed`` for whatever hierarchy delimiter the server
      reports ('/', '.', ...).
    - INBOX is matched case-insensitively (RFC 3501); other names are
      case-sensitive.
    - Leniency: an allowlist entry written with '/' still matches on servers
      whose delimiter differs (config authors usually type '/').
    """
    if not allowlist:
        return True
    delim = delim or "/"
    name_parts = name.split(delim)
    for raw_entry in allowlist:
        entry = str(raw_entry)
        entry_parts = entry.split(delim)
        if delim != "/" and delim not in entry and "/" in entry:
            entry_parts = entry.split("/")
        if len(entry_parts) > len(name_parts):
            continue
        if all(
            _segment_eq(name_parts[i], entry_parts[i], i == 0)
            for i in range(len(entry_parts))
        ):
            return True
    return False


class EmailConnection:
    """Per-operation IMAP/SMTP connection factory plus resolved email config.

    Public attributes (the datasource-setup / tools contract):
        access: 'read' | 'read_write' | 'draft' | 'send' (default 'draft')
        folders: folder allowlist (empty = whole mailbox in scope)
        drafts_folder: fallback Drafts folder name (SPECIAL-USE wins)
        from_address: From header for compositions (defaults to username)
        recipient_allowlist: addresses or @domains permitted for new
            (non-reply) compositions
        unattended_send: direct-SMTP send permitted (default False — gated)
        username: mailbox login / identity
    """

    def __init__(self, credentials: Dict[str, Any], config: Dict[str, Any]):
        credentials = credentials or {}
        config = config or {}

        self.username: str = str(credentials.get("username") or "")
        self._password: str = str(credentials.get("password") or "")
        self._imap: Dict[str, Any] = dict(credentials.get("imap") or {})
        self._smtp: Dict[str, Any] = dict(credentials.get("smtp") or {})

        access = str(config.get("access") or "draft")
        if access not in ACCESS_TIERS:
            # Fail closed to the most restrictive functional tier: an unknown
            # tier must never unlock mutations.
            logger.warning(
                "email datasource: unknown access tier %r — clamping to 'read'",
                access,
            )
            access = "read"
        self.access: str = access

        self.folders: List[str] = [
            str(f).strip() for f in (config.get("folders") or []) if str(f).strip()
        ]
        self.drafts_folder: str = str(config.get("drafts_folder") or "Drafts")
        self.from_address: str = str(config.get("from_address") or self.username)
        self.recipient_allowlist: List[str] = [
            str(a).strip()
            for a in (config.get("recipient_allowlist") or [])
            if str(a).strip()
        ]
        self.unattended_send: bool = bool(config.get("unattended_send") or False)

    def close(self) -> None:
        """No-op — connections are per-operation and closed by ``connect()``."""

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _new_mailbox(self) -> Any:
        """Build and log in a fresh imap_tools MailBox (override point for tests)."""
        from imap_tools import MailBox, MailBoxStartTls, MailBoxUnencrypted

        host = self._imap.get("host")
        if not host:
            raise EmailToolError("email connector has no IMAP host configured")
        port = self._imap.get("port")
        security = str(self._imap.get("security") or "ssl").lower()
        if security in ("ssl", "tls", "ssl/tls", "ssl_tls"):
            mailbox = MailBox(
                host, port=int(port or 993), timeout=DEFAULT_SOCKET_TIMEOUT
            )
        elif security == "starttls":
            mailbox = MailBoxStartTls(
                host, port=int(port or 143), timeout=DEFAULT_SOCKET_TIMEOUT
            )
        else:  # "none"/"plain" — test servers (GreenMail) or localhost bridges
            mailbox = MailBoxUnencrypted(
                host, port=int(port or 143), timeout=DEFAULT_SOCKET_TIMEOUT
            )
        # initial_folder=None: no implicit INBOX SELECT — folder selection is
        # explicit (and readonly for read paths).
        mailbox.login(self.username, self._password, initial_folder=None)
        return mailbox

    @contextmanager
    def connect(self) -> Iterator[Any]:
        """Yield a logged-in mailbox for one operation; always logs out."""
        mailbox = self._new_mailbox()
        try:
            yield mailbox
        finally:
            try:
                mailbox.logout()
            except Exception as e:  # noqa: BLE001 — teardown must never raise
                logger.debug("email logout failed (ignored): %s", e)

    # ------------------------------------------------------------------
    # Folder helpers (all take the live mailbox from connect())
    # ------------------------------------------------------------------

    def list_folders(self, mailbox: Any) -> List[Any]:
        """LIST the server hierarchy (FolderInfo: name, delim, flags)."""
        return list(mailbox.folder.list())

    def discover_delimiter(self, mailbox: Any) -> str:
        """Server-reported hierarchy delimiter (never assume '/')."""
        try:
            for info in self.list_folders(mailbox):
                delim = getattr(info, "delim", None)
                if delim:
                    return str(delim)
        except Exception as e:
            logger.debug("email LIST for delimiter discovery failed: %s", e)
        return "/"

    def folder_allowed(self, name: str, delim: str) -> bool:
        """Allowlist check for one folder name (subtree + INBOX-insensitive)."""
        return folder_allowed(name, self.folders, delim)

    def selectable_allowed_folders(self, mailbox: Any) -> List[Any]:
        """Allowed, selectable (non-\\Noselect) folders from LIST."""
        result = []
        for info in self.list_folders(mailbox):
            flags = tuple(getattr(info, "flags", ()) or ())
            if any(str(f).lower() == "\\noselect" for f in flags):
                continue
            if self.folder_allowed(info.name, getattr(info, "delim", "/") or "/"):
                result.append(info)
        return result

    def select_folder(
        self, mailbox: Any, folder: str, readonly: bool = True
    ) -> Optional[int]:
        """SELECT/EXAMINE a folder and return its current UIDVALIDITY.

        ``readonly=True`` issues EXAMINE — the belt to ``mark_seen=False``'s
        braces: even an accidental non-peek fetch cannot flip ``\\Seen``.
        """
        mailbox.folder.set(folder, readonly=readonly)
        try:
            status = mailbox.folder.status(folder, ["UIDVALIDITY"])
            value = status.get("UIDVALIDITY")
            return int(value) if value is not None else None
        except Exception as e:
            logger.debug("email STATUS UIDVALIDITY failed for %r: %s", folder, e)
            return None

    def resolve_special_use(self, mailbox: Any, attribute: str) -> Optional[str]:
        """Folder name carrying a SPECIAL-USE attribute (e.g. '\\Drafts')."""
        target = attribute.lower()
        try:
            for info in self.list_folders(mailbox):
                flags = tuple(getattr(info, "flags", ()) or ())
                if any(str(f).lower() == target for f in flags):
                    return info.name
        except Exception as e:
            logger.debug("email SPECIAL-USE resolution failed: %s", e)
        return None

    def resolve_drafts_folder(self, mailbox: Any) -> str:
        """SPECIAL-USE \\Drafts folder, falling back to config.drafts_folder."""
        return self.resolve_special_use(mailbox, "\\Drafts") or self.drafts_folder

    def resolve_move_destination(
        self, mailbox: Any, destination: str
    ) -> Tuple[str, bool, bool]:
        """Resolve an email_move destination.

        Returns ``(resolved_name, is_special_use, exists_on_server)``.
        A destination naming an existing SPECIAL-USE folder, or a friendly
        token ('trash', 'archive', ...), resolves to the server's folder and
        is exempt from the folder allowlist (archive/trash are just move
        destinations per the design). Anything else must pass the allowlist.
        """
        try:
            infos = self.list_folders(mailbox)
        except Exception as e:
            logger.debug("email LIST for destination resolution failed: %s", e)
            infos = []
        for info in infos:
            if folder_name_eq(info.name, destination):
                flags = tuple(getattr(info, "flags", ()) or ())
                is_special = any(str(f) in SPECIAL_USE_FLAGS for f in flags)
                return info.name, is_special, True
        token = destination.strip().lower()
        special_flag = SPECIAL_DESTINATION_TOKENS.get(token)
        if special_flag:
            resolved = self.resolve_special_use(mailbox, special_flag)
            if resolved:
                return resolved, True, True
        return destination, False, False

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------

    def move_uids(self, mailbox: Any, uids: Sequence[str], destination: str) -> None:
        """Move UIDs without ever issuing a bare EXPUNGE.

        imap_tools' own move()/delete() fallback ends in a bare EXPUNGE that
        removes EVERY \\Deleted-flagged message in the folder (the exact
        hazard RFC 6851 was written to avoid). We therefore:

        1. call ``mailbox.move()`` ONLY when the server advertises MOVE — on
           that branch the library does a server-side UID MOVE and its unsafe
           fallback is unreachable;
        2. otherwise do UID COPY + UID STORE \\Deleted + UID EXPUNGE scoped
           to exactly the moved UIDs (RFC 4315 UIDPLUS);
        3. fail closed when the server supports neither.
        """
        uid_list = [str(u) for u in uids]
        uid_set = ",".join(uid_list)
        capabilities = {
            str(c).upper() for c in (getattr(mailbox.client, "capabilities", ()) or ())
        }
        if "MOVE" in capabilities:
            mailbox.move(uid_list, destination)
            return
        if "UIDPLUS" in capabilities:
            mailbox.copy(uid_list, destination)
            mailbox.flag(uid_list, [DELETED_FLAG], True)
            # Scoped UID EXPUNGE — never mailbox.delete()/mailbox.expunge().
            result = mailbox.client.uid("EXPUNGE", uid_set)
            status = result[0] if isinstance(result, tuple) and result else "OK"
            if str(status).upper() != "OK":
                raise EmailToolError(
                    f"UID EXPUNGE failed after copy ({status}); the messages were "
                    f"copied to '{destination}' and flagged \\Deleted in the source "
                    "folder but not expunged"
                )
            return
        raise EmailToolError(
            "this IMAP server supports neither MOVE nor UIDPLUS, so a move "
            "cannot be performed safely (a bare EXPUNGE could destroy other "
            "deleted-flagged messages). Failing closed — no changes were made."
        )

    # ------------------------------------------------------------------
    # SMTP (send tier)
    # ------------------------------------------------------------------

    def open_smtp(self) -> Any:
        """Open and authenticate an SMTP connection (caller must quit())."""
        import smtplib

        host = self._smtp.get("host")
        if not host:
            raise EmailToolError(
                "email connector has no SMTP server configured — sending is "
                "not possible for this mailbox"
            )
        port = self._smtp.get("port")
        security = str(self._smtp.get("security") or "ssl").lower()
        if security in ("ssl", "tls", "ssl/tls", "ssl_tls"):
            smtp = smtplib.SMTP_SSL(
                host, int(port or 465), timeout=DEFAULT_SOCKET_TIMEOUT
            )
        else:
            smtp = smtplib.SMTP(host, int(port or 587), timeout=DEFAULT_SOCKET_TIMEOUT)
            smtp.ehlo()
            if security == "starttls":
                smtp.starttls()
                smtp.ehlo()
        if self._password:
            smtp.login(self.username, self._password)
        return smtp
