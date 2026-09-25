"""IMAP Poller — Background service for inbound email reply processing.

Polls the IMAP inbox for email replies to agent messages. Parses the
``+`` sub-addressing in the ``To``/``Delivered-To`` header to route replies
to the correct job and thread. Strips signatures and quoted text.

Fully optional. When ``IMAP_HOST`` is not configured, all operations are
no-ops and the system works identically without it. Follows the same
graceful degradation pattern as ``NatsBridge`` in
``services/nats_bridge.py``.

Environment variables:
    IMAP_HOST           — IMAP server hostname (required for email replies)
    IMAP_PORT           — IMAP port (default: 993)
    IMAP_USER           — IMAP username
    IMAP_PASSWORD       — IMAP password
    IMAP_USE_SSL            — Use implicit SSL/TLS (default: true). Set to false for
                              STARTTLS on port 1143 (e.g. Proton Bridge).
    IMAP_TRUST_SELF_SIGNED  — Accept self-signed certs (default: false)
    IMAP_POLL_INTERVAL      — Seconds between polls (default: 30)
    AGENT_EMAIL             — Agent email address (e.g., agent@example.com)
    MAIL_DOMAIN             — Mail domain (e.g., example.com)
"""

import asyncio
import email as email_lib
import imaplib
import logging
import os
import ssl
from email.header import decode_header
from typing import Any, Callable, Coroutine

from orchestrator.services.reply_parser import parse_reply

logger = logging.getLogger(__name__)

# Type for the reply handler callback:
#   async (job_id, thread_id, message, sender_email, email_message_id) -> str
ReplyHandler = Callable[
    [str, str, str, str | None, str | None],
    Coroutine[Any, Any, str],
]


class ImapPoller:
    """IMAP inbox poller for inbound email reply routing.

    Graceful degradation: if IMAP_HOST is not set, the poller stays
    disabled and all methods are no-ops.
    """

    def __init__(self) -> None:
        self._host = os.getenv("IMAP_HOST", "")
        self._port = int(os.getenv("IMAP_PORT", "993"))
        self._user = os.getenv("IMAP_USER", "")
        self._password = os.getenv("IMAP_PASSWORD", "")
        self._use_ssl = os.getenv("IMAP_USE_SSL", "true").lower() in (
            "true",
            "1",
            "yes",
        )
        self._trust_self_signed = os.getenv(
            "IMAP_TRUST_SELF_SIGNED", "false"
        ).lower() in (
            "true",
            "1",
            "yes",
        )
        self._poll_interval = int(os.getenv("IMAP_POLL_INTERVAL", "30"))
        self._agent_email = os.getenv("AGENT_EMAIL", "")
        self._mail_domain = os.getenv("MAIL_DOMAIN", "")

        self._db: Any = None
        self._reply_handler: ReplyHandler | None = None
        self._available = False

    @property
    def is_available(self) -> bool:
        """Whether the poller is configured and connected."""
        return self._available

    @property
    def is_configured(self) -> bool:
        """Whether IMAP env vars are set."""
        return bool(self._host and self._user and self._agent_email)

    @property
    def poll_interval(self) -> int:
        """Seconds between IMAP polls."""
        return self._poll_interval

    def connect(self, db: Any, reply_handler: ReplyHandler) -> None:
        """Store DB reference and reply handler. No actual IMAP connection yet.

        The actual IMAP connection is made per-poll to avoid stale connections.

        Args:
            db: PostgresDB instance for dedup and job lookup
            reply_handler: Async callback for routing parsed replies
        """
        self._db = db
        self._reply_handler = reply_handler

        if not self.is_configured:
            logger.info("IMAP not configured. Email reply routing disabled.")
            self._available = False
            return

        self._available = True
        logger.info(
            "IMAP poller configured: %s:%d (poll every %ds)",
            self._host,
            self._port,
            self._poll_interval,
        )

    async def poll_once(self) -> int:
        """Connect to IMAP, fetch unprocessed emails, route replies.

        IMAP I/O is synchronous (stdlib imaplib) and runs in a thread.
        Email processing (DB lookups, reply routing) is async and runs
        on the event loop after the IMAP fetch completes.

        Returns:
            Number of emails successfully processed.
        """
        if not self._available or not self._db or not self._reply_handler:
            return 0

        # Fetch raw emails in a thread (blocking I/O)
        try:
            raw_emails = await asyncio.to_thread(self._fetch_unseen)
        except Exception as e:
            logger.error(f"IMAP fetch failed: {e}")
            return 0

        if not raw_emails:
            return 0

        # Process each email asynchronously
        processed = 0
        duplicates = 0
        mark_seen = []
        mark_unseen = []

        for msg_num, raw_email in raw_emails:
            try:
                result = await self._process_email(raw_email)
                if result == "routed":
                    mark_seen.append(msg_num)
                    processed += 1
                elif result == "duplicate":
                    mark_seen.append(msg_num)
                    duplicates += 1
                else:
                    mark_unseen.append(msg_num)
            except Exception as e:
                logger.warning(f"Failed to process IMAP message {msg_num}: {e}")

        skipped = len(mark_unseen)
        logger.debug(
            "Poll: %d checked, %d routed, %d duplicate, %d skipped",
            len(raw_emails),
            processed,
            duplicates,
            skipped,
        )

        # Update flags in a thread (blocking I/O)
        if mark_seen or mark_unseen:
            try:
                await asyncio.to_thread(
                    self._update_flags,
                    mark_seen,
                    mark_unseen,
                )
            except Exception as e:
                logger.warning(f"Failed to update IMAP flags: {e}")

        return processed

    def _get_ssl_context(self) -> ssl.SSLContext:
        """Build SSL context, optionally trusting self-signed certs."""
        ctx = ssl.create_default_context()
        if self._trust_self_signed:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _connect_imap(self) -> imaplib.IMAP4:
        """Connect to IMAP server with appropriate SSL/STARTTLS mode."""
        ctx = self._get_ssl_context()
        if self._use_ssl:
            # Implicit TLS (port 993 typically)
            return imaplib.IMAP4_SSL(self._host, self._port, ssl_context=ctx)
        else:
            # Plain connection + STARTTLS (port 1143 for Proton Bridge)
            conn = imaplib.IMAP4(self._host, self._port)
            conn.starttls(ssl_context=ctx)
            return conn

    def _fetch_unseen(self) -> list[tuple[bytes, bytes]]:
        """Synchronous IMAP fetch of unseen messages.

        Returns list of (message_number, raw_email_bytes).
        """
        conn = None
        try:
            conn = self._connect_imap()

            conn.login(self._user, self._password)
            conn.select("INBOX")

            status, data = conn.search(None, "UNSEEN")
            if status != "OK" or not data[0]:
                return []

            msg_nums = data[0].split()
            results = []

            for num in msg_nums:
                try:
                    status, msg_data = conn.fetch(num, "(BODY.PEEK[])")
                    if status != "OK" or not msg_data[0]:
                        continue
                    raw = msg_data[0][1]
                    if isinstance(raw, str):
                        raw = raw.encode("utf-8")
                    results.append((num, raw))
                except Exception as e:
                    logger.warning(f"Failed to fetch IMAP message {num}: {e}")

            return results

        except imaplib.IMAP4.error as e:
            logger.error(f"IMAP connection error: {e}")
            return []
        finally:
            if conn:
                try:
                    conn.close()
                    conn.logout()
                except Exception:
                    pass

    def _update_flags(
        self,
        mark_seen: list[bytes],
        mark_unseen: list[bytes],
    ) -> None:
        """Update IMAP flags for processed messages."""
        conn = None
        try:
            conn = self._connect_imap()
            conn.login(self._user, self._password)
            conn.select("INBOX")

            for num in mark_seen:
                conn.store(num, "+FLAGS", "\\Seen")
            for num in mark_unseen:
                # Ensure unroutable emails stay unseen for manual review
                conn.store(num, "-FLAGS", "\\Seen")

        except imaplib.IMAP4.error as e:
            logger.error(f"IMAP flag update error: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                    conn.logout()
                except Exception:
                    pass

    async def _process_email(self, raw_email: bytes) -> str:
        """Parse one email, extract reply, route to job/thread.

        Returns ``"routed"``, ``"duplicate"``, or ``"skipped"``.
        """
        msg = email_lib.message_from_bytes(raw_email)

        # Check email_message_id for dedup
        email_msg_id = msg.get("Message-ID", "")
        if email_msg_id and await self._db.message_exists_by_email_id(email_msg_id):
            return "duplicate"

        # Parse + sub-address from recipient headers
        routing = None
        for header in ("Delivered-To", "X-Original-To", "To"):
            value = msg.get(header, "")
            if not value:
                continue
            # Handle multi-address To: headers
            for addr in value.split(","):
                addr = addr.strip()
                # Extract email from "Name <email>" format
                if "<" in addr:
                    addr = addr.split("<")[1].rstrip(">")
                routing = self._parse_plus_address(addr)
                if routing:
                    break
            if routing:
                break

        if not routing:
            # Fallback: resolve via In-Reply-To header → message_log lookup.
            # Some email clients (e.g. Proton Mail) strip + sub-addressing
            # but preserve the In-Reply-To header referencing our Message-ID.
            routing = await self._resolve_via_in_reply_to(msg)

        if not routing:
            return "skipped"

        job_short_id, thread_id = routing

        # Look up the job
        job = await self._db.get_job_by_short_id(job_short_id)
        if not job:
            logger.warning(
                f"IMAP reply: no job found for short ID '{job_short_id}' "
                f"(email {email_msg_id})"
            )
            return "skipped"

        job_id = str(job["id"])

        # Extract sender email
        sender = msg.get("From", "")
        if "<" in sender:
            sender = sender.split("<")[1].rstrip(">")

        # Extract and clean the reply body
        body = self._extract_text_body(msg)
        if not body:
            logger.warning(f"IMAP reply: empty body in email {email_msg_id}")
            return "skipped"

        cleaned_body = parse_reply(body)
        if not cleaned_body:
            logger.warning(f"IMAP reply: body empty after stripping for {email_msg_id}")
            cleaned_body = body.strip()

        # Atomically claim this email before routing. The early
        # message_exists_by_email_id() check above is a fast path; this
        # insert-as-claim is the authority. Without it a concurrent poller in
        # the transient dual-leader window (leader election has no fencing)
        # could inject the same reply into the job twice. We claim only here,
        # once committed to routing, so non-routable ("skipped") emails are not
        # marked processed and stay eligible for a later retry.
        if email_msg_id and not await self._db.claim_inbound_email(email_msg_id):
            return "duplicate"

        # Route via the reply handler
        strategy = await self._reply_handler(
            job_id,
            thread_id,
            cleaned_body,
            sender,
            email_msg_id,
        )
        logger.info(
            f"IMAP reply routed: job={job_id[:8]}, thread={thread_id}, "
            f"sender={sender}, strategy={strategy}"
        )
        return "routed"

    async def _resolve_via_in_reply_to(
        self, msg: email_lib.message.Message
    ) -> tuple[str, str] | None:
        """Resolve job/thread from In-Reply-To header via message_log lookup.

        Fallback when + sub-addressing is stripped by the email client.
        The In-Reply-To header references the Message-ID of our outbound email,
        which is stored in message_log.email_message_id.

        Returns:
            (job_short_id, thread_id) or None.
        """
        in_reply_to = msg.get("In-Reply-To", "").strip()
        if not in_reply_to:
            return None

        record = await self._db.resolve_message_by_email_id(in_reply_to)
        if not record:
            # Also try References header (some clients put it there)
            references = msg.get("References", "")
            for ref in references.split():
                ref = ref.strip()
                if ref:
                    record = await self._db.resolve_message_by_email_id(ref)
                    if record:
                        break

        if not record:
            return None

        job_id = record["job_id"]
        thread_id = record["thread_id"]
        # Return job_short_id (first 8 chars) for consistency with _parse_plus_address
        return (job_id[:8], thread_id)

    def _parse_plus_address(self, address: str) -> tuple[str, str] | None:
        """Parse ``agent+{job_short}+{thread_id}@domain`` into (job_short, thread_id).

        Returns None if the address doesn't match the expected format.
        """
        if not address or "@" not in address:
            return None

        local = address.split("@")[0]
        parts = local.split("+")

        # Expected: [agent_local, job_short_id, thread_id]
        if len(parts) != 3:
            return None

        # Verify the agent local part matches
        expected_local = self._agent_email.split("@")[0] if self._agent_email else ""
        if expected_local and parts[0] != expected_local:
            return None

        job_short_id = parts[1]
        thread_id = parts[2]

        # Basic validation
        if not job_short_id or not thread_id:
            return None

        return (job_short_id, thread_id)

    @staticmethod
    def _extract_text_body(msg: email_lib.message.Message) -> str:
        """Walk MIME parts and return the body as plain text.

        Prefers text/plain. Falls back to text/html with tag stripping
        when no plain text part exists (e.g. Proton Mail HTML-only replies).
        """
        plain = ""
        html = ""

        parts = msg.walk() if msg.is_multipart() else [msg]
        for part in parts:
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in disposition:
                continue

            payload = part.get_payload(decode=True)
            if not payload:
                continue

            charset = part.get_content_charset() or "utf-8"
            try:
                text = payload.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                text = payload.decode("utf-8", errors="replace")

            if content_type == "text/plain" and not plain:
                plain = text
            elif content_type == "text/html" and not html:
                html = text

        if plain:
            return plain

        # Fallback: convert HTML to plain text
        if html:
            return ImapPoller._html_to_text(html)

        return ""

    @staticmethod
    def _html_to_text(html: str) -> str:
        """Basic HTML to plain text conversion."""
        import re

        # Remove blockquote content (quoted original message)
        text = re.sub(
            r"<blockquote[^>]*>.*?</blockquote>",
            "",
            text := html,
            flags=re.DOTALL | re.IGNORECASE,
        )
        # Remove protonmail_quote div (Proton Mail quoted text)
        text = re.sub(
            r'<div[^>]*class="protonmail_quote"[^>]*>.*?</div>\s*$',
            "",
            text,
            flags=re.DOTALL | re.IGNORECASE,
        )
        # Remove signature blocks
        text = re.sub(
            r'<div[^>]*class="[^"]*signature[^"]*"[^>]*>.*?</div>',
            "",
            text,
            flags=re.DOTALL | re.IGNORECASE,
        )
        # Replace <br> and </div><div> with newlines
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</div>\s*<div", "\n<div", text, flags=re.IGNORECASE)
        # Strip remaining HTML tags
        text = re.sub(r"<[^>]+>", "", text)
        # Decode common HTML entities
        text = text.replace("&nbsp;", " ").replace("&amp;", "&")
        text = text.replace("&lt;", "<").replace("&gt;", ">")
        text = text.replace("&quot;", '"')
        # Collapse whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

        return ""

    @staticmethod
    def _decode_header_value(value: str) -> str:
        """Decode RFC2047 encoded header values."""
        if not value:
            return ""
        decoded_parts = decode_header(value)
        result = []
        for part, charset in decoded_parts:
            if isinstance(part, bytes):
                result.append(part.decode(charset or "utf-8", errors="replace"))
            else:
                result.append(part)
        return "".join(result)


# Module-level singleton
imap_poller = ImapPoller()


async def imap_poll_loop(shutdown_event: asyncio.Event, *, poller: ImapPoller) -> None:
    """Background task that polls IMAP for inbound email replies.

    Runs every IMAP_POLL_INTERVAL seconds (default: 30).
    Gracefully disabled when IMAP is not configured.
    """
    if not poller.is_available:
        logger.info("IMAP poller not started (not configured)")
        # Park until shutdown instead of returning: run_when_leader re-invokes a
        # loop coroutine that returns (recreating the task every poll_seconds),
        # so a bare return here re-logs this line ~once per second on the leader.
        await shutdown_event.wait()
        return

    logger.info("IMAP poller started (interval=%ds)", poller.poll_interval)
    while not shutdown_event.is_set():
        try:
            count = await poller.poll_once()
            if count > 0:
                logger.info("IMAP poller: processed %d email reply(ies)", count)
        except Exception as e:
            logger.error("IMAP poller error: %s", e)

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=poller.poll_interval,
            )
            break
        except asyncio.TimeoutError:
            pass

    logger.info("IMAP poller stopped")
