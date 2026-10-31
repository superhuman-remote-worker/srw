"""
Citation Engine - Core Implementation
=====================================
The main CitationEngine class that provides citation functionality
for AI agents.

Native Superhuman-Remote-Worker subsystem (see
``knowledge-history/done/citation_engine_integration.md``). The engine is **async-native**
and runs exclusively against SRW's managed vector store (``srw_vector``) through
the shared asyncpg pool (``agent.database.postgres_db.PostgresDB``). It owns no
database connection of its own and has no SQLite / standalone mode — the host
application's ``orchestrator/database/migrations/vector/`` owns the schema.

The engine collapsed the DB + embedding + verification stacks onto SRW:
- Data access is ``async`` on the injected vector pool (no ``psycopg2``).
- Chunk embeddings use SRW's async embedding service (``get_embedding_service``),
  matching the host ``source_embeddings.embedding vector(4096)`` column.
- Verification runs on SRW's **auxiliary-LLM service** (Phase 2): ``cite_*``
  writes the citation ``pending`` and returns immediately; a background
  ``VerifyCitationTask`` writes the verdict back (eventually-consistent). The
  engine owns no LLM client or credentials.

Based on the Citation & Provenance Engine Design Document v0.3.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin

from shared.runtime.citation_engine.models import (
    Annotation,
    AnnotationType,
    Citation,
    CitationContext,
    CitationResult,
    Confidence,
    ExtractionMethod,
    Locator,
    Metadata,
    SearchResults,
    Source,
    SourceType,
    VerificationReason,
    VerificationResult,
    VerificationStatus,
)
from shared.citation_handles import format_handle
from shared.url_safety import UnsafeURLError, check_public_http_url

# Set up logging
log = logging.getLogger(__name__)

#: Redirect hops a web-source fetch follows, each re-checked by address.
MAX_WEB_FETCH_REDIRECTS = 5

#: Quotes shorter than this (letters and digits only) are too short for the
#: instant existence check to say anything; they go to the verifier as they are.
MIN_CHECKABLE_QUOTE_CHARS = 12

#: An ellipsis in a quote: the fragments around it are checked in order.
_ELLIPSIS_RE = re.compile(r"\s*(?:\[\s*(?:\.\s*){3}\]|\[\s*…\s*\]|(?:\.\s*){3}|…)\s*")

#: Characters a quote may differ in without changing what it says: punctuation,
#: spacing, line breaks and hyphenation are skipped by the existence check.
_SKIPPABLE = r"[\W_]*"

#: Markup a scraped page carries between the words a reader sees: images,
#: Markdown links (kept as their text; URLs and titles dropped), HTML tags, and
#: reference markers such as Wikipedia's ``[[9]](#cite_note-9)`` → ``[9]``.
_MD_IMAGE_RE = re.compile(r"!\[[^\]]*\]\((?:[^()]|\([^)]*\))*\)")
_MD_LINK_RE = re.compile(
    r"\[([^\[\]]*(?:\[[^\]]*\][^\[\]]*)*)\]\((?:[^()\s]|\([^)]*\))*(?:\s+\"[^\"]*\")?\)"
)
_HTML_TAG_RE = re.compile(r"<[^>\n]{1,200}>")
_REFERENCE_MARK_RE = re.compile(
    r"\[\s*(?:\d{1,4}|[a-z]|(?:note|nb|n)\s*\d{1,4}|citation needed|"
    r"clarification needed|when\?|who\?|which\?|by whom\?)\s*\]",
    re.IGNORECASE,
)


def readable_text(text: str) -> str:
    """``text`` as a reader sees it, for comparing a quote with its source."""
    text = _MD_IMAGE_RE.sub(" ", text)
    # Twice: a link's text may itself be a link (``[[9]](#cite_note-9)``).
    text = _MD_LINK_RE.sub(r"\1", text)
    text = _MD_LINK_RE.sub(r"\1", text)
    text = _HTML_TAG_RE.sub(" ", text)
    return _REFERENCE_MARK_RE.sub(" ", text)


#: Popover and wire fields are capped so one long passage can't bloat a frame.
_WIRE_TEXT_MAX = 500


def find_quote_in_source(quote: str, content: str) -> str | None:
    """Find ``quote`` in ``content`` the way a reader would, or return None.

    The instant half of verification (overhaul §6.3): before any LLM looks at
    a ``direct_quote`` citation, check that the quoted words exist at all.
    Letters and digits must match in order, ignoring case; everything else
    (spacing, line breaks, punctuation, curly vs straight quotes, a hyphen
    split across a PDF line) may differ. An ellipsis splits the quote into
    fragments that must appear in that order. Both sides are NFKC-normalised
    first, so ligatures and full-width forms compare equal, and both are read
    as text (``readable_text``): a scraped page's links, footnote markers and
    tags are not words of the quote.

    Returns the matching passage of ``content`` (NFKC-normalised), or None
    when the words are not there. A quote with fewer than
    ``MIN_CHECKABLE_QUOTE_CHARS`` letters and digits is returned unchanged:
    too short to check, it is left to the verifier.
    """
    quote = readable_text(unicodedata.normalize("NFKC", quote or ""))
    if len(re.sub(r"[\W_]+", "", quote)) < MIN_CHECKABLE_QUOTE_CHARS:
        return quote
    text = readable_text(unicodedata.normalize("NFKC", content or ""))
    start = 0
    first: int | None = None
    end = 0
    for fragment in _ELLIPSIS_RE.split(quote):
        chars = re.sub(r"[\W_]+", "", fragment)
        if not chars:
            continue
        pattern = re.compile(
            _SKIPPABLE.join(re.escape(c) for c in chars), re.IGNORECASE
        )
        match = pattern.search(text, start)
        if match is None:
            return None
        if first is None:
            first = match.start()
        start = end = match.end()
    if first is None:
        return quote
    return text[first:end]


def _clip(value: str | None, limit: int = _WIRE_TEXT_MAX) -> str | None:
    if value is None:
        return None
    return value if len(value) <= limit else value[:limit].rstrip() + "…"


def _get_public_url(url: str, *, headers: dict[str, str], timeout: float):
    """GET ``url`` from the agent pod, following redirects only to public hosts.

    Every hop (the first URL and each ``Location``) passes
    :func:`check_public_http_url` before it is sent: http(s) only, and the
    host must be, or resolve only to, public addresses. A model-supplied URL
    can otherwise make the pod fetch a cluster service, its own pod or cloud
    metadata, and the body becomes readable through ``search_library``.

    The check reads the URL ``requests`` prepared, which is the one it sends
    (its adapter dials that URL's host): requests' own parse decides the host
    (``http://10.0.0.1\\@public.example/`` dials 10.0.0.1, ``%31%30.0.0.1`` is
    10.0.0.1), not the spelling the model wrote. Otherwise this is what
    ``requests.get`` does, environment proxies included.

    Raises:
        UnsafeURLError: a hop was refused.
        requests.TooManyRedirects: more than ``MAX_WEB_FETCH_REDIRECTS`` hops.

    DNS rebinding: ``requests`` resolves the name again when it connects, so
    a name that answers public to the check and private to the connect gets
    through. Pinning the connection to the checked address (name kept for
    Host and TLS, as ``shared.oci_registry`` does with httpx) would close it.
    """
    import requests

    current = url
    with requests.Session() as session:
        for _hop in range(MAX_WEB_FETCH_REDIRECTS + 1):
            prepared = session.prepare_request(
                requests.Request("GET", current, headers=headers)
            )
            check_public_http_url(prepared.url)
            settings = session.merge_environment_settings(
                prepared.url, {}, None, None, None
            )
            response = session.send(
                prepared, timeout=timeout, allow_redirects=False, **settings
            )
            if not response.is_redirect:
                return response
            location = response.headers["location"]
            response.close()
            current = urljoin(prepared.url, location)
    raise requests.TooManyRedirects(
        f"more than {MAX_WEB_FETCH_REDIRECTS} redirects from {url}"
    )


class CitationEngine:
    """
    Citation & Provenance Engine for AI agents (SRW-native, async).

    The engine always runs inside Superhuman-Remote-Worker against the shared
    vector store. It is constructed with an SRW ``PostgresDB`` pool bound to that
    store; it never opens its own connection.

    Usage::

        engine = CitationEngine(db=agent.vector_conn, context=ctx)
        source = await engine.add_doc_source("./document.pdf", name="My Doc")
        result = await engine.cite_doc(
            claim="The regulation requires X",
            source_id=source.id,
            quote_context="Full paragraph...",
            locator={"page": 24},
        )

    Verification runs on SRW's auxiliary-LLM service: ``cite_*`` returns
    ``pending`` and a background ``VerifyCitationTask`` (on ``verify_aux``)
    writes the verdict back. The engine owns no LLM client or credentials.
    """

    def __init__(
        self,
        db: Any,
        context: CitationContext | None = None,
        verify_aux: Any | None = None,
        verify_prompt: str | None = None,
        on_event: Callable[[str, dict[str, Any]], None] | None = None,
    ):
        """
        Initialize the Citation Engine.

        Args:
            db: An SRW ``PostgresDB`` instance bound to the vector store
                (``srw_vector``). The engine performs all I/O through this
                shared async pool and never opens its own connection.
            context: Optional citation context for audit trails. ``session_id``
                is mapped to the citation ``job_id``.
            verify_aux: Optional SRW ``AuxiliaryLLM`` (the citation-verification
                model — a dedicated ``CITATION_LLM`` model or the auxiliary-model
                fallback). When set, ``cite_*`` schedules background verification;
                when ``None``, citations are created ``pending`` and left
                unverified (verification disabled / unavailable).
            verify_prompt: The matrix-resolved citation-verification system
                prompt handed to ``VerifyCitationTask``.
            on_event: Optional ``(method, params)`` listener for live citation
                changes: ``citation.created`` when a cite tool writes a row
                (before the tool returns, so the chip can render as soon as
                the agent writes its handle), ``citation.updated`` after an
                edit or a withdrawal, and ``citation.verdict`` when a check
                lands. ``params`` carries ``citation_id``,
                ``verification_status`` and the full wire row under
                ``citation`` (see ``wire_row``). Persistent sessions wire this
                to their WS/SSE broadcast; worker jobs leave it ``None``.
        """
        log.debug("Initializing CitationEngine (SRW-native async)...")

        if db is None:
            raise ValueError(
                "CitationEngine requires an SRW vector PostgresDB pool. "
                "No vector store is configured (VECTOR_POSTGRES_* unset)."
            )

        self.db = db
        self.context = context

        # Verification runs on SRW's auxiliary-LLM service (Phase 2). The engine
        # holds no LLM client; cite_* schedules a background VerifyCitationTask.
        self._verify_aux = verify_aux
        self._verify_prompt = verify_prompt or ""
        #: Optional live citation listener (persistent sessions only). See the
        #: ``on_event`` arg.
        self._on_event = on_event
        #: In-flight verification tasks — kept referenced so the event loop
        #: doesn't GC them mid-flight; discarded on completion.
        self._verify_tasks: set[asyncio.Task] = set()
        self._verification_closed = False

        # Embedding service (SRW singleton, lazy) + chunker
        self._embedding_service = None
        self._chunker = None

        log.info(
            "CitationEngine initialized (async, vector pool); verification=%s",
            "aux" if verify_aux is not None else "disabled",
        )

    # =========================================================================
    # CONTEXT / HELPERS
    # =========================================================================

    def _job_uuid(self) -> uuid.UUID | None:
        """Resolve the current job_id (context.session_id) as a UUID.

        The host citation tables key on ``job_id UUID``; asyncpg is strict about
        parameter types (unlike psycopg2's text protocol), so the string session
        id must be coerced to ``uuid.UUID`` before it reaches a query.
        """
        jid = self.context.session_id if self.context else None
        return self._as_uuid(jid)

    @staticmethod
    def _as_uuid(value: Any) -> uuid.UUID | None:
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except (ValueError, TypeError, AttributeError):
            log.warning(f"job_id '{value}' is not a valid UUID; skipping job scope")
            return None

    @staticmethod
    def _loads(value: Any) -> Any:
        """Decode a JSONB column value (asyncpg returns jsonb as text)."""
        if isinstance(value, (str, bytes)):
            return json.loads(value)
        return value

    # =========================================================================
    # SOURCE REGISTRATION METHODS
    # =========================================================================

    async def add_doc_source(
        self,
        file_path: str,
        name: str | None = None,
        version: str | None = None,
        metadata: Metadata | None = None,
        identifier: str | None = None,
    ) -> Source:
        """
        Register a document source (PDF, markdown, txt, json, csv, images, etc.).

        Extracts text content (pypdf for PDFs) and stores it for verification.
        ``identifier`` names the document as the reader knows it (its
        workspace path); ``file_path`` may be a temporary local copy that is
        gone once the call returns, so it is only the fallback (D2).

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file type is not supported
        """
        file_path = str(Path(file_path).resolve())
        log.debug(f"Registering document source: {file_path}")

        if not os.path.exists(file_path):
            log.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        # Extraction can be CPU/IO heavy (PDF parsing) — keep the loop responsive.
        content = await asyncio.to_thread(self._extract_document_content, file_path)
        log.debug(f"Extracted {len(content)} characters from document")

        content_hash = hashlib.sha256(content.encode()).hexdigest()

        if name is None:
            name = os.path.basename(file_path)

        source = await self._register_source(
            source_type=SourceType.DOCUMENT,
            identifier=identifier or file_path,
            name=name,
            content=content,
            content_hash=content_hash,
            version=version,
            metadata=metadata,
        )

        log.info(f"Registered document source [{source.id}]: {name}")
        return source

    async def add_web_source(
        self,
        url: str,
        name: str | None = None,
        version: str | None = None,
        metadata: Metadata | None = None,
        content: str | None = None,
    ) -> Source:
        """
        Register a website source.

        When ``content`` is supplied, archive those provider-returned bytes and
        perform no network request. Otherwise, download the page at registration
        time for legacy direct-citation callers.

        Raises:
            UnsafeURLError: the page would be downloaded and the URL (or a
                redirect from it) is not a public http(s) address.
        """
        log.debug(f"Registering web source: {url}")

        if content is None:
            content, fetch_metadata = await asyncio.to_thread(
                self._fetch_web_content, url
            )
            log.debug(f"Fetched {len(content)} characters from web page")
        else:
            fetch_metadata = {
                "url": url,
                "accessed_at": datetime.now(timezone.utc).isoformat(),
                "status_code": None,
                "content_type": "text/plain",
                "title": name,
                "content_source": "provider",
            }
            log.debug(
                "Using %d characters supplied by the web provider for %s",
                len(content),
                url,
            )

        content_hash = hashlib.sha256(content.encode()).hexdigest()

        if metadata:
            fetch_metadata.update(metadata)

        source = await self._register_source(
            source_type=SourceType.WEBSITE,
            identifier=url,
            name=name or url,
            content=content,
            content_hash=content_hash,
            version=version,
            metadata=fetch_metadata,
        )

        log.info(f"Registered web source [{source.id}]: {name or url}")
        return source

    async def add_custom_source(
        self,
        name: str,
        content: str,
        description: str | None = None,
        metadata: Metadata | None = None,
    ) -> Source:
        """Register a custom/AI-generated source (matrices, plots, analyses)."""
        log.debug(f"Registering custom source: {name}")

        custom_metadata = {"description": description}
        if metadata:
            custom_metadata.update(metadata)

        content_hash = hashlib.sha256(content.encode()).hexdigest()

        timestamp = datetime.now(timezone.utc).isoformat()
        identifier = f"custom:{name}:{timestamp}"

        source = await self._register_source(
            source_type=SourceType.CUSTOM,
            identifier=identifier,
            name=name,
            content=content,
            content_hash=content_hash,
            metadata=custom_metadata,
        )

        log.info(f"Registered custom source [{source.id}]: {name}")
        return source

    async def _register_source(
        self,
        source_type: SourceType,
        identifier: str,
        name: str,
        content: str,
        content_hash: str | None = None,
        version: str | None = None,
        metadata: Metadata | None = None,
    ) -> Source:
        """
        Register a source in the database.

        Uses content_hash for deduplication: if a source with the same hash
        already exists, reuses it and creates a job_sources link only. The
        name, identifier and metadata this work unit registered it under go
        on that link, so two jobs citing the same bytes each keep their own
        file name and cloud anchor, and neither sees the other's (WP5, D1).
        """
        metadata_json = json.dumps(metadata) if metadata else None
        job_uuid = self._job_uuid()

        existing_source = None
        if content_hash:
            existing_source = await self._find_source_by_hash(content_hash)

        if existing_source:
            source_id = existing_source.id
            log.info(f"Source with content_hash already exists [{source_id}], reusing")
        else:
            source_id = await self.db.fetchval(
                """
                INSERT INTO sources (type, identifier, name, version, content, content_hash, metadata)
                VALUES ($1::source_type, $2, $3, $4, $5, $6, $7::jsonb)
                RETURNING id
                """,
                source_type.value,
                identifier,
                name,
                version,
                content,
                content_hash,
                metadata_json,
            )

        if job_uuid:
            await self._link_source_to_job(
                source_id,
                job_uuid,
                name=name,
                identifier=identifier,
                metadata_json=metadata_json,
            )

        source = Source(
            id=source_id,
            type=source_type,
            identifier=identifier,
            name=name,
            version=version,
            content=content,
            content_hash=content_hash,
            metadata=metadata,
            created_at=datetime.now(timezone.utc),
        )

        # Auto-embed if an embedding service is available (best-effort).
        if job_uuid:
            await self._auto_embed_source(source_id, content, job_uuid)

        return source

    async def _find_source_by_hash(self, content_hash: str) -> Source | None:
        """Find an existing source by content_hash."""
        row = await self.db.fetchrow(
            "SELECT * FROM sources WHERE content_hash = $1", content_hash
        )
        if row:
            return self._row_to_source(dict(row))
        return None

    async def _link_source_to_job(
        self,
        source_id: int,
        job_uuid: uuid.UUID,
        *,
        name: str | None = None,
        identifier: str | None = None,
        metadata_json: str | None = None,
    ) -> None:
        """Create or refresh a job_sources link with this unit's view of the source."""
        await self.db.execute(
            """
            INSERT INTO job_sources (job_id, source_id, name, identifier, metadata)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            ON CONFLICT (job_id, source_id) DO UPDATE
               SET name = EXCLUDED.name,
                   identifier = EXCLUDED.identifier,
                   metadata = EXCLUDED.metadata
            """,
            job_uuid,
            source_id,
            name,
            identifier,
            metadata_json,
        )
        log.debug(f"Linked source [{source_id}] to job {job_uuid}")

    def _extract_document_content(self, file_path: str) -> str:
        """
        Extract text content from a document file.

        Supports PDF (pypdf), DOCX (docx2txt), and text formats. Runs in a
        worker thread (sync I/O).
        """
        ext = Path(file_path).suffix.lower()
        log.debug(f"Extracting content from {ext} file")

        if ext == ".pdf":
            return self._extract_pdf_content(file_path)
        elif ext == ".docx":
            return self._extract_docx_content(file_path)
        elif ext in (".md", ".txt", ".json", ".csv", ".xml", ".html", ".htm"):
            with open(file_path, encoding="utf-8") as f:
                content = f.read()
            log.debug(f"Read {len(content)} characters from text file")
            return content
        else:
            try:
                with open(file_path, encoding="utf-8") as f:
                    content = f.read()
                log.debug(
                    f"Read {len(content)} characters from file (unknown extension)"
                )
                return content
            except UnicodeDecodeError as e:
                log.error(f"Cannot extract text from binary file: {file_path}")
                raise ValueError(
                    f"Cannot extract text from binary file: {file_path}. "
                    "Supported formats: PDF, DOCX, markdown, txt, json, csv, xml, html."
                ) from e

    def _extract_pdf_content(self, file_path: str) -> str:
        """Extract text content from a PDF file using pypdf, with page markers.

        pypdf (BSD-3-Clause) replaced PyMuPDF here: PyMuPDF is AGPL-3.0-or-
        Artifex-commercial, which `scripts/check_licenses.py` denies for the
        bundled set — it may not ship inside an FSL-1.1-ALv2 product.

        pypdf was chosen over `pdfplumber` (the other permissive extractor
        already pinned) on measured extraction quality: across this repo's PDF
        corpus pypdf's default mode reproduces PyMuPDF's paragraph text to
        within ~0.5% of its word count, whereas pdfplumber at its defaults
        silently deletes every inter-word space on browser-exported PDFs
        (recoverable only by hand-tuning `x_tolerance_ratio` per document).
        Space-collapsed text is the worst possible failure mode here: the
        extraction still "succeeds" and returns a plausible length, but every
        downstream quote/claim verification against it then fails.
        """
        try:
            from pypdf import PdfReader
        except ImportError as e:
            log.error("pypdf not installed, cannot extract PDF content")
            raise ImportError(
                "pypdf is required for PDF extraction. Install it with: pip install pypdf"
            ) from e

        log.debug(f"Opening PDF with pypdf: {file_path}")
        reader = PdfReader(file_path)

        # Permission-only encryption with an empty user password is common on
        # published papers; PyMuPDF opened those transparently, pypdf needs to
        # be told. A genuinely password-protected file still fails on read.
        if reader.is_encrypted:
            try:
                reader.decrypt("")
            except Exception as e:
                log.warning(f"Could not decrypt PDF {file_path}: {e}")

        text_parts = []
        for page_num, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            text_parts.append(f"--- Page {page_num} ---\n{text}")

        content = "\n\n".join(text_parts)
        log.debug(
            f"Extracted {len(content)} characters from {len(text_parts)} PDF pages"
        )
        return content

    def _extract_docx_content(self, file_path: str) -> str:
        """Extract text content from a DOCX file using docx2txt (with fallback)."""
        try:
            import docx2txt

            log.debug(f"Extracting content from DOCX: {file_path}")
            try:
                content = docx2txt.process(file_path)
            except KeyError:
                # docx2txt hard-codes 'word/document.xml'; some files use
                # 'word/document2.xml' or similar. Fall back to direct extraction.
                content = self._extract_docx_fallback(file_path)
            log.debug(f"Extracted {len(content)} characters from DOCX")
            return content

        except ImportError as e:
            log.error("docx2txt not installed, cannot extract DOCX content")
            raise ImportError(
                "docx2txt is required for DOCX extraction. Install it with: pip install docx2txt"
            ) from e

    @staticmethod
    def _extract_docx_fallback(file_path: str) -> str:
        """Extract text from a DOCX whose main document entry is non-standard."""
        import re
        import zipfile
        from xml.etree.ElementTree import XML

        log.debug(f"Falling back to direct XML extraction for: {file_path}")
        nsmap = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
        text_parts: list[str] = []

        with zipfile.ZipFile(file_path) as zf:
            doc_entries = sorted(
                n for n in zf.namelist() if re.match(r"word/document\d*\.xml$", n)
            )
            if not doc_entries:
                raise FileNotFoundError(f"No word/document*.xml found in {file_path}")

            for entry in doc_entries:
                tree = XML(zf.read(entry))
                for para in tree.iter(f"{nsmap}p"):
                    texts = [node.text for node in para.iter(f"{nsmap}t") if node.text]
                    if texts:
                        text_parts.append("".join(texts))

        return "\n\n".join(text_parts)

    def _fetch_web_content(self, url: str) -> tuple[str, dict[str, Any]]:
        """
        Fetch and archive web page content (runs in a worker thread).

        Returns (extracted_text, metadata). On fetch failure, returns a
        placeholder + metadata so the source still registers.

        Raises:
            UnsafeURLError: the URL, or a redirect it sent, is not a public
                http(s) address. Nothing is registered, so the caller sees
                the refusal instead of a placeholder source.
        """
        try:
            import requests
            from bs4 import BeautifulSoup

            log.debug(f"Fetching web content from: {url}")
            headers = {
                "User-Agent": "Mozilla/5.0 (compatible; CitationEngine/1.0)",
            }
            response = _get_public_url(url, headers=headers, timeout=30)
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")

            if "application/pdf" in content_type or url.lower().endswith(".pdf"):
                import tempfile

                log.debug("Detected PDF content, extracting text with pypdf")
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
                    tmp.write(response.content)
                    tmp.flush()
                    text = self._extract_pdf_content(tmp.name)

                metadata = {
                    "url": url,
                    "accessed_at": datetime.now(timezone.utc).isoformat(),
                    "status_code": response.status_code,
                    "content_type": content_type,
                    "title": None,
                }
                log.debug(f"Extracted {len(text)} characters from PDF")
                return text, metadata

            soup = BeautifulSoup(response.text, "html.parser")

            for script in soup(["script", "style"]):
                script.decompose()

            text = soup.get_text(separator="\n", strip=True)
            text = text.replace("\x00", "")

            metadata = {
                "url": url,
                "accessed_at": datetime.now(timezone.utc).isoformat(),
                "status_code": response.status_code,
                "content_type": content_type,
                "title": soup.title.string if soup.title else None,
            }
            log.debug(
                f"Fetched {len(text)} characters, title: {metadata.get('title', 'N/A')}"
            )
            return text, metadata

        except ImportError as e:
            log.error("requests or beautifulsoup4 not installed")
            raise ImportError(
                "requests and beautifulsoup4 are required for web fetching. "
                "Install with: pip install requests beautifulsoup4"
            ) from e
        except UnsafeURLError as e:
            log.warning(f"Refused to fetch web source {url}: {e}")
            raise
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            log.warning(
                f"HTTP {status_code} for {url} - registering with metadata only"
            )
            placeholder = (
                f"[CONTENT NOT ACCESSIBLE] This URL returned HTTP {status_code}. "
                f"The content could not be fetched automatically. "
                f"Use the browser tool to manually download the content from: {url}"
            )
            metadata = {
                "url": url,
                "accessed_at": datetime.now(timezone.utc).isoformat(),
                "status_code": status_code,
                "content_type": None,
                "title": None,
                "fetch_error": str(e),
            }
            return placeholder, metadata
        except Exception as e:
            log.warning(
                f"Failed to fetch URL {url}: {e} - registering with metadata only"
            )
            placeholder = (
                f"[CONTENT NOT ACCESSIBLE] Failed to fetch this URL: {e}. "
                f"Use the browser tool to manually download the content from: {url}"
            )
            metadata = {
                "url": url,
                "accessed_at": datetime.now(timezone.utc).isoformat(),
                "status_code": None,
                "content_type": None,
                "title": None,
                "fetch_error": str(e),
            }
            return placeholder, metadata

    # =========================================================================
    # CITATION METHODS
    # =========================================================================

    async def cite_doc(
        self,
        claim: str,
        source_id: int,
        quote_context: str,
        locator: Locator,
        verbatim_quote: str | None = None,
        relevance_reasoning: str | None = None,
        confidence: str = "high",
        extraction_method: str = "direct_quote",
    ) -> CitationResult:
        """Create a citation from a document source (verified)."""
        log.debug(f"Creating document citation for source [{source_id}]")
        return await self._create_citation(
            claim=claim,
            source_id=source_id,
            quote_context=quote_context,
            locator=locator,
            verbatim_quote=verbatim_quote,
            relevance_reasoning=relevance_reasoning,
            confidence=confidence,
            extraction_method=extraction_method,
        )

    async def cite_web(
        self,
        claim: str,
        source_id: int,
        quote_context: str,
        locator: Locator,
        verbatim_quote: str | None = None,
        relevance_reasoning: str | None = None,
        confidence: str = "high",
        extraction_method: str = "direct_quote",
    ) -> CitationResult:
        """Create a citation from a website source (uses archived content)."""
        log.debug(f"Creating web citation for source [{source_id}]")
        return await self._create_citation(
            claim=claim,
            source_id=source_id,
            quote_context=quote_context,
            locator=locator,
            verbatim_quote=verbatim_quote,
            relevance_reasoning=relevance_reasoning,
            confidence=confidence,
            extraction_method=extraction_method,
        )

    async def cite_custom(
        self,
        claim: str,
        source_id: int,
        quote_context: str,
        locator: Locator | None = None,
        relevance_reasoning: str | None = None,
        confidence: str = "high",
    ) -> CitationResult:
        """Create a citation from a custom/AI-generated source."""
        log.debug(f"Creating custom citation for source [{source_id}]")
        return await self._create_citation(
            claim=claim,
            source_id=source_id,
            quote_context=quote_context,
            locator=locator or {},
            verbatim_quote=None,
            relevance_reasoning=relevance_reasoning,
            confidence=confidence,
            extraction_method="inference",
        )

    async def _create_citation(
        self,
        claim: str,
        source_id: int,
        quote_context: str,
        locator: Locator,
        verbatim_quote: str | None = None,
        relevance_reasoning: str | None = None,
        confidence: str = "high",
        extraction_method: str = "direct_quote",
        quote_language: str | None = None,
    ) -> CitationResult:
        """Create and verify a citation."""
        log.debug(f"Creating citation: claim='{claim[:50]}...', source_id={source_id}")

        source = await self.get_source(source_id)
        if source is None:
            log.error(f"Source not found: {source_id}")
            raise ValueError(f"Source not found: {source_id}")

        self._validate_confidence(confidence)
        self._validate_extraction_method(extraction_method)

        status, reason, notes, matched = self._first_check(source, verbatim_quote)

        job_uuid = self._job_uuid()
        created_by = None
        if self.context:
            created_by = (
                f"{self.context.session_id}:{self.context.agent_id}"
                if self.context.agent_id
                else self.context.session_id
            )

        locator_json = json.dumps(locator)

        citation_id = await self.db.fetchval(
            """
            INSERT INTO citations (
                job_id, claim, verbatim_quote, quote_context, quote_language,
                relevance_reasoning, confidence, extraction_method,
                source_id, locator, verification_status, created_by,
                verification_reason, verification_notes, matched_location
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7::confidence_level, $8::extraction_method,
                $9, $10::jsonb, $12::verification_status, $11, $13, $14, $15::jsonb
            )
            RETURNING id
            """,
            job_uuid,
            claim,
            verbatim_quote,
            quote_context,
            quote_language,
            relevance_reasoning,
            confidence,
            extraction_method,
            source_id,
            locator_json,
            created_by,
            status.value,
            reason.value if reason else None,
            notes,
            json.dumps(matched) if matched else None,
        )
        log.info(
            "Created citation [%s] for source [%s] (%s)",
            citation_id,
            source_id,
            status.value,
        )

        # Announce the row before the tool returns: the agent can only write
        # the handle after it has the tool result, so the cockpit's chip
        # resolves while the text streams (overhaul §6.2).
        await self._emit("citation.created", citation_id)

        # D2: a pending citation returns at once and is checked in the
        # background on the auxiliary model; the verdict is written back to the
        # row later. A quote the first check could not find is already failed
        # and is not sent to the LLM.
        if status is VerificationStatus.PENDING:
            self._schedule_verification(citation_id)

        source_snippet = (
            source.name[:30] + "..." if len(source.name) > 30 else source.name
        )
        return CitationResult(
            citation_id=citation_id,
            verification_status=status,
            summary_note=f"Cited {source_snippet} ({locator}) - {status.value}",
            verification_notes=notes,
            matched_location=matched,
        )

    def _first_check(
        self, source: Source, verbatim_quote: str | None
    ) -> tuple[VerificationStatus, VerificationReason | None, str | None, dict | None]:
        """The status a citation starts with: ``(status, reason, notes, matched)``.

        Existence before support (overhaul §6.3): a verbatim quote is looked
        up in the stored source text at once, without an LLM. Not found →
        ``failed`` with ``quote_not_found``, and no LLM check follows. Found or
        not a quote → ``pending`` for the LLM's support check, or, with no
        verifier wired, ``unverified`` with ``not_checked`` (never a pending
        state that nothing will end, C2).
        """
        matched = None
        if verbatim_quote and (source.content or "").strip():
            passage = find_quote_in_source(verbatim_quote, source.content)
            if passage is None:
                return (
                    VerificationStatus.FAILED,
                    VerificationReason.QUOTE_NOT_FOUND,
                    "The quoted words do not appear in the stored source text. "
                    "Quote the source exactly, or set extraction_method to "
                    "'paraphrase' if you reworded it.",
                    None,
                )
            matched = {"matched_text": _clip(passage), "method": "exact"}
        if self._verify_aux is None:
            notes = "Citation checking is turned off for this agent."
            if matched:
                notes = (
                    "The quote was found in the source; whether it supports the "
                    "claim was not checked (citation checking is turned off)."
                )
            return (
                VerificationStatus.UNVERIFIED,
                VerificationReason.NOT_CHECKED,
                notes,
                matched,
            )
        return VerificationStatus.PENDING, None, None, matched

    async def _emit(self, method: str, citation_id: int) -> None:
        """Send a live citation change to the session, if one listens.

        Best-effort: reads the row back so every frame carries the same shape
        as ``GET /threads/{id}/citations``, and never raises into the tool or
        the verifier.
        """
        if self._on_event is None:
            return
        try:
            citation = await self.get_citation(citation_id)
            if citation is None:
                return
            source = await self.get_source(citation.source_id)
            row = self.wire_row(citation, source)
            self._on_event(
                method,
                {
                    "citation_id": citation.id,
                    "verification_status": citation.verification_status.value,
                    "citation": row,
                },
            )
        except Exception as e:
            log.debug("%s event for citation [%s] failed: %s", method, citation_id, e)

    @staticmethod
    def wire_row(citation: Citation, source: Source | None) -> dict[str, Any]:
        """One citation as the cockpit reads it (list endpoint and live frames).

        Keep in step with ``GET /api/persistent/threads/{id}/citations``
        (``orchestrator/routers/thread_history.py``): the chat upserts frames
        into the same map the endpoint fills.
        """
        metadata = (source.metadata if source else None) or {}
        cloud = metadata.get("cloud") if isinstance(metadata, dict) else None
        cloud = cloud if isinstance(cloud, dict) else {}
        matched = citation.matched_location or {}
        created = citation.created_at
        return {
            "id": citation.id,
            "claim": _clip(citation.claim, 300),
            "quote": _clip(citation.verbatim_quote or citation.quote_context),
            "locator": citation.locator or {},
            "extraction_method": citation.extraction_method.value,
            "source_id": citation.source_id,
            "source_name": source.name if source else None,
            "source_type": source.type.value if source else None,
            "source_identifier": source.identifier if source else None,
            "verification_status": citation.verification_status.value,
            "verification_reason": citation.verification_reason,
            "verification_notes": _clip(citation.verification_notes),
            "matched_text": _clip(matched.get("matched_text"))
            if isinstance(matched, dict)
            else None,
            "confidence": citation.confidence.value,
            "created_at": created.isoformat() if created else None,
            "withdrawn_at": citation.withdrawn_at.isoformat()
            if citation.withdrawn_at
            else None,
            "withdrawn_reason": citation.withdrawn_reason,
            "has_cloud_anchor": bool(cloud),
            "has_snapshot": bool(cloud.get("snapshot_blob_key")),
        }

    def _schedule_verification(self, citation_id: int) -> None:
        """Schedule background verification for a freshly-created citation.

        Fire-and-forget on the running event loop (the agent's loop persists for
        its lifetime, so the verdict lands after ``cite_*`` returns). The task is
        held in ``_verify_tasks`` so it isn't GC'd mid-flight. No-op when no
        verifier is wired or there is no running loop (e.g. a sync context).
        """
        if self._verify_aux is None or self._verification_closed:
            return
        try:
            task = asyncio.create_task(self._run_verification(citation_id))
        except RuntimeError:
            log.debug("No running event loop; skipping citation verification schedule")
            return
        self._verify_tasks.add(task)
        task.add_done_callback(self._verify_tasks.discard)

    async def _run_verification(self, citation_id: int) -> None:
        """Run one citation's verification via the auxiliary-LLM service.

        ``verify_and_store_citation`` always ends the pending state: a verdict,
        or ``unverified`` with a reason when no check could run. The session
        hears about it either way.
        """
        from shared.runtime.services.auxiliary import verify_and_store_citation

        await verify_and_store_citation(
            self._verify_aux, self, citation_id, self._verify_prompt
        )
        await self._emit("citation.verdict", citation_id)

    async def await_pending_verifications(self, timeout: float = 30.0) -> None:
        """Await any in-flight verification tasks (phase boundary, delivery).

        Best-effort: returns when all scheduled verifications have written their
        verdict back, or after ``timeout`` seconds, whichever comes first.
        """
        pending = {t for t in self._verify_tasks if not t.done()}
        if not pending:
            return
        await asyncio.wait(pending, timeout=timeout)

    async def aclose(self, timeout: float = 5.0) -> None:
        """Disarm verdict delivery and terminally join verifier tasks.

        Persistent agent processes are reused across threads.  Merely clearing
        ``ToolContext.citation_event_callback`` does not affect the callback
        copied into this engine, so an old verifier could otherwise broadcast
        into the next thread.  Disarm first, reject new schedules, then cancel
        and join every current verifier before claimant ownership is released.
        """

        self._verification_closed = True
        self._on_event = None
        pending = {task for task in self._verify_tasks if not task.done()}
        if not pending:
            return
        for task in pending:
            task.cancel()
        done, still_pending = await asyncio.wait(
            pending,
            timeout=max(0.0, timeout),
        )
        for task in done:
            try:
                task.result()
            except (asyncio.CancelledError, Exception):
                pass
        if still_pending:
            raise RuntimeError(
                f"{len(still_pending)} citation verifier task(s) ignored cancellation"
            )

    @staticmethod
    def _validate_confidence(confidence: str) -> Confidence:
        """Coerce a confidence label, raising an actionable error if invalid.

        Shared by ``create_citation`` and ``edit_citation`` so the two cannot
        drift: the edit path used to skip validation entirely and let the
        ``::confidence_level`` cast fail in Postgres, whose error names no
        valid values.
        """
        try:
            return Confidence(confidence)
        except ValueError as e:
            log.error(f"Invalid confidence value: {confidence}")
            raise ValueError(
                f"Invalid confidence: {confidence}. Use 'high', 'medium', or 'low'."
            ) from e

    @staticmethod
    def _validate_extraction_method(extraction_method: str) -> ExtractionMethod:
        """Coerce an extraction-method label, raising an actionable error.

        See ``_validate_confidence`` — same rationale. The message must name the
        valid values: it is the agent's only recovery hint when a tool-schema
        constraint isn't in play (non-agent callers, or a model that ignores it).
        """
        try:
            return ExtractionMethod(extraction_method)
        except ValueError as e:
            log.error(f"Invalid extraction_method value: {extraction_method}")
            raise ValueError(
                f"Invalid extraction_method: {extraction_method}. "
                "Use 'direct_quote', 'paraphrase', 'inference', 'aggregation', or 'negative'."
            ) from e

    async def _update_verification_status(
        self,
        citation_id: int,
        verification: VerificationResult,
    ) -> None:
        """Write the LLM verdict. A missing passage keeps the first check's."""
        status = "verified" if verification.is_verified else "failed"
        reason = None if verification.is_verified else VerificationReason.NOT_SUPPORTED

        matched_location_json = None
        if isinstance(verification.matched_location, dict):
            try:
                matched_location_json = json.dumps(verification.matched_location)
            except (TypeError, ValueError) as e:
                log.warning(f"Could not serialize matched_location: {e}")

        await self.db.execute(
            """
            UPDATE citations
            SET verification_status = $1::verification_status,
                verification_notes = $2,
                similarity_score = $3,
                matched_location = COALESCE($4::jsonb, matched_location),
                verification_reason = $6
            WHERE id = $5
            """,
            status,
            verification.reasoning,
            verification.similarity_score,
            matched_location_json,
            citation_id,
            reason.value if reason else None,
        )
        log.debug(f"Updated verification status for citation [{citation_id}]: {status}")

    async def _mark_unchecked(
        self, citation_id: int, reason: VerificationReason, notes: str
    ) -> None:
        """End a pending citation that no check could decide (``unverified``).

        Used when the source is gone or the verifier failed. Before this, an
        outage left the row ``pending`` for a retry that was never built, and
        the chip said "Verifying…" forever (C2). Only a pending row changes,
        so a verdict that landed meanwhile is kept.
        """
        await self.db.execute(
            """
            UPDATE citations
            SET verification_status = 'unverified'::verification_status,
                verification_reason = $2,
                verification_notes = $3
            WHERE id = $1 AND verification_status = 'pending'::verification_status
            """,
            citation_id,
            reason.value,
            notes,
        )

    async def edit_citation(
        self,
        citation_id: int,
        claim: str | None = None,
        verbatim_quote: str | None = None,
        quote_context: str | None = None,
        relevance_reasoning: str | None = None,
        confidence: str | None = None,
        extraction_method: str | None = None,
        locator: dict[str, Any] | None = None,
    ) -> None:
        """
        Edit fields of a citation belonging to the current job.

        When content fields (claim, verbatim_quote, quote_context) change,
        the old verdict no longer applies: the citation is checked again from
        the start (the instant quote check, then the LLM), and the session is
        told. Before this, an edit reset the row to ``pending`` and nothing
        ever checked it again (C1). Scoped to ``context.session_id`` (job_id).

        Raises:
            ValueError: If an enum-backed field is invalid, the citation is not
                found / not owned / withdrawn, or no fields given.
        """
        # Validate before touching the DB. Without this the bad value reaches
        # the ``::confidence_level`` / ``::extraction_method`` casts below and
        # Postgres raises InvalidTextRepresentationError — which is not a
        # ValueError, so it bypasses the tool layer's handler and surfaces as a
        # raw driver string that never names the valid values. The create path
        # has always validated; this path never did.
        if confidence is not None:
            self._validate_confidence(confidence)
        if extraction_method is not None:
            self._validate_extraction_method(extraction_method)

        # Ownership guard (job-scoped).
        current = await self.get_citation(citation_id)
        if current is None:
            raise ValueError(f"Citation {citation_id} not found")
        if current.withdrawn_at is not None:
            raise ValueError(
                f"Citation {citation_id} was withdrawn; create a new citation "
                "instead of editing it"
            )

        content_fields_changed = any(
            v is not None for v in [claim, verbatim_quote, quote_context]
        )

        field_map = [
            ("claim", claim, None),
            ("verbatim_quote", verbatim_quote, None),
            ("quote_context", quote_context, None),
            ("relevance_reasoning", relevance_reasoning, None),
            ("confidence", confidence, "confidence_level"),
            ("extraction_method", extraction_method, "extraction_method"),
            ("locator", locator, "jsonb"),
        ]

        updates: list[str] = []
        values: list[Any] = []
        idx = 1
        for col, val, cast in field_map:
            if val is None:
                continue
            placeholder = f"${idx}" + (f"::{cast}" if cast else "")
            updates.append(f"{col} = {placeholder}")
            values.append(json.dumps(val) if cast == "jsonb" else val)
            idx += 1

        if not updates:
            raise ValueError("No fields provided to edit")

        status = None
        if content_fields_changed:
            source = await self.get_source(current.source_id)
            quote = (
                verbatim_quote if verbatim_quote is not None else current.verbatim_quote
            )
            if source is None:
                status, reason, notes, matched = (
                    VerificationStatus.UNVERIFIED,
                    VerificationReason.SOURCE_UNAVAILABLE,
                    "The cited source is no longer available to check against.",
                    None,
                )
            else:
                status, reason, notes, matched = self._first_check(source, quote)
            for col, val, cast in (
                ("verification_status", status.value, "verification_status"),
                ("verification_reason", reason.value if reason else None, None),
                ("verification_notes", notes, None),
                ("matched_location", json.dumps(matched) if matched else None, "jsonb"),
            ):
                updates.append(f"{col} = ${idx}" + (f"::{cast}" if cast else ""))
                values.append(val)
                idx += 1
            updates.append("similarity_score = NULL")

        values.append(citation_id)
        query = f"UPDATE citations SET {', '.join(updates)} WHERE id = ${idx}"
        await self.db.execute(query, *values)
        log.debug(f"Edited citation {citation_id}")
        await self._emit("citation.updated", citation_id)
        if status is VerificationStatus.PENDING:
            self._schedule_verification(citation_id)

    async def withdraw_citation(self, citation_id: int, reason: str) -> Citation:
        """Mark a citation as no longer used, keeping the record (D7).

        Nothing is deleted: the claim, quote and verdict stay for the audit
        trail and the citations panel, which shows the citation struck
        through. Idempotent; a second call keeps the first reason.

        Raises:
            ValueError: The citation is not found or not owned, or no reason.
        """
        reason = (reason or "").strip()
        if not reason:
            raise ValueError("Give a reason for withdrawing the citation")
        current = await self.get_citation(citation_id)
        if current is None:
            raise ValueError(f"Citation {citation_id} not found")
        if current.withdrawn_at is None:
            await self.db.execute(
                """
                UPDATE citations
                SET withdrawn_at = NOW(), withdrawn_reason = $2
                WHERE id = $1 AND withdrawn_at IS NULL
                """,
                citation_id,
                reason[:1000],
            )
            await self._emit("citation.updated", citation_id)
        updated = await self.get_citation(citation_id)
        return updated or current

    # =========================================================================
    # VERIFICATION HELPERS (verification itself runs on the auxiliary-LLM
    # service — see src/services/auxiliary.py::verify_and_store_citation)
    # =========================================================================

    def _extract_relevant_content(
        self,
        full_content: str,
        locator: dict | None,
    ) -> str:
        """Extract the relevant portion of source content based on locator (page)."""
        if not locator or "page" not in locator:
            log.debug("No page locator, using full content")
            return full_content

        page_num = locator["page"]
        log.debug(f"Extracting content for page {page_num}")

        page_pattern = r"--- Page (\d+) ---"
        pages = re.split(page_pattern, full_content)

        page_contents = {}
        for i in range(1, len(pages), 2):
            if i + 1 < len(pages):
                try:
                    num = int(pages[i])
                    page_contents[num] = pages[i + 1].strip()
                except ValueError:
                    continue

        if not page_contents:
            log.debug("No page markers found in content, using full content")
            return full_content

        context_pages = 1
        extracted_parts = []
        for p in range(page_num - context_pages, page_num + context_pages + 1):
            if p in page_contents:
                extracted_parts.append(f"--- Page {p} ---\n{page_contents[p]}")

        if not extracted_parts:
            log.warning(f"Page {page_num} not found in source, using full content")
            return full_content

        extracted = "\n\n".join(extracted_parts)
        log.debug(f"Extracted {len(extracted)} chars for page {page_num}")
        return extracted

    # =========================================================================
    # RETRIEVAL METHODS
    # =========================================================================

    async def get_source(
        self, source_id: int, job_id: str | None = None
    ) -> Source | None:
        """Get a source by ID, optionally verifying job ownership via job_sources."""
        job_uuid = self._as_uuid(job_id) if job_id is not None else self._job_uuid()
        log.debug(f"Retrieving source [{source_id}], job_id={job_uuid}")

        if job_uuid is not None:
            row = await self.db.fetchrow(
                f"""SELECT s.*, {self._LINK_COLUMNS} FROM sources s
                    JOIN job_sources js ON s.id = js.source_id
                    WHERE s.id = $1 AND js.job_id = $2""",
                source_id,
                job_uuid,
            )
        else:
            row = await self.db.fetchrow(
                "SELECT * FROM sources WHERE id = $1", source_id
            )

        if not row:
            log.debug(f"Source [{source_id}] not found (job_id={job_uuid})")
            return None
        return self._row_to_source(dict(row))

    async def get_citation(
        self, citation_id: int, job_id: str | None = None
    ) -> Citation | None:
        """Get a citation by ID, optionally verifying job_id ownership."""
        job_uuid = self._as_uuid(job_id) if job_id is not None else self._job_uuid()
        log.debug(f"Retrieving citation [{citation_id}], job_id={job_uuid}")

        if job_uuid is not None:
            row = await self.db.fetchrow(
                "SELECT * FROM citations WHERE id = $1 AND job_id = $2",
                citation_id,
                job_uuid,
            )
        else:
            row = await self.db.fetchrow(
                "SELECT * FROM citations WHERE id = $1", citation_id
            )

        if not row:
            log.debug(f"Citation [{citation_id}] not found (job_id={job_uuid})")
            return None
        return self._row_to_citation(dict(row))

    async def list_sources(
        self,
        source_type: str | None = None,
        job_id: str | None = None,
    ) -> list[Source]:
        """List registered sources, filtered by job_id (job_sources join) and type."""
        job_uuid = self._as_uuid(job_id) if job_id is not None else self._job_uuid()
        log.debug(
            f"Listing sources, job_id={job_uuid}, type filter: {source_type or 'all'}"
        )

        params: list[Any] = []
        conditions: list[str] = []

        if job_uuid is not None:
            query = (
                f"SELECT s.*, {self._LINK_COLUMNS} FROM sources s "
                "JOIN job_sources js ON s.id = js.source_id"
            )
            params.append(job_uuid)
            conditions.append(f"js.job_id = ${len(params)}")
        else:
            query = "SELECT s.* FROM sources s"

        if source_type is not None:
            params.append(source_type)
            conditions.append(f"s.type = ${len(params)}::source_type")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY s.created_at DESC"

        rows = await self.db.fetch(query, *params)
        sources = [self._row_to_source(dict(r)) for r in rows]
        log.debug(f"Found {len(sources)} sources for job_id={job_uuid}")
        return sources

    async def list_citations(
        self,
        source_id: int | None = None,
        session_id: str | None = None,
        verification_status: str | None = None,
        job_id: str | None = None,
        include_withdrawn: bool = True,
    ) -> list[Citation]:
        """List citations with optional filters (job-scoped by default).

        ``include_withdrawn=False`` leaves out citations the agent withdrew,
        which is what the failed-citation feedback wants: a withdrawn citation
        needs no more fixing.
        """
        # Validate before the ::verification_status cast below, which would
        # otherwise reject a bad label in Postgres with an unactionable error.
        if verification_status is not None:
            try:
                VerificationStatus(verification_status)
            except ValueError as e:
                raise ValueError(
                    f"Invalid verification_status '{verification_status}'. "
                    "Use: pending, verified, failed, unverified"
                ) from e

        job_uuid = self._as_uuid(job_id) if job_id is not None else self._job_uuid()
        log.debug(
            f"Listing citations: job_id={job_uuid}, source_id={source_id}, "
            f"session_id={session_id}, status={verification_status}"
        )

        params: list[Any] = []
        conditions: list[str] = []

        if job_uuid is not None:
            params.append(job_uuid)
            conditions.append(f"job_id = ${len(params)}")
        if source_id is not None:
            params.append(source_id)
            conditions.append(f"source_id = ${len(params)}")
        if session_id is not None:
            params.append(f"{session_id}%")
            conditions.append(f"created_by LIKE ${len(params)}")
        if verification_status is not None:
            params.append(verification_status)
            conditions.append(
                f"verification_status = ${len(params)}::verification_status"
            )
        if not include_withdrawn:
            conditions.append("withdrawn_at IS NULL")

        query = "SELECT * FROM citations"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY created_at DESC"

        rows = await self.db.fetch(query, *params)
        citations = [self._row_to_citation(dict(r)) for r in rows]
        log.debug(f"Found {len(citations)} citations for job_id={job_uuid}")
        return citations

    #: The work unit's own view of a source, read with a job-scoped join.
    _LINK_COLUMNS = (
        "js.name AS link_name, js.identifier AS link_identifier, "
        "js.metadata AS link_metadata"
    )

    def _row_to_source(self, row: dict[str, Any]) -> Source:
        """Convert a database row to a Source object.

        A job-scoped read carries the link's name, identifier and metadata;
        they win over the source row's, which belong to whoever registered
        the content first. Links written before 0029 have none.
        """
        metadata = self._loads(row.get("link_metadata"))
        if metadata is None:
            metadata = self._loads(row.get("metadata"))

        created_at = row.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))

        return Source(
            id=row["id"],
            type=SourceType(row["type"]),
            identifier=row.get("link_identifier") or row["identifier"],
            name=row.get("link_name") or row["name"],
            version=row.get("version"),
            content=row["content"],
            content_hash=row.get("content_hash"),
            metadata=metadata,
            created_at=created_at,
        )

    def _row_to_citation(self, row: dict[str, Any]) -> Citation:
        """Convert a database row to a Citation object."""
        locator = self._loads(row.get("locator")) or {}
        matched_location = self._loads(row.get("matched_location"))

        created_at = row.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        withdrawn_at = row.get("withdrawn_at")
        if isinstance(withdrawn_at, str):
            withdrawn_at = datetime.fromisoformat(withdrawn_at.replace("Z", "+00:00"))

        return Citation(
            id=row["id"],
            claim=row["claim"],
            verbatim_quote=row.get("verbatim_quote"),
            quote_context=row["quote_context"],
            quote_language=row.get("quote_language"),
            relevance_reasoning=row.get("relevance_reasoning"),
            confidence=Confidence(row.get("confidence", "high")),
            extraction_method=ExtractionMethod(
                row.get("extraction_method", "direct_quote")
            ),
            source_id=row["source_id"],
            locator=locator,
            verification_status=VerificationStatus(
                row.get("verification_status", "pending")
            ),
            verification_notes=row.get("verification_notes"),
            similarity_score=row.get("similarity_score"),
            matched_location=matched_location,
            created_at=created_at,
            created_by=row.get("created_by"),
            verification_reason=row.get("verification_reason"),
            withdrawn_at=withdrawn_at,
            withdrawn_reason=row.get("withdrawn_reason"),
        )

    # =========================================================================
    # EMBEDDING METHODS
    # =========================================================================

    def _get_embedding_service(self):
        """Lazy-resolve SRW's process-wide embedding service. None if unavailable.

        Phase 1: the citation engine shares SRW's embedding service
        (``shared.runtime.services.embedding_service.get_embedding_service`` — 4096-dim,
        matching the host ``source_embeddings.embedding vector(4096)`` column)
        instead of carrying its own ``CITATION_EMBEDDING_*`` stack.
        """
        if self._embedding_service is None:
            try:
                from shared.runtime.services.embedding_service import (
                    get_embedding_service,
                )

                self._embedding_service = get_embedding_service()
            except Exception as e:
                log.debug(f"SRW embedding service unavailable: {e}")
                self._embedding_service = False  # Sentinel: tried, not available

        return self._embedding_service if self._embedding_service is not False else None

    def _get_chunker(self):
        """Lazy-init the chunker.

        Phase 1 uses fixed-size chunking (``embedding_service=None``); semantic
        chunking needs an async chunker and is deferred (search still works on
        fixed chunks).
        """
        if self._chunker is None:
            from agent.citation_engine.chunking import SemanticChunker

            self._chunker = SemanticChunker(embedding_service=None)
        return self._chunker

    @staticmethod
    def _classify_embed_failure(error: Exception) -> str:
        """Typed reason for a persisted embedding failure.

        Distinguishes what the old generic "skipping" collapse could not:
        invalid vectors, config mismatches, exhausted transient retries and
        provider rejections each need a different recovery.
        """
        from shared.runtime.services.embedding_service import (
            EmbeddingDimensionError,
            EmbeddingInvalidVectorError,
        )

        if isinstance(error, EmbeddingInvalidVectorError):
            return "invalid_vector"
        if isinstance(error, EmbeddingDimensionError):
            return "dimension_mismatch"
        status = getattr(error, "status_code", None)
        if status == 429 or (isinstance(status, int) and status >= 500):
            return "transient_exhausted"
        if isinstance(status, int):
            return "provider_rejected"
        return "error"

    async def _set_embedding_state(
        self,
        source_id: int,
        status: str,
        *,
        reason_type: str | None = None,
        reason: str | None = None,
        chunks: int | None = None,
    ) -> None:
        """Persist per-source embedding state on sources.metadata (fix 5).

        Registration keeps succeeding either way — but a source that missed
        its embedding is no longer indistinguishable from an indexed one.
        Best-effort: state bookkeeping must never break the embed path.
        """
        state: dict[str, Any] = {
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if reason_type:
            state["reason_type"] = reason_type
        if reason:
            state["reason"] = str(reason)[:500]
        if chunks is not None:
            state["chunks"] = chunks
        try:
            await self.db.execute(
                """
                UPDATE sources
                SET metadata = jsonb_set(
                    COALESCE(metadata, '{}'::jsonb),
                    '{embedding_state}',
                    $2::jsonb
                )
                WHERE id = $1
                """,
                source_id,
                json.dumps(state),
            )
        except Exception as e:  # noqa: BLE001 — bookkeeping is best-effort
            log.warning(f"Could not persist embedding state for [{source_id}]: {e}")

    async def _auto_embed_source(
        self, source_id: int, content: str, job_uuid: uuid.UUID
    ) -> None:
        """Auto-embed source content after registration.

        Registration still never fails on an embedding problem, but the
        failure is no longer silent: it lands typed on the source row
        (``metadata.embedding_state``) and in citation statistics, so
        semantic-search coverage is observable and backfillable
        (scripts/backfill_source_embeddings.py).
        """
        service = self._get_embedding_service()
        if not service:
            return

        try:
            cnt = await self.db.fetchval(
                "SELECT COUNT(*) FROM source_embeddings WHERE source_id = $1 AND job_id = $2",
                source_id,
                job_uuid,
            )
            if cnt and cnt > 0:
                log.debug(f"Source [{source_id}] already embedded for job {job_uuid}")
                return
            await self._embed_source_content(source_id, content, job_uuid, service)
        except Exception as e:
            reason_type = self._classify_embed_failure(e)
            log.warning(
                f"Auto-embed failed for source [{source_id}] "
                f"({reason_type}), skipping: {e}"
            )
            await self._set_embedding_state(
                source_id, "failed", reason_type=reason_type, reason=str(e)
            )

    async def _embed_source_content(
        self,
        source_id: int,
        content: str,
        job_uuid: uuid.UUID,
        service=None,
    ) -> int:
        """Chunk and embed source content, storing in source_embeddings."""
        if service is None:
            service = self._get_embedding_service()
            if not service:
                raise RuntimeError("Embedding service not available")

        chunks = self._get_chunker().chunk(content)
        if not chunks:
            return 0

        # SRW's embedding service is async; pgvector codec on the pool connection
        # encodes List[float] → vector(4096) directly.
        embeddings = await service.embed_batch(chunks)

        async with self.db.acquire() as conn:
            for idx, (chunk_text, embedding) in enumerate(
                zip(chunks, embeddings, strict=False)
            ):
                await conn.execute(
                    """
                    INSERT INTO source_embeddings (source_id, job_id, chunk_index, chunk_text, embedding)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (source_id, job_id, chunk_index)
                    DO UPDATE SET chunk_text = EXCLUDED.chunk_text, embedding = EXCLUDED.embedding
                    """,
                    source_id,
                    job_uuid,
                    idx,
                    chunk_text,
                    embedding,
                )

        log.info(f"Embedded source [{source_id}]: {len(chunks)} chunks")
        await self._set_embedding_state(source_id, "complete", chunks=len(chunks))
        return len(chunks)

    # =========================================================================
    # SEARCH METHODS
    # =========================================================================

    async def search_library(
        self,
        query: str,
        mode: str = "hybrid",
        tags: list[str] | None = None,
        source_type: str | None = None,
        scope: str = "content",
        top_k: int = 10,
    ) -> SearchResults:
        """Search the source library with keyword, semantic, or hybrid retrieval."""
        from agent.citation_engine.search import (
            keyword_search,
            label_evidence,
            overall_label,
            rrf_merge,
            semantic_search,
        )

        if not query or not query.strip():
            raise ValueError("Search query cannot be empty")
        if mode not in ("hybrid", "keyword", "semantic"):
            raise ValueError(
                f"Invalid search mode '{mode}'. Use: hybrid, keyword, semantic"
            )
        if scope not in ("content", "annotations", "all"):
            raise ValueError(f"Invalid scope '{scope}'. Use: content, annotations, all")
        # `source_type` was the odd one out: its siblings above were checked
        # while it flowed unvalidated into search.py's `s.type = $N::source_type`
        # cast, failing in Postgres with an error that names no valid values.
        if source_type is not None:
            try:
                SourceType(source_type)
            except ValueError as e:
                raise ValueError(
                    f"Invalid source_type '{source_type}'. "
                    "Use: document, website, database, custom"
                ) from e

        job_uuid = self._job_uuid()
        service = self._get_embedding_service()
        can_semantic = service is not None

        effective_mode = mode
        if mode == "semantic" and not can_semantic:
            log.warning("Semantic search unavailable, falling back to keyword")
            effective_mode = "keyword"
        elif mode == "hybrid" and not can_semantic:
            log.info("Semantic search unavailable, hybrid falling back to keyword-only")
            effective_mode = "keyword"

        fetch_k = top_k * 2
        keyword_hits = []
        semantic_hits = []

        # Compute the query embedding (async) before acquiring the connection.
        query_embedding = None
        if effective_mode in ("semantic", "hybrid") and service:
            try:
                query_embedding = await service.embed(query)
            except Exception as e:
                log.warning(f"Query embedding failed, keyword-only: {e}")
                if effective_mode == "semantic":
                    effective_mode = "keyword"

        async with self.db.acquire() as conn:
            if effective_mode in ("keyword", "hybrid"):
                keyword_hits = await keyword_search(
                    conn=conn,
                    query=query,
                    job_id=job_uuid,
                    top_k=fetch_k,
                    source_type=source_type,
                    tags=tags,
                    scope=scope,
                )

            if effective_mode in ("semantic", "hybrid") and query_embedding is not None:
                try:
                    semantic_hits = await semantic_search(
                        conn=conn,
                        query_embedding=query_embedding,
                        job_id=job_uuid,
                        top_k=fetch_k,
                        source_type=source_type,
                        tags=tags,
                    )
                except Exception as e:
                    log.warning(f"Semantic search failed: {e}")

        if effective_mode == "hybrid" and keyword_hits and semantic_hits:
            merged = rrf_merge(keyword_hits, semantic_hits, top_k=top_k)
        elif keyword_hits:
            merged = keyword_hits[:top_k]
        elif semantic_hits:
            merged = semantic_hits[:top_k]
        else:
            merged = []

        labeled = label_evidence(merged)
        summary = overall_label(labeled)

        return SearchResults(
            query=query,
            results=labeled,
            overall_label=summary,
            mode=effective_mode,
        )

    # =========================================================================
    # ANNOTATION & TAG METHODS
    # =========================================================================

    async def annotate_source(
        self,
        source_id: int,
        content: str,
        annotation_type: str = "note",
        page_reference: str | None = None,
    ) -> Annotation:
        """Add an annotation to a source (per-job)."""
        job_uuid = self._job_uuid()

        try:
            ann_type = AnnotationType(annotation_type)
        except ValueError as err:
            valid = ", ".join(t.value for t in AnnotationType)
            raise ValueError(
                f"Invalid annotation_type '{annotation_type}'. Valid: {valid}"
            ) from err

        source = await self.get_source(
            source_id, job_id=str(job_uuid) if job_uuid else None
        )
        if not source:
            raise ValueError(
                f"Source [{source_id}] not found or not linked to current job"
            )

        created_by = None
        if self.context:
            created_by = self.context.agent_id or self.context.session_id

        ann_id = await self.db.fetchval(
            """
            INSERT INTO source_annotations (source_id, job_id, annotation_type, content, page_reference, created_by)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            """,
            source_id,
            job_uuid,
            ann_type.value,
            content,
            page_reference,
            created_by,
        )
        log.info(
            f"Created {ann_type.value} annotation [{ann_id}] on source [{source_id}]"
        )

        return Annotation(
            id=ann_id,
            source_id=source_id,
            job_id=str(job_uuid),
            annotation_type=ann_type,
            content=content,
            page_reference=page_reference,
            created_at=datetime.now(timezone.utc),
            created_by=created_by,
        )

    async def get_annotations(
        self,
        source_id: int,
        annotation_type: str | None = None,
    ) -> list[Annotation]:
        """Get annotations for a source in the current job."""
        job_uuid = self._job_uuid()

        params: list[Any] = [source_id]
        conditions = ["source_id = $1"]

        if job_uuid is not None:
            params.append(job_uuid)
            conditions.append(f"job_id = ${len(params)}")
        if annotation_type is not None:
            params.append(annotation_type)
            conditions.append(f"annotation_type = ${len(params)}")

        query = (
            "SELECT * FROM source_annotations WHERE "
            + " AND ".join(conditions)
            + " ORDER BY created_at DESC"
        )
        rows = await self.db.fetch(query, *params)
        return [self._row_to_annotation(dict(r)) for r in rows]

    async def tag_source(self, source_id: int, tags: list[str]) -> list[str]:
        """Add tags to a source in the current job."""
        job_uuid = self._job_uuid()

        source = await self.get_source(
            source_id, job_id=str(job_uuid) if job_uuid else None
        )
        if not source:
            raise ValueError(
                f"Source [{source_id}] not found or not linked to current job"
            )

        async with self.db.acquire() as conn:
            for tag in tags:
                tag = tag.strip()
                if not tag:
                    continue
                await conn.execute(
                    "INSERT INTO source_tags (source_id, job_id, tag) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                    source_id,
                    job_uuid,
                    tag,
                )

        log.info(f"Tagged source [{source_id}] with {tags}")
        return await self.get_tags(source_id)

    async def remove_tags(self, source_id: int, tags: list[str]) -> list[str]:
        """Remove tags from a source in the current job."""
        job_uuid = self._job_uuid()

        async with self.db.acquire() as conn:
            for tag in tags:
                tag = tag.strip()
                if not tag:
                    continue
                await conn.execute(
                    "DELETE FROM source_tags WHERE source_id = $1 AND job_id = $2 AND tag = $3",
                    source_id,
                    job_uuid,
                    tag,
                )

        log.info(f"Removed tags {tags} from source [{source_id}]")
        return await self.get_tags(source_id)

    async def get_tags(self, source_id: int) -> list[str]:
        """Get all tags for a source in the current job."""
        job_uuid = self._job_uuid()

        if job_uuid is not None:
            rows = await self.db.fetch(
                "SELECT tag FROM source_tags WHERE source_id = $1 AND job_id = $2 ORDER BY tag",
                source_id,
                job_uuid,
            )
        else:
            rows = await self.db.fetch(
                "SELECT tag FROM source_tags WHERE source_id = $1 ORDER BY tag",
                source_id,
            )
        return [r["tag"] for r in rows]

    def _row_to_annotation(self, row: dict[str, Any]) -> Annotation:
        """Convert a database row to an Annotation object."""
        created_at = row.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))

        return Annotation(
            id=row["id"],
            source_id=row["source_id"],
            job_id=str(row["job_id"]),
            annotation_type=AnnotationType(row["annotation_type"]),
            content=row["content"],
            page_reference=row.get("page_reference"),
            created_at=created_at,
            created_by=row.get("created_by"),
        )

    # =========================================================================
    # EXPORT METHODS
    # =========================================================================

    async def format_citation(
        self,
        citation_id: int,
        style: str = "inline",
    ) -> str:
        """Format a single citation for display/export."""
        log.debug(f"Formatting citation [{citation_id}] in {style} style")

        citation = await self.get_citation(citation_id)
        if citation is None:
            raise ValueError(f"Citation not found: {citation_id}")

        source = await self.get_source(citation.source_id)
        if source is None:
            raise ValueError(f"Source not found for citation {citation_id}")

        handle = format_handle(citation.id)
        if style == "inline":
            return handle
        elif style == "harvard":
            return f"{source.name} ({source.version or 'n.d.'})"
        elif style == "ieee":
            return f"{handle} {source.name}, {source.version or 'n.d.'}"
        elif style == "bibtex":
            entry_type = "misc"
            if source.type == SourceType.DOCUMENT:
                entry_type = "book"
            elif source.type == SourceType.WEBSITE:
                entry_type = "online"
            return f"""@{entry_type}{{cite{citation.id},
    title = {{{source.name}}},
    year = {{{source.version or "n.d."}}},
    note = {{{source.identifier}}}
}}"""
        elif style == "apa":
            return f"{source.name}. ({source.version or 'n.d.'})."
        else:
            raise ValueError(
                f"Unknown citation style: {style}. Supported: inline, harvard, ieee, bibtex, apa"
            )

    # =========================================================================
    # STATISTICS & REPORTING
    # =========================================================================
