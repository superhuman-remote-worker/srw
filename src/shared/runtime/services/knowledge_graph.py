"""KnowledgeGraphDB — Neo4j service for the project knowledge base.

Wraps Neo4jDB with knowledge-specific operations: create/read/update notes,
manage relationships, graph traversal queries. This is the system-level
Neo4j connection (not the external datasource connector).

See knowledge-base/knowledge/features/project_knowledge_base.md for full architecture.

Usage:
    ```python
    from shared.runtime.services.knowledge_graph import KnowledgeGraphDB

    kg = KnowledgeGraphDB()  # reads NEO4J_URL, NEO4J_USERNAME, NEO4J_PASSWORD
    kg.connect()

    note_id = kg.create_note(
        project_id="proj-456",
        title="Chose JWT over OAuth",
        note_type="decision",
        content="After evaluating both approaches...",
        tags=["authentication", "security"],
        confidence="high",
        job_id="abc-123",
        phase=3,
        retrieval_messages=["What auth approach?", "Why JWT?"],
    )

    note = kg.read_note("proj-456", "chose-jwt-over-oauth")
    related = kg.get_related("proj-456", "chose-jwt-over-oauth", max_hops=2)
    ```
"""

import hashlib
import logging
import os
import re
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared.runtime.database.neo4j_db import Neo4jDB

logger = logging.getLogger(__name__)

# Valid note types and statuses
NOTE_TYPES = frozenset(
    {
        "goal",
        "plan",
        "decision",
        "learning",
        "code",
        "source",
        "question",
        "state",
        "retrospective",
        "datasource",
        # Backlog ticket types (knowledge-base/knowledge/superpowers/specs/
        # 2026-07-26-project-backlog-pipeline-design.md). These carry no TTL —
        # KB_TTL_BY_NOTE_TYPE's default of None already covers them, so a
        # backlog entry never expires on a clock.
        "feature",
        "issue",
        "idea",
        # Officer types (knowledge-base/knowledge/features/centurion.md §5, vector/0015). 'charter'
        # is the project's pinned commander's-intent note — one ACTIVE charter
        # per project, write-gated to sessions (Legate/officer hands; workers
        # are refused in kb_write). 'report' is a worker recon deliverable —
        # provenance rides job_id; reports inform, never steer.
        "charter",
        "report",
    }
)

NOTE_STATUSES = frozenset({"active", "resolved", "superseded", "archived"})

# Backlog priority. A LABEL, never a contract: it orders the list agents are
# shown and nothing in the engine may gate, refuse, or reorder work on it.
# Stored as a rank so ordering is an index scan rather than a CASE.
PRIORITY_RANKS: dict[str, int] = {"high": 0, "normal": 1, "low": 2}
PRIORITY_WORDS: dict[int, str] = {0: "high", 1: "normal", 2: "low"}
DEFAULT_PRIORITY_RANK = 1

CONFIDENCE_LEVELS = frozenset({"high", "medium", "low"})

RELATIONSHIP_TYPES = frozenset(
    {
        "REFERENCES",
        "DERIVED_FROM",
        "SUPPORTS",
        "CONTRADICTS",
        "ANSWERS",
        "DEPENDS_ON",
        "SUPERSEDES",
        "IMPLEMENTS",
    }
)

# A link whose target note does not exist yet is kept on its source note as
# "<TYPE>|<target>" in ``pending_links`` until the target is written.
_PENDING_SEP = "|"


def slugify(title: str, max_length: int = 80) -> str:
    """Generate a URL-safe slug from a title.

    Args:
        title: Note title
        max_length: Maximum slug length

    Returns:
        Lowercase hyphenated slug (e.g., "chose-jwt-over-oauth")
    """
    slug = title.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")
    return slug[:max_length]


class KnowledgeGraphDB:
    """Neo4j service for project knowledge base operations.

    System-level wrapper around Neo4jDB providing knowledge-specific
    methods. Connection details from environment variables.
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self._uri = uri or os.getenv("NEO4J_URL", "bolt://localhost:7687")
        self._username = username or os.getenv("NEO4J_USERNAME", "neo4j")
        self._password = password or os.getenv("NEO4J_PASSWORD", "")
        self._db = Neo4jDB(self._uri, self._username, self._password)

    def connect(self) -> bool:
        """Connect to Neo4j."""
        return self._db.connect()

    def close(self) -> None:
        """Close connection."""
        self._db.close()

    @property
    def is_connected(self) -> bool:
        return self._db.is_connected

    # =========================================================================
    # Create
    # =========================================================================

    def create_note(
        self,
        project_id: str,
        title: str,
        note_type: str,
        content: str,
        tags: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        confidence: Optional[str] = None,
        job_id: Optional[str] = None,
        phase: Optional[int] = None,
        retrieval_messages: Optional[List[str]] = None,
        links: Optional[List[Dict[str, str]]] = None,
        note_id: Optional[str] = None,
    ) -> str:
        """Create a new knowledge note in Neo4j.

        Args:
            project_id: Project UUID
            title: Note title (used to generate slug ID)
            note_type: One of NOTE_TYPES
            content: Full markdown body
            tags: List of tag names
            keywords: List of keyword strings
            confidence: high, medium, or low
            job_id: Creating job UUID
            phase: Phase number when created
            retrieval_messages: Synthetic queries for retrieval
            links: List of {"target": slug, "type": RELATIONSHIP_TYPE}
            note_id: Server-derived canonical slug. When provided, creation is
                idempotent for that exact note instead of deriving a second
                collision suffix in the projection.

        Returns:
            The note's slug ID

        Raises:
            ValueError: If note_type or confidence is invalid
        """
        if note_type not in NOTE_TYPES:
            raise ValueError(
                f"Invalid note_type: {note_type}. Must be one of {NOTE_TYPES}"
            )
        if confidence and confidence not in CONFIDENCE_LEVELS:
            raise ValueError(
                f"Invalid confidence: {confidence}. Must be one of {CONFIDENCE_LEVELS}"
            )

        slug = note_id or slugify(title)
        if not slug:
            slug = f"note-{secrets.token_hex(4)}"

        # Check for slug collision and append a suffix if needed. The suffix is
        # a deterministic content hash (not a random token) so identical
        # re-writes converge to the same slug instead of forking a fresh note
        # every time — the run-8 twin-file problem (vault features/ §11.1).
        existing = self._db.execute_query(
            "MATCH (n:Note {project_id: $pid, id: $id}) RETURN n.id",
            {"pid": project_id, "id": slug},
        )
        if existing and note_id is not None:
            return slug
        if existing:
            digest = hashlib.sha256(content.encode()).hexdigest()[:6]
            slug = f"{slug}-{digest}"
            # Idempotent: if the suffixed slug is already present (same title +
            # content written before), return it rather than duplicating a node.
            dup = self._db.execute_query(
                "MATCH (n:Note {project_id: $pid, id: $id}) RETURN n.id",
                {"pid": project_id, "id": slug},
            )
            if dup:
                return slug

        now = datetime.now(timezone.utc).isoformat()

        # Create Note node
        self._db.execute_write(
            """
            CREATE (n:Note {
                id: $id,
                project_id: $project_id,
                type: $type,
                title: $title,
                content: $content,
                status: 'active',
                confidence: $confidence,
                job_id: $job_id,
                phase: $phase,
                created: datetime($now),
                modified: datetime($now),
                retrieval_messages: $retrieval_messages
            })
            """,
            {
                "id": slug,
                "project_id": project_id,
                "type": note_type,
                "title": title,
                "content": content,
                "confidence": confidence,
                "job_id": job_id,
                "phase": phase,
                "now": now,
                "retrieval_messages": retrieval_messages or [],
            },
        )

        # Create Tag nodes and relationships
        for tag in tags or []:
            self._db.execute_write(
                """
                MATCH (n:Note {project_id: $pid, id: $nid})
                MERGE (t:Tag {name: $tag, project_id: $pid})
                MERGE (n)-[:TAGGED]->(t)
                """,
                {"pid": project_id, "nid": slug, "tag": tag.lower()},
            )

        # Create Keyword nodes and relationships
        for kw in keywords or []:
            self._db.execute_write(
                """
                MATCH (n:Note {project_id: $pid, id: $nid})
                MERGE (k:Keyword {name: $kw, project_id: $pid})
                MERGE (n)-[:HAS_KEYWORD]->(k)
                """,
                {"pid": project_id, "nid": slug, "kw": kw.lower()},
            )

        # Create relationships to other notes
        for link in links or []:
            rel_type = link.get("type", "REFERENCES")
            target = link.get("target")
            if not target:
                continue
            if rel_type not in RELATIONSHIP_TYPES:
                logger.warning(f"Unknown relationship type: {rel_type}, skipping")
                continue
            self._link(project_id, slug, rel_type, target)

        # Links other notes made to this one before it existed.
        self._resolve_pending_links(project_id, slug)

        logger.debug(
            f"Created knowledge note: {slug} (type={note_type}, project={project_id})"
        )
        return slug

    def _link(self, project_id: str, src: str, rel_type: str, target: str) -> None:
        """Write one outgoing link of ``src``, whether or not ``target`` exists.

        An edge needs both nodes, and a note may link to one written later. The
        link is then kept on ``src`` as a pending entry, which
        ``_resolve_pending_links`` turns into the edge when the target is
        created; making the edge clears the entry. ``rel_type`` must already be
        one of ``RELATIONSHIP_TYPES``: it is spliced into the query.
        """
        self._db.execute_write(
            f"""
            MATCH (a:Note {{project_id: $pid, id: $src}})
            OPTIONAL MATCH (b:Note {{project_id: $pid, id: $tgt}})
            FOREACH (_ IN CASE WHEN b IS NULL THEN [] ELSE [1] END |
                MERGE (a)-[:{rel_type}]->(b))
            WITH a, b,
                 [p IN coalesce(a.pending_links, []) WHERE p <> $entry] AS rest
            SET a.pending_links = CASE
                WHEN b IS NULL THEN rest + $entry
                WHEN size(rest) = 0 THEN null
                ELSE rest
            END
            """,
            {
                "pid": project_id,
                "src": src,
                "tgt": target,
                "entry": f"{rel_type}{_PENDING_SEP}{target}",
            },
        )

    def _resolve_pending_links(self, project_id: str, note_id: str) -> None:
        """Make the edges other notes asked for before ``note_id`` existed."""
        suffix = f"{_PENDING_SEP}{note_id}"
        rows = self._db.execute_query(
            """
            MATCH (a:Note {project_id: $pid})
            WHERE any(p IN coalesce(a.pending_links, []) WHERE p ENDS WITH $suffix)
            RETURN a.id AS src,
                   [p IN a.pending_links WHERE p ENDS WITH $suffix] AS entries
            """,
            {"pid": project_id, "suffix": suffix},
        )
        for row in rows or []:
            for entry in row.get("entries") or []:
                rel_type, _, target = entry.partition(_PENDING_SEP)
                # ENDS WITH also matches a target that merely ends in this id.
                if target == note_id and rel_type in RELATIONSHIP_TYPES:
                    self._link(project_id, row["src"], rel_type, note_id)

    # =========================================================================
    # Read
    # =========================================================================

    def read_note(self, project_id: str, note_id: str) -> Optional[Dict[str, Any]]:
        """Read a full note with metadata and relationships.

        Args:
            project_id: Project UUID
            note_id: Note slug

        Returns:
            Dict with note properties + relationships, or None if not found
        """
        results = self._db.execute_query(
            """
            MATCH (n:Note {project_id: $pid, id: $nid})
            OPTIONAL MATCH (n)-[:TAGGED]->(t:Tag)
            OPTIONAL MATCH (n)-[:HAS_KEYWORD]->(k:Keyword)
            RETURN n,
                   collect(DISTINCT t.name) AS tags,
                   collect(DISTINCT k.name) AS keywords
            """,
            {"pid": project_id, "nid": note_id},
        )

        if not results:
            return None

        record = results[0]
        node = record["n"]

        # Get node properties (handle both dict and Node types)
        if hasattr(node, "items"):
            props = dict(node.items())
        elif isinstance(node, dict):
            props = dict(node)
        else:
            props = {}

        props["tags"] = record.get("tags", [])
        props["keywords"] = record.get("keywords", [])
        # Links to notes not written yet (see ``_link``): no edge, no title.
        props["pending_relationships"] = [
            {"type": rel_type, "target": target}
            for rel_type, _, target in (
                str(entry).partition(_PENDING_SEP)
                for entry in props.pop("pending_links", None) or []
            )
            if target
        ]

        # Get outgoing relationships
        rels = self._db.execute_query(
            """
            MATCH (n:Note {project_id: $pid, id: $nid})-[r]->(m:Note)
            RETURN type(r) AS rel_type, m.id AS target_id, m.title AS target_title
            """,
            {"pid": project_id, "nid": note_id},
        )
        props["relationships"] = [
            {
                "type": r["rel_type"],
                "target": r["target_id"],
                "target_title": r.get("target_title"),
            }
            for r in rels
        ]

        # Get incoming relationships
        incoming = self._db.execute_query(
            """
            MATCH (m:Note)-[r]->(n:Note {project_id: $pid, id: $nid})
            RETURN type(r) AS rel_type, m.id AS source_id, m.title AS source_title
            """,
            {"pid": project_id, "nid": note_id},
        )
        props["incoming_relationships"] = [
            {
                "type": r["rel_type"],
                "source": r["source_id"],
                "source_title": r.get("source_title"),
            }
            for r in incoming
        ]

        return props

    def list_notes(
        self,
        project_id: str,
        note_type: Optional[str] = None,
        tag: Optional[str] = None,
        status: Optional[str] = None,
        job_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """List notes with optional filters.

        Args:
            project_id: Project UUID
            note_type: Filter by type
            tag: Filter by tag name
            status: Filter by status (default: all)
            job_id: Filter by creating job
            limit: Max results

        Returns:
            List of note summary dicts
        """
        conditions = ["n.project_id = $pid"]
        params: Dict[str, Any] = {"pid": project_id, "limit": limit}

        if note_type:
            conditions.append("n.type = $type")
            params["type"] = note_type
        if status:
            conditions.append("n.status = $status")
            params["status"] = status
        if job_id:
            conditions.append("n.job_id = $job_id")
            params["job_id"] = job_id

        where = " AND ".join(conditions)

        if tag:
            query = f"""
                MATCH (n:Note)-[:TAGGED]->(t:Tag {{name: $tag, project_id: $pid}})
                WHERE {where}
                RETURN n.id AS id, n.title AS title, n.type AS type,
                       n.status AS status, n.confidence AS confidence,
                       n.created AS created, n.modified AS modified,
                       n.job_id AS job_id
                ORDER BY n.modified DESC
                LIMIT $limit
            """
            params["tag"] = tag.lower()
        else:
            query = f"""
                MATCH (n:Note)
                WHERE {where}
                RETURN n.id AS id, n.title AS title, n.type AS type,
                       n.status AS status, n.confidence AS confidence,
                       n.created AS created, n.modified AS modified,
                       n.job_id AS job_id
                ORDER BY n.modified DESC
                LIMIT $limit
            """

        return self._db.execute_query(query, params)

    # =========================================================================
    # Update
    # =========================================================================

    def update_note(
        self,
        project_id: str,
        note_id: str,
        content: Optional[str] = None,
        append: Optional[str] = None,
        status: Optional[str] = None,
        confidence: Optional[str] = None,
        add_tags: Optional[List[str]] = None,
        add_links: Optional[List[Dict[str, str]]] = None,
        remove_tags: Optional[List[str]] = None,
    ) -> bool:
        """Update an existing note.

        Args:
            project_id: Project UUID
            note_id: Note slug
            content: Replace content entirely
            append: Append to existing content
            status: New status
            confidence: New confidence
            add_tags: Additional tags
            add_links: Additional relationships
            remove_tags: Tags to detach. Removal exists because tags became
                dispatch state, not just labels: a ``ready`` tag that can only
                be added is a one-way door, and a ticket whose category
                changes would otherwise match two pools at once
                (knowledge-base/knowledge/features/officer_backlog_pools.md §4). Detaching the
                ``:TAGGED`` edge leaves the ``:Tag`` node — it is shared across
                the project's notes and may still have other holders.

        Returns:
            True if note was found and updated
        """
        # Verify note exists
        existing = self._db.execute_query(
            "MATCH (n:Note {project_id: $pid, id: $nid}) RETURN n.id",
            {"pid": project_id, "nid": note_id},
        )
        if not existing:
            return False

        now = datetime.now(timezone.utc).isoformat()
        set_clauses = ["n.modified = datetime($now)"]
        params: Dict[str, Any] = {"pid": project_id, "nid": note_id, "now": now}

        if content is not None:
            set_clauses.append("n.content = $content")
            params["content"] = content
        elif append is not None:
            set_clauses.append("n.content = n.content + $append")
            params["append"] = "\n\n" + append

        if status is not None:
            if status not in NOTE_STATUSES:
                raise ValueError(f"Invalid status: {status}")
            set_clauses.append("n.status = $status")
            params["status"] = status

        if confidence is not None:
            if confidence not in CONFIDENCE_LEVELS:
                raise ValueError(f"Invalid confidence: {confidence}")
            set_clauses.append("n.confidence = $confidence")
            params["confidence"] = confidence

        self._db.execute_write(
            f"""
            MATCH (n:Note {{project_id: $pid, id: $nid}})
            SET {", ".join(set_clauses)}
            """,
            params,
        )

        # Remove before add, so a caller that does both in one call (a category
        # change: drop category:researcher, add category:executor) cannot have
        # the removal undo the addition when the two lists overlap.
        for tag in remove_tags or []:
            self._db.execute_write(
                """
                MATCH (n:Note {project_id: $pid, id: $nid})-[r:TAGGED]->
                      (t:Tag {name: $tag, project_id: $pid})
                DELETE r
                """,
                {"pid": project_id, "nid": note_id, "tag": tag.lower()},
            )

        # Add new tags
        for tag in add_tags or []:
            self._db.execute_write(
                """
                MATCH (n:Note {project_id: $pid, id: $nid})
                MERGE (t:Tag {name: $tag, project_id: $pid})
                MERGE (n)-[:TAGGED]->(t)
                """,
                {"pid": project_id, "nid": note_id, "tag": tag.lower()},
            )

        # Add new relationships
        for link in add_links or []:
            rel_type = link.get("type", "REFERENCES")
            target = link.get("target")
            if not target or rel_type not in RELATIONSHIP_TYPES:
                continue
            self._link(project_id, note_id, rel_type, target)

        logger.debug(f"Updated knowledge note: {note_id}")
        return True

    # =========================================================================
    # Graph Queries
    # =========================================================================

    def get_related(
        self,
        project_id: str,
        note_id: str,
        max_hops: int = 2,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Get notes within N hops of a given note.

        Args:
            project_id: Project UUID
            note_id: Starting note slug
            max_hops: Maximum relationship hops (1-3)
            limit: Max results

        Returns:
            List of related notes with relationship paths
        """
        max_hops = min(max_hops, 3)  # Cap at 3 to avoid expensive traversals

        results = self._db.execute_query(
            f"""
            MATCH (start:Note {{project_id: $pid, id: $nid}})
            MATCH path = (start)-[*1..{max_hops}]-(related:Note {{project_id: $pid}})
            WHERE related.id <> $nid
            WITH DISTINCT related, length(path) AS distance,
                 [r IN relationships(path) | type(r)] AS rel_types
            RETURN related.id AS id, related.title AS title, related.type AS type,
                   related.status AS status, distance, rel_types
            ORDER BY distance, related.modified DESC
            LIMIT $limit
            """,
            {"pid": project_id, "nid": note_id, "limit": limit},
        )
        return results

    def get_contradictions(self, project_id: str) -> List[Dict[str, Any]]:
        """Get all pairs of notes connected by CONTRADICTS.

        Args:
            project_id: Project UUID

        Returns:
            List of contradiction pairs
        """
        return self._db.execute_query(
            """
            MATCH (a:Note {project_id: $pid})-[:CONTRADICTS]->(b:Note {project_id: $pid})
            WHERE a.status = 'active' AND b.status = 'active'
            RETURN a.id AS note_a, a.title AS title_a,
                   b.id AS note_b, b.title AS title_b
            """,
            {"pid": project_id},
        )

    def get_provenance(
        self,
        project_id: str,
        note_id: str,
        max_depth: int = 5,
    ) -> List[Dict[str, Any]]:
        """Trace DERIVED_FROM chain back from a note.

        Args:
            project_id: Project UUID
            note_id: Starting note slug
            max_depth: Max chain length

        Returns:
            Ordered provenance chain
        """
        return self._db.execute_query(
            f"""
            MATCH path = (n:Note {{project_id: $pid, id: $nid}})
                         -[:DERIVED_FROM*1..{min(max_depth, 10)}]->
                         (source:Note {{project_id: $pid}})
            UNWIND nodes(path) AS node
            WITH DISTINCT node, length(path) AS depth
            WHERE node.id <> $nid
            RETURN node.id AS id, node.title AS title, node.type AS type, depth
            ORDER BY depth
            """,
            {"pid": project_id, "nid": note_id},
        )

    def get_unanswered(self, project_id: str) -> List[Dict[str, Any]]:
        """Get question notes that have no ANSWERS relationship.

        Args:
            project_id: Project UUID

        Returns:
            List of unanswered question notes
        """
        return self._db.execute_query(
            """
            MATCH (q:Note {project_id: $pid, type: 'question', status: 'active'})
            WHERE NOT ()-[:ANSWERS]->(q)
              AND NOT (q)-[:ANSWERS]->()
            RETURN q.id AS id, q.title AS title, q.content AS content,
                   q.created AS created, q.job_id AS job_id
            ORDER BY q.created DESC
            """,
            {"pid": project_id},
        )

    # =========================================================================
    # Export
    # =========================================================================

    def get_all_notes_for_export(self, project_id: str) -> List[Dict[str, Any]]:
        """Get all notes with full data for Obsidian export.

        Args:
            project_id: Project UUID

        Returns:
            List of note dicts with tags, keywords, and relationships
        """
        notes = self._db.execute_query(
            """
            MATCH (n:Note {project_id: $pid})
            OPTIONAL MATCH (n)-[:TAGGED]->(t:Tag)
            OPTIONAL MATCH (n)-[:HAS_KEYWORD]->(k:Keyword)
            RETURN n,
                   collect(DISTINCT t.name) AS tags,
                   collect(DISTINCT k.name) AS keywords
            ORDER BY n.type, n.modified DESC
            """,
            {"pid": project_id},
        )

        result = []
        for record in notes:
            node = record["n"]
            if hasattr(node, "items"):
                props = dict(node.items())
            elif isinstance(node, dict):
                props = dict(node)
            else:
                continue

            note_id = props.get("id")
            props["tags"] = record.get("tags", [])
            props["keywords"] = record.get("keywords", [])

            # Get outgoing relationships
            rels = self._db.execute_query(
                """
                MATCH (n:Note {project_id: $pid, id: $nid})-[r]->(m:Note)
                RETURN type(r) AS rel_type, m.id AS target_id
                """,
                {"pid": project_id, "nid": note_id},
            )
            props["relationships"] = [
                {"type": r["rel_type"], "target": r["target_id"]} for r in rels
            ]

            result.append(props)

        return result

    # =========================================================================
    # Cleanup
    # =========================================================================

    def delete_project_knowledge(self, project_id: str) -> int:
        """Delete all knowledge nodes for a project.

        Used by orchestrator's delete_project() path.

        Args:
            project_id: Project UUID

        Returns:
            Number of nodes deleted
        """
        result = self._db.execute_write(
            """
            MATCH (n {project_id: $pid})
            WITH n, count(n) AS cnt
            DETACH DELETE n
            RETURN cnt
            """,
            {"pid": project_id},
        )
        count = result[0]["cnt"] if result else 0
        logger.info(f"Deleted {count} knowledge nodes for project {project_id}")
        return count

    def get_note_content_hash(self, content: str) -> str:
        """Generate SHA-256 hash of note content."""
        return hashlib.sha256(content.encode()).hexdigest()


__all__ = [
    "KnowledgeGraphDB",
    "slugify",
    "NOTE_TYPES",
    "NOTE_STATUSES",
    "CONFIDENCE_LEVELS",
    "RELATIONSHIP_TYPES",
]
