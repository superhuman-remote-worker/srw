/**
 * Cache-specific models for IndexedDB storage.
 * These wrap the existing domain models with job context and indexing fields.
 */

import { AuditEntry } from './audit.model';
import { ChatEntry } from './chat.model';
import { GraphDelta } from '../../workbench/graph.model';

/**
 * Cached audit entry with job context and index for efficient retrieval.
 */
export interface CachedAuditEntry {
  /** Composite key: `${jobId}_${index}` */
  id: string;
  jobId: string;
  /** Sequential index within the job (based on step_number) */
  index: number;
  timestamp: string;
  stepType: string;
  /** The original audit entry data */
  data: AuditEntry;
}

/**
 * Cached chat entry with job context and timestamp ordering.
 */
export interface CachedChatEntry {
  /** MongoDB _id (unique per entry) */
  id: string;
  jobId: string;
  timestamp: string;
  /** The original chat entry data */
  data: ChatEntry;
}

/**
 * Cached graph delta with job context and index.
 */
export interface CachedGraphDelta {
  /** Composite key: `${jobId}_${index}` */
  id: string;
  jobId: string;
  /** Index based on toolCallIndex */
  index: number;
  timestamp: string;
  /** The original graph delta data */
  data: GraphDelta;
}

/**
 * Metadata about cached data for a job.
 * Used for cache validation and incremental updates.
 */
export interface JobCacheMetadata {
  jobId: string;
  auditEntryCount: number;
  chatEntryCount: number;
  graphDeltaCount: number;
  /** Earliest timestamp in cached data */
  firstTimestamp: string | null;
  /** Latest timestamp in cached data */
  lastTimestamp: string | null;
  /** When this cache was last updated */
  cachedAt: string;
  /** Cache version for migration support */
  version: number;
}

/**
 * Persistent-thread SSE replay cursor. Carried as `Last-Event-ID: <epoch>:<seq>`
 * on reconnect so the orchestrator can pick up from where the cockpit left off,
 * surviving full-tab close. Bumped each time we yield an event from the
 * server-sent stream; cleared on `gone_beyond_horizon`.
 */
export interface ThreadCursor {
  /** Thread UUID — primary key. */
  threadId: string;
  /** Event epoch (bumps on server-side rebuild — cold-checkpoint restart). */
  epoch: number;
  /** Highest seq we've successfully observed inside this epoch. */
  seq: number;
  /** ISO timestamp of the last cursor update — useful for stale-cursor cleanup. */
  updatedAt: string;
}

/** Persisted authority floor for the isolated v2 thread-history cache. */
export interface ThreadCacheEpoch {
  /** Thread UUID — primary key. */
  threadId: string;
  /** Event epoch returned with the same snapshot as the cached rows. */
  eventsEpoch: number;
  /** Transcript revision returned with the same snapshot as the cached rows. */
  conversationRevision: number;
  updatedAt: string;
}

/**
 * Cached persistent-thread message — mirrors the orchestrator's
 * `thread_messages` display row plus the owning `threadId`. The full
 * conversation is cached so the cockpit can paint instantly on reopen and
 * catch up on reconnect via an `?after=` cursor instead of re-downloading the
 * whole thread. Keyed by message `id`; upsert-by-id keeps the cache a
 * contiguous prefix (never full-replaced — that would lose history).
 */
export interface CachedThreadMessage {
  /** thread_messages.id — primary key, unique per message. */
  id: string;
  /** Owning thread UUID — index for per-thread queries. */
  threadId: string;
  role: string;
  content: string | null;
  tool_calls:
    | {
          name: string;
          args: Record<string, unknown>;
          id: string;
          // Raw backend status. Everything except 'denied' is a gate that
          // was never answered — 'expired' (TTL, or swept at turn end),
          // 'interrupted' (Stop), 'unavailable' (the wait itself failed).
          // Persisted distinctly so a reload can render neither a refusal
          // the user never made nor a call that never ran as completed.
          decision?: string;
      }[]
    | null;
  turn_number: number | null;
  tool_call_id?: string | null;
  thinking?: string | null;
  /** ISO-8601; lexicographically sortable — the `[threadId+createdAt]` index. */
  created_at: string | null;
}
