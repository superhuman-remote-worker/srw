import { Injectable, signal, PLATFORM_ID, inject } from '@angular/core';
import { isPlatformBrowser } from '@angular/common';
import Dexie, { Table } from 'dexie';
import { AuditEntry } from '../models/audit.model';
import { ChatEntry } from '../models/chat.model';
import { GraphDelta } from '../../workbench/graph.model';
import {
  CachedAuditEntry,
  CachedChatEntry,
  CachedGraphDelta,
  CachedThreadMessage,
  JobCacheMetadata,
  ThreadCacheEpoch,
  ThreadCursor,
} from '../models/cache.model';

/** Current cache schema version */
const CACHE_VERSION = 4;

export function compareThreadHistoryFence(
  current: Pick<ThreadCacheEpoch, 'eventsEpoch' | 'conversationRevision'> | null,
  incoming: Pick<ThreadCacheEpoch, 'eventsEpoch' | 'conversationRevision'>,
): 'replace' | 'merge' | 'discard' {
  if (!current) return 'replace';
  if (
    incoming.eventsEpoch < current.eventsEpoch ||
    incoming.conversationRevision < current.conversationRevision
  ) {
    return 'discard';
  }
  if (
    incoming.eventsEpoch > current.eventsEpoch ||
    incoming.conversationRevision > current.conversationRevision
  ) {
    return 'replace';
  }
  return 'merge';
}

/**
 * Dexie database class for cockpit cache.
 * Defines tables and indexes for efficient querying.
 */
class CockpitDatabase extends Dexie {
  auditEntries!: Table<CachedAuditEntry>;
  chatEntries!: Table<CachedChatEntry>;
  graphDeltas!: Table<CachedGraphDelta>;
  jobMetadata!: Table<JobCacheMetadata>;
  threadCursors!: Table<ThreadCursor>;
  threadMessages!: Table<CachedThreadMessage>;

  constructor() {
    super('cockpit-cache');
    this.version(1).stores({
      auditEntries: 'id, jobId, [jobId+index], [jobId+stepType+index]',
      chatEntries: 'id, jobId, [jobId+sequenceNumber]',
      graphDeltas: 'id, jobId, [jobId+index]',
      jobMetadata: 'jobId',
    });
    this.version(2).stores({
      // Primary key: id, indexes: jobId, compound [jobId+index], compound [jobId+stepType+index]
      auditEntries: 'id, jobId, [jobId+index], [jobId+stepType+index]',
      // Primary key: id (MongoDB _id), indexes: jobId, compound [jobId+timestamp]
      chatEntries: 'id, jobId, [jobId+timestamp]',
      // Primary key: id, indexes: jobId, compound [jobId+index]
      graphDeltas: 'id, jobId, [jobId+index]',
      // Primary key: jobId
      jobMetadata: 'jobId',
    });
    this.version(3).stores({
      // v2 tables unchanged.
      auditEntries: 'id, jobId, [jobId+index], [jobId+stepType+index]',
      chatEntries: 'id, jobId, [jobId+timestamp]',
      graphDeltas: 'id, jobId, [jobId+index]',
      jobMetadata: 'jobId',
      // New: SSE replay cursors keyed by threadId, used by PersistentChatService.
      threadCursors: 'threadId',
    });
    this.version(4).stores({
      // v3 tables unchanged.
      auditEntries: 'id, jobId, [jobId+index], [jobId+stepType+index]',
      chatEntries: 'id, jobId, [jobId+timestamp]',
      graphDeltas: 'id, jobId, [jobId+index]',
      jobMetadata: 'jobId',
      threadCursors: 'threadId',
      // New: full per-thread message cache for the persistent-chat display.
      threadMessages: 'id, threadId, [threadId+createdAt]',
    });
  }
}

/**
 * Version-fenced persistent-session history. Keeping this in a distinct
 * database prevents an already-open pre-rewind Cockpit build from writing
 * unversioned rows into the authoritative cache used by this build.
 */
class ThreadHistoryDatabase extends Dexie {
  threadCursors!: Table<ThreadCursor>;
  threadMessages!: Table<CachedThreadMessage>;
  threadEpochs!: Table<ThreadCacheEpoch>;

  constructor() {
    super('srw-thread-history-v2');
    this.version(1).stores({
      threadCursors: 'threadId',
      threadMessages: 'id, threadId, [threadId+createdAt]',
      threadEpochs: 'threadId',
    });
  }
}

/**
 * IndexedDB caching service using Dexie.js.
 * Provides client-side storage for audit entries, chat history, and graph deltas.
 *
 * Usage:
 * - Call cacheAuditEntries() to store entries from API response
 * - Call getAuditEntries() to retrieve cached entries by index range
 * - Use getJobMetadata() to check cache state before fetching from API
 */
@Injectable({ providedIn: 'root' })
export class IndexedDbService {
  private db: CockpitDatabase | null = null;
  private historyDb: ThreadHistoryDatabase | null = null;
  private readonly platformId = inject(PLATFORM_ID);
  private readonly isBrowser: boolean;
  private historyChannel: BroadcastChannel | null = null;

  /** Best-effort wake-up edge; the database fence remains authoritative. */
  readonly threadHistoryEpochChanged = signal<ThreadCacheEpoch | null>(null);

  /** Whether the database is ready for operations */
  readonly isReady = signal(false);

  /** Error message if initialization failed */
  readonly error = signal<string | null>(null);

  constructor() {
    this.isBrowser = isPlatformBrowser(this.platformId);
    if (this.isBrowser) {
      this.db = new CockpitDatabase();
      this.historyDb = new ThreadHistoryDatabase();
      if (typeof BroadcastChannel !== 'undefined') {
        this.historyChannel = new BroadcastChannel('srw-thread-history-v2');
        this.historyChannel.onmessage = (event) => {
          const value = event.data as ThreadCacheEpoch | null;
          if (value?.threadId && Number.isInteger(value.eventsEpoch)) {
            this.threadHistoryEpochChanged.set(value);
          }
        };
      }
      this.init();
    }
    // On server, db stays null and isReady stays false - that's fine for SSR
  }

  private async init(): Promise<void> {
    if (!this.db || !this.historyDb) return;
    try {
      await Promise.all([this.db.open(), this.historyDb.open()]);
      this.isReady.set(true);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.error.set(`IndexedDB initialization failed: ${message}`);
      console.error('IndexedDB initialization failed:', err);
    }
  }

  // ===== Job Metadata =====

  /**
   * Get cache metadata for a job.
   */
  async getJobMetadata(jobId: string): Promise<JobCacheMetadata | undefined> {
    if (!this.db) return undefined;
    return this.db.jobMetadata.get(jobId);
  }

  /**
   * Set or update cache metadata for a job.
   */
  async setJobMetadata(metadata: JobCacheMetadata): Promise<void> {
    if (!this.db) return;
    await this.db.jobMetadata.put(metadata);
  }

  /**
   * Check if a job has cached data.
   */
  async hasJob(jobId: string): Promise<boolean> {
    if (!this.db) return false;
    const metadata = await this.db.jobMetadata.get(jobId);
    return metadata !== undefined;
  }

  // ===== Audit Entries =====

  /**
   * Cache audit entries for a job.
   * Entries are indexed by step_number for efficient range queries.
   *
   * @param jobId The job ID
   * @param entries Audit entries to cache (must have step_number)
   * @param startIndex Starting index for these entries (for incremental caching)
   */
  async cacheAuditEntries(
    jobId: string,
    entries: AuditEntry[],
    startIndex: number = 0,
  ): Promise<void> {
    if (!this.db) return;
    const cached: CachedAuditEntry[] = entries.map((entry, i) => ({
      id: `${jobId}_${startIndex + i}`,
      jobId,
      index: startIndex + i,
      timestamp: entry.timestamp,
      stepType: entry.step_type,
      data: entry,
    }));

    await this.db.auditEntries.bulkPut(cached);

    // Update metadata
    await this.updateAuditMetadata(jobId, cached);
  }

  /**
   * Get audit entries by index range (inclusive).
   */
  async getAuditEntries(
    jobId: string,
    startIndex: number,
    endIndex: number,
  ): Promise<AuditEntry[]> {
    if (!this.db) return [];
    const entries = await this.db.auditEntries
      .where('[jobId+index]')
      .between([jobId, startIndex], [jobId, endIndex], true, true)
      .toArray();

    return entries.map((e) => e.data);
  }

  /**
   * Get audit entries filtered by step type within an index range.
   */
  async getAuditEntriesByType(
    jobId: string,
    stepType: string,
    startIndex: number,
    endIndex: number,
  ): Promise<AuditEntry[]> {
    if (!this.db) return [];
    // Use compound index for efficient filtering
    const entries = await this.db.auditEntries
      .where('[jobId+stepType+index]')
      .between([jobId, stepType, startIndex], [jobId, stepType, endIndex], true, true)
      .toArray();

    return entries.map((e) => e.data);
  }

  /**
   * Get the count of cached audit entries for a job.
   */
  async getAuditEntryCount(jobId: string): Promise<number> {
    if (!this.db) return 0;
    return this.db.auditEntries.where('jobId').equals(jobId).count();
  }

  private async updateAuditMetadata(
    jobId: string,
    newEntries: CachedAuditEntry[],
  ): Promise<void> {
    if (!this.db || newEntries.length === 0) return;

    const existing = await this.db.jobMetadata.get(jobId);
    const count = await this.getAuditEntryCount(jobId);

    const timestamps = newEntries.map((e) => e.timestamp).sort();
    const firstNew = timestamps[0];
    const lastNew = timestamps[timestamps.length - 1];

    const metadata: JobCacheMetadata = {
      jobId,
      auditEntryCount: count,
      chatEntryCount: existing?.chatEntryCount ?? 0,
      graphDeltaCount: existing?.graphDeltaCount ?? 0,
      firstTimestamp: this.minTimestamp(existing?.firstTimestamp, firstNew),
      lastTimestamp: this.maxTimestamp(existing?.lastTimestamp, lastNew),
      cachedAt: new Date().toISOString(),
      version: CACHE_VERSION,
    };

    await this.db.jobMetadata.put(metadata);
  }

  // ===== Chat Entries =====

  /**
   * Cache chat entries for a job.
   */
  async cacheChatEntries(jobId: string, entries: ChatEntry[]): Promise<void> {
    if (!this.db) return;
    const cached: CachedChatEntry[] = entries.map((entry) => ({
      id: entry._id,
      jobId,
      timestamp: entry.timestamp,
      data: entry,
    }));

    await this.db.chatEntries.bulkPut(cached);

    // Update metadata
    await this.updateChatMetadata(jobId, cached);
  }

  /**
   * Get all chat entries for a job, ordered by timestamp.
   */
  async getChatEntries(jobId: string): Promise<ChatEntry[]> {
    if (!this.db) return [];
    const entries = await this.db.chatEntries
      .where('[jobId+timestamp]')
      .between([jobId, Dexie.minKey], [jobId, Dexie.maxKey], true, true)
      .toArray();

    return entries.map((e) => e.data);
  }

  /**
   * Get the count of cached chat entries for a job.
   */
  async getChatEntryCount(jobId: string): Promise<number> {
    if (!this.db) return 0;
    return this.db.chatEntries.where('jobId').equals(jobId).count();
  }

  private async updateChatMetadata(
    jobId: string,
    newEntries: CachedChatEntry[],
  ): Promise<void> {
    if (!this.db || newEntries.length === 0) return;

    const existing = await this.db.jobMetadata.get(jobId);
    const count = await this.getChatEntryCount(jobId);

    const timestamps = newEntries.map((e) => e.timestamp).sort();
    const firstNew = timestamps[0];
    const lastNew = timestamps[timestamps.length - 1];

    const metadata: JobCacheMetadata = {
      jobId,
      auditEntryCount: existing?.auditEntryCount ?? 0,
      chatEntryCount: count,
      graphDeltaCount: existing?.graphDeltaCount ?? 0,
      firstTimestamp: this.minTimestamp(existing?.firstTimestamp, firstNew),
      lastTimestamp: this.maxTimestamp(existing?.lastTimestamp, lastNew),
      cachedAt: new Date().toISOString(),
      version: CACHE_VERSION,
    };

    await this.db.jobMetadata.put(metadata);
  }

  // ===== Graph Deltas =====

  /**
   * Cache graph deltas for a job.
   */
  async cacheGraphDeltas(jobId: string, deltas: GraphDelta[]): Promise<void> {
    if (!this.db) return;
    const cached: CachedGraphDelta[] = deltas.map((delta) => ({
      id: `${jobId}_${delta.toolCallIndex}`,
      jobId,
      index: delta.toolCallIndex,
      timestamp: delta.timestamp,
      data: delta,
    }));

    await this.db.graphDeltas.bulkPut(cached);

    // Update metadata
    await this.updateGraphMetadata(jobId, cached);
  }

  /**
   * Get graph deltas by index range (inclusive).
   */
  async getGraphDeltas(
    jobId: string,
    startIndex: number,
    endIndex: number,
  ): Promise<GraphDelta[]> {
    if (!this.db) return [];
    const entries = await this.db.graphDeltas
      .where('[jobId+index]')
      .between([jobId, startIndex], [jobId, endIndex], true, true)
      .toArray();

    return entries.map((e) => e.data);
  }

  /**
   * Get the count of cached graph deltas for a job.
   */
  async getGraphDeltaCount(jobId: string): Promise<number> {
    if (!this.db) return 0;
    return this.db.graphDeltas.where('jobId').equals(jobId).count();
  }

  private async updateGraphMetadata(
    jobId: string,
    newEntries: CachedGraphDelta[],
  ): Promise<void> {
    if (!this.db || newEntries.length === 0) return;

    const existing = await this.db.jobMetadata.get(jobId);
    const count = await this.getGraphDeltaCount(jobId);

    const timestamps = newEntries.map((e) => e.timestamp).sort();
    const firstNew = timestamps[0];
    const lastNew = timestamps[timestamps.length - 1];

    const metadata: JobCacheMetadata = {
      jobId,
      auditEntryCount: existing?.auditEntryCount ?? 0,
      chatEntryCount: existing?.chatEntryCount ?? 0,
      graphDeltaCount: count,
      firstTimestamp: this.minTimestamp(existing?.firstTimestamp, firstNew),
      lastTimestamp: this.maxTimestamp(existing?.lastTimestamp, lastNew),
      cachedAt: new Date().toISOString(),
      version: CACHE_VERSION,
    };

    await this.db.jobMetadata.put(metadata);
  }

  // ===== Thread Cursors (SSE replay) =====

  /**
   * Look up the SSE replay cursor for a thread. Returns `null` when the
   * cockpit hasn't seen this thread yet — caller should open the SSE stream
   * without a `Last-Event-ID` header in that case.
   */
  async getThreadCursor(threadId: string): Promise<ThreadCursor | null> {
    if (!this.historyDb) return null;
    const row = await this.historyDb.threadCursors.get(threadId);
    return row ?? null;
  }

  /**
   * Upsert the cursor. Called for every event yielded by the SSE stream;
   * cheap on Dexie (single-row put) and keyed by threadId so we never
   * grow the table beyond the number of distinct threads the user has
   * viewed.
   */
  async setThreadCursor(threadId: string, epoch: number, seq: number): Promise<void> {
    if (!this.historyDb) return;
    await this.historyDb.transaction('rw', this.historyDb.threadCursors, async () => {
      const current = await this.historyDb!.threadCursors.get(threadId);
      if (current && (epoch < current.epoch || (epoch === current.epoch && seq <= current.seq))) {
        return;
      }
      await this.historyDb!.threadCursors.put({
        threadId,
        epoch,
        seq,
        updatedAt: new Date().toISOString(),
      });
    });
  }

  /**
   * Drop the cursor. Used when the server emits `gone_beyond_horizon` —
   * the cockpit must reload via REST snapshot and re-subscribe with a
   * fresh stream rather than insist on the stale cursor.
   */
  async deleteThreadCursor(threadId: string): Promise<void> {
    if (!this.historyDb) return;
    await this.historyDb.threadCursors.delete(threadId);
  }

  // ===== Thread Messages (full conversation cache) =====

  /**
   * All cached messages for a thread, ascending by `created_at`. Empty when the
   * thread hasn't been cached yet. Feed straight into `historyToTurns`.
   */
  async getThreadMessages(threadId: string): Promise<CachedThreadMessage[]> {
    if (!this.db || !this.historyDb) return [];
    const epoch = await this.historyDb.threadEpochs.get(threadId);
    const table = epoch ? this.historyDb.threadMessages : this.db.threadMessages;
    return table
      .where('[threadId+createdAt]')
      .between([threadId, Dexie.minKey], [threadId, Dexie.maxKey], true, true)
      .toArray();
  }

  /**
   * The newest cached `created_at` for a thread, or `null` when nothing is
   * cached. Used as the `?after=` cursor to fetch only what we've missed.
   */
  async getNewestCachedCreatedAt(threadId: string): Promise<string | null> {
    if (!this.db || !this.historyDb) return null;
    const epoch = await this.historyDb.threadEpochs.get(threadId);
    const table = epoch ? this.historyDb.threadMessages : this.db.threadMessages;
    const row = await table
      .where('[threadId+createdAt]')
      .between([threadId, Dexie.minKey], [threadId, Dexie.maxKey], true, true)
      .last();
    return row?.created_at ?? null;
  }

  /**
   * Upsert messages by `id` (append-only in practice — never full-replace,
   * which would lose history). Idempotent: re-receiving a row overwrites it.
   */
  async upsertThreadMessages(rows: CachedThreadMessage[]): Promise<void> {
    if (!this.db || !this.historyDb || rows.length === 0) return;
    // Unversioned responses stay in the legacy database. Once a thread has
    // observed the versioned contract they can no longer poison or downgrade
    // its isolated v2 cache.
    if (await this.historyDb.threadEpochs.get(rows[0].threadId)) return;
    await this.db.threadMessages.bulkPut(rows);
  }

  /** Current authoritative cache floor, if this tab has observed v2 history. */
  async getThreadCacheEpoch(threadId: string): Promise<ThreadCacheEpoch | null> {
    if (!this.historyDb) return null;
    return (await this.historyDb.threadEpochs.get(threadId)) ?? null;
  }

  /**
   * Merge one snapshot-consistent history response under its revision fence.
   * A newer epoch/revision atomically replaces rows and the replay cursor; an
   * equal fence merges by id; an older completion is discarded.
   */
  async applyThreadHistoryPage(
    threadId: string,
    eventsEpoch: number,
    conversationRevision: number,
    rows: CachedThreadMessage[],
  ): Promise<{ accepted: boolean; replaced: boolean; messages: CachedThreadMessage[] }> {
    if (!this.historyDb) return { accepted: true, replaced: false, messages: rows };
    const result = await this.historyDb.transaction(
      'rw',
      this.historyDb.threadEpochs,
      this.historyDb.threadMessages,
      this.historyDb.threadCursors,
      async () => {
        const current = await this.historyDb!.threadEpochs.get(threadId);
        const fence = compareThreadHistoryFence(current ?? null, {
          eventsEpoch,
          conversationRevision,
        });
        if (fence === 'discard') {
          const messages = await this.historyDb!.threadMessages
            .where('[threadId+createdAt]')
            .between([threadId, Dexie.minKey], [threadId, Dexie.maxKey], true, true)
            .toArray();
          return { accepted: false, replaced: false, messages };
        }
        const replaced = fence === 'replace';
        if (replaced) {
          await this.historyDb!.threadMessages.where('threadId').equals(threadId).delete();
          await this.historyDb!.threadCursors.delete(threadId);
        }
        await this.historyDb!.threadEpochs.put({
          threadId,
          eventsEpoch,
          conversationRevision,
          updatedAt: new Date().toISOString(),
        });
        if (rows.length) await this.historyDb!.threadMessages.bulkPut(rows);
        const messages = await this.historyDb!.threadMessages
          .where('[threadId+createdAt]')
          .between([threadId, Dexie.minKey], [threadId, Dexie.maxKey], true, true)
          .toArray();
        return { accepted: true, replaced, messages };
      },
    );
    if (result.accepted && result.replaced) {
      const epoch = await this.historyDb.threadEpochs.get(threadId);
      if (epoch) {
        this.threadHistoryEpochChanged.set(epoch);
        this.historyChannel?.postMessage(epoch);
      }
    }
    return result;
  }

  /** Drop a thread's cached messages (e.g. manual cache reset). */
  async clearThreadMessages(threadId: string): Promise<void> {
    if (!this.db || !this.historyDb) return;
    await Promise.all([
      this.db.threadMessages.where('threadId').equals(threadId).delete(),
      this.historyDb.threadMessages.where('threadId').equals(threadId).delete(),
    ]);
  }

  // ===== Cache Management =====

  /**
   * Clear all cached data for a specific job.
   */
  async clearJob(jobId: string): Promise<void> {
    if (!this.db) return;
    await Promise.all([
      this.db.auditEntries.where('jobId').equals(jobId).delete(),
      this.db.chatEntries.where('jobId').equals(jobId).delete(),
      this.db.graphDeltas.where('jobId').equals(jobId).delete(),
      this.db.jobMetadata.delete(jobId),
    ]);
  }

  /**
   * Clear all cached data.
   */
  async clearAll(): Promise<void> {
    if (!this.db) return;
    await Promise.all([
      this.db.auditEntries.clear(),
      this.db.chatEntries.clear(),
      this.db.graphDeltas.clear(),
      this.db.jobMetadata.clear(),
      this.db.threadCursors.clear(),
      this.db.threadMessages.clear(),
      this.historyDb?.threadCursors.clear(),
      this.historyDb?.threadMessages.clear(),
      this.historyDb?.threadEpochs.clear(),
    ]);
  }

  /**
   * Get storage usage estimate.
   * Returns usage and quota in bytes.
   */
  async getStorageEstimate(): Promise<{ usage: number; quota: number }> {
    if (!this.isBrowser) return { usage: 0, quota: 0 };
    if (navigator.storage && navigator.storage.estimate) {
      const estimate = await navigator.storage.estimate();
      return {
        usage: estimate.usage ?? 0,
        quota: estimate.quota ?? 0,
      };
    }
    return { usage: 0, quota: 0 };
  }

  // ===== Helpers =====

  private minTimestamp(a: string | null | undefined, b: string): string {
    if (!a) return b;
    return a < b ? a : b;
  }

  private maxTimestamp(a: string | null | undefined, b: string): string {
    if (!a) return b;
    return a > b ? a : b;
  }
}
