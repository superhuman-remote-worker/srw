import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import Dexie from 'dexie';
import { AuditEntry, AuditStepType } from '../models/audit.model';
import { ChatEntry, ChatInput, ChatResponse } from '../models/chat.model';
import { GraphDelta, GraphChanges } from '../../workbench/graph.model';
import { compareThreadHistoryFence } from './indexed-db.service';

// Since IndexedDbService uses Angular's @Injectable and signal,
// we test the database logic directly using Dexie
// The Angular integration is tested separately via e2e tests

interface CachedAuditEntry {
  id: string;
  jobId: string;
  index: number;
  timestamp: string;
  stepType: string;
  data: AuditEntry;
}

interface CachedChatEntry {
  id: string;
  jobId: string;
  timestamp: string;
  data: ChatEntry;
}

interface CachedGraphDelta {
  id: string;
  jobId: string;
  index: number;
  timestamp: string;
  data: GraphDelta;
}

interface JobCacheMetadata {
  jobId: string;
  auditEntryCount: number;
  chatEntryCount: number;
  graphDeltaCount: number;
  firstTimestamp: string | null;
  lastTimestamp: string | null;
  cachedAt: string;
  version: number;
}

// Test database class matching the service's schema
class TestDatabase extends Dexie {
  auditEntries!: Dexie.Table<CachedAuditEntry>;
  chatEntries!: Dexie.Table<CachedChatEntry>;
  graphDeltas!: Dexie.Table<CachedGraphDelta>;
  jobMetadata!: Dexie.Table<JobCacheMetadata>;

  constructor(name: string) {
    super(name);
    this.version(1).stores({
      auditEntries: 'id, jobId, [jobId+index], [jobId+stepType+index]',
      chatEntries: 'id, jobId, [jobId+timestamp]',
      graphDeltas: 'id, jobId, [jobId+index]',
      jobMetadata: 'jobId',
    });
  }
}

// Factory functions for test data
function createMockAuditEntry(overrides: Partial<AuditEntry> = {}): AuditEntry {
  return {
    _id: `audit_${Math.random().toString(36).slice(2)}`,
    job_id: 'test-job-1',
    step_number: 1,
    step_type: 'llm' as AuditStepType,
    node_name: 'execute',
    timestamp: new Date().toISOString(),
    iteration: 1,
    ...overrides,
  };
}

function createMockChatEntry(overrides: Partial<ChatEntry> = {}): ChatEntry {
  const inputs: ChatInput[] = [
    {
      type: 'human',
      content: 'Test message',
      content_preview: 'Test message',
    },
  ];
  const response: ChatResponse = {
    content: 'Test response',
    content_preview: 'Test response',
    has_tool_calls: false,
  };

  return {
    _id: `chat_${Math.random().toString(36).slice(2)}`,
    job_id: 'test-job-1',
    agent_type: 'creator',
    timestamp: new Date().toISOString(),
    iteration: 1,
    model: 'gpt-4',
    inputs,
    response,
    ...overrides,
  };
}

function createMockGraphDelta(overrides: Partial<GraphDelta> = {}): GraphDelta {
  const changes: GraphChanges = {
    nodesCreated: [],
    nodesDeleted: [],
    nodesModified: [],
    relationshipsCreated: [],
    relationshipsDeleted: [],
  };

  return {
    timestamp: new Date().toISOString(),
    toolCallIndex: 0,
    cypherQuery: 'CREATE (n:Test)',
    toolCallId: `tool_${Math.random().toString(36).slice(2)}`,
    changes,
    ...overrides,
  };
}

describe('IndexedDB Schema', () => {
  let db: TestDatabase;
  let dbName: string;

  beforeEach(async () => {
    // Use unique database name to avoid conflicts between tests
    dbName = `test-db-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    db = new TestDatabase(dbName);
    await db.open();
  });

  afterEach(async () => {
    await db.close();
    await Dexie.delete(dbName);
  });

  describe('Database Initialization', () => {
    it('should create all required tables', () => {
      expect(db.auditEntries).toBeDefined();
      expect(db.chatEntries).toBeDefined();
      expect(db.graphDeltas).toBeDefined();
      expect(db.jobMetadata).toBeDefined();
    });

    it('should have correct schema version', () => {
      expect(db.verno).toBe(1);
    });
  });

  describe('Audit Entries CRUD', () => {
    const jobId = 'test-job-1';

    it('should store and retrieve audit entries', async () => {
      const entry = createMockAuditEntry({ job_id: jobId, step_number: 1 });
      const cached: CachedAuditEntry = {
        id: `${jobId}_0`,
        jobId,
        index: 0,
        timestamp: entry.timestamp,
        stepType: entry.step_type,
        data: entry,
      };

      await db.auditEntries.put(cached);

      const retrieved = await db.auditEntries.get(`${jobId}_0`);
      expect(retrieved).toBeDefined();
      expect(retrieved!.data.step_number).toBe(1);
      expect(retrieved!.jobId).toBe(jobId);
    });

    it('should bulk insert audit entries', async () => {
      const entries = Array.from({ length: 10 }, (_, i) =>
        createMockAuditEntry({ job_id: jobId, step_number: i })
      );
      const cached: CachedAuditEntry[] = entries.map((entry, i) => ({
        id: `${jobId}_${i}`,
        jobId,
        index: i,
        timestamp: entry.timestamp,
        stepType: entry.step_type,
        data: entry,
      }));

      await db.auditEntries.bulkPut(cached);

      const count = await db.auditEntries.where('jobId').equals(jobId).count();
      expect(count).toBe(10);
    });

    it('should query audit entries by index range', async () => {
      // Create entries with indices 0-9
      const entries = Array.from({ length: 10 }, (_, i) =>
        createMockAuditEntry({ job_id: jobId, step_number: i })
      );
      const cached: CachedAuditEntry[] = entries.map((entry, i) => ({
        id: `${jobId}_${i}`,
        jobId,
        index: i,
        timestamp: entry.timestamp,
        stepType: entry.step_type,
        data: entry,
      }));
      await db.auditEntries.bulkPut(cached);

      // Query indices 3-7 (inclusive)
      const results = await db.auditEntries
        .where('[jobId+index]')
        .between([jobId, 3], [jobId, 7], true, true)
        .toArray();

      expect(results.length).toBe(5);
      expect(results[0].index).toBe(3);
      expect(results[4].index).toBe(7);
    });

    it('should query audit entries by step type', async () => {
      const llmEntry = createMockAuditEntry({ job_id: jobId, step_type: 'llm', step_number: 0 });
      const toolEntry = createMockAuditEntry({ job_id: jobId, step_type: 'tool', step_number: 1 });
      const errorEntry = createMockAuditEntry({ job_id: jobId, step_type: 'error', step_number: 2 });

      await db.auditEntries.bulkPut([
        { id: `${jobId}_0`, jobId, index: 0, timestamp: llmEntry.timestamp, stepType: 'llm', data: llmEntry },
        { id: `${jobId}_1`, jobId, index: 1, timestamp: toolEntry.timestamp, stepType: 'tool', data: toolEntry },
        { id: `${jobId}_2`, jobId, index: 2, timestamp: errorEntry.timestamp, stepType: 'error', data: errorEntry },
      ]);

      const llmResults = await db.auditEntries
        .where('[jobId+stepType+index]')
        .between([jobId, 'llm', 0], [jobId, 'llm', 10], true, true)
        .toArray();

      expect(llmResults.length).toBe(1);
      expect(llmResults[0].stepType).toBe('llm');
    });

    it('should delete audit entries for a job', async () => {
      const entries = Array.from({ length: 5 }, (_, i) =>
        createMockAuditEntry({ job_id: jobId, step_number: i })
      );
      const cached: CachedAuditEntry[] = entries.map((entry, i) => ({
        id: `${jobId}_${i}`,
        jobId,
        index: i,
        timestamp: entry.timestamp,
        stepType: entry.step_type,
        data: entry,
      }));
      await db.auditEntries.bulkPut(cached);

      await db.auditEntries.where('jobId').equals(jobId).delete();

      const count = await db.auditEntries.where('jobId').equals(jobId).count();
      expect(count).toBe(0);
    });
  });

  describe('Chat Entries CRUD', () => {
    const jobId = 'test-job-1';

    it('should store and retrieve chat entries', async () => {
      const entry = createMockChatEntry({ job_id: jobId });
      const cached: CachedChatEntry = {
        id: entry._id,
        jobId,
        timestamp: entry.timestamp,
        data: entry,
      };

      await db.chatEntries.put(cached);

      const retrieved = await db.chatEntries.get(entry._id);
      expect(retrieved).toBeDefined();
      expect(retrieved!.data.job_id).toBe(jobId);
    });

    it('should query chat entries by timestamp range', async () => {
      const baseTime = Date.now();
      const entries = Array.from({ length: 10 }, (_, i) =>
        createMockChatEntry({
          job_id: jobId,
          _id: `chat_${i}`,
          timestamp: new Date(baseTime + i * 1000).toISOString(),
        })
      );
      const cached: CachedChatEntry[] = entries.map((entry) => ({
        id: entry._id,
        jobId,
        timestamp: entry.timestamp,
        data: entry,
      }));
      await db.chatEntries.bulkPut(cached);

      // Query entries between timestamp[2] and timestamp[5]
      const results = await db.chatEntries
        .where('[jobId+timestamp]')
        .between(
          [jobId, entries[2].timestamp],
          [jobId, entries[5].timestamp],
          true, true,
        )
        .toArray();

      expect(results.length).toBe(4);
      expect(results[0].timestamp).toBe(entries[2].timestamp);
      expect(results[3].timestamp).toBe(entries[5].timestamp);
    });
  });

  describe('Graph Deltas CRUD', () => {
    const jobId = 'test-job-1';

    it('should store and retrieve graph deltas', async () => {
      const delta = createMockGraphDelta({ toolCallIndex: 0 });
      const cached: CachedGraphDelta = {
        id: `${jobId}_0`,
        jobId,
        index: 0,
        timestamp: delta.timestamp,
        data: delta,
      };

      await db.graphDeltas.put(cached);

      const retrieved = await db.graphDeltas.get(`${jobId}_0`);
      expect(retrieved).toBeDefined();
      expect(retrieved!.data.cypherQuery).toBe('CREATE (n:Test)');
    });

    it('should query graph deltas by index range', async () => {
      const deltas = Array.from({ length: 5 }, (_, i) =>
        createMockGraphDelta({ toolCallIndex: i })
      );
      const cached: CachedGraphDelta[] = deltas.map((delta) => ({
        id: `${jobId}_${delta.toolCallIndex}`,
        jobId,
        index: delta.toolCallIndex,
        timestamp: delta.timestamp,
        data: delta,
      }));
      await db.graphDeltas.bulkPut(cached);

      const results = await db.graphDeltas
        .where('[jobId+index]')
        .between([jobId, 1], [jobId, 3], true, true)
        .toArray();

      expect(results.length).toBe(3);
    });
  });

  describe('Job Metadata', () => {
    const jobId = 'test-job-1';

    it('should store and retrieve job metadata', async () => {
      const metadata: JobCacheMetadata = {
        jobId,
        auditEntryCount: 100,
        chatEntryCount: 50,
        graphDeltaCount: 25,
        firstTimestamp: '2024-01-01T00:00:00Z',
        lastTimestamp: '2024-01-01T12:00:00Z',
        cachedAt: new Date().toISOString(),
        version: 1,
      };

      await db.jobMetadata.put(metadata);

      const retrieved = await db.jobMetadata.get(jobId);
      expect(retrieved).toBeDefined();
      expect(retrieved!.auditEntryCount).toBe(100);
      expect(retrieved!.chatEntryCount).toBe(50);
      expect(retrieved!.graphDeltaCount).toBe(25);
    });

    it('should update job metadata', async () => {
      const metadata: JobCacheMetadata = {
        jobId,
        auditEntryCount: 10,
        chatEntryCount: 5,
        graphDeltaCount: 2,
        firstTimestamp: null,
        lastTimestamp: null,
        cachedAt: new Date().toISOString(),
        version: 1,
      };

      await db.jobMetadata.put(metadata);

      // Update with more entries
      metadata.auditEntryCount = 20;
      metadata.firstTimestamp = '2024-01-01T00:00:00Z';
      await db.jobMetadata.put(metadata);

      const retrieved = await db.jobMetadata.get(jobId);
      expect(retrieved!.auditEntryCount).toBe(20);
      expect(retrieved!.firstTimestamp).toBe('2024-01-01T00:00:00Z');
    });

    it('should check if job exists', async () => {
      const existsBefore = (await db.jobMetadata.get(jobId)) !== undefined;
      expect(existsBefore).toBe(false);

      await db.jobMetadata.put({
        jobId,
        auditEntryCount: 0,
        chatEntryCount: 0,
        graphDeltaCount: 0,
        firstTimestamp: null,
        lastTimestamp: null,
        cachedAt: new Date().toISOString(),
        version: 1,
      });

      const existsAfter = (await db.jobMetadata.get(jobId)) !== undefined;
      expect(existsAfter).toBe(true);
    });
  });

  describe('Cache Management', () => {
    const jobId1 = 'test-job-1';
    const jobId2 = 'test-job-2';

    beforeEach(async () => {
      // Seed data for two jobs
      await db.auditEntries.bulkPut([
        { id: `${jobId1}_0`, jobId: jobId1, index: 0, timestamp: '', stepType: 'llm', data: createMockAuditEntry({ job_id: jobId1 }) },
        { id: `${jobId1}_1`, jobId: jobId1, index: 1, timestamp: '', stepType: 'tool', data: createMockAuditEntry({ job_id: jobId1 }) },
        { id: `${jobId2}_0`, jobId: jobId2, index: 0, timestamp: '', stepType: 'llm', data: createMockAuditEntry({ job_id: jobId2 }) },
      ]);
      await db.chatEntries.bulkPut([
        { id: `${jobId1}_chat_0`, jobId: jobId1, timestamp: '2024-01-01T00:00:00Z', data: createMockChatEntry({ job_id: jobId1 }) },
        { id: `${jobId2}_chat_0`, jobId: jobId2, timestamp: '2024-01-01T00:00:00Z', data: createMockChatEntry({ job_id: jobId2 }) },
      ]);
      await db.jobMetadata.bulkPut([
        { jobId: jobId1, auditEntryCount: 2, chatEntryCount: 1, graphDeltaCount: 0, firstTimestamp: null, lastTimestamp: null, cachedAt: '', version: 1 },
        { jobId: jobId2, auditEntryCount: 1, chatEntryCount: 1, graphDeltaCount: 0, firstTimestamp: null, lastTimestamp: null, cachedAt: '', version: 1 },
      ]);
    });

    it('should clear data for a single job', async () => {
      // Clear job1 only
      await Promise.all([
        db.auditEntries.where('jobId').equals(jobId1).delete(),
        db.chatEntries.where('jobId').equals(jobId1).delete(),
        db.graphDeltas.where('jobId').equals(jobId1).delete(),
        db.jobMetadata.delete(jobId1),
      ]);

      // Job1 should be cleared
      const job1Audits = await db.auditEntries.where('jobId').equals(jobId1).count();
      const job1Chats = await db.chatEntries.where('jobId').equals(jobId1).count();
      const job1Meta = await db.jobMetadata.get(jobId1);
      expect(job1Audits).toBe(0);
      expect(job1Chats).toBe(0);
      expect(job1Meta).toBeUndefined();

      // Job2 should still exist
      const job2Audits = await db.auditEntries.where('jobId').equals(jobId2).count();
      const job2Chats = await db.chatEntries.where('jobId').equals(jobId2).count();
      const job2Meta = await db.jobMetadata.get(jobId2);
      expect(job2Audits).toBe(1);
      expect(job2Chats).toBe(1);
      expect(job2Meta).toBeDefined();
    });

    it('should clear all data', async () => {
      await Promise.all([
        db.auditEntries.clear(),
        db.chatEntries.clear(),
        db.graphDeltas.clear(),
        db.jobMetadata.clear(),
      ]);

      const totalAudits = await db.auditEntries.count();
      const totalChats = await db.chatEntries.count();
      const totalDeltas = await db.graphDeltas.count();
      const totalMeta = await db.jobMetadata.count();

      expect(totalAudits).toBe(0);
      expect(totalChats).toBe(0);
      expect(totalDeltas).toBe(0);
      expect(totalMeta).toBe(0);
    });
  });

  describe('Multi-job Isolation', () => {
    it('should keep data isolated between jobs', async () => {
      const job1 = 'job-1';
      const job2 = 'job-2';

      // Add entries for both jobs with same indices
      await db.auditEntries.bulkPut([
        { id: `${job1}_0`, jobId: job1, index: 0, timestamp: '', stepType: 'llm', data: createMockAuditEntry({ job_id: job1, step_number: 100 }) },
        { id: `${job2}_0`, jobId: job2, index: 0, timestamp: '', stepType: 'llm', data: createMockAuditEntry({ job_id: job2, step_number: 200 }) },
      ]);

      // Query job1 only
      const job1Results = await db.auditEntries
        .where('[jobId+index]')
        .between([job1, 0], [job1, 10], true, true)
        .toArray();

      expect(job1Results.length).toBe(1);
      expect(job1Results[0].data.step_number).toBe(100);

      // Query job2 only
      const job2Results = await db.auditEntries
        .where('[jobId+index]')
        .between([job2, 0], [job2, 10], true, true)
        .toArray();

      expect(job2Results.length).toBe(1);
      expect(job2Results[0].data.step_number).toBe(200);
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty range queries', async () => {
      const jobId = 'empty-job';
      const results = await db.auditEntries
        .where('[jobId+index]')
        .between([jobId, 0], [jobId, 100], true, true)
        .toArray();

      expect(results).toEqual([]);
    });

    it('should handle duplicate puts (upsert behavior)', async () => {
      const jobId = 'test-job';
      const entry1 = createMockAuditEntry({ job_id: jobId, step_number: 1 });
      const entry2 = createMockAuditEntry({ job_id: jobId, step_number: 2 });

      // Put first version
      await db.auditEntries.put({
        id: `${jobId}_0`,
        jobId,
        index: 0,
        timestamp: entry1.timestamp,
        stepType: entry1.step_type,
        data: entry1,
      });

      // Put updated version with same id
      await db.auditEntries.put({
        id: `${jobId}_0`,
        jobId,
        index: 0,
        timestamp: entry2.timestamp,
        stepType: entry2.step_type,
        data: entry2,
      });

      // Should only have one entry
      const count = await db.auditEntries.where('jobId').equals(jobId).count();
      expect(count).toBe(1);

      // Should have the updated data
      const retrieved = await db.auditEntries.get(`${jobId}_0`);
      expect(retrieved!.data.step_number).toBe(2);
    });

    it('should handle large batch inserts', async () => {
      const jobId = 'large-job';
      const count = 1000;

      const entries: CachedAuditEntry[] = Array.from({ length: count }, (_, i) => ({
        id: `${jobId}_${i}`,
        jobId,
        index: i,
        timestamp: new Date(Date.now() + i * 1000).toISOString(),
        stepType: 'llm',
        data: createMockAuditEntry({ job_id: jobId, step_number: i }),
      }));

      await db.auditEntries.bulkPut(entries);

      const storedCount = await db.auditEntries.where('jobId').equals(jobId).count();
      expect(storedCount).toBe(count);

      // Range query should still be fast
      const rangeResults = await db.auditEntries
        .where('[jobId+index]')
        .between([jobId, 500], [jobId, 510], true, true)
        .toArray();
      expect(rangeResults.length).toBe(11);
    });
  });
});

describe('thread-history v2 fence ordering', () => {
  const current = { eventsEpoch: 8, conversationRevision: 2 };

  it('replaces on a newer epoch or revision and merges an equal fence', () => {
    expect(compareThreadHistoryFence(current, current)).toBe('merge');
    expect(
      compareThreadHistoryFence(current, { eventsEpoch: 9, conversationRevision: 2 }),
    ).toBe('replace');
    expect(
      compareThreadHistoryFence(current, { eventsEpoch: 8, conversationRevision: 3 }),
    ).toBe('replace');
  });

  it('discards either dimension moving backwards', () => {
    expect(
      compareThreadHistoryFence(current, { eventsEpoch: 7, conversationRevision: 2 }),
    ).toBe('discard');
    expect(
      compareThreadHistoryFence(current, { eventsEpoch: 9, conversationRevision: 1 }),
    ).toBe('discard');
  });
});
