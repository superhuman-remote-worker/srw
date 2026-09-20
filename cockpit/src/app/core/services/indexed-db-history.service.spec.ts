import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Injector, PLATFORM_ID, runInInjectionContext } from '@angular/core';
import Dexie from 'dexie';
import { IndexedDbService } from './indexed-db.service';
import { CachedThreadMessage } from '../models/cache.model';

const threadId = 'history-regression';
const early: CachedThreadMessage = {
  id: 'user-first',
  threadId,
  role: 'human',
  content: 'Earlier question',
  tool_calls: null,
  turn_number: 1,
  created_at: '2026-09-07T08:00:00Z',
};
const late: CachedThreadMessage = {
  id: 'assistant-last',
  threadId,
  role: 'ai',
  content: 'Last answer',
  tool_calls: null,
  turn_number: 17,
  created_at: '2026-09-16T14:57:13Z',
};
const databases = ['cockpit-cache', 'srw-thread-history-v2'];
let services: IndexedDbService[] = [];

async function openService(): Promise<IndexedDbService> {
  const service = runInInjectionContext(
    Injector.create({ providers: [{ provide: PLATFORM_ID, useValue: 'browser' }] }),
    () => new IndexedDbService(),
  );
  services.push(service);
  await vi.waitFor(() => expect(service.isReady()).toBe(true));
  return service;
}

// Resource cleanup stays in the test, not on the production service API.
function closeService(service: IndexedDbService): void {
  const resources = service as unknown as {
    db: Dexie;
    historyDb: Dexie;
    historyChannel: BroadcastChannel | null;
  };
  resources.db.close();
  resources.historyDb.close();
  resources.historyChannel?.close();
}

beforeEach(async () => {
  for (const name of databases) await Dexie.delete(name);
});
afterEach(async () => {
  services.forEach(closeService);
  services = [];
  for (const name of databases) await Dexie.delete(name);
});

describe('IndexedDbService session history storage', () => {
  it('reads REST-shaped versioned messages chronologically on cold load and reopen', async () => {
    const service = await openService();
    const result = await service.applyThreadHistoryPage(threadId, 0, 0, [late, early]);
    expect(result.messages.map((m) => m.id)).toEqual(['user-first', 'assistant-last']);
    expect(await service.getNewestCachedCreatedAt(threadId)).toBe('2026-09-16T14:57:13Z');
    closeService(service);
    const reopened = await openService();
    expect((await reopened.getThreadMessages(threadId)).map((m) => m.id)).toEqual([
      'user-first',
      'assistant-last',
    ]);
  });

  it('reads unversioned REST-shaped messages for an older server', async () => {
    const service = await openService();
    await service.upsertThreadMessages([late, early]);
    expect((await service.getThreadMessages(threadId)).map((m) => m.id)).toEqual([
      'user-first',
      'assistant-last',
    ]);
    expect(await service.getNewestCachedCreatedAt(threadId)).toBe('2026-09-16T14:57:13Z');
  });

  it.each(['legacy', 'versioned'])(
    'repairs existing %s rows without losing unrelated data',
    async (kind) => {
      const old = new Dexie(kind === 'legacy' ? 'cockpit-cache' : 'srw-thread-history-v2');
      if (kind === 'legacy') {
        old.version(4).stores({
          auditEntries: 'id, jobId, [jobId+index], [jobId+stepType+index]',
          chatEntries: 'id, jobId, [jobId+timestamp]',
          graphDeltas: 'id, jobId, [jobId+index]',
          jobMetadata: 'jobId',
          threadCursors: 'threadId',
          threadMessages: 'id, threadId, [threadId+createdAt]',
        });
        await old.table('jobMetadata').put({ jobId: 'unrelated-job', chatEntryCount: 42 });
      } else {
        old.version(1).stores({
          threadCursors: 'threadId',
          threadMessages: 'id, threadId, [threadId+createdAt]',
          threadEpochs: 'threadId',
        });
        await old.table('threadEpochs').put({ threadId, eventsEpoch: 3, conversationRevision: 2 });
      }
      await old.table('threadMessages').bulkPut([late, early]);
      await old.table('threadCursors').put({ threadId, epoch: 3, seq: 99 });
      old.close();
      const service = await openService();
      expect((await service.getThreadMessages(threadId)).map((m) => m.id)).toEqual([
        'user-first',
        'assistant-last',
      ]);
      expect(await service.getNewestCachedCreatedAt(threadId)).toBe('2026-09-16T14:57:13Z');
      if (kind === 'legacy') {
        expect(await service.getJobMetadata('unrelated-job')).toMatchObject({ chatEntryCount: 42 });
      } else {
        expect(await service.getThreadCacheEpoch(threadId)).toMatchObject({
          eventsEpoch: 3,
          conversationRevision: 2,
        });
        expect(await service.getThreadCursor(threadId)).toMatchObject({ epoch: 3, seq: 99 });
      }
    },
  );

  it('keeps a rewound transcript when a stale history response arrives', async () => {
    const service = await openService();
    await service.applyThreadHistoryPage(threadId, 0, 0, [early, late]);
    const rewind = await service.applyThreadHistoryPage(threadId, 1, 1, [early]);
    expect(rewind.messages.map((m) => m.id)).toEqual(['user-first']);
    const stale = await service.applyThreadHistoryPage(threadId, 0, 0, [early, late]);
    expect(stale.accepted).toBe(false);
    expect(stale.messages.map((m) => m.id)).toEqual(['user-first']);
    const merged = await service.applyThreadHistoryPage(threadId, 1, 1, [early]);
    expect(merged.replaced).toBe(false);
    expect(merged.messages.map((m) => m.id)).toEqual(['user-first']);
  });
});
