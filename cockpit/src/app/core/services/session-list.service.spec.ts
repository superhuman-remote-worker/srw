import {describe, expect, it, vi} from 'vitest';
import {Injector, runInInjectionContext} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {of, Subject, throwError} from 'rxjs';
import {SessionListService} from './session-list.service';
import type {Thread} from '../models/api.model';

function thread(id: string, lastActivity: string): Thread {
  return {
    id, title: `Session ${id}`, status: 'active', config_name: 'session_base',
    permission_mode: 'autonomous', created_at: lastActivity, last_activity: lastActivity,
  } as Thread;
}

function create(threads: Thread[]) {
  const http = {get: vi.fn(() => of({threads}))};
  const injector = Injector.create({providers: [{provide: HttpClient, useValue: http}]});
  const service = runInInjectionContext(injector, () => new SessionListService());
  return {service, http};
}

describe('SessionListService', () => {
  it('exposes threads after refresh', async () => {
    const {service} = create([thread('a', new Date().toISOString())]);
    await service.refresh();
    expect(service.threads().map((t) => t.id)).toEqual(['a']);
  });

  it('groups by recency into today, yesterday and earlier', async () => {
    // A 26-hour-old thread would pass under both a correct calendar-day
    // comparison AND a naive `24 <= hours < 48` one, so it doesn't actually
    // discriminate. Pin the clock to 9am and use 11pm the day before: only
    // 10 hours elapsed (an hours-based check would call this "today"), but
    // it's yesterday's calendar date, so only a correct implementation
    // puts it in "yesterday".
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2026, 0, 15, 9, 0, 0));
    try {
      const now = new Date();
      const yesterdayLate = new Date(2026, 0, 14, 23, 0, 0);
      const lastWeek = new Date(now.getTime() - 8 * 24 * 3600_000);
      const {service} = create([
        thread('t', now.toISOString()),
        thread('y', yesterdayLate.toISOString()),
        thread('e', lastWeek.toISOString()),
      ]);
      await service.refresh();
      expect(service.grouped().map((g) => g.label)).toEqual(['today', 'yesterday', 'earlier']);
      expect(service.grouped()[0].threads.map((t) => t.id)).toEqual(['t']);
      expect(service.grouped()[1].threads.map((t) => t.id)).toEqual(['y']);
    } finally {
      vi.useRealTimers();
    }
  });

  it('omits a group with no threads', async () => {
    const {service} = create([thread('t', new Date().toISOString())]);
    await service.refresh();
    expect(service.grouped().map((g) => g.label)).toEqual(['today']);
  });

  it('resolves rather than rejects when the HTTP call errors, so fire-and-forget callers never see an unhandled rejection', async () => {
    const http = {get: vi.fn(() => throwError(() => new Error('boom')))};
    const injector = Injector.create({providers: [{provide: HttpClient, useValue: http}]});
    const service = runInInjectionContext(injector, () => new SessionListService());

    await expect(service.refresh()).resolves.toBeUndefined();

    expect(service.threads()).toEqual([]);
    expect(service.loading()).toBe(false);
  });

  it('a failed refresh leaves prior threads on screen and only clears loading', async () => {
    const {service, http} = create([thread('a', new Date().toISOString())]);
    await service.refresh();
    expect(service.threads().map((t) => t.id)).toEqual(['a']);

    http.get.mockReturnValueOnce(throwError(() => new Error('blip')));
    await service.refresh();

    expect(service.threads().map((t) => t.id)).toEqual(['a']);
    expect(service.loading()).toBe(false);
  });

  it('keeps the newer typed workspace state when an older refresh finishes later', async () => {
    const {service, http} = create([]);
    const older = new Subject<{threads: Thread[]}>();
    const newer = new Subject<{threads: Thread[]}>();
    http.get.mockReturnValueOnce(older).mockReturnValueOnce(newer);
    const first = service.refresh();
    const second = service.refresh();
    newer.next({threads: [{...thread('a', new Date().toISOString()),
      workspace_creation: {stage: 'readiness', state: 'attention',
        reason_code: 'invalid_image', readiness_deadline_at: null}}]});
    newer.complete();
    await second;
    older.next({threads: [{...thread('a', new Date().toISOString()),
      workspace_creation: {stage: 'scheduling', state: 'observing',
        reason_code: 'observation_pending', readiness_deadline_at: null}}]});
    older.complete();
    await first;
    expect(service.threads()[0].workspace_creation?.state).toBe('attention');
  });

  it('an empty thread list produces no groups', async () => {
    const {service} = create([]);
    await service.refresh();
    expect(service.threads()).toEqual([]);
    expect(service.grouped()).toEqual([]);
  });

  it('a malformed last_activity falls through to earlier rather than throwing', async () => {
    const {service} = create([thread('bad', 'not-a-date')]);
    await service.refresh();
    expect(service.grouped().map((g) => g.label)).toEqual(['earlier']);
  });
});

describe('SessionListService.renameLocal', () => {
  it('patches the title of the matching thread in place, without a re-fetch', async () => {
    const {service, http} = create([
      thread('a', new Date().toISOString()),
      thread('b', new Date().toISOString()),
    ]);
    await service.refresh();

    service.renameLocal('a', 'Renamed session');

    expect(service.threads().map((t) => [t.id, t.title])).toEqual([
      ['a', 'Renamed session'],
      ['b', 'Session b'],
    ]);
    // No navigation refreshes this list for a rename (see the doc comment on
    // renameLocal) — confirm the patch really is local, not a side-effect of
    // an extra fetch this test would otherwise miss.
    expect(http.get).toHaveBeenCalledTimes(1);
  });

  it('is a no-op for an id this list does not carry', async () => {
    const {service} = create([thread('a', new Date().toISOString())]);
    await service.refresh();

    service.renameLocal('does-not-exist', 'Renamed session');

    expect(service.threads().map((t) => t.title)).toEqual(['Session a']);
  });
});
