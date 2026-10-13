import { HttpClient } from '@angular/common/http';
import { Injector, NgZone, runInInjectionContext } from '@angular/core';
import { Subject, firstValueFrom, of, throwError } from 'rxjs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { CallEventsPage } from '../models/call.model';
import { callEventFixture as event, callFixture as call } from '../models/fixtures/call.fixture';
import { CallService, CallWatch } from './call.service';

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  static CLOSED = 2;
  readyState = 0;
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  listeners = new Map<string, (frame: MessageEvent) => void>();
  close = vi.fn(() => {
    this.readyState = 2;
  });
  constructor(
    readonly url: string,
    readonly options: EventSourceInit,
  ) {
    FakeEventSource.instances.push(this);
  }
  addEventListener(name: string, listener: (frame: MessageEvent) => void): void {
    this.listeners.set(name, listener);
  }
  emit(name: string, data: unknown): void {
    this.listeners.get(name)?.(new MessageEvent(name, { data: JSON.stringify(data) }));
  }
  open(): void {
    this.readyState = 1;
    this.onopen?.();
  }
  fail(): void {
    this.readyState = 2;
    this.onerror?.();
  }
}

const owners: CallWatch[] = [];
function create() {
  const http = { get: vi.fn().mockReturnValue(of({ events: [], next_after_id: 0 })) };
  const injector = Injector.create({
    providers: [
      { provide: HttpClient, useValue: http },
      { provide: NgZone, useValue: { run: (fn: () => void) => fn() } },
    ],
  });
  const service = runInInjectionContext(injector, () => new CallService());
  const watch = (initial = call()) => {
    const owner = service.watchCall('job-1', initial);
    owners.push(owner);
    return owner;
  };
  return { service, http, watch };
}

describe('CallService', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    FakeEventSource.instances = [];
    vi.stubGlobal('EventSource', FakeEventSource);
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
  });
  afterEach(() => {
    owners.splice(0).forEach((owner) => owner.destroy());
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('uses the existing authenticated API base for both GETs and bounds page size', async () => {
    const { service, http } = create();
    http.get.mockReturnValueOnce(of({ calls: [call()] }));
    expect(await firstValueFrom(service.getCalls('job-1'))).toEqual({ calls: [call()] });
    await firstValueFrom(service.getEvents('job-1', 'call-1', 123, 900));
    expect(http.get).toHaveBeenNthCalledWith(
      1,
      expect.stringMatching(/\/api\/jobs\/job-1\/calls$/),
      {
        withCredentials: true,
        headers: { 'ngsw-bypass': '1' },
      },
    );
    expect(http.get).toHaveBeenNthCalledWith(2, expect.stringMatching(/\/calls\/call-1\/events$/), {
      withCredentials: true,
      headers: { 'ngsw-bypass': '1' },
      params: { after_id: '123', limit: '500' },
    });
  });

  it('pages a long ended call, orders and deduplicates the journal', async () => {
    const { http, watch } = create();
    http.get.mockReturnValueOnce(
      of({ events: Array.from({ length: 500 }, (_, i) => event(i + 1)), next_after_id: 500 }),
    );
    http.get.mockReturnValueOnce(
      of({ events: [event(502), event(500), event(501)], next_after_id: 502 }),
    );
    const owner = watch(call({ status: 'ended' }));
    await owner.refresh();
    expect(owner.events()).toHaveLength(502);
    expect(
      owner
        .events()
        .slice(-3)
        .map((row) => row.id),
    ).toEqual([500, 501, 502]);
    expect(http.get.mock.calls[1][1].params.after_id).toBe('500');
    expect(FakeEventSource.instances).toHaveLength(0);
    expect(owner.loading()).toBe(false);
  });

  it('handles named SSE events with cookie auth and deduplicates out-of-order live rows', async () => {
    const { http, watch } = create();
    http.get.mockReturnValue(of({ events: [event(1)], next_after_id: 1 }));
    const owner = watch();
    await owner.refresh();
    const stream = FakeEventSource.instances[0];
    expect(stream.url).toMatch(/\/calls\/call-1\/stream\?ngsw-bypass=true$/);
    expect(stream.options).toEqual({ withCredentials: true });
    stream.open();
    await owner.refresh();
    stream.emit('call_event', event(3, { speaker: 'assistant' }));
    stream.emit('call_event', event(2));
    stream.emit('call_event', event(3, { speaker: 'assistant' }));
    stream.emit('call_event', event(4, { call_id: 'other-call' }));
    expect(owner.events().map((row) => row.id)).toEqual([1, 2, 3]);
    expect(owner.connected()).toBe(true);
    stream.emit('call', call({ line_name: 'Anmeldung' }));
    expect(owner.call().line_name).toBe('Anmeldung');
  });

  it('resumes after the last live id and backfills rows missed during a disconnect', async () => {
    const { http, watch } = create();
    http.get.mockReturnValue(of({ events: [event(1)], next_after_id: 1 }));
    const owner = watch();
    FakeEventSource.instances[0].open();
    await vi.advanceTimersByTimeAsync(0);
    FakeEventSource.instances[0].emit('call_event', event(2));
    FakeEventSource.instances[0].fail();
    http.get.mockImplementation((_url, options) => {
      const after = Number(options.params.after_id);
      return of({
        events: after < 4 ? [event(3), event(4)] : [],
        next_after_id: Math.max(after, 4),
      });
    });
    await vi.advanceTimersByTimeAsync(1000);
    expect(http.get.mock.calls.at(-1)?.[1].params.after_id).toBe('2');
    expect(FakeEventSource.instances).toHaveLength(2);
    FakeEventSource.instances[1].open();
    FakeEventSource.instances[1].emit('call_event', event(4));
    FakeEventSource.instances[1].emit('call_event', event(5));
    await vi.advanceTimersByTimeAsync(0);
    expect(owner.events().map((row) => row.id)).toEqual([1, 2, 3, 4, 5]);
    expect(owner.connected()).toBe(true);
    expect(FakeEventSource.instances[0].close).toHaveBeenCalledOnce();
  });

  it('does not let newer live ids skip pages while an initial replay is in flight', async () => {
    const { http, watch } = create();
    const first = new Subject<CallEventsPage>();
    http.get.mockImplementation((_url, options) => {
      const after = Number(options.params.after_id);
      if (after === 0) return first;
      if (after === 500) return of({ events: [event(501), event(502)], next_after_id: 502 });
      return of({ events: [event(600)], next_after_id: 600 });
    });
    const owner = watch();
    FakeEventSource.instances[0].open();
    FakeEventSource.instances[0].emit('call_event', event(600));
    first.next({ events: Array.from({ length: 500 }, (_, i) => event(i + 1)), next_after_id: 500 });
    first.complete();
    await vi.advanceTimersByTimeAsync(0);
    expect(http.get.mock.calls.map((args) => args[1].params.after_id)).toEqual(['0', '500', '502']);
    expect(owner.events()).toHaveLength(503);
  });

  it('polls every two seconds when EventSource is unavailable and stops on disposal', async () => {
    vi.stubGlobal('EventSource', undefined);
    const { http, watch } = create();
    const owner = watch();
    await vi.advanceTimersByTimeAsync(0);
    http.get.mockReturnValue(of({ events: [event(1)], next_after_id: 1 }));
    await vi.advanceTimersByTimeAsync(2000);
    expect(owner.events()).toEqual([event(1)]);
    expect(http.get).toHaveBeenCalledTimes(2);
    owner.destroy();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(http.get).toHaveBeenCalledTimes(2);
  });

  it('keeps two-second fallback polling during an SSE/API outage', async () => {
    const { http, watch } = create();
    const owner = watch();
    await vi.advanceTimersByTimeAsync(0);
    FakeEventSource.instances[0].fail();
    http.get.mockReturnValue(throwError(() => ({ status: 503 })));
    await vi.advanceTimersByTimeAsync(2000);
    expect(http.get).toHaveBeenCalledTimes(3); // initial, reconnect probe, 2s poll
    expect(owner.error()).toBe(true);
    expect(FakeEventSource.instances).toHaveLength(1);
  });

  it.each([401, 403, 404, 410])(
    'halts recovery after an authenticated probe returns %s',
    async (status) => {
      const { http, watch } = create();
      const owner = watch();
      await vi.advanceTimersByTimeAsync(0);
      FakeEventSource.instances[0].fail();
      http.get.mockReturnValue(throwError(() => ({ status })));
      await vi.advanceTimersByTimeAsync(20_000);
      expect(http.get).toHaveBeenCalledTimes(2);
      expect(FakeEventSource.instances).toHaveLength(1);
      expect(owner.error()).toBe(true);
      expect(owner.connected()).toBe(false);
    },
  );

  it('closes terminal streams, performs a final read, and supports post-call poll refreshes', async () => {
    const { http, watch } = create();
    const owner = watch();
    await vi.advanceTimersByTimeAsync(0);
    const stream = FakeEventSource.instances[0];
    const ended = call({ status: 'ended', ended_at: '2026-10-12T08:00:47Z' });
    const followUp = event(1, { kind: 'follow_up' });
    http.get.mockReturnValue(of({ events: [followUp], next_after_id: 1 }));
    stream.emit('call', ended);
    await vi.advanceTimersByTimeAsync(0);
    expect(stream.close).toHaveBeenCalledOnce();
    expect(owner.events()).toEqual([followUp]);
    stream.emit('call_event', event(99));
    owner.updateCall(call()); // stale pre-hang-up job poll
    expect(owner.call().status).toBe('ended');
    const answer = event(2, { kind: 'answer' });
    http.get.mockReturnValue(of({ events: [answer], next_after_id: 2 }));
    owner.updateCall(ended);
    await vi.advanceTimersByTimeAsync(0);
    expect(owner.events()).toEqual([followUp, answer]);
    const count = http.get.mock.calls.length;
    await vi.advanceTimersByTimeAsync(5000);
    expect(http.get).toHaveBeenCalledTimes(count);
  });

  it('ignores malformed/foreign frames and fences a disposed in-flight replay', async () => {
    const { http, watch } = create();
    const pending = new Subject<CallEventsPage>();
    http.get.mockReturnValue(pending);
    const owner = watch();
    const stream = FakeEventSource.instances[0];
    stream.listeners.get('call_event')?.(new MessageEvent('call_event', { data: '{' }));
    stream.emit('call', call({ id: 'another-call', status: 'failed' }));
    expect(owner.call().status).toBe('active');
    owner.destroy();
    pending.next({ events: [event(1)], next_after_id: 1 });
    pending.complete();
    await vi.advanceTimersByTimeAsync(0);
    expect(owner.events()).toEqual([]);
    expect(stream.close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('stops an invalid paging cursor instead of looping forever', async () => {
    const { http, watch } = create();
    http.get.mockReturnValue(
      of({ events: Array.from({ length: 500 }, (_, i) => event(i + 1)), next_after_id: 0 }),
    );
    const owner = watch(call({ status: 'ended' }));
    await vi.advanceTimersByTimeAsync(0);
    expect(owner.error()).toBe(true);
    expect(http.get).toHaveBeenCalledOnce();
  });
});
