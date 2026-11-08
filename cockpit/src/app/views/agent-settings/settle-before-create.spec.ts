import {createEnvironmentInjector, effect, EnvironmentInjector, signal, untracked} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {LatestLoad, settleBeforeCreate, SettleOutcome} from './settle-before-create';

/** A load kind as the create forms run it: a flag, and its latest load. */
function loadKind() {
  const busy = signal(false);
  const load = new LatestLoad();
  return {
    busy,
    load,
    /** Start a load; the returned callback is its answer arriving (after the
     *  form's serial check, so only the latest one gets to call it). */
    start(): () => void {
      busy.set(true);
      const done = load.start();
      return () => { busy.set(false); done(); };
    },
  };
}

function injector(): EnvironmentInjector {
  return createEnvironmentInjector([], TestBed.inject(EnvironmentInjector));
}

/** Whether `promise` has settled after the queued work has run. */
async function settledYet(promise: Promise<SettleOutcome>): Promise<SettleOutcome | 'pending'> {
  return Promise.race([promise, new Promise<'pending'>((resolve) => setTimeout(() => resolve('pending'), 20))]);
}

describe('settleBeforeCreate', () => {
  afterEach(() => {
    vi.useRealTimers();
    TestBed.resetTestingModule();
  });

  it('waits for the latest load, then one render', async () => {
    const preview = loadKind();
    const answer = preview.start();

    const outcome = settleBeforeCreate([preview], injector());
    expect(await settledYet(outcome)).toBe('pending');

    answer();
    expect(await outcome).toBe('settled');
  });

  it('a superseded load never releases it; the newer one does', async () => {
    const preview = loadKind();
    preview.start(); // superseded: the form drops its answer, so it never settles
    const latest = preview.start();

    const outcome = settleBeforeCreate([preview], injector());
    expect(await settledYet(outcome)).toBe('pending');

    latest();
    expect(await outcome).toBe('settled');
  });

  it('follows a load that starts another', async () => {
    const detail = loadKind();
    const preview = loadKind();
    const detailAnswer = detail.start();

    const outcome = settleBeforeCreate([preview, detail], injector());
    const previewAnswer = preview.start(); // the detail's answer starts a preview
    detailAnswer();
    expect(await settledYet(outcome)).toBe('pending');

    previewAnswer();
    expect(await outcome).toBe('settled');
  });

  it('waits for a load that the render itself starts', async () => {
    // The session form re-reads its connectors from an effect when the
    // preview answers a different tier; that load starts during the render.
    const preview = loadKind();
    const connectors = loadKind();
    const scope = injector();
    const tier = signal('virtual');
    let connectorsAnswer: (() => void) | null = null;
    effect(() => {
      if (tier() === 'sandbox') untracked(() => (connectorsAnswer = connectors.start()));
    }, {injector: scope});
    const previewAnswer = preview.start();

    const outcome = settleBeforeCreate([preview, connectors], scope);
    previewAnswer();
    tier.set('sandbox');
    expect(await settledYet(outcome)).toBe('pending');
    expect(connectors.busy()).toBe(true);

    connectorsAnswer!();
    expect(await outcome).toBe('settled');
  });

  it('gives up at once on a flag left set over a settled load, rather than spin', async () => {
    const stuck = loadKind();
    stuck.busy.set(true); // no load in flight: nothing will ever clear it

    expect(await settledYet(settleBeforeCreate([stuck], injector()))).toBe('timeout');
  });

  it('gives up at the deadline', async () => {
    vi.useFakeTimers();
    const preview = loadKind();
    preview.start();

    const outcome = settleBeforeCreate([preview], injector(), 8000);
    await vi.advanceTimersByTimeAsync(7999);
    let result: SettleOutcome | null = null;
    void outcome.then((o) => (result = o));
    await vi.advanceTimersByTimeAsync(0);
    expect(result).toBeNull();
    await vi.advanceTimersByTimeAsync(1);
    expect(await outcome).toBe('timeout');
  });

  it('reports a destroyed form', async () => {
    const preview = loadKind();
    preview.start();
    const scope = injector();

    const outcome = settleBeforeCreate([preview], scope);
    scope.destroy();
    expect(await outcome).toBe('destroyed');
  });
});
