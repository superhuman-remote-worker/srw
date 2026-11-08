import {afterNextRender, DestroyRef, Injector} from '@angular/core';
import {SESSION_TOOL_GROUPS_TIMEOUT_MS} from '../../core/services/api.service';

/**
 * The latest of a series of overlapping loads of one kind (tool previews,
 * Expert details, eligible-connector lists) on a create form.
 *
 * The forms guard each load with a serial: a superseded load's answer is
 * dropped and never clears the loading flag; only the latest one does. A
 * create that waits must therefore wait for the LATEST load, never one it
 * captured. `start()` makes a new load the latest and returns its `done`
 * callback, which the load calls where it clears its flag, after its serial
 * check.
 */
export class LatestLoad {
  private latest: Promise<void> = Promise.resolve();

  start(): () => void {
    let done!: () => void;
    this.latest = new Promise<void>((resolve) => (done = resolve));
    return done;
  }

  /** Settles once the latest load started so far has called `done`. */
  get settled(): Promise<void> {
    return this.latest;
  }
}

/** One load a create request depends on: its loading flag and its loads. */
export interface PendingLoad {
  busy: () => boolean;
  load: LatestLoad;
}

/** `settled`: build the request now. `timeout`: still loading at the deadline.
 *  `destroyed`: the form is gone; send nothing. */
export type SettleOutcome = 'settled' | 'timeout' | 'destroyed';

/**
 * Wait until none of `loads` is in flight, then for one render, so what the
 * loads put into signals has reached the child components the request is
 * read from (the tools group's `[resolvedToolset]`). Re-checks after every
 * settle and after the render, since one load can start another (an Expert's
 * detail starts a preview). Bounded by the tool preview's own deadline.
 *
 * Callers skip this when nothing is in flight, so a plain click still sends
 * its request synchronously.
 */
export async function settleBeforeCreate(
  loads: readonly PendingLoad[],
  injector: Injector,
  deadlineMs = SESSION_TOOL_GROUPS_TIMEOUT_MS,
): Promise<SettleOutcome> {
  let onDestroy!: () => void;
  const destroyed = new Promise<'destroyed'>((resolve) => (onDestroy = () => resolve('destroyed')));
  const unregister = injector.get(DestroyRef).onDestroy(() => onDestroy());
  try {
    const deadline = Date.now() + deadlineMs;
    const tracked = new Set<Promise<void>>();
    const settled = new Set<Promise<void>>();
    for (;;) {
      for (let waits = inFlight(loads); waits.length > 0; waits = inFlight(loads)) {
        for (const w of waits) {
          if (!tracked.has(w)) {
            tracked.add(w);
            void w.then(() => settled.add(w));
          }
        }
        // A flag still set over a load that already settled leaves nothing to
        // wait for: give up rather than spin.
        const open = waits.filter((w) => !settled.has(w));
        if (open.length === 0) return 'timeout';
        const left = deadline - Date.now();
        if (left <= 0) return 'timeout';
        let timer: ReturnType<typeof setTimeout> | undefined;
        const expired = new Promise<'timeout'>((resolve) => (timer = setTimeout(() => resolve('timeout'), left)));
        // Any settle re-polls: a load awaited here can be superseded by a newer
        // one of the same kind, and then never settles.
        const outcome = await Promise.race([...open.map((w) => w.then(() => 'settled' as const)), expired, destroyed]);
        clearTimeout(timer);
        if (outcome !== 'settled') return outcome;
      }
      const rendered = new Promise<'settled'>((resolve) => afterNextRender(() => resolve('settled'), {injector}));
      if ((await Promise.race([rendered, destroyed])) === 'destroyed') return 'destroyed';
      // That render can start a load of its own (an effect re-reads the
      // connectors for the tier the preview answered): wait for that too.
      if (inFlight(loads).length === 0) return 'settled';
    }
  } finally {
    unregister();
  }
}

function inFlight(loads: readonly PendingLoad[]): Promise<void>[] {
  return loads.filter((l) => l.busy()).map((l) => l.load.settled);
}
