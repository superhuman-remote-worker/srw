import {Injectable, computed, inject, signal} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {firstValueFrom} from 'rxjs';
import {environment} from '../environment';
import type {Thread} from '../models/api.model';

export type RecencyLabel = 'today' | 'yesterday' | 'earlier';
export interface ThreadGroup {
  label: RecencyLabel;
  threads: Thread[];
}

/**
 * Single owner of the session list. Both the rail and /sessions read from
 * here — previously the page fetched inline and the rail had no access.
 */
@Injectable({providedIn: 'root'})
export class SessionListService {
  private readonly http = inject(HttpClient);

  private readonly _threads = signal<Thread[]>([]);
  private readonly _loading = signal(false);
  private refreshSerial = 0;

  readonly threads = this._threads.asReadonly();
  readonly loading = this._loading.asReadonly();

  readonly grouped = computed<ThreadGroup[]>(() => {
    const buckets: Record<RecencyLabel, Thread[]> = {today: [], yesterday: [], earlier: []};
    for (const t of this._threads()) {
      buckets[recency(t.last_activity)].push(t);
    }
    return (['today', 'yesterday', 'earlier'] as const)
      .map((label) => ({label, threads: buckets[label]}))
      .filter((g) => g.threads.length > 0);
  });

  /**
   * In-place title patch, for callers that mutate a thread's title without
   * navigating (rename). Create and end both work incidentally — they
   * navigate, and the rail refreshes on NavigationEnd — but a rename is an
   * in-place mutation with no navigation of its own, so nothing else ever
   * tells this list about it. A no-op for an id this list doesn't carry
   * (nothing loaded yet, or a thread since removed elsewhere).
   */
  renameLocal(id: string, title: string): void {
    this._threads.update((threads) =>
      threads.map((t) => (t.id === id ? {...t, title} : t)),
    );
  }

  refresh(): Promise<void> {
    const serial = ++this.refreshSerial;
    this._loading.set(true);
    return firstValueFrom(
      this.http.get<{threads: Thread[]}>(`${environment.apiUrl}/persistent/threads`),
    ).then(
      (r) => {
        if (serial !== this.refreshSerial) return;
        this._threads.set(r?.threads ?? []);
        this._loading.set(false);
      },
      () => {
        if (serial !== this.refreshSerial) return;
        // Leave _threads untouched: a transient failure should not blank
        // whatever was already on screen (or, for a first-ever load, it's
        // already the empty initial value — either way there's nothing to
        // clear here).
        this._loading.set(false);
      },
    );
  }
}

/** Calendar-day comparison, not elapsed hours: "yesterday" must mean yesterday. */
function recency(iso: string): RecencyLabel {
  const startOfDay = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  const days = Math.round((startOfDay(new Date()) - startOfDay(new Date(iso))) / 86_400_000);
  if (days <= 0) return 'today';
  if (days === 1) return 'yesterday';
  return 'earlier';
}
