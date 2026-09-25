import type { ConsoleMessage, Page, Request, Response } from '@playwright/test';

export type JourneyPhase =
  | 'pre-navigation'
  | 'landing'
  | 'creating'
  | 'turn'
  | 'reload'
  | 'hydration'
  | 'list-navigation'
  | 'sessions-list'
  | 'closing';

interface ResponseEntry {
  kind: 'response';
  method: string;
  pathname: string;
  status: number;
  phase: JourneyPhase;
  elapsed_ms: number | null;
}

interface FailureEntry {
  kind: 'requestfailed';
  method: string;
  pathname: string;
  phase: JourneyPhase;
  failure: string;
  classification: 'expected-navigation-cancellation' | 'expected-warmup-reconnect' | 'unexpected';
}

interface PageErrorEntry {
  kind: 'pageerror';
  name: string;
  message: string;
  phase: JourneyPhase;
}

interface ConsoleErrorEntry {
  kind: 'console';
  level: 'error' | 'warning';
  message: string;
  pathname: string | null;
  phase: JourneyPhase;
  classification: 'expected-warmup' | 'unexpected';
}

export type NetworkEntry = ResponseEntry | FailureEntry | PageErrorEntry | ConsoleErrorEntry;

const THREAD_STREAM = /^\/api\/persistent\/threads\/([^/]+)\/stream$/;
const CONNECTION = /^\/api\/sessions\/([^/]+)\/connection$/;
const CONTROL_WEBSOCKET = /^\/p\/([^/]+)\/ws$/;
const GLOBAL_SSE = new Set(['/api/notifications/events', '/api/sudo/events']);
const ABORT_REASON = /(ERR_ABORTED|NS_BINDING_ABORTED|cancelled|canceled|Target .*closed)/i;
const WARMUP_RECONNECT_WARNING =
  '[persistent-chat] no SSE data within 5000ms of send — forcing reconnect';
const WARM_PHASES = new Set<JourneyPhase>(['creating', 'turn', 'reload']);
const CONTROL_SOCKET_PHASES = new Set<JourneyPhase>([
  'creating',
  'turn',
  'reload',
  'hydration',
  'list-navigation',
  'sessions-list',
  'closing',
]);

function safePathname(raw: string): string | null {
  try {
    return new URL(raw).pathname;
  } catch {
    return null;
  }
}

function diagnosticUrl(raw: string): URL | null {
  const match = raw.match(/(?:https?|wss?):\/\/[^\s"')]+/i);
  if (!match) return null;
  try {
    return new URL(match[0]);
  } catch {
    return null;
  }
}

export function sanitizedDiagnostic(raw: string): string {
  return raw
    .replace(/Bearer\s+[^\s"']+/gi, 'Bearer [redacted]')
    .replace(/\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{16,}\b/g, '[redacted-jwt]')
    .replace(/([?&](?:t|token|key|secret|password)=)[^&#\s"']+/gi, '$1[redacted]')
    .replace(/(cookie|password|token)=([^\s;&]+)/gi, '$1=[redacted]')
    .replace(/(?:https?|wss?):\/\/[^\s"')]+/gi, (value) => {
      try {
        const url = new URL(value);
        return `${url.origin}${url.pathname}`;
      } catch {
        return '[url]';
      }
    })
    .slice(0, 300);
}

export class NetworkLedger {
  private phase: JourneyPhase = 'pre-navigation';
  private readonly started = new WeakMap<Request, { at: number; phase: JourneyPhase }>();
  private readonly ownedThreadIds = new Set<string>();
  private readonly records: NetworkEntry[] = [];
  private pendingWarmupStreamReconnects = 0;

  constructor(
    page: Page,
    private readonly applicationOrigin: string,
  ) {
    page.on('request', (request) => this.onRequest(request));
    page.on('response', (response) => this.onResponse(response));
    page.on('requestfailed', (request) => this.onRequestFailed(request));
    page.on('pageerror', (error) => {
      this.records.push({
        kind: 'pageerror',
        name: error.name,
        message: sanitizedDiagnostic(error.message),
        phase: this.phase,
      });
    });
    page.on('console', (message) => this.onConsole(message));
  }

  setPhase(phase: JourneyPhase): void {
    this.phase = phase;
  }

  registerThread(threadId: string): void {
    this.ownedThreadIds.add(threadId);
  }

  entries(): readonly NetworkEntry[] {
    return this.records;
  }

  responses(method: string, pathname: string): ResponseEntry[] {
    return this.records.filter(
      (entry): entry is ResponseEntry =>
        entry.kind === 'response' && entry.method === method && entry.pathname === pathname,
    );
  }

  private isFirstParty(raw: string): boolean {
    try {
      return new URL(raw).origin === this.applicationOrigin;
    } catch {
      return false;
    }
  }

  private onRequest(request: Request): void {
    if (!this.isFirstParty(request.url())) return;
    this.started.set(request, { at: Date.now(), phase: this.phase });
  }

  private onResponse(response: Response): void {
    const request = response.request();
    if (!this.isFirstParty(response.url())) return;
    const start = this.started.get(request);
    const pathname = safePathname(response.url());
    if (!pathname) return;
    this.records.push({
      kind: 'response',
      method: request.method(),
      pathname,
      status: response.status(),
      phase: start?.phase ?? this.phase,
      elapsed_ms: start ? Date.now() - start.at : null,
    });
  }

  private onRequestFailed(request: Request): void {
    if (!this.isFirstParty(request.url())) return;
    const pathname = safePathname(request.url());
    if (!pathname) return;
    const reason = sanitizedDiagnostic(request.failure()?.errorText ?? 'unknown network failure');
    const classification = this.cancellationClassification(
      request.method(), pathname, reason, this.started.get(request)?.phase,
    );
    this.records.push({
      kind: 'requestfailed',
      method: request.method(),
      pathname,
      phase: this.phase,
      failure: reason,
      classification,
    });
  }

  private cancellationClassification(
    method: string,
    pathname: string,
    reason: string,
    startedPhase?: JourneyPhase,
  ): FailureEntry['classification'] {
    if (method !== 'GET' || !ABORT_REASON.test(reason)) return 'unexpected';

    const threadMatch = pathname.match(THREAD_STREAM);
    const ownedThreadStream =
      threadMatch !== null && this.ownedThreadIds.has(decodeURIComponent(threadMatch[1]));

    if (
      ownedThreadStream &&
      (this.phase === 'creating' || this.phase === 'turn') &&
      this.pendingWarmupStreamReconnects > 0
    ) {
      this.pendingWarmupStreamReconnects -= 1;
      return 'expected-warmup-reconnect';
    }
    if (this.phase === 'list-navigation' && ownedThreadStream) {
      return 'expected-navigation-cancellation';
    }
    const connectionMatch = pathname.match(CONNECTION);
    // A Chrome reload cancels a poll from the old document. Keep failures of
    // polls started by the new document visible to the network guard.
    if (
      this.phase === 'reload' &&
      startedPhase === 'turn' &&
      reason === 'net::ERR_ABORTED' &&
      connectionMatch !== null &&
      this.ownedThreadIds.has(decodeURIComponent(connectionMatch[1]))
    ) {
      return 'expected-navigation-cancellation';
    }
    if (this.phase === 'reload' || this.phase === 'closing') {
      if (ownedThreadStream || GLOBAL_SSE.has(pathname)) {
        return 'expected-navigation-cancellation';
      }
    }
    return 'unexpected';
  }

  private onConsole(message: ConsoleMessage): void {
    if (message.type() !== 'error' && message.type() !== 'warning') return;
    const location = message.location();
    const rawMessage = message.text();
    const messageUrl = diagnosticUrl(rawMessage);
    let locationUrl: URL | null = null;
    try {
      locationUrl = location.url ? new URL(location.url) : null;
    } catch {
      locationUrl = null;
    }
    const diagnosticLocation = messageUrl ?? locationUrl;
    const pathname = diagnosticLocation?.pathname ?? null;
    if (
      message.type() === 'warning' &&
      (this.phase === 'creating' || this.phase === 'turn') &&
      rawMessage === WARMUP_RECONNECT_WARNING
    ) {
      this.pendingWarmupStreamReconnects += 1;
    }
    this.records.push({
      kind: 'console',
      level: message.type() === 'error' ? 'error' : 'warning',
      message: sanitizedDiagnostic(rawMessage),
      pathname,
      phase: this.phase,
      classification: this.isExpectedConsoleError(rawMessage, pathname, diagnosticLocation)
        ? 'expected-warmup'
        : 'unexpected',
    });
  }

  private isExpectedConsoleError(
    message: string,
    pathname: string | null,
    location: URL | null,
  ): boolean {
    if (
      (this.phase === 'creating' || this.phase === 'turn') &&
      message === WARMUP_RECONNECT_WARNING
    ) {
      return true;
    }
    if (!pathname || !location) return false;
    const applicationUrl = new URL(this.applicationOrigin);
    const connectionMatch = pathname.match(CONNECTION);
    const expectedConnectionFailure =
      location.origin === this.applicationOrigin &&
      WARM_PHASES.has(this.phase) &&
      connectionMatch !== null &&
      this.ownedThreadIds.has(decodeURIComponent(connectionMatch[1])) &&
      /Failed to load resource/i.test(message) &&
      /(?:409 \(Conflict\)|425 \(Too Early\))/i.test(message);
    if (expectedConnectionFailure) return true;

    const socketMatch = pathname.match(CONTROL_WEBSOCKET);
    return (
      CONTROL_SOCKET_PHASES.has(this.phase) &&
      /WebSocket connection/i.test(message) &&
      (location.protocol === 'ws:' || location.protocol === 'wss:') &&
      location.hostname === applicationUrl.hostname &&
      socketMatch !== null &&
      this.ownedThreadIds.has(decodeURIComponent(socketMatch[1]))
    );
  }

  problems(): string[] {
    const problems: string[] = [];
    for (const entry of this.records) {
      if (entry.kind === 'pageerror') {
        problems.push(`pageerror during ${entry.phase}: ${entry.name}: ${entry.message}`);
        continue;
      }
      if (
        entry.kind === 'console' &&
        entry.level === 'error' &&
        entry.classification === 'unexpected'
      ) {
        problems.push(`console error during ${entry.phase}: ${entry.message}`);
        continue;
      }
      if (entry.kind === 'requestfailed' && entry.classification === 'unexpected') {
        problems.push(
          `unexpected network failure during ${entry.phase}: ` +
            `${entry.method} ${entry.pathname} (${entry.failure})`,
        );
        continue;
      }
      if (entry.kind !== 'response') continue;

      const isApi = entry.pathname.startsWith('/api/') || entry.pathname.startsWith('/auth/');
      const connectionMatch = entry.pathname.match(CONNECTION);
      const warmConnection =
        entry.method === 'GET' &&
        connectionMatch !== null &&
        this.ownedThreadIds.has(decodeURIComponent(connectionMatch[1])) &&
        (entry.status === 409 || entry.status === 425) &&
        (entry.phase === 'creating' || entry.phase === 'turn' || entry.phase === 'reload');
      if (warmConnection) continue;

      if (entry.status === 401 || entry.status === 403) {
        problems.push(
          `unexpected auth response during ${entry.phase}: ` +
            `${entry.status} ${entry.method} ${entry.pathname}`,
        );
      } else if (entry.status >= 500) {
        problems.push(
          `unexpected server response during ${entry.phase}: ` +
            `${entry.status} ${entry.method} ${entry.pathname}`,
        );
      } else if (isApi && entry.status >= 400) {
        problems.push(
          `unexpected API response during ${entry.phase}: ` +
            `${entry.status} ${entry.method} ${entry.pathname}`,
        );
      }
    }
    return problems;
  }

  assertClean(): void {
    const problems = this.problems();
    if (problems.length > 0) {
      throw new Error(
        `Sanitized network ledger found ${problems.length} problem(s):\n${problems.join('\n')}`,
      );
    }
  }
}
