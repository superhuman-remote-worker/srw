/** EventSource hides the HTTP status of a failed stream handshake. Classify
 * the matching authenticated REST probe instead. Unknown/network failures
 * remain retryable; neither a failed probe nor an SSE hint grants authority. */
export function classifySseProbeFailure(error: unknown): 'auth' | 'gone' | 'retry' {
  const status = (error as { status?: unknown } | null)?.status;
  if (status === 401 || status === 403) return 'auth';
  if (status === 404 || status === 410) return 'gone';
  return 'retry';
}

/** Capped rate with jitter so synchronized tabs do not stampede a recovering
 * orchestrator. One service owner schedules at most one timer/in-flight probe. */
export function sseRetryDelayMs(attempt: number): number {
  const base = Math.min(30_000, 1000 * 2 ** Math.min(Math.max(attempt, 0), 5));
  return Math.round(base * (0.8 + Math.random() * 0.4));
}
