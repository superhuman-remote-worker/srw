export interface ProviderCounter {
  run_id: string;
  model: string | null;
  endpoint: string;
  stream: boolean;
  outcome: string;
  count: number;
}

export interface ProviderCall {
  run_id: string;
  model: string | null;
  endpoint: string;
  stream: boolean;
  outcome: string;
  correlation_id?: string | null;
}

export interface ProviderScenarioState {
  closed?: boolean;
  expected_cancelled?: number;
  run_id: string;
  scenario: string;
  required_responses: number;
  consumed_required_responses: number;
  remaining_required_responses: number;
  unexpected_count: number;
  pending_calls: number;
  counters: ProviderCounter[];
  calls: ProviderCall[];
}

export interface ProviderOverview {
  runs: ProviderScenarioState[];
  closed_runs?: ProviderScenarioState[];
  unscoped_unexpected_calls: number;
}

interface RequestOptions {
  method?: 'GET' | 'POST' | 'DELETE';
  body?: Record<string, unknown>;
}

export class ProviderControlClient {
  constructor(
    private readonly origin: string,
    private readonly token: string,
  ) {}

  private async call<T>(pathname: string, options: RequestOptions = {}): Promise<T> {
    const response = await fetch(new URL(pathname, `${this.origin}/`), {
      method: options.method ?? 'GET',
      headers: {
        Authorization: `Bearer ${this.token}`,
        ...(options.body ? { 'Content-Type': 'application/json' } : {}),
      },
      body: options.body ? JSON.stringify(options.body) : undefined,
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok) {
      throw new Error(
        `Deterministic-provider control request failed with HTTP ${response.status}.`,
      );
    }
    if (response.status === 204) return undefined as T;
    try {
      return (await response.json()) as T;
    } catch {
      throw new Error('Deterministic-provider control request returned invalid JSON.');
    }
  }

  async health(): Promise<void> {
    await this.call('/control/health');
  }

  async arm(runId: string): Promise<ProviderScenarioState> {
    return this.call(`/control/scenarios/${encodeURIComponent(runId)}/arm`, {
      method: 'POST',
      body: { scenario: 'reply', required_responses: 1 },
    });
  }

  async state(runId: string): Promise<ProviderScenarioState> {
    return this.call(`/control/scenarios/${encodeURIComponent(runId)}`);
  }

  async overview(): Promise<ProviderOverview> {
    return this.call('/control/scenarios');
  }

  async close(runId: string): Promise<ProviderScenarioState> {
    const root = `/control/scenarios/${encodeURIComponent(runId)}`;
    const closed = await this.call<ProviderScenarioState>(`${root}/close`, {
      method: 'POST', body: { expected_cancelled: 0 },
    });
    const observed = await this.state(runId);
    const accounting = (state: ProviderScenarioState) => JSON.stringify([
      state.run_id, state.closed, state.expected_cancelled, state.pending_calls,
      state.unexpected_count, state.required_responses, state.consumed_required_responses,
      state.remaining_required_responses, state.counters, state.calls,
    ]);
    if (closed.run_id !== runId || closed.closed !== true ||
        closed.expected_cancelled !== 0 || closed.pending_calls !== 0 ||
        closed.unexpected_count !== 0 || closed.remaining_required_responses !== 0 ||
        accounting(closed) !== accounting(observed)) {
      throw new Error('Closed provider accounting readback changed.');
    }
    return observed;
  }
}

export function expectedReplyWasConsumed(
  state: ProviderScenarioState,
  runId: string,
  chatModel: string,
): boolean {
  const expectedSuccessCounters = state.counters.filter(
    (counter) =>
      counter.run_id === runId &&
      counter.model === chatModel &&
      counter.stream === true &&
      counter.outcome === 'success' &&
      counter.endpoint === 'chat.completions',
  );

  return (
    state.run_id === runId &&
    state.scenario === 'reply' &&
    state.required_responses === 1 &&
    state.remaining_required_responses === 0 &&
    state.consumed_required_responses === 1 &&
    expectedSuccessCounters.length === 1 &&
    expectedSuccessCounters[0].count === 1
  );
}

export function requireCleanProviderState(
  state: ProviderScenarioState,
  runId: string,
  chatModel: string,
  requirements: { requireIdle: boolean; requireReply: boolean },
  overview?: ProviderOverview,
): void {
  if (requirements.requireReply && !expectedReplyWasConsumed(state, runId, chatModel)) {
    throw new Error('The deterministic provider did not consume the required streamed reply.');
  }
  if (state.unexpected_count !== 0) {
    throw new Error(
      `The deterministic provider recorded ${state.unexpected_count} unexpected call(s).`,
    );
  }
  if (requirements.requireIdle && state.pending_calls !== 0) {
    throw new Error(`The deterministic provider still has ${state.pending_calls} pending call(s).`);
  }
  if (overview && overview.unscoped_unexpected_calls !== 0) {
    throw new Error(
      'The deterministic provider recorded ' +
        `${overview.unscoped_unexpected_calls} unscoped unexpected call(s).`,
    );
  }
  if (overview && (overview.runs.length !== 1 || overview.runs[0]?.run_id !== runId)) {
    throw new Error('The deterministic provider contains a scenario outside this E2E run.');
  }
}
