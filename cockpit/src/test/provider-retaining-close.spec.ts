import { afterEach, describe, expect, it, vi } from 'vitest';
import { ProviderControlClient, ProviderScenarioState } from '../../e2e/app/provider-control';

describe('owned provider retaining close', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('uses retaining close and reads the same cumulative accounting back', async () => {
    const state = {
      run_id: 'retaining-client', closed: true, expected_cancelled: 0,
      pending_calls: 0, unexpected_count: 0, required_responses: 1,
      consumed_required_responses: 1, remaining_required_responses: 0,
      scenario: 'reply', counters: [{ endpoint: 'chat.completions', count: 1 }],
      calls: [{ sequence: 1, outcome: 'success' }],
    };
    const fetch = vi.fn().mockImplementation(async () => new Response(JSON.stringify(state), { status: 200 }));
    vi.stubGlobal('fetch', fetch);
    const client = new ProviderControlClient('http://provider.test', 'fixture-only-control') as
      ProviderControlClient & { close(runId: string): Promise<ProviderScenarioState> };
    expect(client.close).toBeTypeOf('function');
    expect(await client.close(state.run_id)).toEqual(state);
    expect(fetch.mock.calls.map(([url, options]) => [new URL(url).pathname, options.method])).toEqual([
      ['/control/scenarios/retaining-client/close', 'POST'],
      ['/control/scenarios/retaining-client', 'GET'],
    ]);
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({ expected_cancelled: 0 });
  });
});
