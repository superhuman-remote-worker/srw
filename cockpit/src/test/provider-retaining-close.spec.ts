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

  it.each(['wrong-life', 'not-closed', 'pending', 'counter-reset', 'changed-readback'])(
    'refuses %s closure evidence', async (fault) => {
      const closed = { run_id: 'retaining-guard', closed: true, expected_cancelled: 0,
        pending_calls: 0, unexpected_count: 0, remaining_required_responses: 0,
        required_responses: 1, consumed_required_responses: 1, counters: [{ count: 1 }], calls: [{ sequence: 1 }] };
      const observed = JSON.parse(JSON.stringify(closed));
      if (fault === 'wrong-life') closed.run_id = 'successor-life';
      if (fault === 'not-closed') closed.closed = false;
      if (fault === 'pending') closed.pending_calls = 1;
      if (fault === 'counter-reset') observed.counters = [];
      if (fault === 'changed-readback') observed.calls = [];
      vi.stubGlobal('fetch', vi.fn()
        .mockResolvedValueOnce(new Response(JSON.stringify(closed), { status: 200 }))
        .mockResolvedValueOnce(new Response(JSON.stringify(observed), { status: 200 })));
      const client = new ProviderControlClient('http://provider.test', 'fixture-only-control');
      await expect(client.close('retaining-guard')).rejects.toThrow('accounting readback');
    },
  );
});
