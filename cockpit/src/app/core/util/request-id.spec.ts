import {afterEach, describe, expect, it, vi} from 'vitest';
import {newRequestId} from './request-id';

const V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe('newRequestId', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('uses crypto.randomUUID when it exists', () => {
    const spy = vi.spyOn(crypto, 'randomUUID').mockReturnValue('11111111-2222-4333-8444-555555555555');
    expect(newRequestId()).toBe('11111111-2222-4333-8444-555555555555');
    expect(spy).toHaveBeenCalledOnce();
    spy.mockRestore();
  });

  it('builds a valid v4 UUID when randomUUID is unavailable (plain-HTTP origin)', () => {
    vi.stubGlobal('crypto', {
      getRandomValues: crypto.getRandomValues.bind(crypto),
    });
    for (let i = 0; i < 50; i++) {
      expect(newRequestId()).toMatch(V4);
    }
  });

  it('produces distinct values', () => {
    vi.stubGlobal('crypto', {
      getRandomValues: crypto.getRandomValues.bind(crypto),
    });
    const ids = new Set(Array.from({length: 500}, () => newRequestId()));
    expect(ids.size).toBe(500);
  });
});
