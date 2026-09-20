import {afterEach, describe, expect, it, vi} from 'vitest';

afterEach(() => {
  delete (window as any).env;
  vi.resetModules();
});

describe('deployment capability flags', () => {
  it.each([
    [undefined, true], [false, false], [true, true],
    ['false', false], ['true', true], ['invalid', true],
  ])('preserves %s as %s', async (value, expected) => {
    (window as any).env = {serviceWorkerEnabled: value, externalClientsEnabled: value, adminToolsEnabled: value};
    vi.resetModules();
    const {environment} = await import('./environment');
    expect(environment.serviceWorkerEnabled).toBe(expected);
    expect(environment.externalClientsEnabled).toBe(expected);
    expect(environment.adminToolsEnabled).toBe(expected);
  });
});
