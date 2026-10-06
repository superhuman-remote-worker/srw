import {Component, Type} from '@angular/core';
import {describe, expect, it, vi} from 'vitest';
import {ComponentMetadata, ComponentType} from '../../workbench/layout.model';
import {ComponentRegistryService} from './component-registry.service';

@Component({selector: 'app-a', template: 'A'})
class A {}

@Component({selector: 'app-b', template: 'B'})
class B {}

const TYPE_A = 'placeholder-a' as ComponentType;
const TYPE_B = 'placeholder-b' as ComponentType;

function meta(type: ComponentType, load: () => Promise<Type<unknown>>): ComponentMetadata {
  return {type, displayName: type, load};
}

describe('ComponentRegistryService.loadComponent', () => {
  it('resolves undefined for an unregistered type', async () => {
    const registry = new ComponentRegistryService();
    await expect(registry.loadComponent(TYPE_A)).resolves.toBeUndefined();
  });

  it('resolves the class from the registered loader', async () => {
    const registry = new ComponentRegistryService();
    registry.register(meta(TYPE_A, () => Promise.resolve(A)));
    await expect(registry.loadComponent(TYPE_A)).resolves.toBe(A);
  });

  it('runs a type\'s loader once, including for concurrent calls', async () => {
    const registry = new ComponentRegistryService();
    const load = vi.fn(() => Promise.resolve(A as Type<unknown>));
    registry.register(meta(TYPE_A, load));

    const [first, second] = await Promise.all([
      registry.loadComponent(TYPE_A),
      registry.loadComponent(TYPE_A),
    ]);
    const third = await registry.loadComponent(TYPE_A);

    expect([first, second, third]).toEqual([A, A, A]);
    expect(load).toHaveBeenCalledTimes(1);
  });

  it('forgets a rejected load so the next call retries', async () => {
    const registry = new ComponentRegistryService();
    const load = vi
      .fn<() => Promise<Type<unknown>>>()
      .mockRejectedValueOnce(new Error('chunk failed'))
      .mockResolvedValueOnce(A);
    registry.register(meta(TYPE_A, load));

    await expect(registry.loadComponent(TYPE_A)).rejects.toThrow('chunk failed');
    await expect(registry.loadComponent(TYPE_A)).resolves.toBe(A);
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('drops only the re-registered type\'s cache', async () => {
    const registry = new ComponentRegistryService();
    const loadB = vi.fn(() => Promise.resolve(B as Type<unknown>));
    registry.register(meta(TYPE_A, () => Promise.resolve(A)));
    registry.register(meta(TYPE_B, loadB));
    await registry.loadComponent(TYPE_A);
    await registry.loadComponent(TYPE_B);

    registry.register(meta(TYPE_A, () => Promise.resolve(B)));

    await expect(registry.loadComponent(TYPE_A)).resolves.toBe(B);
    await expect(registry.loadComponent(TYPE_B)).resolves.toBe(B);
    expect(loadB).toHaveBeenCalledTimes(1);
  });
});
