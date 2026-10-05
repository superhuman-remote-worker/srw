import { Injectable, Type } from '@angular/core';
import { ComponentMetadata, ComponentType } from '../../workbench/layout.model';

/**
 * Registry for components that can be loaded into layout panels.
 * Components must be registered before they can be used in layouts.
 */
@Injectable({
  providedIn: 'root',
})
export class ComponentRegistryService {
  private registry = new Map<ComponentType, ComponentMetadata>();
  private loads = new Map<ComponentType, Promise<Type<unknown>>>();

  /**
   * Register a component for use in the layout system.
   */
  register(metadata: ComponentMetadata): void {
    this.registry.set(metadata.type, metadata);
    this.loads.delete(metadata.type);
  }

  /**
   * Get metadata for a component type.
   */
  get(type: ComponentType): ComponentMetadata | undefined {
    return this.registry.get(type);
  }

  /**
   * Load the Angular component class for a type. Resolves `undefined` for an
   * unregistered type. The load runs once per type; a failed load is forgotten
   * so the next call retries (a chunk fetch can fail transiently).
   */
  loadComponent(type: ComponentType): Promise<Type<unknown> | undefined> {
    const meta = this.registry.get(type);
    if (!meta) return Promise.resolve(undefined);

    let pending = this.loads.get(type);
    if (!pending) {
      const started = meta.load();
      pending = started;
      this.loads.set(type, started);
      started.catch(() => {
        if (this.loads.get(type) === started) this.loads.delete(type);
      });
    }
    return pending;
  }

  /**
   * Get display name for a component type.
   */
  getDisplayName(type: ComponentType): string {
    return this.registry.get(type)?.displayName ?? type;
  }

  /**
   * Check if a component type is registered.
   */
  has(type: ComponentType): boolean {
    return this.registry.has(type);
  }

  /**
   * Get all registered component types.
   */
  getRegisteredTypes(): ComponentType[] {
    return Array.from(this.registry.keys());
  }
}
