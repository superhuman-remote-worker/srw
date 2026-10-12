import { Type } from '@angular/core';

/**
 * Available component types for the layout system.
 * Placeholder types are for testing; actual components will be added later.
 */
export type ComponentType =
  | 'placeholder-a'
  | 'placeholder-b'
  | 'placeholder-c'
  | 'agent-activity'
  | 'request-viewer'
  | 'agent-chat'
  | 'workspace'
  | 'db-table'
  | 'graph'
  | 'graph-timeline'
  | 'timeline'
  | 'job-selector'
  | 'metrics'
  | 'logs'
  | 'json-viewer'
  | 'todo-list'
  // Job Management Components
  | 'agent-list'
  | 'job-list'
  | 'job-create'
  | 'statistics'
  | 'datasource-list'
  | 'experts-list'
  | 'job-review'
  | 'workspace-browser'
  | 'project-list'
  | 'memory-panel'
  | 'action-center'
  | 'config-editor';

/**
 * Layout configuration for the tiling system.
 * Can be either a leaf (component) or a split (container with children).
 */
export interface LayoutConfig {
  /** Type of layout node */
  type: 'component' | 'split';
  /** Component type for leaf nodes */
  component?: ComponentType;
  /** Split direction: 'horizontal' = top/bottom, 'vertical' = left/right */
  direction?: 'horizontal' | 'vertical';
  /** Size percentages for each child (must sum to 100) */
  sizes?: number[];
  /** Child layout configs for split nodes */
  children?: LayoutConfig[];
}

/**
 * Metadata for registering a component with the layout system.
 */
export interface ComponentMetadata {
  /** Unique identifier matching ComponentType */
  type: ComponentType;
  /** Display name for panel header */
  displayName: string;
  /** Resolves the Angular component class. Loaded on first use, so panel code stays out of the initial bundle. */
  load: () => Promise<Type<unknown>>;
  /** Optional icon class */
  icon?: string;
}
