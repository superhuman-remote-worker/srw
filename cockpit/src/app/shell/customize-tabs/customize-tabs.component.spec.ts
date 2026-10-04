import {describe, expect, it} from 'vitest';
import {CustomizeTabsComponent} from './customize-tabs.component';

describe('CustomizeTabsComponent', () => {
  it('puts Workspaces second, between Experts and Skills', () => {
    const tabs = (new CustomizeTabsComponent() as unknown as {tabs: {path: string; labelKey: string}[]}).tabs;
    expect(tabs.map((t) => t.path)).toEqual(['/experts', '/workspaces', '/skills', '/datasources', '/contacts']);
    expect(tabs[1].labelKey).toBe('nav.workspaces');
  });
});
