import {describe, expect, it} from 'vitest';
import {Injector, runInInjectionContext, signal} from '@angular/core';
import {ToolsGroupComponent} from './tools-group.component';
import type {SessionToolGroupsResponse} from '../../core/services/api.service';

function answer(states: Record<string, 'on' | 'off'>): SessionToolGroupsResponse {
  const categories: Record<string, unknown> = {};
  for (const [key, state] of Object.entries(states)) {
    categories[key] = {state, settable: true, tools: state === 'on' ? [`${key}_tool`] : []};
  }
  return {source: 'resolved', categories} as unknown as SessionToolGroupsResponse;
}

function mount(resolved: ReturnType<typeof signal<SessionToolGroupsResponse | null>>) {
  const component = runInInjectionContext(Injector.create({providers: []}), () => new ToolsGroupComponent());
  // Signal inputs cannot be set outside a template here; stub them.
  Object.defineProperty(component, 'mode', {value: () => 'job'});
  Object.defineProperty(component, 'config', {value: () => ({})});
  Object.defineProperty(component, 'resolved', {value: resolved});
  return component;
}

/**
 * The expert detail can land after the tool preview. Its config prefill then
 * anchors the switches while an older answer is still on screen, and rows read
 * "modified" against that answer although the user touched nothing. Edits are
 * measured against the anchor, so the next preview may still re-anchor.
 */
describe('ToolsGroupComponent edit detection vs the anchor', () => {
  it('a config prefill under a different server answer is not a user edit', () => {
    const resolved = signal<SessionToolGroupsResponse | null>(answer({research: 'on', loop: 'on'}));
    const tools = mount(resolved);
    tools.prefillFromResolved(resolved()!.categories);
    // The next expert's config says nothing about `loop`; the preview for it
    // will say `loop` is off.
    resolved.set(answer({research: 'on', loop: 'off'}));
    tools.prefillFromConfig({tools: {research: ['web_search']}});
    expect(tools.hasToolEdits()).toBe(false);
    expect(tools.getOverrides()).toEqual({});

    // So the preview's answer may re-anchor, and every row is pristine again.
    tools.prefillFromResolved(resolved()!.categories);
    expect(tools.rows().every((row) => row.pristine)).toBe(true);
  });

  it('a real toggle after the anchor still counts', () => {
    const resolved = signal<SessionToolGroupsResponse | null>(answer({research: 'on', loop: 'off'}));
    const tools = mount(resolved);
    tools.prefillFromResolved(resolved()!.categories);
    tools.toggleCategory('loop');
    expect(tools.hasToolEdits()).toBe(true);
  });
});
