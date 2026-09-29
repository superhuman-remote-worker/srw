import {describe, expect, it} from 'vitest';
import {Injector, runInInjectionContext} from '@angular/core';
import {
  allToolCategoriesSelected,
  delegationCapScopeForMode,
  delegationOverride,
  disabledToolCategoriesFromConfig,
  resolveSessionDelegationCap,
  sessionDelegationCapValue,
  ToolsGroupComponent,
} from './tools-group.component';

/**
 * Select-all state for the tool toggles. A category is "selected" (enabled)
 * unless it appears in the disabled set; grant-blocked categories are filtered
 * out by the caller before reaching here, so only toggleable keys are passed.
 */
describe('tools-group select-all state', () => {
  it('is true when nothing is disabled', () => {
    expect(allToolCategoriesSelected(['research', 'shell', 'delegation'], new Set())).toBe(true);
  });

  it('is false when one category is disabled', () => {
    expect(
      allToolCategoriesSelected(['research', 'shell', 'delegation'], new Set(['shell'])),
    ).toBe(false);
  });

  it('is false when all categories are disabled', () => {
    const keys = ['research', 'shell'];
    expect(allToolCategoriesSelected(keys, new Set(keys))).toBe(false);
  });

  it('is false when there are no selectable categories', () => {
    expect(allToolCategoriesSelected([], new Set())).toBe(false);
  });

  it('ignores disabled keys that are not in the selectable set', () => {
    // A grant-blocked category lingering in the disabled set must not flip the
    // state false, since it is excluded from the selectable keys.
    expect(allToolCategoriesSelected(['research'], new Set(['browser_direct']))).toBe(true);
  });
});

describe('resolved tool config', () => {
  it('recognizes categories disabled by persistent session defaults', () => {
    const disabled = disabledToolCategoriesFromConfig({
      tools: {
        communication: [],
        delegation: [],
        agent_catalog: [],
        workflows: [],
      },
    }, ['communication', 'delegation', 'agent_catalog', 'workflows', 'research']);

    expect(disabled).toEqual(new Set([
      'communication',
      'delegation',
      'agent_catalog',
      'workflows',
    ]));
  });
});

describe('delegation override', () => {
  it('emits only max_concurrent for an inline parameter edit', () => {
    expect(delegationOverride(true, true, 6)).toEqual({max_concurrent: 6});
  });

  it('keeps enabled synchronization and omits the retired keys', () => {
    const override = delegationOverride(false, true, 3);
    expect(override).toEqual({enabled: false, max_concurrent: 3});
    expect(override).not.toHaveProperty('max_depth');
    expect(override).not.toHaveProperty('default_timeout');
  });

  it('emits nothing when delegation still matches its baseline', () => {
    expect(delegationOverride(true, true, null)).toBeNull();
  });
});

describe('session delegation cap (parallel_subagents.md §6.4, D2)', () => {
  // A session allowed to fan out ignores `delegation.max_concurrent`, so the
  // session surfaces write `delegation.session_max_concurrent`; jobs, workers
  // and the expert editor keep `max_concurrent`.

  it('maps a session thread to the session cap and a job to the worker cap', () => {
    expect(delegationCapScopeForMode('session')).toBe('session');
    expect(delegationCapScopeForMode('live')).toBe('session');
    expect(delegationCapScopeForMode('job')).toBe('worker');
  });

  it('writes the key its scope names, and still defaults to the worker key', () => {
    expect(delegationOverride(true, true, 2, 'session_max_concurrent')).toEqual({
      session_max_concurrent: 2,
    });
    expect(delegationOverride(false, true, 3, 'session_max_concurrent')).toEqual({
      enabled: false,
      session_max_concurrent: 3,
    });
    expect(delegationOverride(true, true, 2)).toEqual({max_concurrent: 2});
    expect(delegationOverride(true, true, null, 'session_max_concurrent')).toBeNull();
  });

  it('accepts exactly what the orchestrator accepts: an integer from 1 to 20', () => {
    for (const ok of [1, 6, 20]) expect(sessionDelegationCapValue(ok)).toBe(ok);
    for (const refused of [0, -1, 21, 100, 2.5, Number.NaN, Infinity, true, false, '5', null, undefined]) {
      expect(sessionDelegationCapValue(refused)).toBeNull();
    }
  });

  it('an unset cap is inherited: default 6, else the model family value', () => {
    expect(resolveSessionDelegationCap({})).toEqual({explicit: null, inherited: 6});
    expect(resolveSessionDelegationCap({delegation: {max_concurrent: 2}})).toEqual({
      explicit: null,
      inherited: 6,
    });
    expect(
      resolveSessionDelegationCap({delegation: {family_session_max_concurrent: 4}}),
    ).toEqual({explicit: null, inherited: 4});
  });

  it('reads an explicit cap the way the runtime does (clamped into range)', () => {
    expect(resolveSessionDelegationCap({delegation: {session_max_concurrent: 3}}).explicit).toBe(3);
    expect(resolveSessionDelegationCap({delegation: {session_max_concurrent: 30}}).explicit).toBe(20);
    expect(resolveSessionDelegationCap({delegation: {session_max_concurrent: 0}}).explicit).toBe(1);
    expect(resolveSessionDelegationCap({delegation: {session_max_concurrent: 'x'}}).explicit).toBeNull();
  });
});

describe('ToolsGroupComponent delegation cap overrides', () => {
  function group(options: {
    scope?: 'worker' | 'session';
    config?: Record<string, unknown>;
  } = {}): ToolsGroupComponent {
    const component = runInInjectionContext(
      Injector.create({providers: []}),
      () => new ToolsGroupComponent(),
    );
    // Signal inputs cannot be set outside a template here; stub them.
    if (options.scope) {
      Object.defineProperty(component, 'delegationCapScope', {value: () => options.scope});
    }
    Object.defineProperty(component, 'config', {value: () => options.config ?? {}});
    // Delegation already on in the baseline, so only the cap can produce a diff.
    component.prefillFromConfig({tools: {delegation: ['delegate_agent']}});
    return component;
  }

  it('a session writes session_max_concurrent, never max_concurrent', () => {
    const component = group({scope: 'session'});
    component.onDelegationCapChange(2);
    expect(component.getOverrides()).toEqual({delegation: {session_max_concurrent: 2}});
  });

  it('a job (the default scope) still writes max_concurrent', () => {
    const component = group();
    component.onDelegationCapChange(2);
    expect(component.getOverrides()).toEqual({delegation: {max_concurrent: 2}});
  });

  it('an untouched session cap writes nothing, whatever the config carries', () => {
    expect(group({scope: 'session'}).getOverrides()).toEqual({});
    expect(
      group({scope: 'session', config: {delegation: {session_max_concurrent: 3}}}).getOverrides(),
    ).toEqual({});
  });

  it('an out-of-range or fractional session cap is flagged and never written', () => {
    const component = group({scope: 'session'});
    for (const refused of [0, 21, 2.5]) {
      component.onDelegationCapChange(refused);
      expect(component.delegationCapInvalid()).toBe(true);
      expect(component.delegationCapValid()).toBe(false);
      expect(component.getOverrides()).toEqual({});
    }
    component.onDelegationCapChange(20);
    expect(component.delegationCapValid()).toBe(true);
    expect(component.getOverrides()).toEqual({delegation: {session_max_concurrent: 20}});
  });

  it('an emptied field is no edit: back to inherited, nothing written', () => {
    const component = group({scope: 'session'});
    component.onDelegationCapChange(4);
    component.onDelegationCapChange(null);
    expect(component.delegationCap()).toBeNull();
    expect(component.delegationCapValid()).toBe(true);
    expect(component.getOverrides()).toEqual({});
  });

  it('a typed cap is an edit a late preview must not re-anchor away', () => {
    // The creation forms re-read the tool preview on every change and
    // re-anchor unless hasToolEdits(); the anchor clears the cap, so the
    // preview answering the cap edit used to wipe the value just typed.
    const component = group({scope: 'session'});
    expect(component.hasToolEdits()).toBe(false);
    component.onDelegationCapChange(3);
    expect(component.hasToolEdits()).toBe(true);
    component.resetDelegationCap();
    expect(component.hasToolEdits()).toBe(false);
  });

  it('the worker cap keeps its old, unvalidated pass-through', () => {
    const component = group();
    component.onDelegationCapChange(25);
    expect(component.delegationCapValid()).toBe(true);
    expect(component.getOverrides()).toEqual({delegation: {max_concurrent: 25}});
  });
});
