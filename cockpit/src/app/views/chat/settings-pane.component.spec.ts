import {Injector, runInInjectionContext, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import {Observable, Subject, of} from 'rxjs';
import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import en from '../../../assets/i18n/en.json';
import {ApiService} from '../../core/services/api.service';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {ModelService} from '../../core/services/model.service';
import {PersistentChatService} from '../../core/services/persistent-chat.service';
import type {
  SessionToolCategory,
  SessionToolGroupsResponse,
} from '../../core/services/api.service';
import {ToolsGroupComponent} from '../agent-settings/tools-group.component';
import {SettingsPaneComponent} from './settings-pane.component';

/** Build the resolved tool-groups answer the pane now reads. */
function toolsetAnswer(
  states: Record<string, 'on' | 'off' | 'unavailable'>,
  over: Partial<SessionToolGroupsResponse> = {},
): SessionToolGroupsResponse {
  const categories: Record<string, SessionToolCategory> = {};
  for (const [key, state] of Object.entries(states)) {
    categories[key] = {
      state,
      settable: state !== 'unavailable',
      reason: state === 'unavailable' ? 'the agent bound none' : null,
      decided_by: 'base',
      tools: state === 'on' ? ['x'] : [],
    };
  }
  return {
    thread_id: 'thread-1',
    source: 'resolved',
    origin: 'agent',
    observed_at: '2026-08-03T10:00:00Z',
    enumerate_only: {shell: ['run_command', 'shell_read']},
    tool_groups: null,
    categories,
    ...over,
  };
}

/**
 * The live settings pane's desired-state diff (live_session_settings.md
 * Slice A): control edits collect through the hosted settings surface, the
 * pane diffs against what was last applied, and dispatches only the delta —
 * permission/narration through their dedicated verbs, everything else through
 * one coalesced config.update.
 */

function createPane(options: {
  override?: Record<string, unknown>;
  grants?: Record<string, unknown> | null;
  attachedIds?: string[];
  eligible?: Array<{id: string; type: string; name: string}>;
  /** The resolved toolset; null models an orchestrator that predates the
   *  endpoint (the tools surface then degrades to its static list). */
  toolGroups?: SessionToolGroupsResponse | null;
  /** Observable for the tool-groups call, to drive the load-ordering test. */
  toolGroups$?: Observable<SessionToolGroupsResponse | null>;
  /** Thread ids whose metadata differs, for the thread-switch test. */
  threadsById?: Record<string, Record<string, unknown>>;
} = {}) {
  const chat = {
    threadId: signal<string | null>('thread-1'),
    isConnected: signal(true),
    modelName: signal<string | null>('gemma-4-moe'),
    temperature: signal<number | null>(0.7),
    permissionMode: signal('supervised'),
    narrationMode: signal('auto'),
    workspaceTier: signal<string | null>(null),
    workspaceUpgradeInProgress: signal<{tier: string; elapsed?: number} | null>(null),
    updateConfig: vi.fn().mockResolvedValue({
      ok: true,
      requestId: 'req-1',
      transport: 'websocket',
      applied: {},
      effective: 'now',
    }),
    setMode: vi.fn(),
    setNarrationMode: vi.fn(),
    upgradeWorkspace: vi.fn(),
    controlTransport: vi.fn(() => 'websocket'),
  };
  const api = {
    getPersistentThread: vi.fn().mockReturnValue(
      of({
        metadata: {
          config_override: options.override ?? {},
          datasource_ids: options.attachedIds ?? [],
        },
        project_ids: [],
      }),
    ),
    getEligibleDatasources: vi.fn().mockReturnValue(of(options.eligible ?? [])),
    getSessionToolGroups: vi
      .fn()
      .mockReturnValue(options.toolGroups$ ?? of(options.toolGroups ?? null)),
    // Locked-row facts: display only, never part of the diff.
    getExperts: vi.fn().mockReturnValue(of([])),
    getProject: vi.fn().mockReturnValue(of(null)),
    getThreadResolvedConfig: vi.fn().mockReturnValue(of(null)),
  };
  const capabilities = {
    grants: signal(options.grants ?? null),
    datasourceScopeAutoAttachAvailable: () => true,
  };
  const modelService = {load: vi.fn()};

  TestBed.configureTestingModule({
    providers: [
      SettingsPaneComponent,
      {provide: PersistentChatService, useValue: chat},
      {provide: ApiService, useValue: api},
      {provide: CapabilitiesService, useValue: capabilities},
      {provide: ModelService, useValue: modelService},
    ],
  });
  const component = TestBed.inject(SettingsPaneComponent);
  // Pane is open; templates aren't rendered, so stub the input + viewChild.
  Object.defineProperty(component, 'active', {value: () => true});
  const fakeSettings = {
    prefillFromConfig: vi.fn(),
    prefillFromResolvedToolset: vi.fn(),
    hasToolEdits: vi.fn().mockReturnValue(false),
    getOverrides: vi.fn().mockReturnValue({}),
    // Real group defaults its selection to the attached set (via
    // initialSelectedIds); mirror that default here.
    getSelectedDatasourceIds: vi.fn(() => options.attachedIds ?? []),
    resetDatasourceSelection: vi.fn(),
    getToolAdditions: vi.fn().mockReturnValue({} as Record<string, string[]>),
  };
  Object.defineProperty(component, 'settings', {value: () => fakeSettings});
  TestBed.tick(); // fire the load effect
  return {component, chat, api, fakeSettings};
}

/** `shell` bound to exactly the code-granted `srw_cloud_status` — the default
 *  topology's shape once a cloud mount is active. `settable` is the only
 *  thing that varies between the two real-world cases this file needs to
 *  tell apart. */
function lockedOnShellAnswer(settable: boolean): SessionToolGroupsResponse {
  return {
    thread_id: 'thread-1',
    source: 'resolved',
    origin: 'agent',
    observed_at: '2026-08-03T10:00:00Z',
    enumerate_only: {shell: ['run_command', 'shell_read']},
    tool_groups: null,
    categories: {
      shell: {
        state: 'on',
        settable,
        reason: settable
          ? null
          : 'the runtime binds srw_cloud_status here regardless of config (cloud_mount_manager.active)',
        decided_by: settable ? 'base' : 'registry',
        tools: ['srw_cloud_status'],
      },
    },
  };
}

/**
 * Same wiring as `createPane`, except `settings()` is backed by a REAL
 * `ToolsGroupComponent` instead of a hand-mocked `getOverrides()`.
 *
 * The B2 fast-follow this exists to pin lives entirely in the interaction
 * between the real `toolsFragment` call inside
 * `ToolsGroupComponent.getOverrides()` and the real one inside
 * `SettingsPaneComponent.applyChanges()` — a `getOverrides` that just returns
 * a literal (as `createPane`'s `fakeSettings` does) cannot exercise either,
 * so this is the one place in the file that needs the real control.
 * `ToolsGroupComponent` injects nothing, so constructing it outside a
 * template (no `TestBed.createComponent`) is the same trick
 * live-mode.spec.ts's `createToolsGroup` uses, and for the same reason.
 */
function createPaneWithRealToolsGroup(toolGroups: SessionToolGroupsResponse) {
  const chat = {
    threadId: signal<string | null>('thread-1'),
    isConnected: signal(true),
    modelName: signal<string | null>(null),
    temperature: signal<number | null>(null),
    permissionMode: signal('supervised'),
    narrationMode: signal('auto'),
    workspaceTier: signal<string | null>(null),
    workspaceUpgradeInProgress: signal<{tier: string; elapsed?: number} | null>(null),
    updateConfig: vi.fn().mockResolvedValue({
      ok: true,
      requestId: 'req-1',
      transport: 'websocket',
      applied: {},
      effective: 'now',
    }),
    setMode: vi.fn(),
    setNarrationMode: vi.fn(),
    upgradeWorkspace: vi.fn(),
    controlTransport: vi.fn(() => 'websocket'),
  };
  const api = {
    getPersistentThread: vi.fn().mockReturnValue(
      of({metadata: {config_override: {}, datasource_ids: []}, project_ids: []}),
    ),
    getEligibleDatasources: vi.fn().mockReturnValue(of([])),
    getSessionToolGroups: vi.fn().mockReturnValue(of(toolGroups)),
    getExperts: vi.fn().mockReturnValue(of([])),
    getProject: vi.fn().mockReturnValue(of(null)),
    getThreadResolvedConfig: vi.fn().mockReturnValue(of(null)),
  };
  const capabilities = {
    grants: signal(null),
    datasourceScopeAutoAttachAvailable: () => true,
  };
  const modelService = {load: vi.fn()};

  TestBed.configureTestingModule({
    providers: [
      SettingsPaneComponent,
      {provide: PersistentChatService, useValue: chat},
      {provide: ApiService, useValue: api},
      {provide: CapabilitiesService, useValue: capabilities},
      {provide: ModelService, useValue: modelService},
    ],
  });
  const component = TestBed.inject(SettingsPaneComponent);
  Object.defineProperty(component, 'active', {value: () => true});

  const toolsGroup = runInInjectionContext(Injector.create({providers: []}), () => new ToolsGroupComponent());
  Object.defineProperty(toolsGroup, 'resolved', {value: () => toolGroups});

  const settings = {
    prefillFromConfig: (config: Record<string, unknown>) => toolsGroup.prefillFromConfig(config),
    prefillFromResolvedToolset: (categories: Record<string, SessionToolCategory>) =>
      toolsGroup.prefillFromResolved(categories),
    hasToolEdits: () => toolsGroup.hasToolEdits(),
    getOverrides: () => toolsGroup.getOverrides(),
    getSelectedDatasourceIds: () => [] as string[],
    resetDatasourceSelection: () => {},
    getToolAdditions: () => toolsGroup.getToolAdditions(),
  };
  Object.defineProperty(component, 'settings', {value: () => settings});
  TestBed.tick(); // fire the load effect
  return {component, chat, toolsGroup};
}

describe('SettingsPaneComponent locked-on categories', () => {
  // `compose_tool_view` marks a category `on` + `settable: false` when every
  // tool it bound is a per-tool code grant — the default topology's `shell`,
  // once a cloud mount adds `srw_cloud_status`. Two facts hold at once and the
  // pane must honour both: the category cannot be turned OFF (the runtime
  // re-appends after the merge, so `[]` is a promise nothing can keep), and
  // config can still ADD the four settable shell tools, which is how a running
  // session gains shell at all.
  //
  // The off half is now structural — `toolsFragment` refuses every unsettable
  // key in both directions, with no exemption to strip afterwards — and the add
  // half rides its own tracked path, because a locked-on category's boolean is
  // `true` before the gesture and `true` after it, so the switch diff is blind
  // to it by construction.
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    vi.useRealTimers();
    TestBed.resetTestingModule();
  });

  it('unsettable + turning off: nothing dispatched, however many times it is toggled', () => {
    const {component, chat, toolsGroup} = createPaneWithRealToolsGroup(lockedOnShellAnswer(false));

    toolsGroup.toggleCategory('shell'); // untick
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).not.toHaveBeenCalled();

    toolsGroup.toggleCategory('shell'); // re-tick — no longer a route to the enable
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).not.toHaveBeenCalled();
  });

  it('unsettable + the add affordance: the enable is dispatched, in ONE gesture', () => {
    const {component, chat, toolsGroup} = createPaneWithRealToolsGroup(lockedOnShellAnswer(false));

    toolsGroup.requestAdditions('shell');
    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({
      tools: {shell: {only: ['run_command', 'shell_read']}},
    });
  });

  it('the add request dispatches once, not on every later unrelated edit', () => {
    const {component, chat, toolsGroup} = createPaneWithRealToolsGroup(lockedOnShellAnswer(false));

    toolsGroup.requestAdditions('shell');
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledTimes(1);

    // A second apply with nothing new must be silent: the tracked path is now
    // part of the baseline, so the request is not re-sent forever.
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledTimes(1);
  });

  it('a locked-on category has no add request to make when config cannot add to it', () => {
    // `enumerate_only` carries no entry, so the policy would be `true`, whose
    // expansion the client cannot see. Nothing is offered and nothing is sent.
    const answer = lockedOnShellAnswer(false);
    answer.enumerate_only = {};
    const {component, chat, toolsGroup} = createPaneWithRealToolsGroup(answer);

    toolsGroup.requestAdditions('shell');
    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).not.toHaveBeenCalled();
  });

  it('control: the SAME untick dispatches the off-write when the server marks it settable', () => {
    // Sanity check on the harness, not the fix: with `settable: true` the
    // untick DOES reach the wire — the pre-existing, correct behaviour this
    // task must leave alone — so the silence in the first test above is the
    // refusal, not a mount that never dispatches anything.
    const {component, chat, toolsGroup} = createPaneWithRealToolsGroup(lockedOnShellAnswer(true));

    toolsGroup.toggleCategory('shell');
    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({tools: {shell: []}});
  });
});

/**
 * From a RENDERED click to the wire, in one test.
 *
 * The tests above drive `toolsGroup.requestAdditions('shell')` and the ones in
 * tools-group.render.spec.ts click the real button and read the component back.
 * Neither, alone, is what failed: the defect was a control that rendered
 * `disabled` while every method behind it worked, so a suite that calls methods
 * and a suite that never dispatches both stay green. This one starts at a DOM
 * element and ends at `chat.updateConfig`.
 */
describe('SettingsPaneComponent locked-on categories, from the DOM', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    vi.useRealTimers();
    TestBed.resetTestingModule();
  });

  function mountPaneWithRenderedToolsGroup(toolGroups: SessionToolGroupsResponse) {
    const chat = {
      threadId: signal<string | null>('thread-1'),
      isConnected: signal(true),
      modelName: signal<string | null>(null),
      temperature: signal<number | null>(null),
      permissionMode: signal('supervised'),
      narrationMode: signal('auto'),
      workspaceTier: signal<string | null>(null),
      workspaceUpgradeInProgress: signal<{tier: string; elapsed?: number} | null>(null),
      updateConfig: vi.fn().mockResolvedValue({
      ok: true,
      requestId: 'req-1',
      transport: 'websocket',
      applied: {},
      effective: 'now',
    }),
      setMode: vi.fn(),
      setNarrationMode: vi.fn(),
      upgradeWorkspace: vi.fn(),
      controlTransport: vi.fn(() => 'websocket'),
    };
    const api = {
      getPersistentThread: vi.fn().mockReturnValue(
        of({metadata: {config_override: {}, datasource_ids: []}, project_ids: []}),
      ),
      getEligibleDatasources: vi.fn().mockReturnValue(of([])),
      getSessionToolGroups: vi.fn().mockReturnValue(of(toolGroups)),
      getExperts: vi.fn().mockReturnValue(of([])),
      getProject: vi.fn().mockReturnValue(of(null)),
      getThreadResolvedConfig: vi.fn().mockReturnValue(of(null)),
    };

    TestBed.configureTestingModule({
      imports: [
        ToolsGroupComponent,
        TranslocoTestingModule.forRoot({
          langs: {en},
          translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
        }),
      ],
      providers: [
        SettingsPaneComponent,
        {provide: PersistentChatService, useValue: chat},
        {provide: ApiService, useValue: api},
        {
          provide: CapabilitiesService,
          useValue: {grants: signal(null), datasourceScopeAutoAttachAvailable: () => true},
        },
        {provide: ModelService, useValue: {load: vi.fn()}},
      ],
    });

    // The REAL control, rendered. Signal inputs cannot be set through
    // setInput() in this pipeline (no ngtsc for vitest) — same workaround as
    // tools-group.render.spec.ts.
    const fixture = TestBed.createComponent(ToolsGroupComponent);
    const toolsGroup = fixture.componentInstance;
    Object.defineProperty(toolsGroup, 'mode', {value: () => 'live'});
    Object.defineProperty(toolsGroup, 'resolved', {value: () => toolGroups});
    fixture.detectChanges();

    const component = TestBed.inject(SettingsPaneComponent);
    Object.defineProperty(component, 'active', {value: () => true});
    Object.defineProperty(component, 'settings', {
      value: () => ({
        prefillFromConfig: (config: Record<string, unknown>) => toolsGroup.prefillFromConfig(config),
        prefillFromResolvedToolset: (categories: Record<string, SessionToolCategory>) =>
          toolsGroup.prefillFromResolved(categories),
        hasToolEdits: () => toolsGroup.hasToolEdits(),
        getOverrides: () => toolsGroup.getOverrides(),
        getSelectedDatasourceIds: () => [] as string[],
        resetDatasourceSelection: () => {},
        getToolAdditions: () => toolsGroup.getToolAdditions(),
      }),
    });
    // The control emits `change`; in the app the host binds it to
    // onSettingsChange(). Wire the same edge here rather than calling the pane
    // by hand, so the click really is the only input to this test.
    toolsGroup.change.subscribe(() => component.onSettingsChange());
    TestBed.tick(); // fire the pane's load effect
    fixture.detectChanges();
    return {component, chat, fixture};
  }

  function shellAdditions(fixture: {nativeElement: unknown}): HTMLElement | null {
    return (fixture.nativeElement as HTMLElement).querySelector(
      '.tool-additions[data-category="shell"]',
    );
  }

  it('clicking the rendered ADD button dispatches the enumeration', () => {
    const {chat, fixture} = mountPaneWithRenderedToolsGroup(lockedOnShellAnswer(false));

    const box = (fixture.nativeElement as HTMLElement).querySelector(
      'input[type="checkbox"]',
    ) as HTMLInputElement;
    expect(box.checked).toBe(true);
    expect(box.disabled).toBe(true);

    const button = shellAdditions(fixture)?.querySelector('button') as HTMLButtonElement;
    expect(button).toBeTruthy();
    button.click();
    fixture.detectChanges();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({
      tools: {shell: {only: ['run_command', 'shell_read']}},
    });
  });

  it('clicking the rendered CHECKBOX of the same row dispatches nothing', () => {
    // The positive control is the test above: the harness demonstrably reaches
    // the wire, so this silence is the refusal and not a dead mount.
    const {chat, fixture} = mountPaneWithRenderedToolsGroup(lockedOnShellAnswer(false));

    const box = (fixture.nativeElement as HTMLElement).querySelector(
      'input[type="checkbox"]',
    ) as HTMLInputElement;
    box.click();
    box.dispatchEvent(new Event('change')); // forced, past the disabled attribute
    fixture.detectChanges();
    vi.runAllTimers();

    expect(chat.updateConfig).not.toHaveBeenCalled();
    expect(box.checked).toBe(true);
  });
});

describe('SettingsPaneComponent apply diff', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    vi.useRealTimers();
    TestBed.resetTestingModule();
  });

  it('prefills from thread metadata and dispatches nothing without edits', () => {
    const {component, chat, fakeSettings} = createPane();
    expect(fakeSettings.prefillFromConfig).toHaveBeenCalled();

    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).not.toHaveBeenCalled();
    expect(chat.setMode).not.toHaveBeenCalled();
  });

  it('a model pin dispatches exactly the llm delta', () => {
    const {component, chat, fakeSettings} = createPane();
    fakeSettings.getOverrides.mockReturnValue({llm: {model: 'minimax-m3'}});

    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({llm: {model: 'minimax-m3'}});
    expect(chat.setMode).not.toHaveBeenCalled();
  });

  it('permission mode rides its dedicated verb, never config.update', () => {
    const {component, chat, fakeSettings} = createPane();
    fakeSettings.getOverrides.mockReturnValue({interactive: {permission_mode: 'autonomous'}});

    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.setMode).toHaveBeenCalledExactlyOnceWith('autonomous');
    expect(chat.updateConfig).not.toHaveBeenCalled();
  });

  it('tool toggles send [] to disable and a policy to re-enable', () => {
    // Baseline must have canvas OFF and workflows ON for both directions to
    // diff. The AGENT's answer is that baseline now — not a config merge,
    // which cannot see the runtime injection layer or the capability gate.
    const {component, chat, fakeSettings} = createPane({
      toolGroups: toolsetAnswer({
        orchestrator: 'off',
        agent_catalog: 'off',
        workflows: 'on',
        canvas: 'off',
      }),
    });
    // Re-enable canvas, disable workflows in one batch.
    fakeSettings.getOverrides.mockReturnValue({
      tools: {canvas: true, workflows: []},
    });

    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({
      tools: {canvas: true, workflows: []},
    });
  });

  it('the tools baseline is anchored to the AGENT answer, not the config', () => {
    // The reported bug: config_override says nothing about orchestrator, so
    // the pane used to render it ticked while the agent had zero fleet tools.
    const {fakeSettings} = createPane({
      override: {},
      toolGroups: toolsetAnswer({
        orchestrator: 'off',
        agent_catalog: 'off',
        workflows: 'off',
        canvas: 'on',
      }),
    });

    const anchored = fakeSettings.prefillFromResolvedToolset.mock.calls[0][0] as Record<
      string,
      SessionToolCategory
    >;
    expect(anchored['orchestrator'].state).toBe('off');
    expect(anchored['canvas'].state).toBe('on');
    // And the config prefill no longer carries a synthesised tools layer.
    const prefilled = fakeSettings.prefillFromConfig.mock.calls[0][0] as Record<string, unknown>;
    expect(prefilled['tools']).toBeUndefined();
  });

  it('the surface degrades honestly when the endpoint is unavailable', () => {
    // Older orchestrator (404) or a failed request → null. The pane must not
    // invent a baseline; it anchors nothing and dispatches no tool fragment.
    const {component, chat, fakeSettings} = createPane({override: {}, toolGroups: null});

    expect(fakeSettings.prefillFromResolvedToolset).not.toHaveBeenCalled();
    fakeSettings.getOverrides.mockReturnValue({llm: {model: 'a'}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({llm: {model: 'a'}});
  });

  it('every category the answer returns is diffable, not just four', () => {
    // The live pane could only carry canvas/orchestrator/agent_catalog/
    // workflows. `knowledge` is one of the eight it could not express.
    const {component, chat, fakeSettings} = createPane({
      toolGroups: toolsetAnswer({canvas: 'on', knowledge: 'on', git: 'off'}),
    });
    fakeSettings.getOverrides.mockReturnValue({tools: {knowledge: []}});

    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({tools: {knowledge: []}});
  });

  it('an unavailable category is never dispatched, however the surface reports it', () => {
    // The server refused. Writing config against a grant or a workspace tier
    // produces a fragment the PDP will reject and changes no binding.
    const {component, chat, fakeSettings} = createPane({
      toolGroups: toolsetAnswer({shell: 'unavailable', canvas: 'on'}),
    });
    fakeSettings.getOverrides.mockReturnValue({tools: {shell: true}});

    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).not.toHaveBeenCalled();
  });

  it('re-enabling a base-disabled group dispatches a policy', () => {
    // This path was dead before the fix: the group rendered ticked, so its
    // baseline was "enabled" and turning it on could never produce a delta.
    const {component, chat, fakeSettings} = createPane({
      override: {},
      toolGroups: toolsetAnswer({
        orchestrator: 'off',
        agent_catalog: 'off',
        workflows: 'off',
        canvas: 'on',
      }),
    });
    fakeSettings.getOverrides.mockReturnValue({tools: {orchestrator: true}});

    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({
      tools: {orchestrator: true},
    });
  });

  it('late-arriving server defaults never fire a spurious config.update', () => {
    // The race this design exists to prevent: if the tool-group answer landed
    // after the lastApplied anchor, liveConfig() would shift under a stale
    // baseline and the next change would silently disable three tool groups
    // the user never touched.
    const toolGroups$ = new Subject<Record<string, boolean> | null>();
    const {component, chat, fakeSettings} = createPane({override: {}, toolGroups$});

    // Nothing is prefilled or anchored while the join is still open.
    expect(fakeSettings.prefillFromConfig).not.toHaveBeenCalled();
    component.onSettingsChange();
    // Bounded advance, not runAllTimers: with no baseline yet, applyChanges
    // deliberately re-arms the debounce rather than absorbing the edit, so the
    // queue never drains until the fetch lands (400 ms re-arm, self-limiting).
    vi.advanceTimersByTime(2000);
    expect(chat.updateConfig).not.toHaveBeenCalled();

    // If this landed after the anchor, the baseline would hold a different
    // set from what the surface renders and the next change would diff
    // against it.
    toolGroups$.next(
      toolsetAnswer({
        orchestrator: 'on',
        agent_catalog: 'off',
        workflows: 'off',
        canvas: 'on',
      }),
    );
    toolGroups$.complete();

    expect(fakeSettings.prefillFromConfig).toHaveBeenCalled();
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).not.toHaveBeenCalled();
  });

  it('a thread switch drops the diff baseline BEFORE the fetch', () => {
    // The race: `lastApplied` used to survive `loadThread`. For the whole
    // request window — sub-second once, now up to 8s because the read probes
    // the agent pod — an edit would diff the NEW session's desired state
    // against the OLD session's baseline and dispatch the difference into the
    // new session. Concretely: thread-1 has canvas ON, thread-2 has it OFF, so
    // a stale baseline turns an unrelated model pin into "disable canvas".
    const toolGroups$ = new Subject<SessionToolGroupsResponse | null>();
    const {component, chat, api, fakeSettings} = createPane({
      toolGroups: toolsetAnswer({canvas: 'on'}),
    });
    // Baseline for thread-1 is anchored.
    fakeSettings.getOverrides.mockReturnValue({llm: {model: 'a'}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledTimes(1);

    // Switch threads; the new read is still in flight.
    api.getSessionToolGroups.mockReturnValue(toolGroups$);
    chat.threadId.set('thread-2');
    TestBed.tick();

    // An edit lands inside the window.
    fakeSettings.getOverrides.mockReturnValue({llm: {model: 'b'}});
    component.onSettingsChange();
    vi.advanceTimersByTime(2000);
    expect(chat.updateConfig).toHaveBeenCalledTimes(1);

    // Only once the new thread's answer lands does anything dispatch, and it
    // dispatches against the NEW baseline.
    toolGroups$.next(toolsetAnswer({canvas: 'off'}));
    toolGroups$.complete();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledTimes(2);
    expect(chat.updateConfig).toHaveBeenNthCalledWith(2, {llm: {model: 'b'}});
  });

  it('a thread switch drops the previous thread config too', () => {
    // `threadOverride` had the same shape of bug: liveConfig() kept serving
    // the previous session's durable config while the pane already pointed
    // elsewhere, so every control read from the wrong session for the window.
    const toolGroups$ = new Subject<SessionToolGroupsResponse | null>();
    const {component, chat, api} = createPane({
      override: {workspace: {backend: 'vm'}},
      attachedIds: ['ds-pg'],
    });
    expect(component.workspaceTier()).toBe('vm');

    chat.workspaceTier.set(null);
    api.getSessionToolGroups.mockReturnValue(toolGroups$);
    chat.threadId.set('thread-2');
    TestBed.tick();

    expect(component.workspaceTier()).toBe('virtual');
    expect(component.attachedIds()).toEqual([]);
  });

  it('a response for a superseded thread is ignored', () => {
    const toolGroups$ = new Subject<Record<string, boolean> | null>();
    const {chat, fakeSettings} = createPane({override: {}, toolGroups$});

    chat.threadId.set('thread-2');
    toolGroups$.next(null);
    toolGroups$.complete();

    expect(fakeSettings.prefillFromConfig).not.toHaveBeenCalled();
  });

  it('rapid edits coalesce into one config.update (one cache invalidation)', () => {
    const {component, chat, fakeSettings} = createPane();
    fakeSettings.getOverrides.mockReturnValue({llm: {model: 'a'}});
    component.onSettingsChange();
    fakeSettings.getOverrides.mockReturnValue({llm: {model: 'a', temperature: 1.2}});
    component.onSettingsChange();

    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({
      llm: {model: 'a', temperature: 1.2},
    });
  });

  it('an unchanged batch after an applied one dispatches nothing new', () => {
    const {component, chat, fakeSettings} = createPane();
    fakeSettings.getOverrides.mockReturnValue({llm: {model: 'a'}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledTimes(1);

    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledTimes(1);
  });
});

describe('SettingsPaneComponent datasources (Slice B)', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    vi.useRealTimers();
    TestBed.resetTestingModule();
  });

  const PG = {id: 'ds-pg', type: 'postgresql', name: 'PG'};
  const WEB = {id: 'ds-web', type: 'webdav', name: 'Cloud'};

  it('a datasource toggle dispatches the FULL desired set, never a delta', () => {
    const {component, chat, fakeSettings} = createPane({
      attachedIds: ['ds-pg'],
      eligible: [PG, WEB],
    });
    fakeSettings.getSelectedDatasourceIds.mockReturnValue(['ds-pg', 'ds-web']);

    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({}, ['ds-pg', 'ds-web']);
  });

  it('renders unavailable attached ids so an explicit uncheck can detach them', () => {
    const {component, chat, fakeSettings} = createPane({
      attachedIds: ['ds-pg', 'kb-hidden'],
      eligible: [PG, WEB],
    });
    expect(component.pickerDatasources().find(ds => ds.id === 'kb-hidden')?.unavailable)
      .toBe(true);
    // The user explicitly unchecks both the ordinary and unavailable rows.
    fakeSettings.getSelectedDatasourceIds.mockReturnValue([]);

    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({}, []);
  });

  it('a config-only edit sends no datasource_ids at all', () => {
    const {component, chat, fakeSettings} = createPane({
      attachedIds: ['ds-pg'],
      eligible: [PG],
    });
    fakeSettings.getOverrides.mockReturnValue({llm: {model: 'minimax-m3'}});

    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({llm: {model: 'minimax-m3'}});
  });

  it('pairwise preservation: model pin then datasource toggle — each dispatch carries only its own change', () => {
    const {component, chat, fakeSettings} = createPane({
      attachedIds: ['ds-pg'],
      eligible: [PG, WEB],
    });
    fakeSettings.getOverrides.mockReturnValue({llm: {model: 'minimax-m3'}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenNthCalledWith(1, {llm: {model: 'minimax-m3'}});

    fakeSettings.getSelectedDatasourceIds.mockReturnValue(['ds-pg', 'ds-web']);
    component.onSettingsChange();
    vi.runAllTimers();
    // The datasource dispatch must not re-send (or reset) the model pin.
    expect(chat.updateConfig).toHaveBeenNthCalledWith(2, {}, ['ds-pg', 'ds-web']);
    expect(chat.updateConfig).toHaveBeenCalledTimes(2);
  });

  it('pairwise preservation: datasource toggle then permission mode — the mode verb fires without a datasource resend', () => {
    const {component, chat, fakeSettings} = createPane({
      attachedIds: [],
      eligible: [PG],
    });
    fakeSettings.getSelectedDatasourceIds.mockReturnValue(['ds-pg']);
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({}, ['ds-pg']);

    fakeSettings.getOverrides.mockReturnValue({interactive: {permission_mode: 'autonomous'}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.setMode).toHaveBeenCalledExactlyOnceWith('autonomous');
    expect(chat.updateConfig).toHaveBeenCalledTimes(1);
  });

  it('unattached kb datasources are hidden from the picker; attached ones render locked', () => {
    const KB_ATTACHED = {id: 'kb-1', type: 'kb', name: 'Docs KB'};
    const KB_FOREIGN = {id: 'kb-2', type: 'kb', name: 'Other KB'};
    const {component} = createPane({
      attachedIds: ['kb-1'],
      eligible: [PG, KB_ATTACHED, KB_FOREIGN],
    });

    expect(component.pickerDatasources().map((d) => d.id)).toEqual(['ds-pg', 'kb-1']);
    expect(component.lockedDatasourceIds()).toEqual(['kb-1']);
  });
});

describe('SettingsPaneComponent workspace tier', () => {
  afterEach(() => TestBed.resetTestingModule());

  it('initializes the tier from thread metadata and gates upgrades', () => {
    const {component} = createPane({
      override: {workspace: {backend: 'sandbox'}},
      grants: {vm_workspace: true},
    });
    expect(component.workspaceTier()).toBe('sandbox');
    // Already there, so `sandbox` is absent from the map — the row labels the
    // running tier itself; only the moves away from it are described here.
    expect(component.tierReachability()['sandbox']).toBeUndefined();
    expect(component.tierReachability()['vm']).toBe('ok');
  });

  it('denies the VM upgrade without the vm grant; admin (null grants) allows', () => {
    const {component} = createPane({grants: {vm_workspace: false}});
    expect(component.workspaceTier()).toBe('virtual');
    expect(component.tierReachability()['sandbox']).toBe('ok');
    expect(component.tierReachability()['vm']).toBe('needsApproval');

    TestBed.resetTestingModule();
    const {component: admin} = createPane({grants: null});
    expect(admin.tierReachability()['vm']).toBe('ok');
  });

  it('a session with no upgrade transport reaches no tier, and says so', () => {
    const {component, chat} = createPane({grants: null});
    chat.controlTransport.mockReturnValue('unavailable');
    // Re-read: the reachability computed depends on the tier signal, so
    // nudge it the way a real thread-meta load would.
    chat.workspaceTier.set('virtual');
    expect(component.tierReachability()).toEqual({
      sandbox: 'sessionUnsupported',
      vm: 'sessionUnsupported',
      none: 'downgrade',
    });
    component.onTierPicked('sandbox');
    expect(component.pendingTier()).toBeNull();
  });

  it('vm tier offers no further upgrades (upgrade-only, no downgrades)', () => {
    const {component} = createPane({
      override: {workspace: {backend: 'vm'}},
      grants: null,
    });
    // Every other tier is refused, and says why rather than going missing.
    expect(component.tierReachability()).toEqual({
      sandbox: 'downgrade',
      virtual: 'downgrade',
      none: 'downgrade',
    });
  });

  it('a none-tier session can reach nothing at all', () => {
    // No durable anchor to seed from (workspace_tier_upgrade.md non-goals).
    const {component} = createPane({
      override: {workspace: {backend: 'none'}},
      grants: null,
    });
    expect(component.tierReachability()).toEqual({});
  });
});

describe('SettingsPaneComponent tier upgrade confirmation', () => {
  afterEach(() => TestBed.resetTestingModule());

  it('a reachable pick opens the dialog instead of dispatching', () => {
    const {component, chat} = createPane({grants: null});
    component.onTierPicked('sandbox');

    expect(component.pendingTier()).toBe('sandbox');
    expect(chat.upgradeWorkspace).not.toHaveBeenCalled();
  });

  it('confirming dispatches the upgrade verb and closes', () => {
    const {component, chat} = createPane({grants: null});
    component.onTierPicked('vm');
    component.confirmUpgrade();

    expect(chat.upgradeWorkspace).toHaveBeenCalledExactlyOnceWith('vm');
    expect(component.pendingTier()).toBeNull();
  });

  it('dismissing dispatches nothing — the tier is unchanged', () => {
    const {component, chat} = createPane({grants: null});
    component.onTierPicked('sandbox');
    component.pendingTier.set(null); // what (closed) and Cancel both do

    expect(chat.upgradeWorkspace).not.toHaveBeenCalled();
    expect(component.workspaceTier()).toBe('virtual');
  });

  // The row is a view; this is the gate. An unreachable tier can only arrive
  // through a stale render or a disabled option that slipped through.
  it('refuses an unreachable pick outright', () => {
    const {component, chat} = createPane({grants: {vm_workspace: false}});
    component.onTierPicked('vm'); // needsApproval
    component.onTierPicked('none'); // downgrade

    expect(component.pendingTier()).toBeNull();
    expect(chat.upgradeWorkspace).not.toHaveBeenCalled();
  });

  it('refuses a second pick while an upgrade is already running', () => {
    const {component, chat} = createPane({grants: null});
    chat.workspaceUpgradeInProgress.set({tier: 'sandbox'});
    component.onTierPicked('vm');

    expect(component.pendingTier()).toBeNull();
    expect(chat.upgradeWorkspace).not.toHaveBeenCalled();
  });
});

describe('SettingsPaneComponent settlement (speculative apply, authoritative settle)', () => {
  // The pane advances its diff baseline at dispatch so a second edit during
  // the round trip diffs against what was asked for — and then SETTLES on the
  // session's answer. A refused or unanswered update rolls the pane back to
  // the effective config; a REST-confirmed one (queue-served sessions) folds
  // the accepted fragment into the durable overlay, because no
  // `config.changed` ack will do it. Before this, the baseline advanced and
  // nobody looked again: a session with no control socket showed `max`
  // reasoning it never took (live_settings_silently_dropped_on_stateless_sessions).
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    vi.useRealTimers();
    TestBed.resetTestingModule();
  });

  async function flushOutcome() {
    // Let the resolved updateConfig promise settle (two hops: then + void).
    await Promise.resolve();
    await Promise.resolve();
  }

  it('a refused update rolls the sub-groups back to the effective config', async () => {
    const {component, chat, fakeSettings} = createPane({});
    chat.updateConfig.mockResolvedValue({
      ok: false,
      requestId: 'req-x',
      transport: 'rest',
      message: 'denied',
    });
    expect(fakeSettings.prefillFromConfig).toHaveBeenCalledTimes(1); // load

    fakeSettings.getOverrides.mockReturnValue({llm: {reasoning_level: 'max'}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({llm: {reasoning_level: 'max'}});

    await flushOutcome();
    // Rolled back: the controls are re-seeded from the effective config and
    // the selection is reset — the pin the session refused is gone.
    expect(fakeSettings.prefillFromConfig).toHaveBeenCalledTimes(2);
    expect(fakeSettings.resetDatasourceSelection).toHaveBeenCalledTimes(2);

    // And the baseline went back with it: the same pin re-dispatches.
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledTimes(2);
  });

  it('a confirmed update does not re-prefill, and the same value is not re-sent', async () => {
    const {component, chat, fakeSettings} = createPane({});
    fakeSettings.getOverrides.mockReturnValue({llm: {reasoning_level: 'max'}});
    component.onSettingsChange();
    vi.runAllTimers();
    await flushOutcome();

    expect(fakeSettings.prefillFromConfig).toHaveBeenCalledTimes(1);
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledTimes(1);
  });

  it('a REST-confirmed update folds the accepted fragment into the durable overlay', async () => {
    const {component, chat, fakeSettings} = createPane({});
    chat.updateConfig.mockResolvedValue({
      ok: true,
      requestId: 'req-r',
      transport: 'rest',
      applied: {llm: {reasoning_level: 'max'}},
      effective: 'next_turn',
    });
    fakeSettings.getOverrides.mockReturnValue({llm: {reasoning_level: 'max'}});
    component.onSettingsChange();
    vi.runAllTimers();
    await flushOutcome();

    // The overlay now carries the accepted value, so liveConfig() reports it
    // without a pane reopen — exactly what the socket ack does for pinned
    // sessions via the live signals.
    expect((component.liveConfig()['llm'] as Record<string, unknown>)['reasoning_level']).toBe('max');
    // A later un-pinned edit therefore diffs against `max`, not the old value.
    fakeSettings.getOverrides.mockReturnValue({});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledTimes(1);
  });

  it('a late answer for a session the pane has left does not touch the pane', async () => {
    const {component, chat, fakeSettings} = createPane({});
    let settle!: (o: unknown) => void;
    chat.updateConfig.mockReturnValue(new Promise((resolve) => (settle = resolve)));
    fakeSettings.getOverrides.mockReturnValue({llm: {reasoning_level: 'max'}});
    component.onSettingsChange();
    vi.runAllTimers();

    // Switch sessions while the answer is in flight.
    chat.threadId.set('thread-2');
    TestBed.tick();
    const prefills = fakeSettings.prefillFromConfig.mock.calls.length;
    settle({ok: false, requestId: 'req-l', transport: 'rest', message: 'late'});
    await flushOutcome();
    expect(fakeSettings.prefillFromConfig).toHaveBeenCalledTimes(prefills);
  });
});

describe('SettingsPaneComponent delegation gate', () => {
  // `delegate_agent` and its control plane are `grant: explicit` twice over:
  // the factory needs the names in `tools.delegation` AND `delegation.enabled`.
  // The tools group emits both; the pane used to forward only the names, so a
  // ticked Delegation row on main-dev produced "11 configured tool(s) did not
  // bind". The gate now follows the row's switch position in the same batch.
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    vi.useRealTimers();
    TestBed.resetTestingModule();
  });
  const NAMES = ['delegate_agent', 'list_agents', 'message_agent', 'stop_agent', 'wait_agent'];

  it('ticking Delegation dispatches the names AND delegation.enabled: true', () => {
    const {component, chat, fakeSettings} = createPane({
      toolGroups: toolsetAnswer({delegation: 'off'}, {enumerate_only: {delegation: NAMES}}),
    });
    fakeSettings.getOverrides.mockReturnValue({
      tools: {delegation: true},
      delegation: {enabled: true},
    });
    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledTimes(1);
    const sent = chat.updateConfig.mock.calls[0][0] as Record<string, any>;
    expect(sent['tools']['delegation']).toBeTruthy();
    expect(sent['delegation']).toEqual({enabled: true});
  });

  it('unticking Delegation dispatches [] AND delegation.enabled: false', () => {
    const {component, chat, fakeSettings} = createPane({
      toolGroups: toolsetAnswer({delegation: 'on'}, {enumerate_only: {delegation: NAMES}}),
    });
    fakeSettings.getOverrides.mockReturnValue({tools: {delegation: []}});
    component.onSettingsChange();
    vi.runAllTimers();

    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({
      tools: {delegation: []},
      delegation: {enabled: false},
    });
  });

  it('the concurrency knob rides the delegation block; other edits never carry it', () => {
    const {component, chat, fakeSettings} = createPane({
      toolGroups: toolsetAnswer({delegation: 'on', canvas: 'on'}, {enumerate_only: {delegation: NAMES}}),
    });
    fakeSettings.getOverrides.mockReturnValue({delegation: {session_max_concurrent: 2}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({
      delegation: {session_max_concurrent: 2},
    });

    // A later, unrelated edit (canvas off) does not re-send the knob.
    fakeSettings.getOverrides.mockReturnValue({
      delegation: {session_max_concurrent: 2},
      tools: {canvas: []},
    });
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenLastCalledWith({tools: {canvas: []}});
  });
});

describe('SettingsPaneComponent session delegation cap', () => {
  // parallel_subagents.md §6.4 / D2. A session allowed to fan out reads its
  // own cap, `delegation.session_max_concurrent`, and ignores the worker cap
  // `delegation.max_concurrent` — so the pane writing the worker key meant a
  // user who set 2 got 6. The pane is only ever a session, so it reads and
  // writes the session key and nothing else.
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => {
    vi.useRealTimers();
    TestBed.resetTestingModule();
  });
  const NAMES = ['delegate_agent', 'list_agents', 'message_agent', 'stop_agent', 'wait_agent'];
  const delegationOn = () =>
    toolsetAnswer({delegation: 'on', canvas: 'on'}, {enumerate_only: {delegation: NAMES}});

  async function flushOutcome() {
    await Promise.resolve();
    await Promise.resolve();
  }

  it('a worker-cap edit is never dispatched from a session', () => {
    const {component, chat, fakeSettings} = createPane({toolGroups: delegationOn()});
    fakeSettings.getOverrides.mockReturnValue({delegation: {max_concurrent: 2}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).not.toHaveBeenCalled();
  });

  it('an unset cap is never written: unrelated edits carry no inherited value', () => {
    const {component, chat, fakeSettings} = createPane({toolGroups: delegationOn()});
    fakeSettings.getOverrides.mockReturnValue({tools: {canvas: []}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({tools: {canvas: []}});
  });

  it('the stored session cap is the baseline: re-pinning the same value sends nothing', () => {
    const {component, chat, fakeSettings} = createPane({
      toolGroups: delegationOn(),
      override: {delegation: {session_max_concurrent: 3}},
    });
    fakeSettings.getOverrides.mockReturnValue({delegation: {session_max_concurrent: 3}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).not.toHaveBeenCalled();

    fakeSettings.getOverrides.mockReturnValue({delegation: {session_max_concurrent: 5}});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({
      delegation: {session_max_concurrent: 5},
    });
  });

  it('a value the orchestrator would refuse is never dispatched', () => {
    const {component, chat, fakeSettings} = createPane({toolGroups: delegationOn()});
    for (const refused of [0, 21, 2.5]) {
      fakeSettings.getOverrides.mockReturnValue({delegation: {session_max_concurrent: refused}});
      component.onSettingsChange();
      vi.runAllTimers();
    }
    expect(chat.updateConfig).not.toHaveBeenCalled();
  });

  it('with the real tools group: typing a cap sends session_max_concurrent only', () => {
    const {component, chat, toolsGroup} = createPaneWithRealToolsGroup(delegationOn());
    Object.defineProperty(toolsGroup, 'delegationCapScope', {value: () => 'session'});

    toolsGroup.onDelegationCapChange(25); // out of range: flagged, not sent
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).not.toHaveBeenCalled();

    toolsGroup.onDelegationCapChange(2);
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledExactlyOnceWith({
      delegation: {session_max_concurrent: 2},
    });
  });

  it('a socket-confirmed cap is folded into the overlay, so the field keeps showing it', async () => {
    const {component, chat, fakeSettings} = createPane({toolGroups: delegationOn()});
    chat.updateConfig.mockResolvedValue({
      ok: true,
      requestId: 'req-cap',
      transport: 'websocket',
      applied: {delegation: {session_max_concurrent: 2}},
      effective: 'now',
    });
    fakeSettings.getOverrides.mockReturnValue({delegation: {session_max_concurrent: 2}});
    component.onSettingsChange();
    vi.runAllTimers();
    await flushOutcome();

    expect(
      (component.liveConfig()['delegation'] as Record<string, unknown>)['session_max_concurrent'],
    ).toBe(2);
    // Emptying the field afterwards is not a change: the session holds 2.
    fakeSettings.getOverrides.mockReturnValue({});
    component.onSettingsChange();
    vi.runAllTimers();
    expect(chat.updateConfig).toHaveBeenCalledTimes(1);
  });
});
