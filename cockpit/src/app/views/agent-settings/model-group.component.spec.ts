import {afterEach, beforeAll, describe, expect, it, vi} from 'vitest';
import {Injector, runInInjectionContext, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {HttpClient} from '@angular/common/http';
import {of} from 'rxjs';
import {TranslocoService, TranslocoTestingModule} from '@jsverse/transloco';
import en from '../../../assets/i18n/en.json';
import {ModelGroupComponent} from './model-group.component';
import {chatModelLabel, chatModelLabels, ModelGroup, ModelService} from '../../core/services/model.service';
import {SettingsService} from '../../core/services/settings.service';

/**
 * Create a ModelGroupComponent in a minimal injection context with a mock ModelService.
 */
function createComponent(overrides?: {
  models?: { group: string; provider: string; configured: boolean; models: string[] }[];
}) {
  const mockModels = signal(overrides?.models ?? [
    {group: 'Local', provider: 'local', configured: true, models: ['openai/gpt-oss-120b']},
    {group: 'OpenAI', provider: 'openai', configured: true, models: ['gpt-5.4', 'gpt-4o']},
    {group: 'Anthropic', provider: 'anthropic', configured: false, models: ['claude-opus-4-6', 'claude-sonnet-4-5-20250929']},
  ]);

  const mockModelService = {
    models: mockModels,
    auxiliaryModels: signal([]),
    visionModels: signal([]),
    whisperModels: signal([]),
    embeddingModels: signal([]),
    providers: signal([]),
    reasoningByModel: signal<Record<string, {method: string; default: string | null; options: string[]}>>({
      'gpt-5.4': {method: 'effort_enum', default: 'high', options: ['low', 'medium', 'high']},
      'gemma-4-moe': {method: 'binary_toggle', default: 'on', options: ['on', 'off']},
    }),
    loading: signal(false),
    loaded: signal(true),
    load: vi.fn(),
  };

  const mockHttp = {get: vi.fn().mockReturnValue(of({}))};

  const mockSettings = {
    apiKeys: signal([]),
    preferences: signal({}),
    updatePreferences: vi.fn().mockReturnValue(of({status: 'ok'})),
  };

  const mockTransloco = {
    translate: (key: string) => key,
    langChanges$: of('en'),
    getActiveLang: () => 'en',
  };

  const injector = Injector.create({
    providers: [
      {provide: ModelService, useValue: mockModelService},
      {provide: HttpClient, useValue: mockHttp},
      {provide: SettingsService, useValue: mockSettings},
      {provide: TranslocoService, useValue: mockTransloco},
    ],
  });

  const component = runInInjectionContext(injector, () => new ModelGroupComponent());
  return {component, mockModelService, mockModels, mockSettings};
}

/** `mode` is a signal input(); this harness never renders a template, so
 *  replace the property directly (the same trick the rendering tests below
 *  rely on). */
function inMode(component: ModelGroupComponent, mode: 'job' | 'session' | 'live'): void {
  Object.defineProperty(component, 'mode', {value: () => mode});
}

/** `config` is a signal input() too — same workaround. */
function withConfig(component: ModelGroupComponent, config: Record<string, unknown>): void {
  Object.defineProperty(component, 'config', {value: () => config});
}

describe('ModelGroupComponent', () => {
  afterEach(() => localStorage.clear());

  describe('signal wiring', () => {
    it('should read models from ModelService', () => {
      const {component} = createComponent();
      expect(component.availableModels()).toHaveLength(3);
      expect(component.availableModels()[0].group).toBe('Local');
    });

    it('should react to model service updates', () => {
      const {component, mockModels} = createComponent();
      expect(component.availableModels()).toHaveLength(3);

      mockModels.set([{group: 'Test', provider: 'local', configured: true, models: ['test-model']}]);
      expect(component.availableModels()).toHaveLength(1);
      expect(component.availableModels()[0].group).toBe('Test');
    });
  });

  describe('override state (one model since U1)', () => {
    it('should start with null overrides (use defaults)', () => {
      const {component} = createComponent();
      expect(component.model()).toBeNull();
      expect(component.subagentModel()).toBeNull();
    });

    it('should track modified count for job mode: model + subagent model', () => {
      const {component} = createComponent();
      // Default mode is 'job'
      expect(component.modifiedCount()).toBe(0);

      component.model.set('gpt-5.4');
      expect(component.modifiedCount()).toBe(1);

      component.subagentModel.set('gpt-4o');
      expect(component.modifiedCount()).toBe(2);

      // Session-only fields don't count in job mode.
      component.sessionReasoning.set('high');
      expect(component.modifiedCount()).toBe(2);
    });

    it('should track modified count for session mode: model + reasoning + temperature', () => {
      const {component} = createComponent();
      inMode(component, 'session');
      component.model.set('gpt-5.4');
      component.sessionReasoning.set('high');
      expect(component.modifiedCount()).toBe(2);
      component.temperature.set(0.3);
      expect(component.modifiedCount()).toBe(3);
      // The subagent row is job-only.
      component.subagentModel.set('gpt-4o');
      expect(component.modifiedCount()).toBe(3);
    });
  });

  describe('getOverrides', () => {
    it('should return empty when no overrides set', () => {
      const {component} = createComponent();
      expect(component.getOverrides()).toEqual({});
    });

    it('job mode writes the ONE model to llm.model — exactly what session mode does', () => {
      const {component} = createComponent();
      component.model.set('gpt-5.4');
      expect(component.getOverrides()).toEqual({llm: {model: 'gpt-5.4'}});

      const {component: session} = createComponent();
      inMode(session, 'session');
      session.model.set('gpt-5.4');
      expect(session.getOverrides()).toEqual({llm: {model: 'gpt-5.4'}});
    });

    it('job mode adds the roster-wide subagent model under subagents.llm.model', () => {
      const {component} = createComponent();
      component.model.set('claude-opus-4-6');
      component.subagentModel.set('gpt-4o');
      expect(component.getOverrides()).toEqual({
        llm: {model: 'claude-opus-4-6'},
        subagents: {llm: {model: 'gpt-4o'}},
      });
    });

    it('emits only the parts the user set (subagent alone → no llm key)', () => {
      const {component} = createComponent();
      component.subagentModel.set('gpt-4o');
      expect(component.getOverrides()).toEqual({subagents: {llm: {model: 'gpt-4o'}}});
    });

    it('never emits the legacy per-phase tiers', () => {
      const {component} = createComponent();
      component.model.set('gpt-5.4');
      component.subagentModel.set('gpt-4o');
      const llm = component.getOverrides()['llm'] as Record<string, unknown>;
      expect(Object.keys(llm)).toEqual(['model']);
      expect(llm['strategic']).toBeUndefined();
      expect(llm['tactical']).toBeUndefined();
      expect(llm['subagent']).toBeUndefined();
    });

    it('session mode never emits the subagent fragment', () => {
      const {component} = createComponent();
      inMode(component, 'session');
      component.subagentModel.set('gpt-4o');
      expect(component.getOverrides()).toEqual({});
    });
  });

  describe('persist (UI-only, no account write)', () => {
    it('remembers the pick per surface in localStorage but never PATCHes account preferences', () => {
      const {component, mockSettings} = createComponent();

      component.onModelChange('gpt-4o');
      component.onSubagentModelChange('gpt-5.4');

      // Remembered locally for next-time preselect…
      expect(localStorage.getItem('ui.lastModel.job')).toBe('gpt-4o');
      expect(localStorage.getItem('ui.lastModel.subagent')).toBe('gpt-5.4');
      // …the retired per-phase keys are never written…
      expect(localStorage.getItem('ui.lastModel.strategic')).toBeNull();
      expect(localStorage.getItem('ui.lastModel.tactical')).toBeNull();
      // …but NOT written to the user's global account defaults (the original bug).
      expect(mockSettings.updatePreferences).not.toHaveBeenCalled();
    });

    it('the session form remembers under its own key', () => {
      const {component} = createComponent();
      inMode(component, 'session');
      component.onModelChange('gpt-4o');
      expect(localStorage.getItem('ui.lastModel.session')).toBe('gpt-4o');
      expect(localStorage.getItem('ui.lastModel.job')).toBeNull();
    });

    it('clears the localStorage key when reset to default (null)', () => {
      const {component} = createComponent();
      component.onModelChange('gpt-4o');
      expect(localStorage.getItem('ui.lastModel.job')).toBe('gpt-4o');

      component.onModelChange(null);
      expect(localStorage.getItem('ui.lastModel.job')).toBeNull();
    });
  });

  describe('effective-model default label (Layer 3)', () => {
    // mockTransloco.translate echoes the key, so we assert which label branch
    // (and thus which i18n key) defaultLabel selects.
    it('uses the with-source label when a server effective model + source exist', () => {
      const {component} = createComponent();
      expect(
        component.defaultLabel({model: 'gpt-5.5', source: 'account_default'}, null),
      ).toBe('agentSettings.model.defaultResolved');
    });

    it('uses the no-source label when only a config fallback model exists', () => {
      const {component} = createComponent();
      expect(component.defaultLabel(null, 'gemma-4-moe')).toBe(
        'agentSettings.model.defaultResolvedNoSource',
      );
    });

    it('falls back to a bare "Default" when no model is resolvable', () => {
      const {component} = createComponent();
      expect(component.defaultLabel(null, null)).toBe('agentSettings.model.default');
    });

    it('prefers the server effective model over the config fallback', () => {
      const {component} = createComponent();
      expect(component.defaultLabel({model: 'eff', source: 'expert'}, 'cfg')).toBe(
        'agentSettings.model.defaultResolved',
      );
    });

    it('reads the `model` slot (not a per-phase alias) for both rows', () => {
      const {component} = createComponent();
      Object.defineProperty(component, 'effectiveModels', {
        value: () => ({
          model: {model: 'top', source: 'expert'},
          subagent: {model: 'reader', source: 'expert'},
          session: {model: 'top', source: 'expert'},
        }),
      });
      expect(component.effectiveModel()).toEqual({model: 'top', source: 'expert'});
      expect(component.effectiveSubagent()).toEqual({model: 'reader', source: 'expert'});
    });
  });

  describe('model in effect (override > effective > config)', () => {
    it('an explicit override wins', () => {
      const {component} = createComponent();
      component.model.set('chosen');
      expect(component.modelInEffect()).toBe('chosen');
    });

    it('is null when nothing is set (empty config, no server effective_models)', () => {
      const {component} = createComponent();
      expect(component.modelInEffect()).toBeNull();
    });

    it('falls back to the config llm.model', () => {
      const {component} = createComponent();
      withConfig(component, {llm: {model: 'cfg-model'}});
      expect(component.resolvedModel()).toBe('cfg-model');
      expect(component.modelInEffect()).toBe('cfg-model');
    });
  });

  describe('config-derived fallbacks read the compat mapping', () => {
    it('a legacy llm.strategic pin in a merged config shows as THE model', () => {
      const {component} = createComponent();
      // A merged base ⊕ expert dict: the base's placeholder llm.model must not
      // shadow the pre-U1 expert's phase pin (the server's merged-dict rule).
      withConfig(component, {llm: {model: 'base-placeholder', strategic: {model: 'legacy-pin'}}});
      expect(component.resolvedModel()).toBe('legacy-pin');
    });

    it('the subagent fallback reads subagents.llm.model, treating inherit as the model', () => {
      const {component} = createComponent();
      withConfig(component, {llm: {model: 'top'}, subagents: {llm: {model: 'inherit'}}});
      expect(component.resolvedSubagentModel()).toBe('top');

      const {component: pinned} = createComponent();
      withConfig(pinned, {llm: {model: 'top'}, subagents: {llm: {model: 'reader'}}});
      expect(pinned.resolvedSubagentModel()).toBe('reader');
    });

    it('a legacy llm.subagent tier feeds the subagent fallback', () => {
      const {component} = createComponent();
      withConfig(component, {llm: {model: 'top', subagent: {model: 'old-reader'}}});
      expect(component.resolvedSubagentModel()).toBe('old-reader');
      expect(component.resolvedModel()).toBe('top');
    });
  });

  describe('resetAll', () => {
    it('should clear all model overrides', () => {
      const {component} = createComponent();
      component.model.set('model-a');
      component.subagentModel.set('model-b');

      component.resetAll();

      expect(component.model()).toBeNull();
      expect(component.subagentModel()).toBeNull();
    });
  });

  describe('prefillFromConfig', () => {
    it('should not treat config models as user overrides', () => {
      const {component} = createComponent();
      component.prefillFromConfig({
        llm: {model: 'base-model'},
        subagents: {llm: {model: 'reader-model'}},
      });

      expect(component.model()).toBeNull();
      expect(component.subagentModel()).toBeNull();
      expect(component.getOverrides()).toEqual({});
    });

    it('should not send base config model as an override', () => {
      const {component} = createComponent();
      component.prefillFromConfig({
        llm: {model: 'gpt-5.4'},
      });

      expect(component.model()).toBeNull();
      expect(component.getOverrides()).toEqual({});
    });

    it('should handle empty config', () => {
      const {component} = createComponent();
      component.prefillFromConfig({});

      expect(component.model()).toBeNull();
      expect(component.subagentModel()).toBeNull();
    });

    it('preselects the remembered pick only when the config pins no model', () => {
      localStorage.setItem('ui.lastModel.job', 'saved-job-model');
      const {component} = createComponent();
      component.prefillFromConfig({});
      expect(component.model()).toBe('saved-job-model');

      component.prefillFromConfig({llm: {model: 'pinned'}});
      expect(component.model()).toBeNull();
    });

    it('a legacy llm.strategic pin counts as the model (compat mapping), never as an override', () => {
      localStorage.setItem('ui.lastModel.job', 'saved-job-model');
      const {component} = createComponent();
      component.prefillFromConfig({llm: {strategic: {model: 'strategic-pin'}}});
      // Resolved by the config → no saved-model preselect, no override.
      expect(component.model()).toBeNull();
      expect(component.getOverrides()).toEqual({});
    });

    it('a legacy llm.subagent pin counts as the subagent model', () => {
      localStorage.setItem('ui.lastModel.subagent', 'saved-model');
      const {component} = createComponent();
      component.prefillFromConfig({llm: {subagent: {model: 'reader-model'}}});
      expect(component.subagentModel()).toBeNull();
      expect(component.getOverrides()).toEqual({});
    });

    it('does not preselect a saved subagent model when the model is pinned (subagents inherit it)', () => {
      localStorage.setItem('ui.lastModel.subagent', 'saved-model');
      const {component} = createComponent();
      component.prefillFromConfig({llm: {model: 'the-model'}});
      expect(component.subagentModel()).toBeNull();
    });
  });

  describe('session reasoning override (Settings-tab field)', () => {
    it('starts null, captures a pick, and clears via a null change', () => {
      const {component} = createComponent();
      expect(component.sessionReasoning()).toBeNull();
      component.onSessionReasoningChange('high');
      expect(component.sessionReasoning()).toBe('high');
      component.onSessionReasoningChange(null);
      expect(component.sessionReasoning()).toBeNull();
    });

    it('derives concrete options (no Default sentinel) from the model in effect', () => {
      const {component} = createComponent();
      component.model.set('gemma-4-moe');
      expect(component.sessionReasoningOptions()).toEqual([
        {value: 'on', label: 'On'},
        {value: 'off', label: 'Off'},
      ]);
    });

    it('resolves to the family default and pins it when picked', () => {
      const {component} = createComponent();
      component.model.set('gemma-4-moe');
      expect(component.resolvedSessionReasoning()).toBe('on');

      // Picking the concrete level that happens to be the default is intent,
      // so it lands in the override rather than collapsing back to inherit.
      // (`mode` defaults to 'job' in this harness, which leaves the session
      // fields out of the fragment — assert under the mode that emits them.)
      inMode(component, 'session');
      component.onSessionReasoningChange('on');
      expect(component.sessionReasoning()).toBe('on');
      expect(component.getOverrides()).toEqual({
        llm: {model: 'gemma-4-moe', reasoning_level: 'on'},
      });

      component.onSessionReasoningChange('off');
      expect(component.sessionReasoning()).toBe('off');

      // The explicit "Default" option is what clears it.
      component.onSessionReasoningChange(null);
      expect(component.sessionReasoning()).toBeNull();
      expect(component.getOverrides()).toEqual({llm: {model: 'gemma-4-moe'}});
    });

    it('the "(default)" marker stays on the family default while a pin selects another value', () => {
      // A session pin of `low` on gpt-5.4 (family default `high`): the select
      // preselects `low` (what is in effect), but `high` keeps the marker.
      // The marker used to follow the resolved value, so pinning max
      // re-labelled max as "(default)" — the family had not changed.
      const pinned = createComponent().component;
      inMode(pinned, 'live');
      Object.defineProperty(pinned, 'config', {
        value: () => ({llm: {model: 'gpt-5.4', reasoning_level: 'low'}}),
      });
      expect(pinned.resolvedSessionReasoning()).toBe('low');
      expect(pinned.familyReasoningDefault()).toBe('high');

      // No pin: both coincide, as before.
      const unpinned = createComponent().component;
      inMode(unpinned, 'live');
      Object.defineProperty(unpinned, 'config', {value: () => ({llm: {model: 'gpt-5.4'}})});
      expect(unpinned.resolvedSessionReasoning()).toBe('high');
      expect(unpinned.familyReasoningDefault()).toBe('high');
    });

    it('offers nothing for a model without a selectable capability (field hidden)', () => {
      const {component} = createComponent();
      component.model.set('gpt-4o');
      expect(component.sessionReasoningOptions()).toEqual([]);
      expect(component.resolvedSessionReasoning()).toBeNull();
    });

    it('is dropped on a model change (no cross-family leak)', () => {
      const {component} = createComponent();
      component.onModelChange('gpt-5.4');
      component.onSessionReasoningChange('high');
      component.onModelChange('gemma-4-moe');
      expect(component.sessionReasoning()).toBeNull();
    });

    it('is cleared by resetAll and by an expert prefill', () => {
      const {component} = createComponent();
      component.onSessionReasoningChange('high');
      component.resetAll();
      expect(component.sessionReasoning()).toBeNull();

      component.onSessionReasoningChange('low');
      component.prefillFromConfig({});
      expect(component.sessionReasoning()).toBeNull();
    });

    it('does not leak into job-mode overrides', () => {
      const {component} = createComponent();
      component.onSessionReasoningChange('high');
      // mode defaults to 'job' in this harness — the session-only field must
      // not surface in the job override fragment.
      expect(component.getOverrides()).toEqual({});
    });
  });

  describe('reasoning reset notice (Task 3 fix)', () => {
    // The reset itself is correct and stays (decided, not open — reasoning
    // vocabularies are per-family and don't translate). What was missing is
    // any sign it happened: the select just snaps back to the family default,
    // which looks identical to having never picked anything.
    it('starts false and does not fire when there was nothing to lose', () => {
      const {component} = createComponent();
      expect(component.reasoningResetNotice()).toBe(false);

      component.onModelChange('gemma-4-moe');
      expect(component.reasoningResetNotice()).toBe(false);

      component.prefillFromConfig({});
      expect(component.reasoningResetNotice()).toBe(false);
    });

    it('fires when a model change clears an existing pick', () => {
      const {component} = createComponent();
      component.onSessionReasoningChange('high');
      component.onModelChange('gemma-4-moe');
      expect(component.sessionReasoning()).toBeNull();
      expect(component.reasoningResetNotice()).toBe(true);
    });

    it('fires when a config prefill clears an existing pick — not only on a deliberate expert switch', () => {
      const {component} = createComponent();
      component.onSessionReasoningChange('low');
      // No model change, no expert-card click modeled here — prefillFromConfig
      // is the same method SessionCreateComponent's applyEffectiveDefault()
      // invokes automatically once an in-flight default-expert lookup
      // resolves (see session-create.component.spec.ts). This asserts the
      // sink's behavior in isolation from that trigger.
      component.prefillFromConfig({});
      expect(component.sessionReasoning()).toBeNull();
      expect(component.reasoningResetNotice()).toBe(true);
    });

    it('is dismissed by the next deliberate reasoning pick', () => {
      const {component} = createComponent();
      component.onSessionReasoningChange('high');
      component.onModelChange('gemma-4-moe'); // clears + raises the notice
      expect(component.reasoningResetNotice()).toBe(true);

      component.onSessionReasoningChange('on');
      expect(component.reasoningResetNotice()).toBe(false);
    });

    it('is dismissed by re-confirming the shown default via pinReasoning', () => {
      const {component} = createComponent();
      component.model.set('gemma-4-moe');
      component.onSessionReasoningChange('off');
      component.onModelChange('gemma-4-moe'); // clears 'off' back to null, raises the notice
      expect(component.reasoningResetNotice()).toBe(true);

      component.pinReasoning();
      expect(component.reasoningResetNotice()).toBe(false);
      // pinReasoning also does what pinValue always does: promotes the shown
      // resolved default into an explicit pin.
      expect(component.sessionReasoning()).toBe(component.resolvedSessionReasoning());
    });

    it('is cleared by resetAll', () => {
      const {component} = createComponent();
      component.onSessionReasoningChange('high');
      component.onModelChange('gemma-4-moe');
      expect(component.reasoningResetNotice()).toBe(true);

      component.resetAll();
      expect(component.reasoningResetNotice()).toBe(false);
    });
  });

  describe('subagent (roster-wide) model', () => {
    it('starts null and is counted in job-mode modifiedCount', () => {
      const {component} = createComponent();
      expect(component.subagentModel()).toBeNull();
      expect(component.modifiedCount()).toBe(0);

      component.subagentModel.set('gpt-4o');
      expect(component.modifiedCount()).toBe(1);
    });

    it('remembers the pick under its own localStorage key, never account prefs', () => {
      const {component, mockSettings} = createComponent();
      component.onSubagentModelChange('gpt-4o');
      expect(localStorage.getItem('ui.lastModel.subagent')).toBe('gpt-4o');
      expect(mockSettings.updatePreferences).not.toHaveBeenCalled();
    });

    it('does not treat a config roster pin as a user override', () => {
      const {component} = createComponent();
      component.prefillFromConfig({subagents: {llm: {model: 'reader-model'}}});
      expect(component.subagentModel()).toBeNull();
      expect(component.getOverrides()).toEqual({});
    });

    it('is cleared by resetAll', () => {
      const {component} = createComponent();
      component.subagentModel.set('gpt-4o');
      component.resetAll();
      expect(component.subagentModel()).toBeNull();
    });

    it('is shown by default for standalone use', () => {
      const {component} = createComponent();
      expect(component.showSubagent()).toBe(true);
    });
  });
});

// Task 3 review fix — every test above builds ModelGroupComponent via
// runInInjectionContext, which never renders a template: it can prove the
// signal flips, but nothing above it proves the notice is actually visible,
// or that its transloco key resolves to real text at runtime rather than the
// raw key string (a missing/misspelled key renders literally — AOT's
// strictTemplates catches a *bad binding*, not an *absent translation
// entry*). This mounts the real component through TestBed and asserts against
// the rendered DOM, using the actual shipped en.json (not a hand-inlined
// translation stub) so a key present in the template but missing from the
// real asset file would fail this test.
describe('ModelGroupComponent — rendering', () => {
  // The template renders <app-icon>, whose @Component uses an external
  // `styleUrl`. The Angular CLI normally inlines styleUrl content at build
  // time; this project's vitest setup JIT-compiles raw TS with no such
  // transform, so `TestBed.configureTestingModule` throws ("Component
  // 'AppIconComponent' is not resolved") unless the pending resource queue is
  // drained first (same pattern as memory-panel.component.spec.ts). These
  // tests DO render the template and assert on real DOM text, but the style
  // content itself is irrelevant to that — an empty-string resolver is enough
  // to unblock compilation without pretending to test CSS.
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  afterEach(() => {
    TestBed.resetTestingModule();
    localStorage.clear();
  });

  function mount(mode: 'job' | 'session', groups: ModelGroup[] = []) {
    const modelServiceMock = {
      models: signal(groups),
      auxiliaryModels: signal([]),
      visionModels: signal([]),
      whisperModels: signal([]),
      embeddingModels: signal([]),
      providers: signal([]),
      reasoningByModel: signal<Record<string, {method: string; default: string | null; options: string[]}>>({
        'gpt-5.6-sol': {method: 'effort_enum', default: 'high', options: ['low', 'medium', 'high', 'xhigh', 'max']},
      }),
      loading: signal(false),
      loaded: signal(true),
      load: vi.fn(),
    };

    TestBed.configureTestingModule({
      imports: [
        ModelGroupComponent,
        TranslocoTestingModule.forRoot({
          langs: {en},
          translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
        }),
      ],
      providers: [{provide: ModelService, useValue: modelServiceMock}],
    });

    const fixture = TestBed.createComponent(ModelGroupComponent);
    // `mode` is a signal input(); this repo's vitest pipeline doesn't run
    // components through ngtsc, so ɵcmp.inputs is empty here and both
    // template property binding and fixture.componentRef.setInput() throw
    // NG0303 for every signal-input component (see the identical workaround
    // and its full explanation in contact-form.component.spec.ts). Replace
    // the property directly.
    Object.defineProperty(fixture.componentInstance, 'mode', {value: () => mode});
    fixture.detectChanges();
    return fixture;
  }

  it('job mode renders ONE model row plus the subagent row — no per-phase pickers', () => {
    const fixture = mount('job');
    const root = fixture.nativeElement as HTMLElement;
    const labels = Array.from(root.querySelectorAll('.field-label')).map((el) =>
      (el.textContent ?? '').trim(),
    );
    expect(labels).toEqual([en.agentSettings.model.single, en.agentSettings.model.subagent]);
    expect(root.querySelectorAll('select')).toHaveLength(2);
    // The retired strategic/tactical labels are gone from the asset file too.
    expect((en.agentSettings.model as Record<string, unknown>)['strategic']).toBeUndefined();
    expect((en.agentSettings.model as Record<string, unknown>)['tactical']).toBeUndefined();
    // The roster hint resolves to real text, not the raw key.
    const hint = root.querySelector('.field-hint');
    expect(hint).not.toBeNull();
    expect((hint!.textContent ?? '').trim()).toBe(en.agentSettings.model.subagentRoster);
  });

  it('shows each catalog label as the option text and keeps the id as the value', () => {
    // Two rows serving the same upstream model on two transports: the label
    // tells them apart, the id is what the config stores.
    const fixture = mount('job', [
      {
        group: 'System: Claude subscription proxy',
        provider: 'system',
        configured: true,
        models: ['claude-opus-5-5'],
        labels: {'claude-opus-5-5': 'Claude Opus 5.5 (subscription)'},
      },
      {
        group: 'Anthropic',
        provider: 'anthropic',
        configured: true,
        models: ['claude-opus-5-5-anthropic', 'claude-haiku-4-5'],
        labels: {'claude-opus-5-5-anthropic': 'Claude Opus 5.5 (API key)'},
      },
    ]);
    const root = fixture.nativeElement as HTMLElement;
    for (const select of Array.from(root.querySelectorAll('select'))) {
      const options = Array.from(select.querySelectorAll('optgroup option')).map((o) => [
        (o as HTMLOptionElement).value,
        (o.textContent ?? '').trim(),
      ]);
      expect(options).toEqual([
        ['claude-opus-5-5', 'Claude Opus 5.5 (subscription)'],
        ['claude-opus-5-5-anthropic', 'Claude Opus 5.5 (API key)'],
        // No label from an older backend (or none for this id): the id.
        ['claude-haiku-4-5', 'claude-haiku-4-5'],
      ]);
    }
    expect(root.querySelectorAll('select')).toHaveLength(2);
  });

  it('session mode renders the same single model row and no subagent row', () => {
    const fixture = mount('session');
    const root = fixture.nativeElement as HTMLElement;
    const labels = Array.from(root.querySelectorAll('.field-label')).map((el) =>
      (el.textContent ?? '').trim(),
    );
    expect(labels).toEqual([en.agentSettings.model.single]);
  });

  it('renders nothing when there is no reset to report', () => {
    const fixture = mount('session');
    const root = fixture.nativeElement as HTMLElement;
    expect(root.querySelector('.reasoning-reset-notice')).toBeNull();
  });

  it('renders the resolved notice text — not the raw translation key — once an involuntary prefill clears a pick', () => {
    const fixture = mount('session');
    const component = fixture.componentInstance;

    // Drives the field into existence and gives it something to lose.
    component.onModelChange('gpt-5.6-sol');
    component.onSessionReasoningChange('max');
    fixture.detectChanges();
    expect((fixture.nativeElement as HTMLElement).querySelector('.reasoning-reset-notice')).toBeNull();

    // The involuntary path itself: SessionCreateComponent's
    // applyEffectiveDefault() cascade calls exactly this, with exactly this
    // shape of config, once a default-expert resolution lands (see
    // session-create.component.spec.ts's Task 3 describe block).
    component.prefillFromConfig({llm: {}});
    fixture.detectChanges();

    const notice = (fixture.nativeElement as HTMLElement).querySelector('.reasoning-reset-notice');
    expect(notice).not.toBeNull();
    // The message lives in its own <span>, separate from the <app-icon>
    // sibling: in JSDOM (no Material Symbols font) app-icon's ligature name
    // ("info") renders as literal projected text, which would otherwise
    // contaminate a whole-container text assertion — querying the span is
    // the more correct check, not a workaround, since a real browser
    // wouldn't render "info" as legible text at all.
    const message = notice!.querySelector('span');
    expect(message).not.toBeNull();
    const text = (message!.textContent ?? '').trim();
    // Proves the pipe actually resolved the real, shipped en.json entry.
    // A missing or misspelled key renders literally as
    // "agentSettings.model.reasoningResetNotice" — AOT's strictTemplates
    // does not catch that, since the binding itself is well-typed either way.
    expect(text).not.toContain('agentSettings.model.reasoningResetNotice');
    expect(text).toBe(en.agentSettings.model.reasoningResetNotice);
  });

  it('the notice disappears once the user dismisses it with a fresh pick', () => {
    const fixture = mount('session');
    const component = fixture.componentInstance;

    component.onModelChange('gpt-5.6-sol');
    component.onSessionReasoningChange('max');
    component.prefillFromConfig({llm: {}});
    fixture.detectChanges();
    expect((fixture.nativeElement as HTMLElement).querySelector('.reasoning-reset-notice')).not.toBeNull();

    component.onSessionReasoningChange('high');
    fixture.detectChanges();
    expect((fixture.nativeElement as HTMLElement).querySelector('.reasoning-reset-notice')).toBeNull();
  });
});

describe('chatModelLabel / chatModelLabels', () => {
  const groups: ModelGroup[] = [
    {group: 'A', provider: 'a', configured: true, models: ['x', 'y'], labels: {x: 'X model', y: ''}},
    {group: 'B', provider: 'b', configured: true, models: ['z']},
  ];

  it('returns the label, else the id (also for an empty label)', () => {
    expect(chatModelLabel(groups[0], 'x')).toBe('X model');
    expect(chatModelLabel(groups[0], 'y')).toBe('y');
    expect(chatModelLabel(groups[1], 'z')).toBe('z');
  });

  it('maps every chat id across groups', () => {
    expect(chatModelLabels(groups)).toEqual({x: 'X model', y: 'y', z: 'z'});
  });
});
