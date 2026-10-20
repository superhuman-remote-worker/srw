import {describe, expect, it} from 'vitest';
import {
  buildModelFragment,
  buildPromptsPayload,
  buildSubagentsFragment,
  buildTagsPayload,
  expertBaseConfigName,
  expertEditorMode,
  expertToolPreviewRequest,
  forkNoticeTranslationArgs,
  parseConfigText,
  slugify,
  splitTags,
  stripEmptySubagents,
} from './expert-editor.component';
import {assembleExpertConfig, liftLegacyTiers} from './expert-config';

const FIELDS = {
  persona: 'P',
  instructions: 'I',
  strategic: 'S',
  tactical: 'T',
  summarization: 'Z',
};

describe('slugify', () => {
  it('lowercases and dashes', () => expect(slugify('My Cool Expert!')).toBe('my-cool-expert'));
  it('prefixes when not starting with a letter', () =>
    expect(slugify('123 go')).toMatch(/^[a-z][a-z0-9_-]*$/));
  it('strips non-ascii', () => expect(slugify('Über Helper')).toMatch(/^[a-z][a-z0-9_-]*$/));
  it('empty falls back to expert', () => expect(slugify('   ')).toBe('expert'));
});

describe('parseConfigText', () => {
  it('blank yields empty object', () => expect(parseConfigText('  ')).toEqual({config: {}}));
  it('valid object parses', () =>
    expect(parseConfigText('{"llm":{"model":"x"}}')).toEqual({config: {llm: {model: 'x'}}}));
  it('array is rejected', () => expect(parseConfigText('[]').error).toBeTruthy());
  it('non-json is rejected', () => expect(parseConfigText('{nope').error).toBeTruthy());
});

describe('expert type base', () => {
  it('maps each immutable expert type to its matching mode and base', () => {
    expect(expertBaseConfigName('worker')).toBe('worker_base');
    expect(expertEditorMode('worker')).toBe('job');
    expect(expertBaseConfigName('session')).toBe('session_base');
    expect(expertEditorMode('session')).toBe('session');
  });
});

describe('expertToolPreviewRequest', () => {
  it('resolves against the base the edited type inherits from', () => {
    expect(expertToolPreviewRequest('worker', {}).config_name).toBe('worker_base');
    expect(expertToolPreviewRequest('session', {}).config_name).toBe('session_base');
  });

  it('forwards the type, so the server defaults nothing on our behalf', () => {
    // The endpoint's own default is `session`; omitting this predicts
    // session_base for a worker expert.
    expect(expertToolPreviewRequest('worker', {}).expert_type).toBe('worker');
    expect(expertToolPreviewRequest('session', {}).expert_type).toBe('session');
  });

  it('sends the fragment as the override layer', () => {
    const fragment = {tools: {shell: ['run_command']}};
    expect(expertToolPreviewRequest('worker', fragment).config_override).toEqual(fragment);
  });

  it('sends null rather than an empty layer when there is no fragment', () => {
    expect(expertToolPreviewRequest('worker', {}).config_override).toBeNull();
  });

  it('never identifies the expert by id', () => {
    // The whole point: base ⊕ fragment. An `expert_id` layer underneath cannot
    // express a key the author DELETED, so the pane would keep showing a
    // category the expert is about to lose. Asserted structurally because the
    // failure mode is an extra field, not a wrong one.
    expect(Object.keys(expertToolPreviewRequest('worker', {tools: {}})).sort()).toEqual([
      'config_name',
      'config_override',
      'expert_type',
    ]);
  });
});

describe('buildPromptsPayload', () => {
  it('worker mode emits all five segments', () => {
    expect(buildPromptsPayload(FIELDS, 'job')).toEqual({
      persona: 'P',
      instructions: 'I',
      strategic: 'S',
      tactical: 'T',
      summarization: 'Z',
    });
  });

  it('session mode drops the worker-only phase prompts', () => {
    const out = buildPromptsPayload(FIELDS, 'session');
    expect(out).toEqual({persona: 'P', instructions: 'I', summarization: 'Z'});
    expect(out['strategic']).toBeUndefined();
    expect(out['tactical']).toBeUndefined();
  });

  it('omits empty/whitespace segments (empty ⇒ inherit / clear)', () => {
    const out = buildPromptsPayload(
      {persona: 'P', instructions: '', strategic: '   ', tactical: 'T', summarization: ''},
      'job',
    );
    expect(out).toEqual({persona: 'P', tactical: 'T'});
  });
});

// U1: ONE model per expert (llm.model) for worker and session alike; the
// roster-wide subagent model lives at subagents.llm.model.
describe('buildModelFragment', () => {
  it('writes llm.model, and nothing for an empty pick (inherit)', () => {
    expect(buildModelFragment('gpt-5.4')).toEqual({llm: {model: 'gpt-5.4'}});
    expect(buildModelFragment('')).toEqual({});
  });
});

describe('buildSubagentsFragment', () => {
  it('merges the subagent model into the roster editor value', () => {
    expect(
      buildSubagentsFragment({default: 'explorer', roster: {explorer: {$ref: 'subagents/explorer'}}}, 'gpt-4o'),
    ).toEqual({
      default: 'explorer',
      roster: {explorer: {$ref: 'subagents/explorer'}},
      llm: {model: 'gpt-4o'},
    });
  });

  it('is {} (never absent) when there is nothing — the host replaces the stored block wholesale', () => {
    expect(buildSubagentsFragment(null, '')).toEqual({});
  });

  it('a subagent model alone yields the roster-wide llm', () => {
    expect(buildSubagentsFragment(null, 'gpt-4o')).toEqual({llm: {model: 'gpt-4o'}});
  });

  it('an empty pick clears llm.model but keeps the rest of llm', () => {
    expect(buildSubagentsFragment({llm: {model: 'old', provider: 'openai'}} as never, '')).toEqual({
      llm: {provider: 'openai'},
    });
    expect(buildSubagentsFragment({llm: {model: 'old'}} as never, '')).toEqual({});
  });

  it('end to end: a removed roster entry stays removed, an empty block is stripped', () => {
    const stored = {
      llm: {model: 'lead'},
      subagents: {default: 'b', llm: {model: 'reader'}, roster: {a: {$ref: 'critic'}, b: {$ref: 'scholar'}}},
    };
    const overrides = {
      subagents: buildSubagentsFragment({roster: {a: {$ref: 'critic'}}}, 'reader'),
    };
    expect(stripEmptySubagents(assembleExpertConfig(stored, overrides, {}))).toEqual({
      llm: {model: 'lead'},
      subagents: {roster: {a: {$ref: 'critic'}}, llm: {model: 'reader'}},
    });
    // Everything cleared → no subagents key at all.
    expect(
      stripEmptySubagents(assembleExpertConfig(stored, {subagents: buildSubagentsFragment(null, '')}, {})),
    ).toEqual({llm: {model: 'lead'}});
  });

  it('a legacy fragment is prefilled through the lift and saved back in the new shape', () => {
    // What ngOnInit does with the export bundle's config before anything reads it.
    const lifted = liftLegacyTiers({
      llm: {strategic: {model: 'strat', reasoning_level: 'high'}, subagent: {model: 'reader'}},
    });
    expect(lifted).toEqual({
      llm: {model: 'strat', reasoning_level: 'high'},
      subagents: {llm: {model: 'reader'}},
    });
    // …and the save writes exactly that shape, not the tiers.
    const saved = stripEmptySubagents(
      assembleExpertConfig(
        lifted,
        {...buildModelFragment('strat'), subagents: buildSubagentsFragment(null, 'reader')},
        {},
      ),
    );
    expect(saved).toEqual({
      llm: {model: 'strat', reasoning_level: 'high'},
      subagents: {llm: {model: 'reader'}},
    });
    expect((saved['llm'] as Record<string, unknown>)['strategic']).toBeUndefined();
  });
});

describe('stripEmptySubagents', () => {
  it('drops only an empty subagents block', () => {
    expect(stripEmptySubagents({llm: {}, subagents: {}})).toEqual({llm: {}});
    expect(stripEmptySubagents({subagents: {roster: {}}})).toEqual({subagents: {roster: {}}});
    expect(stripEmptySubagents({llm: {}})).toEqual({llm: {}});
  });
});

// Tags: role chips (the expert's own type locked on) + free text, one array.
describe('buildTagsPayload / splitTags', () => {
  it('puts the role tags first, in canonical order, with the type always on', () => {
    expect(buildTagsPayload('session', ['subagent'], 'research, academic')).toEqual([
      'session',
      'subagent',
      'research',
      'academic',
    ]);
    expect(buildTagsPayload('worker', [], '')).toEqual(['worker']);
  });

  it('de-duplicates and trims', () => {
    expect(buildTagsPayload('worker', ['worker', 'session'], ' worker , x,, x ')).toEqual([
      'worker',
      'session',
      'x',
    ]);
  });

  it('splits stored tags back into chips and free text', () => {
    expect(splitTags(['session', 'research', 'worker', 'academic'])).toEqual({
      roles: ['worker', 'session'],
      free: 'research, academic',
    });
    expect(splitTags([])).toEqual({roles: [], free: ''});
  });

  it('round-trips', () => {
    const {roles, free} = splitTags(['worker', 'subagent', 'a', 'b']);
    expect(buildTagsPayload('worker', roles, free)).toEqual(['worker', 'subagent', 'a', 'b']);
  });
});

// Task 4 (2026-08-04 plan): landing here right after `fork_my_expert_default`
// stripped something must surface it once, via router state — see
// ExpertEditorNavigationState and the constructor read in
// expert-editor.component.ts. Unlike duplicateResultTranslationArgs
// (experts-list.component.ts), a clean fork has nothing to say: arriving on
// this page already IS the success signal, so only the stripped case renders
// anything at all.
describe('forkNoticeTranslationArgs', () => {
  it('renders nothing when dropped is absent', () => {
    expect(forkNoticeTranslationArgs(undefined)).toBeNull();
  });

  it('renders nothing when dropped is empty', () => {
    expect(forkNoticeTranslationArgs([])).toBeNull();
  });

  it('names the missing grants, joined, when something was dropped', () => {
    expect(forkNoticeTranslationArgs(['shell_tools', 'delegation'])).toEqual([
      'experts.forkedMissingGrants',
      {grants: 'shell_tools, delegation'},
    ]);
  });

  it('says "duplicated", not "new default", after the view\'s Duplicate to edit', () => {
    expect(forkNoticeTranslationArgs(['shell_tools'], true)).toEqual([
      'experts.duplicatedMissingGrants',
      {grants: 'shell_tools'},
    ]);
  });

  it('handles a single dropped grant without a trailing separator', () => {
    expect(forkNoticeTranslationArgs(['shell_tools'])).toEqual([
      'experts.forkedMissingGrants',
      {grants: 'shell_tools'},
    ]);
  });
});
