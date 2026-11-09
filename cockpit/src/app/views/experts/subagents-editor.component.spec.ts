import {describe, expect, it, vi} from 'vitest';
import {Injector, runInInjectionContext} from '@angular/core';
import {of} from 'rxjs';
import {ApiService} from '../../core/services/api.service';
import type {Expert, SubagentsConfig} from '../../core/models/api.model';
import {
  draftFromEntry,
  entryFromDraft,
  mergeExpertsById,
  parseExtra,
  referenceSpelling,
  ROSTER_NAME_PATTERN,
  SubagentsEditorComponent,
  type RosterEntryDraft,
} from './subagents-editor.component';

const mkExpert = (over: Partial<Expert>): Expert => ({
  id: 'x',
  display_name: 'X',
  description: '',
  icon: '',
  color: '',
  tags: [],
  ...over,
});

const LIBRARY: Expert[] = [
  mkExpert({id: 'explorer', name: 'subagents/explorer', display_name: 'Explorer', source: 'library', tags: ['subagent']}),
];
const ALL: Expert[] = [
  mkExpert({id: 'critic', display_name: 'Critic', source: 'bundled', expert_type: 'worker', tags: ['worker']}),
  mkExpert({id: '3f2a0000-0000-4000-8000-000000000000', display_name: 'Mine', source: 'user', expert_type: 'session', tags: ['session']}),
];

function createEditor() {
  const api = {
    getExperts: vi.fn((type?: string, opts?: {showAll?: boolean}) =>
      of(opts?.showAll ? ALL : type === 'subagent' ? LIBRARY : []),
    ),
  };
  const injector = Injector.create({providers: [{provide: ApiService, useValue: api}]});
  const editor = runInInjectionContext(injector, () => new SubagentsEditorComponent());
  return {editor, api};
}

const draft = (over: Partial<RosterEntryDraft>): RosterEntryDraft => ({
  key: 1,
  name: 'x',
  kind: 'inline',
  ref: '',
  description: '',
  model: '',
  isolation: '',
  writePolicy: '',
  extraText: '',
  ...over,
});

describe('ROSTER_NAME_PATTERN', () => {
  it('mirrors the schema propertyNames grammar', () => {
    for (const ok of ['explorer', 'code_reviewer', 'a-b', '9lives']) expect(ROSTER_NAME_PATTERN.test(ok)).toBe(true);
    for (const bad of ['', '-x', '_x', 'a b', 'a/b', 'subagents/x']) expect(ROSTER_NAME_PATTERN.test(bad)).toBe(false);
  });
});

describe('parseExtra', () => {
  it('blank is an empty object', () => expect(parseExtra('  ')).toEqual({value: {}}));
  it('parses an object', () => expect(parseExtra('{"limits":{"max_turns":5}}')).toEqual({value: {limits: {max_turns: 5}}}));
  it('flags broken JSON and non-objects separately', () => {
    expect(parseExtra('{nope').error).toBe('invalid');
    expect(parseExtra('[1]').error).toBe('notObject');
    expect(parseExtra('"s"').error).toBe('notObject');
  });
});

describe('draftFromEntry / entryFromDraft', () => {
  it('splits an inline entry into the structured fields and the JSON remainder', () => {
    const d = draftFromEntry(
      'implementer',
      {
        description: 'Implements ONE bounded change.',
        llm: {model: 'claude-haiku-4-5', temperature: 0.1},
        tools: {workspace: ['read_file', 'write_file']},
        isolation: 'shared',
        write_policy: 'owned_paths',
        limits: {max_turns: 150},
        return: 'diff',
      },
      7,
    );
    expect(d.key).toBe(7);
    expect(d.name).toBe('implementer');
    expect(d.kind).toBe('inline');
    expect(d.model).toBe('claude-haiku-4-5');
    expect(d.isolation).toBe('shared');
    expect(d.writePolicy).toBe('owned_paths');
    // Everything the controls do not own — including the rest of llm — rides
    // in the textarea, so a hand-authored entry round-trips.
    expect(JSON.parse(d.extraText)).toEqual({
      tools: {workspace: ['read_file', 'write_file']},
      limits: {max_turns: 150},
      return: 'diff',
      llm: {temperature: 0.1},
    });
  });

  it('reads llm.model: inherit as the unset model', () => {
    const d = draftFromEntry('explorer', {llm: {model: 'inherit'}}, 1);
    expect(d.model).toBe('');
    expect(d.extraText).toBe('');
  });

  it('a $ref entry is a reference draft', () => {
    const d = draftFromEntry('reviewer', {$ref: 'critic', description: 'Reviews.'}, 2);
    expect(d.kind).toBe('reference');
    expect(d.ref).toBe('critic');
    expect(d.description).toBe('Reviews.');
  });

  it('ignores unknown isolation / write_policy values instead of crashing', () => {
    const d = draftFromEntry('x', {isolation: 'moon' as never, write_policy: 'all' as never}, 1);
    expect(d.isolation).toBe('');
    expect(d.writePolicy).toBe('');
  });

  it('rebuilds an inline entry; an unset model is absent (the overlay default is inherit)', () => {
    const out = entryFromDraft(
      draft({name: 'implementer', description: 'Implements.', isolation: 'worktree', writePolicy: 'full'}),
      {tools: {workspace: ['read_file']}, return: 'diff', llm: {temperature: 0.2}},
    );
    expect(out).toEqual({
      description: 'Implements.',
      llm: {temperature: 0.2},
      isolation: 'worktree',
      write_policy: 'full',
      tools: {workspace: ['read_file']},
      return: 'diff',
    });
    expect(out.$ref).toBeUndefined();
  });

  it('rebuilds a reference entry as {$ref, ...overrides}', () => {
    const out = entryFromDraft(draft({kind: 'reference', ref: 'subagents/explorer', model: 'gpt-4o'}), {});
    expect(out).toEqual({$ref: 'subagents/explorer', llm: {model: 'gpt-4o'}});
  });

  it('the structured fields win over same-named keys typed into the JSON box', () => {
    const out = entryFromDraft(draft({kind: 'inline', description: 'Real', model: 'gpt-4o'}), {
      $ref: 'smuggled',
      description: 'Typed',
      llm: {model: 'typed-model', base_url: 'http://x'},
      isolation: 'shared',
    });
    expect(out.$ref).toBeUndefined();
    expect(out.description).toBe('Real');
    expect(out.llm).toEqual({base_url: 'http://x', model: 'gpt-4o'});
    expect(out.isolation).toBeUndefined();
  });

  it('round-trips a stored entry through the draft', () => {
    const stored = {
      $ref: 'critic',
      description: 'A reviewer.',
      llm: {model: 'gpt-4o', provider: 'openai'},
      tools: {git: ['git_diff']},
      isolation: 'shared' as const,
      write_policy: 'none' as const,
      limits: {max_turns: 20, return_budget_tokens: 3000},
      return: 'evidence' as const,
      prompts: {system: 'reviewer.txt'},
    };
    const d = draftFromEntry('reviewer', stored, 1);
    expect(entryFromDraft(d, parseExtra(d.extraText).value!)).toEqual(stored);
  });
});

describe('referenceSpelling / mergeExpertsById', () => {
  it('library rows reference by their subagents/<id> name, bundled and DB rows by id', () => {
    expect(referenceSpelling(LIBRARY[0])).toBe('subagents/explorer');
    expect(referenceSpelling(ALL[0])).toBe('critic');
    expect(referenceSpelling(ALL[1])).toBe('3f2a0000-0000-4000-8000-000000000000');
    expect(referenceSpelling(mkExpert({id: 'x', source: 'library', name: undefined}))).toBe('subagents/x');
  });

  it('merges by id, first list winning', () => {
    const merged = mergeExpertsById(LIBRARY, [...ALL, mkExpert({id: 'explorer', display_name: 'dupe'})]);
    expect(merged.map((e) => e.id)).toEqual(['explorer', 'critic', '3f2a0000-0000-4000-8000-000000000000']);
    expect(merged[0].display_name).toBe('Explorer');
  });
});

describe('SubagentsEditorComponent', () => {
  it('starts empty and yields nothing', () => {
    const {editor} = createEditor();
    expect(editor.entries()).toEqual([]);
    expect(editor.getValue()).toBeNull();
    expect(editor.hasErrors()).toBe(false);
  });

  it('round-trips an existing roster (inline + reference + default) on load', () => {
    const {editor} = createEditor();
    const stored: SubagentsConfig = {
      default: 'explorer',
      llm: {model: 'claude-haiku-4-5'},
      roster: {
        explorer: {$ref: 'subagents/explorer'},
        implementer: {
          description: 'Implements.',
          llm: {model: 'inherit'},
          tools: {workspace: ['read_file']},
          isolation: 'shared',
          write_policy: 'owned_paths',
          limits: {max_turns: 150},
          return: 'diff',
        },
      },
    };
    editor.prefill(stored);
    expect(editor.entries().map((d) => d.name)).toEqual(['explorer', 'implementer']);
    expect(editor.defaultEntry()).toBe('explorer');

    const value = editor.getValue()!;
    // `llm` is the host's (the Subagent model select) — never emitted here.
    expect(value.llm).toBeUndefined();
    expect(value.default).toBe('explorer');
    expect(value.roster).toEqual({
      explorer: {$ref: 'subagents/explorer'},
      implementer: {
        description: 'Implements.',
        isolation: 'shared',
        write_policy: 'owned_paths',
        tools: {workspace: ['read_file']},
        limits: {max_turns: 150},
        return: 'diff',
      },
    });
  });

  it('add / patch / remove emit the roster the drafts describe', () => {
    const {editor} = createEditor();
    const changes = vi.fn();
    editor.settingsChange.subscribe(changes);

    editor.add();
    const key = editor.entries()[0].key;
    editor.patch(key, {name: 'reader', description: 'Reads.', model: 'gpt-4o'});
    editor.setIsolation(key, 'worktree');
    editor.setWritePolicy(key, 'none');
    expect(editor.getValue()).toEqual({
      roster: {reader: {description: 'Reads.', llm: {model: 'gpt-4o'}, isolation: 'worktree', write_policy: 'none'}},
    });

    editor.setDefault('reader');
    expect(editor.getValue()!.default).toBe('reader');

    editor.remove(key);
    expect(editor.entries()).toEqual([]);
    // The default is dropped with the entry it named.
    expect(editor.getValue()).toBeNull();
    expect(changes).toHaveBeenCalled();
  });

  it('a reference entry emits {$ref, ...overrides} and the picker loads the library lazily', () => {
    const {editor, api} = createEditor();
    editor.add();
    expect(api.getExperts).not.toHaveBeenCalled();

    const key = editor.entries()[0].key;
    editor.setKind(key, 'reference');
    expect(api.getExperts).toHaveBeenCalledWith('subagent');
    expect(editor.referenceOptions()).toEqual([{value: 'subagents/explorer', label: 'Explorer · subagents/explorer'}]);

    editor.patch(key, {name: 'explorer', ref: 'subagents/explorer', model: 'gpt-4o'});
    expect(editor.getValue()).toEqual({
      roster: {explorer: {$ref: 'subagents/explorer', llm: {model: 'gpt-4o'}}},
    });
  });

  it('"show all experts" merges every expert behind the library rows', () => {
    const {editor, api} = createEditor();
    editor.setShowAll(true);
    expect(api.getExperts).toHaveBeenCalledWith('subagent');
    expect(api.getExperts).toHaveBeenCalledWith(undefined, {showAll: true});
    expect(editor.referenceOptions().map((o) => o.value)).toEqual([
      'subagents/explorer',
      'critic',
      '3f2a0000-0000-4000-8000-000000000000',
    ]);
    expect(editor.isKnownReference('critic')).toBe(true);
    expect(editor.isKnownReference('scholar')).toBe(false);
  });

  it('loading a roster with a reference fetches the library once', () => {
    const {editor, api} = createEditor();
    editor.prefill({roster: {reviewer: {$ref: 'critic'}}});
    editor.prefill({roster: {reviewer: {$ref: 'critic'}}});
    expect(api.getExperts).toHaveBeenCalledTimes(1);
  });

  it('validates names, duplicate names, a missing reference and the JSON box', () => {
    const {editor} = createEditor();
    editor.add();
    editor.add();
    editor.add();
    const [a, b, c] = editor.entries().map((d) => d.key);
    editor.patch(a, {name: '-bad'});
    editor.patch(b, {name: 'dup'});
    editor.patch(c, {name: 'dup', kind: 'reference', extraText: '{oops'});

    expect(editor.hasErrors()).toBe(true);
    expect(editor.issuesFor(a)).toEqual(['experts.subagents.nameInvalid']);
    expect(editor.issuesFor(b)).toEqual([]);
    expect(editor.issuesFor(c)).toEqual([
      'experts.subagents.nameDuplicate',
      'experts.subagents.referenceRequired',
      'experts.subagents.extraInvalid',
    ]);

    editor.patch(a, {name: 'fine'});
    editor.patch(c, {name: 'other', ref: 'critic', extraText: '{"limits": {"max_turns": 3}}'});
    expect(editor.hasErrors()).toBe(false);
  });

  it('skips nameless entries and only emits a default that names a surviving entry', () => {
    const {editor} = createEditor();
    editor.prefill({default: 'gone', roster: {kept: {description: 'k'}}});
    editor.add(); // nameless
    expect(editor.getValue()).toEqual({roster: {kept: {description: 'k'}}});
  });

  it('carries unknown top-level keys of the block through, never llm', () => {
    const {editor} = createEditor();
    editor.prefill({llm: {model: 'x'}, roster: {}, future_key: 1} as SubagentsConfig);
    expect(editor.getValue()).toEqual({future_key: 1});
  });
});
