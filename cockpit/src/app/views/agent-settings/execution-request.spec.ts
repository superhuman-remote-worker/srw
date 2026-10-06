import {describe, expect, it} from 'vitest';
import {load} from 'js-yaml';
import {executionManifest, inlineExpertSelection, toYaml} from './execution-request';

describe('inlineExpertSelection', () => {
  const template = {
    runtimeConfig: {
      config_name: 'worker_base',
      asset_name: 'developer',
      config: {llm: {model: 'm1', reasoning_level: 'high'}, tools: {shell: ['run_command'], web: true}},
      prompts: {persona: 'P', instructions: 'I'},
      layers: [],
    },
    workspacePreference: {backend: 'sandbox'},
  };

  it('deep-sets the changes into the authored config and keeps everything else as authored', () => {
    const sel = inlineExpertSelection(template, {llm: {model: 'm2'}, tools: {shell: []}}, null);
    expect(sel.inline.runtime.adapter).toBe('srw/v1');
    expect(sel.inline.runtime.config).toEqual({
      config_name: 'worker_base',
      asset_name: 'developer',
      config: {llm: {model: 'm2', reasoning_level: 'high'}, tools: {shell: [], web: true}},
      prompts: {persona: 'P', instructions: 'I'},
      layers: [],
    });
    expect(sel.inline.workspacePreference).toEqual({backend: 'sandbox'});
  });

  it('never mutates the template', () => {
    inlineExpertSelection(template, {llm: {model: 'm2'}}, 'new');
    expect(template.runtimeConfig.config.llm.model).toBe('m1');
    expect(template.runtimeConfig.prompts.instructions).toBe('I');
  });

  it('edited instructions replace the authored ones; cleared ones are removed', () => {
    expect(inlineExpertSelection(template, {}, 'new').inline.runtime.config.prompts?.['instructions']).toBe('new');
    expect(inlineExpertSelection(template, {}, '  ').inline.runtime.config.prompts).not.toHaveProperty('instructions');
  });

  it('a template without prompts or a preference stays without them', () => {
    const sel = inlineExpertSelection({runtimeConfig: {config: {a: 1}}}, {b: 2}, null);
    expect(sel.inline).toEqual({runtime: {adapter: 'srw/v1', config: {config: {a: 1, b: 2}}}});
  });
});

describe('toYaml', () => {
  it('round-trips plain data through a real YAML parser', () => {
    const data = {
      apiVersion: 'srw/v1alpha1',
      kind: 'Job',
      metadata: {name: 'fix-it', scope: {kind: 'Project', name: 'srw-platform'}},
      spec: {
        task: {text: 'Line one\nLine two: with a colon'},
        execution: {
          expert: {ref: {name: 'developer'}},
          workspace: null,
          connectors: {},
        },
        settings: {autonomy: 'full', tags: ['a', 'b'], list: [], nested: [{x: 1, y: 'yes'}], flag: true, n: 3},
      },
    };
    expect(load(toYaml(data))).toEqual(data);
  });

  it('quotes strings YAML would misread', () => {
    expect(load(toYaml({a: 'true', b: '123', c: '', d: '- dash', e: 'x: y'}))).toEqual(
      {a: 'true', b: '123', c: '', d: '- dash', e: 'x: y'},
    );
  });
});

describe('executionManifest', () => {
  it('renders a Job with the three parts under execution', () => {
    const doc = executionManifest({
      kind: 'job', name: 'Fix the flaky test', projectName: 'SRW Platform', task: 'Do it',
      expert: 'developer', workspace: {template: {ref: {name: 'container-full'}}},
      connectors: [{id: 'ds-1', name: 'Team wiki'}], settings: {priority: 7},
    }) as {kind: string; metadata: {name: string; scope: unknown}; spec: {execution: Record<string, unknown>; settings: unknown}};
    expect(doc.kind).toBe('Job');
    expect(doc.metadata.scope).toEqual({kind: 'Project', name: 'srw-platform'});
    expect(doc.metadata.name).toBe('fix-the-flaky-test');
    expect(Object.keys(doc.spec.execution)).toEqual(['expert', 'workspace', 'connectors']);
    expect(doc.spec.execution['connectors']).toEqual({
      'team-wiki': {inline: {driver: 'srw.datasource/v1', config: {datasourceId: 'ds-1'}}},
    });
    expect(doc.spec.settings).toEqual({priority: 7});
  });

  it('omits workspace for the default choice and renders a session as its execution block', () => {
    const doc = executionManifest({kind: 'session', expert: null, connectors: []}) as Record<string, unknown>;
    expect(doc).toEqual({execution: {connectors: {}}});
  });
});
