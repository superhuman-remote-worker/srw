import {describe, expect, it} from 'vitest';
import {workspaceCreationFields, workspacePreviewConfig} from './workspace-selection';
import {WorkspacePreview} from '../../core/models/workspace.model';
import {CATALOG_SHARED} from '../../core/models/workspace-template.model';

describe('execution workspace selection (Slice A3)', () => {
  it('omits workspace for the default and strips the legacy backend and VM sizing', () => {
    const config = {workspace: {backend: 'vm', vm: {cpu_cores: 8}, max_read_words: 100}, llm: {model: 'chosen'}};
    expect(workspaceCreationFields(config, {kind: 'default'})).toEqual({
      config_override: {workspace: {max_read_words: 100}, llm: {model: 'chosen'}},
    });
    expect(config.workspace.backend).toBe('vm');
  });

  it('sends null for no workspace', () => {
    expect(workspaceCreationFields({}, {kind: 'none'})).toEqual({config_override: {}, workspace: null});
  });

  it('sends a template reference', () => {
    const choice = {kind: 'ref' as const, ref: {name: 'container-minimal', scope: CATALOG_SHARED}, backend: 'sandbox' as const, label: 'x'};
    expect(workspaceCreationFields({workspace: {backend: 'sandbox'}}, choice)).toEqual({
      config_override: {}, workspace: {template: {ref: {name: 'container-minimal', scope: {kind: 'Catalog', name: 'shared'}}}},
    });
  });

  it('sends an inline recipe', () => {
    expect(workspaceCreationFields({}, {kind: 'inline', spec: {backend: 'vm', resources: {cpu: 4}}})).toEqual({
      config_override: {}, workspace: {template: {inline: {backend: 'vm', resources: {cpu: 4}}}},
    });
  });

  it('renders the resolved tier into the preview config without changing Expert behavior', () => {
    const preview: WorkspacePreview = {backend: 'sandbox', source: 'default', binding: null};
    const expert = {workspace: {backend: 'virtual', git_versioning: false}, tools: {shell: ['run_command']}};
    expect(workspacePreviewConfig(expert, preview)).toEqual({...expert, workspace: {backend: 'sandbox', git_versioning: false}});
    expect(expert.workspace.backend).toBe('virtual');
  });
});
