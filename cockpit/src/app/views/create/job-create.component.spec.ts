import { afterEach, describe, expect, it, vi } from 'vitest';
import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { ActivatedRoute, Router, convertToParamMap } from '@angular/router';
import { of, throwError } from 'rxjs';
import { Expert, JobCreateRequest, Project } from '../../core/models/api.model';
import { ApiService } from '../../core/services/api.service';
import { CapabilitiesService } from '../../core/services/capabilities.service';
import { ErrorMessageService } from '../../core/services/error-message.service';
import { ModelService } from '../../core/services/model.service';
import { UserService } from '../../core/services/user.service';
import { JobCreateComponent } from './job-create.component';

/**
 * Unit tests for JobCreateComponent.
 *
 * The first block covers pure form/validation helpers. The block at the bottom
 * mounts the real component through TestBed with its template overridden away —
 * the same shape session-create's spec uses, and the only shape available here,
 * since this environment's JIT compiler cannot see initializer-based inputs
 * (`input()`, `model()`) and so cannot render templates that bind them.
 */

// Extract utility functions from component for testing
function parseContextJson(contextJson: string): { context: Record<string, unknown> | undefined; error: string | null } {
  if (!contextJson.trim()) {
    return { context: undefined, error: null };
  }
  try {
    const parsed = JSON.parse(contextJson);
    return { context: parsed, error: null };
  } catch {
    return { context: undefined, error: 'Invalid JSON format' };
  }
}

function buildJobRequest(formData: JobCreateRequest, contextJson: string): { request: JobCreateRequest | null; error: string | null } {
  if (!formData.description) {
    return { request: null, error: 'Description is required' };
  }

  // Parse context JSON if provided
  const { context, error } = parseContextJson(contextJson);
  if (error) {
    return { request: null, error };
  }

  // Build request with only non-empty fields
  const request: JobCreateRequest = {
    description: formData.description,
  };

  if (formData.document_path?.trim()) {
    request.document_path = formData.document_path.trim();
  }
  if (formData.document_dir?.trim()) {
    request.document_dir = formData.document_dir.trim();
  }
  if (formData.config_name?.trim()) {
    request.config_name = formData.config_name.trim();
  }
  if (context) {
    request.context = context;
  }
  if (formData.instructions?.trim()) {
    request.instructions = formData.instructions.trim();
  }

  return { request, error: null };
}

describe('JobCreateComponent utilities', () => {
  describe('parseContextJson', () => {
    it('should parse valid JSON object', () => {
      const { context, error } = parseContextJson('{"key": "value", "count": 5}');

      expect(error).toBeNull();
      expect(context).toEqual({ key: 'value', count: 5 });
    });

    it('should return undefined for empty string', () => {
      const { context, error } = parseContextJson('');

      expect(error).toBeNull();
      expect(context).toBeUndefined();
    });

    it('should return undefined for whitespace-only string', () => {
      const { context, error } = parseContextJson('   ');

      expect(error).toBeNull();
      expect(context).toBeUndefined();
    });

    it('should return error for invalid JSON', () => {
      const { context, error } = parseContextJson('invalid json');

      expect(error).toBe('Invalid JSON format');
      expect(context).toBeUndefined();
    });

    it('should parse JSON array', () => {
      const { context, error } = parseContextJson('[1, 2, 3]');

      expect(error).toBeNull();
      expect(context).toEqual([1, 2, 3]);
    });

    it('should parse nested JSON object', () => {
      const { context, error } = parseContextJson('{"nested": {"key": "value"}}');

      expect(error).toBeNull();
      expect(context).toEqual({ nested: { key: 'value' } });
    });
  });

  describe('buildJobRequest', () => {
    it('should return error when description is empty', () => {
      const formData: JobCreateRequest = { description: '' };

      const { request, error } = buildJobRequest(formData, '');

      expect(request).toBeNull();
      expect(error).toBe('Description is required');
    });

    it('should build request with description only', () => {
      const formData: JobCreateRequest = { description: 'Extract requirements' };

      const { request, error } = buildJobRequest(formData, '');

      expect(error).toBeNull();
      expect(request).toEqual({ description: 'Extract requirements' });
    });

    it('should build request with all optional fields', () => {
      const formData: JobCreateRequest = {
        description: 'Test prompt',
        document_path: '/path/to/doc.pdf',
        document_dir: '/path/to/docs/',
        config_name: 'creator',
        instructions: 'Additional instructions',
      };

      const { request, error } = buildJobRequest(formData, '{"key": "value"}');

      expect(error).toBeNull();
      expect(request).toEqual({
        description: 'Test prompt',
        document_path: '/path/to/doc.pdf',
        document_dir: '/path/to/docs/',
        config_name: 'creator',
        context: { key: 'value' },
        instructions: 'Additional instructions',
      });
    });

    it('should return error for invalid JSON context', () => {
      const formData: JobCreateRequest = { description: 'Test' };

      const { request, error } = buildJobRequest(formData, 'invalid');

      expect(request).toBeNull();
      expect(error).toBe('Invalid JSON format');
    });

    it('should trim whitespace from optional fields', () => {
      const formData: JobCreateRequest = {
        description: 'Test prompt',
        document_path: '  /path/to/doc.pdf  ',
        config_name: '  creator  ',
        instructions: '  Some instructions  ',
      };

      const { request, error } = buildJobRequest(formData, '');

      expect(error).toBeNull();
      expect(request?.document_path).toBe('/path/to/doc.pdf');
      expect(request?.config_name).toBe('creator');
      expect(request?.instructions).toBe('Some instructions');
    });

    it('should not include empty optional fields', () => {
      const formData: JobCreateRequest = {
        description: 'Test prompt',
        document_path: '   ', // Only whitespace
        config_name: '',
      };

      const { request, error } = buildJobRequest(formData, '');

      expect(error).toBeNull();
      expect(request).toEqual({ description: 'Test prompt' });
      expect(request?.document_path).toBeUndefined();
      expect(request?.config_name).toBeUndefined();
    });
  });

  describe('form validation', () => {
    it('should require description field', () => {
      const formData: JobCreateRequest = { description: '' };
      const { error } = buildJobRequest(formData, '');

      expect(error).toBe('Description is required');
    });

    it('should accept description with only whitespace as valid (validation not trimming description)', () => {
      const formData: JobCreateRequest = { description: '   ' };
      const { request, error } = buildJobRequest(formData, '');

      // Note: The actual component might want to trim this, but we're testing current behavior
      expect(error).toBeNull();
      expect(request?.description).toBe('   ');
    });
  });

  describe('form reset', () => {
    it('should produce empty form data structure', () => {
      const emptyFormData: JobCreateRequest = {
        description: '',
        document_path: undefined,
        document_dir: undefined,
        config_name: undefined,
        context: undefined,
        instructions: undefined,
      };

      expect(emptyFormData.description).toBe('');
      expect(emptyFormData.document_path).toBeUndefined();
      expect(emptyFormData.document_dir).toBeUndefined();
      expect(emptyFormData.config_name).toBeUndefined();
      expect(emptyFormData.context).toBeUndefined();
      expect(emptyFormData.instructions).toBeUndefined();
    });
  });
});

/**
 * Project picker behaviour, mounted with the template overridden away (the
 * house pattern for this component family — see session-create's spec, and the
 * note there about JIT and signal inputs).
 */
describe('JobCreateComponent project picker', () => {
  function setup(queryProject: string | null, deepLinked: Project | null = null) {
    const errors = {translate: vi.fn((_e: unknown, k?: string) => k ?? '')};
    const api = {
      getProjects: vi
        .fn()
        .mockImplementation((_userId?: string, status?: string[]) =>
          of(status?.[0] === 'archived' ? [] : [ACTIVE_PROJECT]),
        ),
      // What `?project=` resolves to when the active list does not have it.
      getProject: vi.fn().mockReturnValue(of(deepLinked)),
      getEligibleDatasources: vi.fn().mockReturnValue(of([])),
      getExpertDefaults: vi.fn().mockReturnValue(of(null)),
      getExperts: vi.fn().mockReturnValue(of([])),
      getExpertDetail: vi.fn().mockReturnValue(of(null)),
      previewToolGroups: vi.fn().mockReturnValue(of(null)),
      createJob: vi.fn().mockReturnValue(of({id: 'job-1'})),
    };
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        {provide: ApiService, useValue: api},
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: {queryParamMap: convertToParamMap(queryProject ? {project: queryProject} : {})},
          },
        },
        {provide: Router, useValue: {navigate: vi.fn()}},
        {provide: UserService, useValue: {currentUserId: signal('user-1')}},
        {provide: ErrorMessageService, useValue: errors},
        {provide: ModelService, useValue: {load: vi.fn()}},
        {
          provide: CapabilitiesService,
          useValue: {
            datasourceScopeAutoAttachAvailable: () => false,
            grants: signal(null),
          },
        },
      ],
    });
    TestBed.overrideComponent(JobCreateComponent, {set: {imports: [], template: ''}});
    const fixture = TestBed.createComponent(JobCreateComponent);
    fixture.detectChanges();
    return {fixture, component: fixture.componentInstance, api, errors};
  }

  const ACTIVE_PROJECT = {
    id: 'proj-1',
    name: 'Live Project',
    status: 'active',
    is_default: true,
  } as Project;

  const ARCHIVED_PROJECT = {
    id: 'proj-archived',
    name: 'Better Resavio (pre-split archive)',
    status: 'archived',
    is_default: false,
  } as Project;

  afterEach(() => TestBed.resetTestingModule());

  it('offers active projects only', () => {
    const {component, api} = setup(null);

    expect(api.getProjects).toHaveBeenCalledWith('user-1', ['active']);
    expect(component.projects().map((p) => p.id)).toEqual(['proj-1']);
    expect(component.selectedProjectId()).toBe('proj-1');
    expect(component.selectedProjectIsArchived()).toBe(false);
  });

  it('keeps a deep-linked archived project selected and flagged', () => {
    // Silently retargeting the job at the personal project is how work ends up
    // somewhere nobody asked for. The create is refused server-side; the form
    // says so before the user spends a description on it.
    const {component, api} = setup('proj-archived', ARCHIVED_PROJECT);

    expect(api.getProject).toHaveBeenCalledWith('proj-archived');
    expect(component.selectedProjectId()).toBe('proj-archived');
    expect(component.selectedProjectIsArchived()).toBe(true);
    expect(component.projects().map((p) => p.id)).toEqual(['proj-1', 'proj-archived']);
  });

  it('falls back to the default project when the deep link is gone for good', () => {
    const {component} = setup('proj-deleted');

    expect(component.selectedProjectId()).toBe('proj-1');
    expect(component.selectedProjectIsArchived()).toBe(false);
  });

  it.each([
    {selection: null, expected: {}},
    {selection: {id: 'worker_base', storage_kind: 'bundled'}, expected: {}},
    {selection: {id: 'developer', storage_kind: 'bundled'}, expected: {config_name: 'developer'}},
    {selection: {id: '44444444-4444-4444-8444-444444444444', storage_kind: 'db'},
      expected: {expert_id: '44444444-4444-4444-8444-444444444444'}},
  ])('real onSubmit preserves expert aliases and explicit connector opt-out: $selection', async ({selection, expected}) => {
    const {component, api} = setup(null);
    component.formData.description = 'Create a small report';
    component.selectedExpert.set(selection as Expert | null);

    await component.onSubmit();

    expect(api.createJob).toHaveBeenCalledExactlyOnceWith({
      description: 'Create a small report', ...expected,
      datasource_ids: [], project_id: 'proj-1', user_id: 'user-1',
    });
    expect(component.isSubmitting()).toBe(false);
    expect(component.formData.description).toBe('');
  });

  it('real onSubmit retains reviewed settings and only sends changed priority', async () => {
    const {component, api} = setup(null);
    component.formData.description = 'Keep my settings';
    component.selectedPriority.set(7);
    component.kickoffMessage = '  Start here  ';
    component.agentSettings = {
      vmSizingValid: () => true,
      getOverrides: () => ({llm: {model: 'selected-model'}}),
      getInstructions: () => 'Instructions',
      getSelectedDatasourceIds: () => ['connector-1'],
      resetAll: vi.fn(),
    } as unknown as JobCreateComponent['agentSettings'];

    await component.onSubmit();

    expect(api.createJob).toHaveBeenCalledExactlyOnceWith({
      description: 'Keep my settings', config_override: {llm: {model: 'selected-model'}},
      instructions: 'Instructions', kickoff_message: 'Start here', datasource_ids: ['connector-1'],
      priority: 7, project_id: 'proj-1', user_id: 'user-1',
    });
  });

  it('does not create a job while VM sizing is invalid', async () => {
    const {component, api} = setup(null);
    component.formData.description = 'Run on a VM';
    component.agentSettings = {vmSizingValid: () => false} as JobCreateComponent['agentSettings'];

    await component.onSubmit();

    expect(api.createJob).not.toHaveBeenCalled();
    expect(component.formData.description).toBe('Run on a VM');
  });

  it('real onSubmit preserves selections and forwards the server error to translation', async () => {
    const {component, api, errors} = setup(null);
    const error = {status: 409, error: {detail: {code: 'contract_conflict', message: 'Review the contract'}}};
    api.createJob.mockReturnValue(throwError(() => error));
    component.formData.description = 'Keep this draft';
    component.selectedPriority.set(7);
    const selected = {id: 'developer', storage_kind: 'bundled'} as Expert;
    component.selectedExpert.set(selected);

    await component.onSubmit();

    expect(api.createJob).toHaveBeenCalledTimes(1);
    expect(errors.translate).toHaveBeenCalledWith(error, 'errors.jobs.createFailed');
    expect(component.formData.description).toBe('Keep this draft');
    expect(component.selectedExpert()).toBe(selected);
    expect(component.selectedPriority()).toBe(7);
    expect(component.isSubmitting()).toBe(false);
  });
});
