import {Injector, runInInjectionContext, signal} from '@angular/core';
import {TranslocoService} from '@jsverse/transloco';
import {beforeEach, describe, expect, it, vi} from 'vitest';
import {of, Subject, throwError} from 'rxjs';
import {AdminModelsService} from '../../../core/services/admin-models.service';
import {AdminProvidersService} from '../../../core/services/admin-providers.service';
import {AdminModelsCoordinatorService} from '../models/admin-models-coordinator.service';
import {AdminProvidersComponent} from './admin-providers.component';
import {DiscoveryCandidate} from '../../../core/models/api.model';

/**
 * Post-save discovery dialog → "Add selected": a refused row reports the
 * server's own reason (a model ID already held by another provider is a 409
 * naming that provider), not a guess that it was a duplicate.
 */

function candidate(model_id: string): DiscoveryCandidate {
  return {
    model_id,
    detected_family: 'claude-opus-5-5',
    supported: true,
    suggested_capabilities: ['chat', 'auxiliary'],
    suggested_display_label: model_id,
  };
}

const createModel = vi.fn();

function setup(): AdminProvidersComponent {
  // Logic only: the dialog's add path never needs the rendered template.
  const injector = Injector.create({
    providers: [
      {provide: TranslocoService, useValue: {translate: vi.fn((key: string) => key)}},
      {
        provide: AdminModelsService,
        useValue: {models: signal([]), createModel, loadModels: vi.fn()},
      },
      {
        provide: AdminProvidersService,
        useValue: {
          systemApiKeys: signal([]),
          systemEndpoints: signal([]),
          loadSystemApiKeys: vi.fn(),
          loadSystemEndpoints: vi.fn(),
        },
      },
      {
        provide: AdminModelsCoordinatorService,
        useValue: {discoverEndpoint$: new Subject<string>()},
      },
    ],
  });
  return runInInjectionContext(injector, () => new AdminProvidersComponent());
}

function openDialog(component: AdminProvidersComponent, ids: string[]) {
  component.discoveryProvider.set('anthropic');
  component.discoveryPayload.set({
    ready: true,
    fresh: true,
    cached_at: null,
    payload: {
      provider: 'anthropic',
      fetched_at: '2026-10-17T00:00:00Z',
      count: ids.length,
      candidates: ids.map(candidate),
    },
  });
  component.discoverySelections.set(
    Object.fromEntries(ids.map((id) => [id, {selected: true, capabilities: []}])),
  );
}

describe('AdminProvidersComponent — discovery add failures', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("lists each refused model with the server's detail", () => {
    const detail =
      "Model id 'claude-opus-5-5' is already in the catalog under endpoint 'Claude subscription proxy'.";
    createModel.mockImplementation((body: {model_id: string}) =>
      body.model_id === 'claude-opus-5-5'
        ? throwError(() => ({status: 409, error: {detail}}))
        : of({}),
    );
    const component = setup();
    openDialog(component, ['claude-opus-5-5', 'claude-haiku-4-5']);

    component.addSelectedCandidates();

    expect(component.discoveryAdding()).toBe(false);
    expect(component.discoveryError()).toBe('Added with 1 failure(s):');
    expect(component.discoveryFailures()).toEqual([{modelId: 'claude-opus-5-5', detail}]);
  });

  it('falls back to a generic line when the server sent no detail', () => {
    createModel.mockReturnValue(throwError(() => ({status: 500, error: null})));
    const component = setup();
    openDialog(component, ['claude-opus-5-5']);

    component.addSelectedCandidates();

    expect(component.discoveryFailures()).toEqual([
      {modelId: 'claude-opus-5-5', detail: 'Failed to add model.'},
    ]);
  });

  it('clears the failures and closes when every row was added', () => {
    createModel.mockReturnValue(of({}));
    const component = setup();
    component.discoveryFailures.set([{modelId: 'old', detail: 'stale'}]);
    openDialog(component, ['claude-haiku-4-5']);

    component.addSelectedCandidates();

    expect(component.discoveryFailures()).toEqual([]);
    expect(component.discoveryError()).toBe('');
  });
});
