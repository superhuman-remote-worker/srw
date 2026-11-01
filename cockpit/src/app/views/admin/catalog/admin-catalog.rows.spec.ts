import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {of, Subject, throwError} from 'rxjs';
import {AdminModelsService} from '../../../core/services/admin-models.service';
import {AdminProvidersService} from '../../../core/services/admin-providers.service';
import {AdminModelsCoordinatorService} from '../models/admin-models-coordinator.service';
import {AdminCatalogComponent} from './admin-catalog.component';
import {CatalogModel} from '../../../core/models/api.model';

const models = {
  models: signal<CatalogModel[]>([]),
  families: signal(['default']),
  familyDefaults: signal<Record<string, number>>({}),
  loading: signal(false),
  loadModels: vi.fn(),
  loadFamilies: vi.fn(),
  detectFamily: vi.fn(() => of({family: 'default', source: 'fallback'})),
  createModel: vi.fn(() => of({} as never)),
  updateModel: vi.fn(() => of({})),
  deleteModel: vi.fn(() => of({})),
  testModel: vi.fn(() => of({})),
};

const providers = {
  systemApiKeys: signal([]),
  systemEndpoints: signal([]),
  subscriptionAvailability: signal(null),
  loadSystemApiKeys: vi.fn(),
  loadSystemEndpoints: vi.fn(),
  loadSubscriptionAvailability: vi.fn(),
  discoverSystemEndpointModels: vi.fn(),
  importSubscriptionModels: vi.fn(),
};

function setup() {
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({
    imports: [AdminCatalogComponent],
    providers: [
      {provide: TranslocoService, useValue: {translate: vi.fn((key: string) => key)}},
      {provide: AdminModelsService, useValue: models},
      {provide: AdminProvidersService, useValue: providers},
      {
        provide: AdminModelsCoordinatorService,
        useValue: {discoverEndpoint$: new Subject<string>()},
      },
    ],
  });
  TestBed.overrideComponent(AdminCatalogComponent, {
    set: {imports: [], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
  });
  return TestBed.createComponent(AdminCatalogComponent);
}

function row(overrides: Partial<CatalogModel> = {}): CatalogModel {
  return {
    id: 'row-1',
    provider_kind: 'system',
    provider_ref: 'openai',
    model_id: 'gpt-6-sol',
    display_label: 'gpt-6-sol',
    capabilities: ['chat', 'auxiliary'],
    family: 'gpt-6',
    context_window: null,
    resolved_context_window: 1_048_576,
    context_window_source: 'family_default',
    reasoning_level: null,
    params_json: null,
    enabled: true,
    seeded_from: null,
    notes: null,
    created_at: null,
    updated_at: null,
    client_protocol: null,
    ...overrides,
  } as CatalogModel;
}

describe('catalog rows', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    vi.clearAllMocks();
    models.models.set([]);
    models.loading.set(false);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('keeps the table on screen while the list reloads', () => {
    // Every edit reloads the list. Swapping the rows for "Loading…" meanwhile
    // shrank the page under the scroll position, so a toggle jumped to the top.
    models.models.set([row()]);
    const fixture = setup();
    fixture.detectChanges();
    models.loading.set(true);
    fixture.detectChanges();
    const el = fixture.nativeElement as HTMLElement;
    expect(el.querySelectorAll('.model-row').length).toBe(1);
    expect(el.textContent).not.toContain('Loading…');
  });

  it('shows the placeholder only for the first load', () => {
    models.loading.set(true);
    const fixture = setup();
    fixture.detectChanges();
    const el = fixture.nativeElement as HTMLElement;
    expect(el.textContent).toContain('Loading…');
    expect(el.querySelector('.model-row')).toBeNull();
  });

  it('puts every provider group on the one shared grid', () => {
    models.models.set([
      row(),
      row({id: 'row-2', provider_ref: 'anthropic', model_id: 'claude', display_label: 'claude'}),
    ]);
    const fixture = setup();
    fixture.detectChanges();
    const grids = (fixture.nativeElement as HTMLElement).querySelectorAll('.catalog-grid');
    expect(grids.length).toBe(1);
    expect(grids[0].querySelectorAll('.provider-group').length).toBe(2);
    expect(grids[0].querySelectorAll('.model-row').length).toBe(2);
  });

  it('patches the row when the box is ticked', () => {
    const component = setup().componentInstance;
    component.toggleEnabled(row(), false);
    expect(models.updateModel).toHaveBeenCalledWith('row-1', {enabled: false});
    expect(component.rowError()).toBe('');
  });

  it('resets the box without a PATCH when the Helm override is cancelled', () => {
    vi.spyOn(window, 'confirm').mockReturnValue(false);
    const component = setup().componentInstance;
    const before = component.resyncKey(row());
    component.toggleEnabled(row({managed_by_helm: true}), false);
    expect(models.updateModel).not.toHaveBeenCalled();
    expect(component.resyncKey(row())).not.toBe(before);
  });

  it('reloads, resets the box and says why when the PATCH fails', () => {
    models.updateModel.mockReturnValueOnce(
      throwError(() => ({error: {detail: 'last chat model'}})) as never,
    );
    const component = setup().componentInstance;
    const before = component.resyncKey(row());
    component.toggleEnabled(row(), false);
    expect(models.loadModels).toHaveBeenCalled();
    expect(component.resyncKey(row())).not.toBe(before);
    expect(component.rowError()).toBe('gpt-6-sol: last chat model');
  });

  it("re-creates only that row's checkbox on a resync", () => {
    // A cancelled toggle rebuilds the clicked box so it shows the stored value
    // again; the other rows keep theirs (rebuilding every row's controls also
    // flooded the dev console with NG0956).
    vi.spyOn(window, 'confirm').mockReturnValue(false);
    const other = row({id: 'row-2', model_id: 'claude', display_label: 'claude'});
    models.models.set([row({managed_by_helm: true}), other]);
    const fixture = setup();
    fixture.detectChanges();
    const boxes = () =>
      Array.from(
        (fixture.nativeElement as HTMLElement).querySelectorAll('.model-row .col-enabled app-checkbox'),
      );
    const [firstBefore, otherBefore] = boxes();
    fixture.componentInstance.toggleEnabled(row({managed_by_helm: true}), false);
    fixture.detectChanges();
    const [firstAfter, otherAfter] = boxes();
    expect(firstAfter).not.toBeUndefined();
    expect(firstAfter).not.toBe(firstBefore);
    expect(otherAfter).toBe(otherBefore);
  });
});
