import {TestBed} from '@angular/core/testing';
import {provideHttpClient} from '@angular/common/http';
import {HttpTestingController, provideHttpClientTesting} from '@angular/common/http/testing';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {AdminModelsService} from './admin-models.service';
import {ModelService} from './model.service';
import {ReadinessService} from './readiness.service';
import {CatalogModel} from '../models/api.model';
import {environment} from '../environment';

const URL = `${environment.apiUrl}/admin/providers/models`;

function rows(...ids: string[]): CatalogModel[] {
  return ids.map((id) => ({id}) as CatalogModel);
}

describe('AdminModelsService.loadModels', () => {
  let service: AdminModelsService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: ReadinessService, useValue: {load: vi.fn()}},
        {provide: ModelService, useValue: {load: vi.fn()}},
      ],
    });
    service = TestBed.inject(AdminModelsService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('drops an older reload that answers after a newer one', () => {
    // Two quick toggles race two GETs; the first one's answer must not put the
    // first toggle's state back on screen.
    service.loadModels();
    service.loadModels();
    const [older, newer] = http.match(URL);
    newer.flush(rows('new'));
    older.flush(rows('old'));
    expect(service.models().map((m) => m.id)).toEqual(['new']);
    expect(service.loading()).toBe(false);
  });

  it('keeps the rows on screen when a reload fails', () => {
    service.loadModels();
    http.expectOne(URL).flush(rows('a', 'b'));
    service.loadModels();
    http.expectOne(URL).flush('boom', {status: 500, statusText: 'Server Error'});
    expect(service.models().map((m) => m.id)).toEqual(['a', 'b']);
    expect(service.loading()).toBe(false);
  });
});
