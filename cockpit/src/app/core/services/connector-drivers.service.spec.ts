import {Injector, runInInjectionContext} from '@angular/core';
import {of, throwError} from 'rxjs';
import {describe, expect, it, vi} from 'vitest';
import {ApiService} from './api.service';
import {ConnectorDriversService} from './connector-drivers.service';
import fixture from '../models/fixtures/connector-drivers.json';

function create(response: () => unknown) {
  const api = {getConnectorDrivers: vi.fn().mockImplementation(response)};
  const injector = Injector.create({providers: [{provide: ApiService, useValue: api}]});
  const service = runInInjectionContext(injector, () => new ConnectorDriversService());
  return {api, service};
}

describe('ConnectorDriversService', () => {
  it('reads the matrix once and finds a driver by stored type', () => {
    const {api, service} = create(() => of(fixture));
    expect(service.drivers()).toBeNull();
    service.load();
    service.load();
    expect(api.getConnectorDrivers).toHaveBeenCalledTimes(1);
    expect(service.forType('kb')?.name).toBe('srw.kb/v1');
    service.load(true);
    expect(api.getConnectorDrivers).toHaveBeenCalledTimes(2);
  });

  it('stays unknown after a failed read, so callers keep their old rules', () => {
    const {service} = create(() => throwError(() => new Error('502')));
    service.load();
    expect(service.loadFailed()).toBe(true);
    expect(service.drivers()).toBeNull();
    expect(service.forType('kb')).toBeNull();
  });
});
