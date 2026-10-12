import {Component, Type, ViewContainerRef, signal, ɵresolveComponentResources} from '@angular/core';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {By} from '@angular/platform-browser';
import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {ComponentRegistryService} from '../../../core/services/component-registry.service';
import {ComponentType} from '../../layout.model';
import {LayoutService} from '../../services/layout.service';
import {PanelHeaderComponent} from '../panel-header/panel-header.component';
import {ComponentHostComponent} from './component-host.component';

// Harness note (same as ui/multi-select spec): this vitest pipeline does not
// compile signal-input metadata, so inputs are seeded by replacing the
// InputSignal fields with plain signals before the first change detection.
// The same gap hits viewChild(), so the outlet is seeded with a real
// ViewContainerRef taken from the content element.

@Component({selector: 'app-panel-header', template: ''})
class PanelHeaderStub {}

@Component({selector: 'app-panel-a', template: '<p class="panel">A</p>'})
class PanelA {}

@Component({selector: 'app-panel-b', template: '<p class="panel">B</p>'})
class PanelB {}

const TYPE_A = 'placeholder-a' as ComponentType;
const TYPE_B = 'placeholder-b' as ComponentType;

interface Deferred {
  promise: Promise<Type<unknown> | undefined>;
  resolve: (cls: Type<unknown> | undefined) => void;
  reject: (err: unknown) => void;
}

function deferred(): Deferred {
  let resolve!: Deferred['resolve'];
  let reject!: Deferred['reject'];
  const promise = new Promise<Type<unknown> | undefined>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return {promise, resolve, reject};
}

describe('ComponentHostComponent', () => {
  let loads: Map<ComponentType, Deferred>;
  let componentType: ReturnType<typeof signal<ComponentType>>;
  let fixture: ComponentFixture<ComponentHostComponent>;

  const registryStub = {
    loadComponent: (type: ComponentType) => loads.get(type)!.promise,
    getRegisteredTypes: () => [] as ComponentType[],
    get: () => undefined,
    getDisplayName: (type: ComponentType) => type,
  };
  const layoutStub = {
    getPanelCount: () => 1,
    updateComponent: () => undefined,
    splitPanel: () => undefined,
    closePanel: () => undefined,
  };

  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(async () => {
    loads = new Map([
      [TYPE_A, deferred()],
      [TYPE_B, deferred()],
    ]);
    TestBed.configureTestingModule({
      imports: [ComponentHostComponent],
      providers: [
        {provide: ComponentRegistryService, useValue: registryStub},
        {provide: LayoutService, useValue: layoutStub},
      ],
    });
    TestBed.overrideComponent(ComponentHostComponent, {
      remove: {imports: [PanelHeaderComponent]},
      add: {imports: [PanelHeaderStub]},
    });
    fixture = TestBed.createComponent(ComponentHostComponent);
    componentType = signal<ComponentType>(TYPE_A);
    (fixture.componentInstance as unknown as {componentType: unknown}).componentType = componentType;
    const content = fixture.debugElement.query(By.css('.component-content'));
    (fixture.componentInstance as unknown as {outlet: unknown}).outlet = signal(
      content.injector.get(ViewContainerRef),
    );
    fixture.detectChanges();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    TestBed.resetTestingModule();
  });

  const panels = () =>
    Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('.panel')).map(
      (el) => el.textContent,
    );

  const flush = async () => {
    await Promise.resolve();
    await Promise.resolve();
    fixture.detectChanges();
    await fixture.whenStable();
  };

  it('renders the panel after an async load resolves', async () => {
    expect(panels()).toEqual([]);
    loads.get(TYPE_A)!.resolve(PanelA);
    await flush();
    expect(panels()).toEqual(['A']);
  });

  it('keeps only the latest type when an earlier load resolves after it', async () => {
    componentType.set(TYPE_B);
    fixture.detectChanges();
    await Promise.resolve();

    loads.get(TYPE_B)!.resolve(PanelB);
    await flush();
    loads.get(TYPE_A)!.resolve(PanelA);
    await flush();

    expect(panels()).toEqual(['B']);
  });

  it('does not create the component when the host is destroyed before the load resolves', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    fixture.destroy();
    loads.get(TYPE_A)!.resolve(PanelA);
    await Promise.resolve();
    await Promise.resolve();

    expect(panels()).toEqual([]);
    expect(error).not.toHaveBeenCalled();
  });

  it('leaves the outlet empty and logs when the load rejects', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    loads.get(TYPE_A)!.reject(new Error('chunk failed'));
    await flush();

    expect(panels()).toEqual([]);
    expect(error).toHaveBeenCalledTimes(1);
    expect(String(error.mock.calls[0][0])).toContain(TYPE_A);
  });
});
