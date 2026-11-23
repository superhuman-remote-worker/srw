import {signal, WritableSignal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import {of, throwError} from 'rxjs';
import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';

import en from '../../../assets/i18n/en.json';
import {ApiService} from '../../core/services/api.service';
import {AttachmentThumbComponent, canThumbnail} from './attachment-thumb.component';

describe('canThumbnail (raster allowlist)', () => {
  it('takes PNG, JPEG, GIF and WebP only', () => {
    for (const path of ['uploads/a.png', 'uploads/b.JPG', 'uploads/c.jpeg', 'uploads/d.gif', 'uploads/e.webp']) {
      expect(canThumbnail(path)).toBe(true);
    }
    for (const path of ['uploads/x.svg', 'uploads/y.heic', 'uploads/z.pdf', 'uploads/noext', '', undefined]) {
      expect(canThumbnail(path)).toBe(false);
    }
  });
});

/**
 * This repo's vitest pipeline never runs components through ngtsc, so signal
 * inputs carry no metadata and bindings to them are inert (see
 * ui/multi-select/multi-select.component.spec.ts). The inputs are seeded by
 * replacing the InputSignal fields with plain signals before the first check.
 */
function seed(component: AttachmentThumbComponent, key: 'threadId' | 'path' | 'name', value: string): void {
  (component as unknown as Record<string, WritableSignal<string>>)[key] = signal(value);
}

describe('AttachmentThumbComponent', () => {
  const downloadThreadFile = vi.fn();
  let observed: Element[];
  let fire: () => void;

  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    observed = [];
    downloadThreadFile.mockReset();
    vi.stubGlobal(
      'IntersectionObserver',
      class {
        constructor(private readonly callback: IntersectionObserverCallback) {
          fire = () => this.callback([{isIntersecting: true} as IntersectionObserverEntry], this as never);
        }
        observe(el: Element) {
          observed.push(el);
        }
        disconnect() {}
      },
    );
    vi.stubGlobal('URL', Object.assign(URL, {createObjectURL: vi.fn(() => 'blob:thumb'), revokeObjectURL: vi.fn()}));
    TestBed.configureTestingModule({
      imports: [
        AttachmentThumbComponent,
        TranslocoTestingModule.forRoot({langs: {en}, translocoConfig: {availableLangs: ['en'], defaultLang: 'en'}}),
      ],
      providers: [{provide: ApiService, useValue: {downloadThreadFile}}],
    });
  });

  afterEach(() => vi.unstubAllGlobals());

  function create(path = 'uploads/cat.png') {
    const fixture = TestBed.createComponent(AttachmentThumbComponent);
    seed(fixture.componentInstance, 'threadId', 't-1');
    seed(fixture.componentInstance, 'path', path);
    seed(fixture.componentInstance, 'name', 'cat.png');
    const openedWith: unknown[] = [];
    fixture.componentInstance.opened.subscribe((value) => openedWith.push(value));
    fixture.detectChanges();
    return Object.assign(fixture, {openedWith});
  }

  it('loads only once in view, then opens the dialog on click', async () => {
    downloadThreadFile.mockReturnValue(of(new Blob(['png-bytes'])));
    const fixture = create();
    expect(downloadThreadFile).not.toHaveBeenCalled();
    expect(observed.length).toBe(1);

    fire();
    await vi.waitFor(() => expect(fixture.componentInstance.url()).toBe('blob:thumb'));
    fixture.detectChanges();
    expect(downloadThreadFile).toHaveBeenCalledWith('t-1', 'uploads/cat.png');
    const button = (fixture.nativeElement as HTMLElement).querySelector('button');
    expect(button?.getAttribute('aria-label')).toBe('Open cat.png');
    expect(button?.querySelector('img')?.getAttribute('src')).toBe('blob:thumb');

    button!.click();
    expect(fixture.openedWith).toEqual([{url: 'blob:thumb', name: 'cat.png'}]);
  });

  it('leaves the plain chip when the file cannot be fetched', async () => {
    downloadThreadFile.mockReturnValue(throwError(() => ({status: 409})));
    const fixture = create();
    fire();
    await vi.waitFor(() => expect(downloadThreadFile).toHaveBeenCalled());
    await new Promise((resolve) => setTimeout(resolve, 0));
    fixture.detectChanges();
    expect(fixture.componentInstance.url()).toBeNull();
    expect((fixture.nativeElement as HTMLElement).querySelector('button')).toBeNull();
  });

  it('never fetches a type it may not show', () => {
    create('uploads/drawing.svg');
    expect(observed.length).toBe(0);
    expect(downloadThreadFile).not.toHaveBeenCalled();
  });
});
