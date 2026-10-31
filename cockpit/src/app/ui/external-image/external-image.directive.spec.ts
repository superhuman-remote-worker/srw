import {Component} from '@angular/core';
import {provideHttpClient} from '@angular/common/http';
import {
  HttpTestingController,
  provideHttpClientTesting,
} from '@angular/common/http/testing';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {
  MARKED_EXTENSIONS,
  SANITIZE,
  MarkdownComponent,
  provideMarkdown,
} from 'ngx-markdown';
import {of} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {externalImageExtension} from '../../core/markdown/external-image-extension';
import {sanitizeMarkdownHtml} from '../../core/markdown/markdown-sanitizer';
import {
  ExternalImageDirective,
  rasterImageType,
  reviewExternalImageUrl,
} from './external-image.directive';

@Component({
  standalone: true,
  imports: [MarkdownComponent, ExternalImageDirective],
  template: `<markdown [data]="content"></markdown>`,
})
class ExternalImageHost {
  content = '';
}

describe('ExternalImageDirective', () => {
  let http: HttpTestingController;
  let createObjectUrl: ReturnType<typeof vi.fn>;
  let revokeObjectUrl: ReturnType<typeof vi.fn>;
  let createDescriptor: PropertyDescriptor | undefined;
  let revokeDescriptor: PropertyDescriptor | undefined;

  beforeEach(() => {
    createDescriptor = Object.getOwnPropertyDescriptor(URL, 'createObjectURL');
    revokeDescriptor = Object.getOwnPropertyDescriptor(URL, 'revokeObjectURL');
    createObjectUrl = vi.fn(() => 'blob:reviewed-image');
    revokeObjectUrl = vi.fn();
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      value: createObjectUrl,
    });
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      value: revokeObjectUrl,
    });

    TestBed.configureTestingModule({
      imports: [ExternalImageHost],
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        provideMarkdown({
          markedExtensions: [
            {
              provide: MARKED_EXTENSIONS,
              multi: true,
              useValue: externalImageExtension(),
            },
          ],
          sanitize: {
            provide: SANITIZE,
            useValue: sanitizeMarkdownHtml,
          },
        }),
        {
          provide: TranslocoService,
          useValue: {
            langChanges$: of('en'),
            translate: (key: string, params?: Record<string, string>) => {
              const host = params?.['host'] ? ` ${params['host']}` : '';
              return `${key}${host}`;
            },
          },
        },
      ],
    });
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    http.verify();
    TestBed.resetTestingModule();
    if (createDescriptor) {
      Object.defineProperty(URL, 'createObjectURL', createDescriptor);
    } else {
      delete (URL as unknown as Record<string, unknown>)['createObjectURL'];
    }
    if (revokeDescriptor) {
      Object.defineProperty(URL, 'revokeObjectURL', revokeDescriptor);
    } else {
      delete (URL as unknown as Record<string, unknown>)['revokeObjectURL'];
    }
  });

  it('does no network work until the exact card load button is clicked', async () => {
    const url = 'https://images.example/chart.png?account=review-this';
    const fixture = TestBed.createComponent(ExternalImageHost);
    fixture.componentInstance.content = `![Quarterly chart](${url})`;
    fixture.detectChanges();
    await fixture.whenStable();
    await new Promise((resolve) => setTimeout(resolve, 0));
    fixture.detectChanges();

    const root = fixture.nativeElement as HTMLElement;
    const card = root.querySelector<HTMLElement>('.external-image-card');
    expect(card, root.innerHTML).not.toBeNull();
    expect(card?.textContent).toContain(url);
    expect(root.querySelector('img')).toBeNull();
    http.expectNone(() => true);

    const load = root.querySelector<HTMLButtonElement>(
      'button[data-external-image-action="load"]',
    );
    expect(load?.disabled).toBe(false);
    load?.click();

    const request = http.expectOne((candidate) =>
      candidate.url.endsWith('/api/media/remote-image'),
    );
    expect(request.request.method).toBe('POST');
    expect(request.request.body).toEqual({url});
    expect(root.querySelector('img')).toBeNull();

    const blob = new Blob([new Uint8Array([1, 2, 3])], {type: 'image/png'});
    request.flush(blob);
    fixture.detectChanges();

    const image = root.querySelector<HTMLImageElement>(
      '.external-image-card__image',
    );
    expect(createObjectUrl).toHaveBeenCalledOnce();
    expect(image?.getAttribute('src')).toBe('blob:reviewed-image');
    expect(image?.getAttribute('src')).not.toContain('images.example');

    fixture.destroy();
    expect(revokeObjectUrl).toHaveBeenCalledWith('blob:reviewed-image');
  });

  it('refuses an SVG response instead of showing it from a Cockpit-origin blob', async () => {
    const fixture = TestBed.createComponent(ExternalImageHost);
    fixture.componentInstance.content = '![logo](https://images.example/logo.svg)';
    fixture.detectChanges();
    await fixture.whenStable();
    await new Promise((resolve) => setTimeout(resolve, 0));
    fixture.detectChanges();

    const root = fixture.nativeElement as HTMLElement;
    root
      .querySelector<HTMLButtonElement>('button[data-external-image-action="load"]')
      ?.click();
    const svg = '<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>';
    http
      .expectOne((candidate) => candidate.url.endsWith('/api/media/remote-image'))
      .flush(new Blob([svg], {type: 'image/svg+xml'}));
    fixture.detectChanges();

    expect(createObjectUrl).not.toHaveBeenCalled();
    expect(root.querySelector('img')).toBeNull();
    expect(
      root.querySelector<HTMLElement>('.external-image-card')?.dataset['externalImageState'],
    ).toBe('error');
    fixture.destroy();
  });

  it('shows a raster response under its bare type', async () => {
    const fixture = TestBed.createComponent(ExternalImageHost);
    fixture.componentInstance.content = '![photo](https://images.example/p.webp)';
    fixture.detectChanges();
    await fixture.whenStable();
    await new Promise((resolve) => setTimeout(resolve, 0));
    fixture.detectChanges();

    const root = fixture.nativeElement as HTMLElement;
    root
      .querySelector<HTMLButtonElement>('button[data-external-image-action="load"]')
      ?.click();
    http
      .expectOne((candidate) => candidate.url.endsWith('/api/media/remote-image'))
      .flush(new Blob([new Uint8Array([1, 2, 3])], {type: 'image/webp; q=1'}));
    fixture.detectChanges();

    expect(createObjectUrl).toHaveBeenCalledOnce();
    expect((createObjectUrl.mock.calls[0][0] as Blob).type).toBe('image/webp');
    expect(root.querySelector('.external-image-card__image')).not.toBeNull();
    fixture.destroy();
  });

  it('strips raw img HTML and disables unsupported non-HTTPS cards', async () => {
    const fixture = TestBed.createComponent(ExternalImageHost);
    fixture.componentInstance.content =
      '<img src="https://attacker.example/zero-click">' +
      '\n\n![unsafe](http://attacker.example/not-https.png)';
    fixture.detectChanges();
    await fixture.whenStable();
    await new Promise((resolve) => setTimeout(resolve, 0));
    fixture.detectChanges();

    const root = fixture.nativeElement as HTMLElement;
    expect(root.querySelector('img')).toBeNull();
    const load = root.querySelector<HTMLButtonElement>(
      'button[data-external-image-action="load"]',
    );
    expect(load, root.innerHTML).not.toBeNull();
    expect(load?.disabled).toBe(true);
    http.expectNone(() => true);

    fixture.destroy();
  });
});

describe('rasterImageType', () => {
  it('admits only raster image types, normalized to the bare type', () => {
    expect(rasterImageType('image/png')).toBe('image/png');
    expect(rasterImageType('IMAGE/JPEG; charset=binary')).toBe('image/jpeg');
    expect(rasterImageType('image/gif')).toBe('image/gif');
    expect(rasterImageType('image/webp')).toBe('image/webp');
    expect(rasterImageType('image/avif')).toBe('image/avif');
    expect(rasterImageType('image/svg+xml')).toBeNull();
    expect(rasterImageType('image/svg+xml; charset=utf-8')).toBeNull();
    expect(rasterImageType('text/html')).toBeNull();
    expect(rasterImageType('')).toBeNull();
  });
});

describe('reviewExternalImageUrl', () => {
  it('allows only exact HTTPS/443 URLs without credentials or fragments', () => {
    expect(reviewExternalImageUrl('https://images.example/a.png?q=1')).toEqual({
      loadable: true,
      host: 'images.example',
      hasParameters: true,
    });
    expect(reviewExternalImageUrl('http://images.example/a.png').loadable).toBe(false);
    expect(reviewExternalImageUrl('https://u:p@images.example/a.png').loadable).toBe(false);
    expect(reviewExternalImageUrl('https://images.example:8443/a.png').loadable).toBe(false);
    expect(reviewExternalImageUrl('https://images.example/a.png#x').loadable).toBe(false);
  });
});
