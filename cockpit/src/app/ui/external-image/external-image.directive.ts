import {
  AfterViewInit,
  Directive,
  ElementRef,
  OnDestroy,
  inject,
} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {TranslocoService} from '@jsverse/transloco';
import {MarkdownComponent} from 'ngx-markdown';
import {Subscription} from 'rxjs';
import {environment} from '../../core/environment';
import {copyText} from '../copy-field';

const MAX_REVIEW_URL_CHARS = 8_192;
const COPY_CONFIRMATION_MS = 2_500;

type CardState = 'idle' | 'loading' | 'loaded' | 'error' | 'unsupported';

/** Raster types a fetched image may be shown as. */
const RASTER_IMAGE_TYPES: ReadonlySet<string> = new Set([
  'image/png',
  'image/jpeg',
  'image/gif',
  'image/webp',
  'image/avif',
]);

/**
 * The type a fetched image is shown as, or null when it must not be shown.
 *
 * A `blob:` URL inherits the Cockpit origin, so an `image/svg+xml` response
 * opened through "Open image in new tab" would run its script with the
 * reader's session. Only raster types are shown, re-wrapped under their bare
 * type; every other `image/*` type, SVG included, is refused. Same rule as the
 * citations panel's `snapshotViewType`.
 */
export function rasterImageType(type: string): string | null {
  const mime = type.split(';')[0].trim().toLowerCase();
  return RASTER_IMAGE_TYPES.has(mime) ? mime : null;
}

export interface ExternalImageReview {
  loadable: boolean;
  host: string;
  hasParameters: boolean;
}

/**
 * Perform the browser-side eligibility check used to enable the load action.
 * The orchestrator independently repeats and extends this validation.
 */
export function reviewExternalImageUrl(url: string): ExternalImageReview {
  if (
    !url ||
    url.length > MAX_REVIEW_URL_CHARS ||
    /[\u0000-\u0020\u007f]/.test(url)
  ) {
    return {loadable: false, host: '', hasParameters: false};
  }
  try {
    const parsed = new URL(url);
    const loadable =
      parsed.protocol === 'https:' &&
      !parsed.username &&
      !parsed.password &&
      !parsed.hash &&
      (!parsed.port || parsed.port === '443');
    return {
      loadable,
      host: parsed.host,
      hasParameters: parsed.search.length > 0,
    };
  } catch {
    return {loadable: false, host: '', hasParameters: false};
  }
}

/**
 * Enhances inert image placeholders emitted by `externalImageExtension`.
 *
 * The selector intentionally matches every `<markdown>` in a component that
 * imports this directive. Forgetting the directive remains fail-closed: the
 * parser leaves an inert URL-only fallback and the document CSP blocks remote
 * images.
 */
@Directive({
  selector: 'markdown',
  standalone: true,
})
export class ExternalImageDirective implements AfterViewInit, OnDestroy {
  private readonly host = inject<ElementRef<HTMLElement>>(ElementRef);
  private readonly markdown = inject(MarkdownComponent, {self: true});
  private readonly http = inject(HttpClient);
  private readonly transloco = inject(TranslocoService);

  private readonly objectUrls = new Map<HTMLElement, string>();
  private readonly requests = new Set<Subscription>();
  private readonly copyTimers = new Set<ReturnType<typeof setTimeout>>();
  private markdownSub?: Subscription;
  private languageSub?: Subscription;

  ngAfterViewInit(): void {
    this.renderCards();
    this.markdownSub = this.markdown.ready.subscribe(() => this.renderCards());
    this.languageSub = this.transloco.langChanges$.subscribe(() => this.localizeCards());
    this.host.nativeElement.addEventListener('click', this.onClick);
  }

  ngOnDestroy(): void {
    this.markdownSub?.unsubscribe();
    this.languageSub?.unsubscribe();
    this.host.nativeElement.removeEventListener('click', this.onClick);
    this.cleanupRenderedState();
  }

  private readonly onClick = (event: Event): void => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    const button = target.closest<HTMLButtonElement>(
      'button[data-external-image-action]',
    );
    const card = button?.closest<HTMLElement>('.external-image-card');
    const action = button?.dataset['externalImageAction'];
    if (!button || !card || !action) return;

    event.preventDefault();
    if (action === 'copy') {
      void this.copyUrl(card);
    } else if (action === 'load') {
      this.loadImage(card);
    }
  };

  private renderCards(): void {
    this.cleanupRenderedState();
    const placeholders = this.host.nativeElement.querySelectorAll<HTMLElement>(
      '.external-image-placeholder[data-external-image-url]',
    );
    placeholders.forEach((card) => this.buildCard(card));
  }

  private buildCard(card: HTMLElement): void {
    const url = card.dataset['externalImageUrl'] ?? '';
    const review = reviewExternalImageUrl(url);
    const doc = card.ownerDocument;

    card.classList.add('external-image-card');
    card.dataset['externalImageState'] = review.loadable ? 'idle' : 'unsupported';
    card.dataset['externalImageHost'] = review.host;
    card.dataset['externalImageHasParameters'] = String(review.hasParameters);
    card.setAttribute('role', 'group');

    const header = this.element(doc, 'span', 'external-image-card__header');
    header.append(
      this.element(doc, 'strong', 'external-image-card__title', '', 'title'),
      this.element(doc, 'span', 'external-image-card__host', '', 'host'),
    );

    const urlValue = this.element(doc, 'code', 'external-image-card__url');
    urlValue.textContent = url;
    urlValue.setAttribute('title', url);

    const explanation = this.element(
      doc,
      'span',
      'external-image-card__explanation',
      '',
      'explanation',
    );
    const parameterWarning = this.element(
      doc,
      'span',
      'external-image-card__warning',
      '',
      'parameterWarning',
    );
    parameterWarning.hidden = !review.hasParameters;
    const unsupported = this.element(
      doc,
      'span',
      'external-image-card__warning',
      '',
      'unsupported',
    );
    unsupported.hidden = review.loadable;

    const preview = this.element(doc, 'span', 'external-image-card__preview');
    preview.hidden = true;

    const actions = this.element(doc, 'span', 'external-image-card__actions');
    const copy = this.button(doc, 'copy', 'external-image-card__button');
    const load = this.button(
      doc,
      'load',
      'external-image-card__button external-image-card__button--primary',
    );
    load.disabled = !review.loadable;
    actions.append(copy, load);

    const status = this.element(
      doc,
      'span',
      'external-image-card__status',
      '',
      'status',
    );
    status.setAttribute('aria-live', 'polite');

    card.replaceChildren(
      header,
      urlValue,
      explanation,
      parameterWarning,
      unsupported,
      preview,
      actions,
      status,
    );
    this.localizeCard(card);
  }

  private button(
    doc: Document,
    action: 'copy' | 'load',
    className: string,
  ): HTMLButtonElement {
    const button = doc.createElement('button');
    button.type = 'button';
    button.className = className;
    button.dataset['externalImageAction'] = action;
    return button;
  }

  private element<K extends keyof HTMLElementTagNameMap>(
    doc: Document,
    tag: K,
    className: string,
    text = '',
    slot?: string,
  ): HTMLElementTagNameMap[K] {
    const element = doc.createElement(tag);
    element.className = className;
    element.textContent = text;
    if (slot) element.dataset['externalImageSlot'] = slot;
    return element;
  }

  private async copyUrl(card: HTMLElement): Promise<void> {
    const url = card.dataset['externalImageUrl'] ?? '';
    if (!(await copyText(url))) return;
    card.dataset['externalImageCopied'] = 'true';
    this.localizeCard(card);
    const timer = setTimeout(() => {
      this.copyTimers.delete(timer);
      if (card.isConnected) {
        delete card.dataset['externalImageCopied'];
        this.localizeCard(card);
      }
    }, COPY_CONFIRMATION_MS);
    this.copyTimers.add(timer);
  }

  private loadImage(card: HTMLElement): void {
    const state = card.dataset['externalImageState'] as CardState | undefined;
    const url = card.dataset['externalImageUrl'] ?? '';
    const review = reviewExternalImageUrl(url);
    if (state === 'loading' || state === 'loaded' || !review.loadable) return;

    card.dataset['externalImageState'] = 'loading';
    this.localizeCard(card);

    let request: Subscription;
    request = this.http.post(
      `${environment.apiUrl}/media/remote-image`,
      {url},
      {responseType: 'blob'},
    ).subscribe({
      next: (blob) => {
        const rasterType = rasterImageType(blob.type);
        if (!card.isConnected || !rasterType || blob.size === 0) {
          if (card.isConnected) this.setError(card);
          return;
        }
        try {
          const objectUrl = URL.createObjectURL(new Blob([blob], {type: rasterType}));
          const preview = card.querySelector<HTMLElement>(
            '[data-external-image-slot="preview"], .external-image-card__preview',
          );
          if (!preview) {
            URL.revokeObjectURL(objectUrl);
            this.setError(card);
            return;
          }
          const image = card.ownerDocument.createElement('img');
          image.className = 'external-image-card__image';
          image.src = objectUrl;
          image.alt =
            card.dataset['externalImageAlt'] ||
            this.t('externalImage.missingAlt');
          image.loading = 'lazy';
          image.decoding = 'async';
          image.referrerPolicy = 'no-referrer';
          image.addEventListener('error', () => {
            URL.revokeObjectURL(objectUrl);
            this.objectUrls.delete(card);
            this.setError(card);
          }, {once: true});
          this.objectUrls.set(card, objectUrl);
          preview.replaceChildren(image);
          preview.hidden = false;
          card.dataset['externalImageState'] = 'loaded';
          this.localizeCard(card);
        } catch {
          this.setError(card);
        }
      },
      error: () => this.setError(card),
    });
    this.requests.add(request);
    request.add(() => this.requests.delete(request));
  }

  private setError(card: HTMLElement): void {
    if (!card.isConnected) return;
    card.dataset['externalImageState'] = 'error';
    this.localizeCard(card);
  }

  private localizeCards(): void {
    this.host.nativeElement
      .querySelectorAll<HTMLElement>('.external-image-card')
      .forEach((card) => this.localizeCard(card));
  }

  private localizeCard(card: HTMLElement): void {
    const host = card.dataset['externalImageHost'] || this.t('externalImage.unknownHost');
    const state = (card.dataset['externalImageState'] || 'idle') as CardState;
    this.slot(card, 'title', this.t('externalImage.title'));
    this.slot(card, 'host', this.t('externalImage.host', {host}));
    this.slot(
      card,
      'explanation',
      this.t('externalImage.explanation', {host}),
    );
    this.slot(card, 'parameterWarning', this.t('externalImage.parameterWarning'));
    this.slot(card, 'unsupported', this.t('externalImage.unsupported'));

    const copy = card.querySelector<HTMLButtonElement>(
      'button[data-external-image-action="copy"]',
    );
    if (copy) {
      copy.textContent = this.t(
        card.dataset['externalImageCopied'] === 'true'
          ? 'externalImage.copied'
          : 'externalImage.copyUrl',
      );
    }
    const load = card.querySelector<HTMLButtonElement>(
      'button[data-external-image-action="load"]',
    );
    if (load) {
      load.textContent = this.t(
        state === 'unsupported' ? 'externalImage.blocked' : `externalImage.${state}`,
      );
      load.disabled = state !== 'idle' && state !== 'error';
    }
    this.slot(
      card,
      'status',
      state === 'error' ? this.t('externalImage.loadFailed') : '',
    );
  }

  private slot(card: HTMLElement, name: string, value: string): void {
    const target = card.querySelector<HTMLElement>(
      `[data-external-image-slot="${name}"]`,
    );
    if (target) target.textContent = value;
  }

  private t(key: string, params?: Record<string, string>): string {
    return String(this.transloco.translate(key, params));
  }

  private cleanupRenderedState(): void {
    for (const request of this.requests) request.unsubscribe();
    this.requests.clear();
    for (const timer of this.copyTimers) clearTimeout(timer);
    this.copyTimers.clear();
    for (const objectUrl of this.objectUrls.values()) URL.revokeObjectURL(objectUrl);
    this.objectUrls.clear();
  }
}
