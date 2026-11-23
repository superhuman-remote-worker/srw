import {
  ChangeDetectionStrategy,
  Component,
  effect,
  ElementRef,
  inject,
  input,
  OnDestroy,
  output,
  signal,
  untracked,
} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {firstValueFrom} from 'rxjs';

import {ApiService} from '../../core/services/api.service';
import {workspaceFileViewType} from '../../core/services/workspace-file-download.service';

/** Raster types a thumbnail may show: never SVG (script) or HEIC (browsers). */
const THUMB_TYPES = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp']);

/** Whether a workspace path can get a thumbnail. */
export function canThumbnail(path: string | undefined | null): boolean {
  if (!path) return false;
  const type = workspaceFileViewType(path);
  return !!type && THUMB_TYPES.has(type);
}

/** At most this many thumbnails load at once across the page, so a long
 *  transcript cannot use up the browser's connections to the API. */
const MAX_PARALLEL = 3;
let running = 0;
const waiting: Array<() => void> = [];

async function withSlot<T>(work: () => Promise<T>): Promise<T> {
  if (running >= MAX_PARALLEL) {
    await new Promise<void>((resolve) => waiting.push(resolve));
  }
  running++;
  try {
    return await work();
  } finally {
    running--;
    waiting.shift()?.();
  }
}

/**
 * A small preview of an image the user sent (image_generation.md WP5). It
 * loads only once it scrolls into view, a few at a time, from the Session's
 * workspace; a click opens the Cockpit's image dialog. Any failure (the file
 * is gone, the workspace is asleep or too large) leaves the plain chip.
 */
@Component({
  selector: 'app-attachment-thumb',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoPipe],
  template: `
    @if (url(); as src) {
      <button
        type="button"
        class="thumb"
        [attr.aria-label]="'chat.attachment.openPreview' | transloco: {name: name()}"
        [title]="'chat.attachment.openPreview' | transloco: {name: name()}"
        (click)="opened.emit({url: src, name: name()})"
      >
        <img [src]="src" alt="" loading="lazy" decoding="async" />
      </button>
    }
  `,
  styles: [`
    :host {
      display: contents;
    }
    .thumb {
      display: block;
      padding: 0;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-sm, 6px);
      background: var(--panel-bg);
      cursor: zoom-in;
      overflow: hidden;
      line-height: 0;
    }
    .thumb img {
      display: block;
      max-height: 72px;
      max-width: 120px;
      object-fit: cover;
    }
  `],
})
export class AttachmentThumbComponent implements OnDestroy {
  readonly threadId = input<string>('');
  readonly path = input<string>('');
  readonly name = input<string>('');
  readonly opened = output<{url: string; name: string}>();

  readonly url = signal<string | null>(null);

  private readonly api = inject(ApiService);
  private readonly el = inject<ElementRef<HTMLElement>>(ElementRef);
  private observer: IntersectionObserver | null = null;
  private destroyed = false;
  private started = false;

  constructor() {
    // Start once both inputs are there (they can arrive after creation).
    effect(() => {
      const threadId = this.threadId();
      const path = this.path();
      untracked(() => this.start(threadId, path));
    });
  }

  private start(threadId: string, path: string): void {
    if (this.started || !threadId || !canThumbnail(path)) return;
    this.started = true;
    if (typeof IntersectionObserver === 'undefined') {
      void this.load();
      return;
    }
    this.observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          this.observer?.disconnect();
          this.observer = null;
          void this.load();
        }
      },
      {rootMargin: '200px'},
    );
    // The host is display: contents, so watch its parent chip.
    const target = this.el.nativeElement.parentElement ?? this.el.nativeElement;
    this.observer.observe(target);
  }

  ngOnDestroy(): void {
    this.destroyed = true;
    this.observer?.disconnect();
    this.observer = null;
    const url = this.url();
    if (url) URL.revokeObjectURL(url);
  }

  private async load(): Promise<void> {
    try {
      const blob = await withSlot(() =>
        firstValueFrom(this.api.downloadThreadFile(this.threadId(), this.path())),
      );
      if (this.destroyed || !blob || blob.size === 0) return;
      const type = workspaceFileViewType(this.path()) ?? 'image/png';
      this.url.set(URL.createObjectURL(new Blob([blob], {type})));
    } catch {
      // 404 / 409 / 413 / 503 and friends: the chip stays as it is.
    }
  }
}
