import {provideHttpClient} from '@angular/common/http';
import {HttpTestingController, provideHttpClientTesting} from '@angular/common/http/testing';
import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {Router} from '@angular/router';
import {TranslocoService} from '@jsverse/transloco';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {CanvasState} from '../../core/models/canvas.model';
import {CanvasService} from '../../core/services/canvas.service';
import {WorkspaceFileDownloadService} from '../../core/services/workspace-file-download.service';
import {CanvasContentController} from './canvas-content.controller';
import {CanvasBrowserController} from './canvas-browser.controller';
import {CanvasEditController} from './canvas-edit.controller';
import {CanvasOfficeController} from './canvas-office.controller';
import {
  CanvasPaneComponent,
  canvasResetTargetMatches,
  openCanvasPopOut,
} from './canvas-pane.component';
import {CanvasViewerController} from './canvas-viewer.controller';

function contentUrl(revision: number): string {
  return (
    '/api/persistent/threads/thread-1/canvases/main/content' +
    `?presentation_revision=${revision}&source_fingerprint=fp&source_version=sha256%3A${revision}` +
    '&ngsw-bypass=true'
  );
}

function canvasState(
  revision: number,
  renderer: CanvasState['renderer'] = 'markdown',
  overrides: Partial<CanvasState> = {},
): CanvasState {
  return {
    canvas_id: 'main',
    source: {type: 'workspace_file', path: 'output/report.md'},
    title: 'Report',
    renderer,
    editable: false,
    alt_text: renderer === 'image' ? 'A chart' : null,
    presentation_revision: revision,
    source_version: `sha256:${revision}`,
    content_url: contentUrl(revision),
    status: 'ready',
    capabilities: {can_edit: false, can_pop_out: false, can_take_control: false},
    updated_at: `2026-07-13T10:00:0${revision}Z`,
    ...overrides,
  };
}

describe('Canvas pane content lifecycle', () => {
  let component: CanvasContentController;
  let viewport: HTMLElement;
  let http: HttpTestingController;
  let canvas: {
    threadId: ReturnType<typeof signal<string | null>>;
    state: ReturnType<typeof signal<CanvasState | null>>;
    loadStatus: ReturnType<typeof signal<'idle' | 'loading' | 'ready' | 'error'>>;
    reconcile: ReturnType<typeof vi.fn>;
  };
  let originalCreateObjectUrl: PropertyDescriptor | undefined;
  let originalRevokeObjectUrl: PropertyDescriptor | undefined;
  let createObjectUrl: ReturnType<typeof vi.fn>;
  let revokeObjectUrl: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    canvas = {
      threadId: signal<string | null>('thread-1'),
      state: signal<CanvasState | null>(null),
      loadStatus: signal<'idle' | 'loading' | 'ready' | 'error'>('ready'),
      reconcile: vi.fn(),
    };
    originalCreateObjectUrl = Object.getOwnPropertyDescriptor(URL, 'createObjectURL');
    originalRevokeObjectUrl = Object.getOwnPropertyDescriptor(URL, 'revokeObjectURL');
    createObjectUrl = vi.fn().mockReturnValueOnce('blob:canvas-one').mockReturnValueOnce('blob:canvas-two');
    revokeObjectUrl = vi.fn();
    Object.defineProperty(URL, 'createObjectURL', {configurable: true, value: createObjectUrl});
    Object.defineProperty(URL, 'revokeObjectURL', {configurable: true, value: revokeObjectUrl});

    TestBed.configureTestingModule({
      providers: [
        CanvasContentController,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: CanvasService, useValue: canvas},
      ],
    });
    component = TestBed.inject(CanvasContentController);
    viewport = document.createElement('div');
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    http?.verify({ignoreCancelled: true});
    if (originalCreateObjectUrl) Object.defineProperty(URL, 'createObjectURL', originalCreateObjectUrl);
    else delete (URL as unknown as {createObjectURL?: unknown}).createObjectURL;
    if (originalRevokeObjectUrl) Object.defineProperty(URL, 'revokeObjectURL', originalRevokeObjectUrl);
    else delete (URL as unknown as {revokeObjectURL?: unknown}).revokeObjectURL;
    TestBed.resetTestingModule();
  });

  it('loads text through the protected URL and preserves it across source drift', () => {
    canvas.state.set(canvasState(1));
    sync();
    http.expectOne(request => request.url.includes('/canvases/main/content')).flush(
      '# Version one',
      {headers: {ETag: '"sha256:1"'}},
    );

    expect(component.textContent()).toBe('# Version one');
    expect(component.contentEtag()).toBe('"sha256:1"');
    expect(component.contentStatus()).toBe('ready');

    canvas.state.set(canvasState(2, 'markdown', {
      status: 'source_changed',
      content_url: null,
      source_version: 'sha256:drifted',
    }));
    sync();

    http.expectNone(request => request.url.includes('/canvases/main/content'));
    expect(component.textContent()).toBe('# Version one');
    expect(component.contentStatus()).toBe('source_changed');
  });

  it('cancels an old content load and clears bytes when the selected thread changes', () => {
    canvas.state.set(canvasState(1, 'text'));
    sync();
    const stale = http.expectOne(request => request.url.includes('/canvases/main/content'));

    canvas.threadId.set('thread-2');
    canvas.state.set(null);
    sync();

    expect(stale.cancelled).toBe(true);
    expect(component.textContent()).toBe('');
    expect(component.contentStatus()).toBe('idle');
  });

  it('cancels a raced content revision and applies only the newest response', () => {
    canvas.state.set(canvasState(1, 'text'));
    sync();
    const stale = http.expectOne(request => request.url.includes('presentation_revision=1'));

    canvas.state.set(canvasState(2, 'text'));
    sync();
    expect(stale.cancelled).toBe(true);
    http.expectOne(request => request.url.includes('presentation_revision=2')).flush('newest');

    expect(component.textContent()).toBe('newest');
    expect(component.displayState()?.presentation_revision).toBe(2);
  });

  it('rejects a non-server or incomplete content pointer without issuing a request', () => {
    canvas.state.set(canvasState(1, 'text', {content_url: 'https://evil.test/content'}));
    sync();

    http.expectNone(() => true);
    expect(component.contentStatus()).toBe('error');
    expect(component.contentErrorCode()).toBe('invalid_content_url');
    expect(component.displayRenderer()).toBe('unsupported');
    expect(component.imageUrl()).toBeNull();
  });

  it('never fetches Office bytes through the Canvas content route', () => {
    canvas.state.set(canvasState(1, 'office', {
      content_url: null,
      capabilities: {
        can_edit: false,
        can_pop_out: true,
        can_take_control: false,
        can_view_office: true,
      },
    }));
    sync();

    http.expectNone(request => request.url.includes('/canvases/main/content'));
    expect(component.textContent()).toBe('');
    expect(component.imageUrl()).toBeNull();
    expect(component.contentStatus()).toBe('idle');
  });

  it('preserves scroll while a same-source text presentation refreshes', async () => {
    canvas.state.set(canvasState(1, 'text'));
    sync();
    http.expectOne(request => request.url.includes('presentation_revision=1')).flush('version one');
    viewport.scrollTop = 137;

    canvas.state.set(canvasState(2, 'text'));
    sync();
    expect(component.textContent()).toBe('version one');
    http.expectOne(request => request.url.includes('presentation_revision=2')).flush('version two');
    await Promise.resolve();

    expect(component.textContent()).toBe('version two');
    expect(viewport.scrollTop).toBe(137);
  });

  it('publishes raster blobs only after success and preserves the prior image on 409', () => {
    canvas.state.set(canvasState(1, 'image'));
    sync();
    http.expectOne(request => request.url.includes('presentation_revision=1'))
      .flush(new Blob(['one'], {type: 'image/png'}));

    expect(component.imageUrl()).toBe('blob:canvas-one');
    expect(createObjectUrl).toHaveBeenCalledOnce();
    expect(revokeObjectUrl).not.toHaveBeenCalled();

    canvas.state.set(canvasState(2, 'image'));
    sync();
    http.expectOne(request => request.url.includes('presentation_revision=2')).flush(
      new Blob([JSON.stringify({detail: {code: 'canvas_source_changed'}})], {type: 'application/json'}),
      {status: 409, statusText: 'Conflict'},
    );

    expect(component.imageUrl()).toBe('blob:canvas-one');
    expect(component.contentStatus()).toBe('source_changed');
    expect(revokeObjectUrl).not.toHaveBeenCalled();
    expect(canvas.reconcile).toHaveBeenCalledOnce();
  });

  it('revokes raster object URLs on replacement and authorization loss', () => {
    canvas.state.set(canvasState(1, 'image'));
    sync();
    http.expectOne(request => request.url.includes('presentation_revision=1'))
      .flush(new Blob(['one'], {type: 'image/png'}));

    canvas.state.set(canvasState(2, 'image'));
    sync();
    http.expectOne(request => request.url.includes('presentation_revision=2'))
      .flush(new Blob(['two'], {type: 'image/png'}));
    expect(component.imageUrl()).toBe('blob:canvas-two');
    expect(revokeObjectUrl).toHaveBeenCalledWith('blob:canvas-one');

    canvas.state.set(canvasState(3, 'image'));
    sync();
    http.expectOne(request => request.url.includes('presentation_revision=3')).flush(
      new Blob([JSON.stringify({detail: {code: 'forbidden'}})], {type: 'application/json'}),
      {status: 403, statusText: 'Forbidden'},
    );
    expect(component.imageUrl()).toBeNull();
    expect(revokeObjectUrl).toHaveBeenCalledWith('blob:canvas-two');
  });

  function sync(): void {
    component.syncPresentation(
      true,
      canvas.threadId(),
      canvas.state(),
      () => viewport.scrollTop,
      value => {
        viewport.scrollTop = value;
      },
    );
  }
});

describe('Canvas pane trusted chrome', () => {
  it('opens only the Cockpit wrapper with opener and referrer isolation', () => {
    const openWindow = vi.fn();

    openCanvasPopOut('/sessions/thread-1/canvas', openWindow);

    expect(openWindow).toHaveBeenCalledWith(
      '/sessions/thread-1/canvas',
      '_blank',
      'noopener,noreferrer',
    );
  });

  it('binds origin-rotation confirmation to the exact app presentation', () => {
    const target = {
      stateEtag: '"canvas:7:app-a"',
      presentationRevision: 7,
      sourceKey: 'workspace_app:/demo',
    };

    expect(canvasResetTargetMatches(
      target,
      '"canvas:7:app-a"',
      7,
      'workspace_app:/demo',
    )).toBe(true);
    expect(canvasResetTargetMatches(
      target,
      '"canvas:8:republished"',
      8,
      'workspace_app:/demo',
    )).toBe(false);
    expect(canvasResetTargetMatches(
      target,
      '"canvas:8:app-b"',
      8,
      'workspace_app:/other',
    )).toBe(false);
  });

  it('syncs the pane-local browser controller for the selected browser renderer', () => {
    const browser = {
      syncPresentation: vi.fn(),
      connectionStatus: signal<'connecting'>('connecting'),
      errorCode: signal<string | null>(null),
    };
    const state = signal<CanvasState | null>(canvasState(4, 'auto', {
      source: {type: 'browser'},
      source_version: null,
      capabilities: {
        can_edit: false,
        can_pop_out: true,
        can_take_control: true,
        can_stream_browser: true,
      },
    }));
    const canvas = {
      threadId: signal<string | null>('thread-1'),
      state,
      stateEtag: signal<string | null>('"canvas:4:browser"'),
      loadStatus: signal<'idle' | 'loading' | 'ready' | 'error'>('ready'),
      browserCapability: signal({
        feature_enabled: true,
        can_open_browser: true,
        workspace_ready: true,
        reason: null,
      }),
      browserCapabilityStatus: signal<'ready'>('ready'),
      browserOpenStatus: signal<'idle' | 'workspace' | 'browser' | 'error'>('idle'),
      browserOpenError: signal<string | null>(null),
      lastSuccessfulSyncAt: signal<number | null>(null),
      reconcile: vi.fn(),
      openBrowser: vi.fn(),
      retryOpenBrowser: vi.fn(),
    };
    const content = {
      displayRenderer: signal('unsupported'),
      displayState: signal<CanvasState | null>(null),
      displaySourceKey: signal<string | null>(null),
      textContent: signal(''),
      contentEtag: signal<string | null>(null),
      imageUrl: signal<string | null>(null),
      contentStatus: signal('idle'),
      contentErrorCode: signal<string | null>(null),
      syncPresentation: vi.fn(),
    };
    const editor = {
      hasSession: signal(false),
      dirty: signal(false),
      conflict: signal(null),
      sessionState: signal<CanvasState | null>(null),
      sessionRenderer: signal('unsupported'),
      buffer: signal(''),
      editMode: signal(false),
      sync: vi.fn(),
    };
    const viewer = {syncPresentation: vi.fn()};
    const office = {
      syncPresentation: vi.fn(),
      session: signal(null),
      officeOrigin: signal<string | null>(null),
      officeStatus: signal<'idle'>('idle'),
      officeErrorCode: signal<string | null>(null),
      modified: signal(false),
      conflictCode: signal<string | null>(null),
      refreshToken: vi.fn(),
      reloadSession: vi.fn(),
      markDocumentLoaded: vi.fn(),
      markModified: vi.fn(),
      markConflict: vi.fn(),
    };

    TestBed.configureTestingModule({
      providers: [
        {provide: CanvasService, useValue: canvas},
        {provide: CanvasContentController, useValue: content},
        {provide: CanvasEditController, useValue: editor},
        {provide: CanvasViewerController, useValue: viewer},
        {provide: CanvasOfficeController, useValue: office},
        {provide: CanvasBrowserController, useValue: browser},
        {provide: WorkspaceFileDownloadService, useValue: {download: vi.fn()}},
        {provide: TranslocoService, useValue: {translate: (key: string) => key}},
        {provide: Router, useValue: {}},
      ],
    });
    try {
      const pane = TestBed.runInInjectionContext(() => new CanvasPaneComponent());
      TestBed.flushEffects();

      expect(pane.effectiveRenderer()).toBe('browser');
      expect(pane.hasVisual()).toBe(true);
      expect(pane.sourceSummary()).toBe('canvas.browser.source');
      expect(pane.sourceKindLabel()).toBe('canvas.sourceKind.browser');
      expect(pane.statusText()).toBe('canvas.browser.status.connecting');
      expect(browser.syncPresentation).toHaveBeenCalledWith(true, 'thread-1', state());
      expect(viewer.syncPresentation).toHaveBeenCalledWith(
        false,
        'thread-1',
        state(),
        '"canvas:4:browser"',
      );
      expect(office.syncPresentation).toHaveBeenCalledWith(
        false,
        'thread-1',
        state(),
        '"canvas:4:browser"',
      );
      expect(content.syncPresentation).toHaveBeenCalledWith(
        false,
        'thread-1',
        state(),
        expect.any(Function),
        expect.any(Function),
      );
      expect(editor.sync).toHaveBeenCalledWith(
        false,
        'thread-1',
        state(),
        null,
        '',
        null,
        false,
        false,
        false,
      );

      state.set(null);
      canvas.stateEtag.set(null);
      canvas.browserOpenStatus.set('workspace');
      TestBed.flushEffects();
      expect(pane.browserEmptyState()).toBe(true);
      expect(pane.browserOpenPending()).toBe(true);
      expect(pane.browserEmptyTextKey()).toBe('canvas.browser.open.phase.workspace');
      expect(pane.statusText()).toBe('canvas.browser.open.phase.workspace');

      canvas.browserOpenStatus.set('browser');
      expect(pane.browserEmptyTextKey()).toBe('canvas.browser.open.phase.browser');
      canvas.browserOpenStatus.set('error');
      canvas.browserOpenError.set('browser_open_timeout');
      expect(pane.browserEmptyTextKey()).toBe('canvas.browser.open.error.timeout');

      canvas.browserOpenStatus.set('idle');
      editor.dirty.set(true);
      expect(pane.browserOpenDisabled()).toBe(true);
      expect(pane.browserEmptyTextKey()).toBe('canvas.browser.open.dirty');
    } finally {
      TestBed.resetTestingModule();
    }
  });
});

describe('Canvas pane durable presentation', () => {
  const downloads = {download: vi.fn().mockResolvedValue(true)};

  function paneWith(
    overrides: Partial<CanvasState>,
    renderer: CanvasState['renderer'] = 'markdown',
  ) {
    const state = signal<CanvasState | null>(canvasState(4, renderer, overrides));
    const canvas = {
      threadId: signal<string | null>('thread-1'),
      state,
      stateEtag: signal<string | null>('"canvas:4:snap"'),
      loadStatus: signal<'idle' | 'loading' | 'ready' | 'error'>('ready'),
      browserCapability: signal(null),
      browserCapabilityStatus: signal<'idle'>('idle'),
      browserOpenStatus: signal<'idle'>('idle'),
      browserOpenError: signal<string | null>(null),
      lastSuccessfulSyncAt: signal<number | null>(null),
      reconcile: vi.fn(),
      openBrowser: vi.fn(),
      retryOpenBrowser: vi.fn(),
    };
    const content = {
      // The stage has not mounted bytes, which is exactly the situation that
      // used to mislabel the source as unsupported.
      displayRenderer: signal('unsupported'),
      displayState: signal<CanvasState | null>(null),
      displaySourceKey: signal<string | null>(null),
      textContent: signal(''),
      contentEtag: signal<string | null>(null),
      imageUrl: signal<string | null>(null),
      contentStatus: signal('idle'),
      contentErrorCode: signal<string | null>(null),
      syncPresentation: vi.fn(),
    };
    const editor = {
      hasSession: signal(false),
      dirty: signal(false),
      conflict: signal(null),
      sessionState: signal<CanvasState | null>(null),
      sessionRenderer: signal('unsupported'),
      buffer: signal(''),
      editMode: signal(false),
      sync: vi.fn(),
    };
    TestBed.configureTestingModule({
      providers: [
        {provide: CanvasService, useValue: canvas},
        {provide: CanvasContentController, useValue: content},
        {provide: CanvasEditController, useValue: editor},
        {provide: CanvasViewerController, useValue: {syncPresentation: vi.fn()}},
        {provide: CanvasOfficeController, useValue: {
          syncPresentation: vi.fn(),
          session: signal(null),
          officeOrigin: signal<string | null>(null),
          officeStatus: signal<'idle'>('idle'),
          officeErrorCode: signal<string | null>(null),
          modified: signal(false),
          conflictCode: signal<string | null>(null),
          refreshToken: vi.fn(),
          reloadSession: vi.fn(),
          markDocumentLoaded: vi.fn(),
          markModified: vi.fn(),
          markConflict: vi.fn(),
        }},
        {provide: CanvasBrowserController, useValue: {
          syncPresentation: vi.fn(),
          connectionStatus: signal<'idle'>('idle'),
          errorCode: signal<string | null>(null),
        }},
        {provide: WorkspaceFileDownloadService, useValue: downloads},
        {provide: TranslocoService, useValue: {translate: (key: string) => key}},
        {provide: Router, useValue: {}},
      ],
    });
    return TestBed.runInInjectionContext(() => new CanvasPaneComponent());
  }

  afterEach(() => {
    downloads.download.mockClear();
    TestBed.resetTestingModule();
  });

  it('downloads the presented workspace file', async () => {
    const pane = paneWith({});
    TestBed.flushEffects();

    expect(pane.downloadPath()).toBe('output/report.md');
    await pane.downloadFile();

    expect(downloads.download).toHaveBeenCalledWith('thread-1', 'output/report.md');
    expect(pane.downloading()).toBe(false);
  });

  it('offers no download for a source that is not a workspace file', async () => {
    const pane = paneWith({source: {type: 'browser'}, source_version: null});
    TestBed.flushEffects();

    expect(pane.downloadPath()).toBeNull();
    await pane.downloadFile();
    expect(downloads.download).not.toHaveBeenCalled();
  });

  it('says a static HTML preview hides scripts and styles', () => {
    expect(paneWith({}, 'html').staticHtmlPreview()).toBe(true);
    TestBed.resetTestingModule();
    expect(paneWith({}, 'html-interactive').staticHtmlPreview()).toBe(false);
  });

  it('labels a snapshot-backed file by its real renderer, never "unsupported"', () => {
    const pane = paneWith({
      content_origin: 'snapshot',
      content_captured_at: '2026-07-27T09:30:00Z',
    });
    TestBed.flushEffects();

    expect(pane.snapshotBacked()).toBe(true);
    expect(pane.snapshotCapturedAt()).toBe('2026-07-27T09:30:00Z');
    // The reported regression: chips read "File" + "Unsupported source" over a
    // valid path whenever the workspace was asleep.
    expect(pane.rendererLabel()).toBe('canvas.renderer.markdown');
    expect(pane.rendererLabel()).not.toBe('canvas.renderer.unsupported');
    expect(pane.sourceKindLabel()).toBe('canvas.sourceKind.file');
  });

  it('treats an absent content_origin as workspace-backed', () => {
    const pane = paneWith({});
    TestBed.flushEffects();

    // Older orchestrators omit the field entirely; absence must never be read
    // as unknown-and-blocked.
    expect(pane.snapshotBacked()).toBe(false);
    expect(pane.snapshotCapturedAt()).toBeNull();
    expect(pane.rendererLabel()).toBe('canvas.renderer.markdown');
  });

  it('still labels a genuinely unknown renderer as unsupported', () => {
    const pane = paneWith({renderer: 'mystery' as CanvasState['renderer']});
    TestBed.flushEffects();

    expect(pane.rendererLabel()).toBe('canvas.renderer.unsupported');
  });
});
