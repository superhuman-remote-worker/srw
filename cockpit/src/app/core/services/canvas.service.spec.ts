import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {provideHttpClient} from '@angular/common/http';
import {
  HttpTestingController,
  provideHttpClientTesting,
} from '@angular/common/http/testing';
import {TestBed} from '@angular/core/testing';
import {BrowserCapability, CanvasState} from '../models/canvas.model';
import {
  BROWSER_CAPABILITY_RECHECK_MS,
  BROWSER_OPEN_TIMEOUT_MS,
  CanvasService,
  browserOpenRetryDelayMs,
  isBrowserCapability,
  isCanvasState,
  parseCanvasBroadcastInvalidation,
} from './canvas.service';
import {PersistentThreadTransportBridge} from './persistent-thread-transport-bridge.service';

function canvasState(
  revision: number,
  overrides: Partial<CanvasState> = {},
): CanvasState {
  return {
    canvas_id: 'main',
    source: {type: 'workspace_file', path: 'output/report.md'},
    title: 'Research report',
    renderer: 'markdown',
    editable: false,
    alt_text: null,
    presentation_revision: revision,
    source_version: 'sha256:abc',
    status: 'unavailable',
    capabilities: {
      can_edit: false,
      can_pop_out: false,
      can_take_control: false,
    },
    updated_at: `2026-07-13T10:00:0${revision}Z`,
    ...overrides,
  };
}

function browserCapability(
  overrides: Partial<BrowserCapability> = {},
): BrowserCapability {
  return {
    feature_enabled: true,
    can_open_browser: true,
    workspace_ready: true,
    reason: null,
    ...overrides,
  };
}

describe('Canvas state validation', () => {
  it('accepts the office renderer and only a boolean optional office capability', () => {
    const office = canvasState(1, {
      renderer: 'office',
      capabilities: {
        can_edit: false,
        can_pop_out: true,
        can_take_control: false,
        can_view_office: true,
      },
    });

    expect(isCanvasState(office)).toBe(true);
    expect(isCanvasState({
      ...office,
      capabilities: {...office.capabilities, can_view_office: false},
    })).toBe(true);
    expect(isCanvasState({
      ...office,
      capabilities: {...office.capabilities, can_view_office: 'true'},
    })).toBe(false);
  });

  it('accepts only a boolean optional browser-stream capability', () => {
    const base = canvasState(1);
    expect(isCanvasState(base)).toBe(true);
    expect(isCanvasState({
      ...base,
      capabilities: {...base.capabilities, can_stream_browser: true},
    })).toBe(true);
    expect(isCanvasState({
      ...base,
      capabilities: {...base.capabilities, can_stream_browser: false},
    })).toBe(true);
    expect(isCanvasState({
      ...base,
      capabilities: {...base.capabilities, can_stream_browser: 'true'},
    })).toBe(false);
    expect(isCanvasState({
      ...base,
      capabilities: {...base.capabilities, future_capability: {version: 2}},
    })).toBe(true);
  });

  it('accepts only the exact four-field browser capability contract', () => {
    expect(isBrowserCapability(browserCapability())).toBe(true);
    expect(isBrowserCapability({...browserCapability(), private_host: 'workspace'})).toBe(false);
    expect(isBrowserCapability({...browserCapability(), can_open_browser: 'true'})).toBe(false);
    expect(isBrowserCapability({...browserCapability(), reason: 'future_reason'})).toBe(false);
    expect(browserOpenRetryDelayMs(null)).toBe(1_000);
    expect(browserOpenRetryDelayMs('2')).toBe(2_000);
    expect(browserOpenRetryDelayMs('999')).toBe(10_000);
  });
});

describe('CanvasService', () => {
  let service: CanvasService;
  let transport: PersistentThreadTransportBridge;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        CanvasService,
        PersistentThreadTransportBridge,
        provideHttpClient(),
        provideHttpClientTesting(),
      ],
    });
    service = TestBed.inject(CanvasService);
    transport = TestBed.inject(PersistentThreadTransportBridge);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    for (const request of http.match(req => req.url.endsWith('/browser/capability'))) {
      if (!request.cancelled) {
        request.flush(browserCapability({
          feature_enabled: false,
          can_open_browser: false,
          workspace_ready: false,
          reason: 'feature_disabled',
        }));
      }
    }
    http.verify({ignoreCancelled: true});
    vi.useRealTimers();
  });

  it('loads the authoritative state and retains its representation ETag', () => {
    service.selectThread('thread-1');

    const request = http.expectOne((req) =>
      req.url.endsWith('/persistent/threads/thread-1/canvases/main'),
    );
    expect(request.request.method).toBe('GET');
    request.flush(canvasState(2), {
      headers: {ETag: '"canvas:2:representation"'},
    });

    expect(service.state()?.presentation_revision).toBe(2);
    expect(service.stateEtag()).toBe('"canvas:2:representation"');
    expect(service.loadStatus()).toBe('ready');
    expect(service.requestError()).toBeNull();
  });

  it('distinguishes a never-created 204 from a revisioned cleared state', () => {
    service.selectThread('never-created');
    http.expectOne((req) => req.url.includes('/never-created/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    expect(service.state()).toBeNull();
    expect(service.stateEtag()).toBeNull();
    expect(service.loadStatus()).toBe('ready');

    service.selectThread('cleared');
    http.expectOne((req) => req.url.includes('/cleared/canvases/main')).flush(
      canvasState(7, {
        source: null,
        title: null,
        renderer: 'auto',
        source_version: null,
        status: 'cleared',
      }),
      {headers: {ETag: '"canvas:7:cleared"'}},
    );
    expect(service.state()?.status).toBe('cleared');
    expect(service.state()?.presentation_revision).toBe(7);
  });

  it('reconciles a newer invalidation and ignores duplicate or older revisions', () => {
    service.selectThread('thread-1');
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(2));

    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 3},
    });
    const refresh = http.expectOne((req) => req.url.includes('/thread-1/canvases/main'));

    // The same journal frame can be replayed while the authoritative GET is in
    // flight. It must not cause a third request after revision 3 is applied.
    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 3},
    });
    refresh.flush(canvasState(3));
    http.expectNone((req) => req.url.includes('/thread-1/canvases/main'));

    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 2},
    });
    http.expectNone((req) => req.url.includes('/thread-1/canvases/main'));
    expect(service.state()?.presentation_revision).toBe(3);
  });

  it('converges directly to the latest REST revision after missed events', () => {
    service.selectThread('thread-1');
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(1));

    // Revisions 2-5 were missed; a later invalidation is only a reload hint.
    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 6},
    });
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(8));

    expect(service.state()?.presentation_revision).toBe(8);
  });

  it('queues a newer invalidation that arrives during an in-flight reconciliation', () => {
    service.selectThread('thread-1');
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(1));

    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 2},
    });
    const firstRefresh = http.expectOne((req) => req.url.includes('/thread-1/canvases/main'));
    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 4},
    });
    firstRefresh.flush(canvasState(2));

    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(4));
    expect(service.state()?.presentation_revision).toBe(4);
  });

  it('bounds a follow-up when REST initially returns below the advertised revision', () => {
    service.selectThread('thread-1');
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(1));

    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 5},
    });
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(3));
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(5));

    expect(service.state()?.presentation_revision).toBe(5);
  });

  it('follows an in-flight GET after a direct reconcile-required frame', () => {
    service.selectThread('thread-1');
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(1));

    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 2},
    });
    const firstRefresh = http.expectOne((req) => req.url.includes('/thread-1/canvases/main'));
    transport.forwardEvent('thread-1', {method: 'canvas.reconcile_required'});
    firstRefresh.flush(canvasState(2));

    const recoveryRefresh = http.expectOne((req) =>
      req.url.includes('/thread-1/canvases/main'),
    );
    recoveryRefresh.flush(canvasState(2));
    expect(service.state()?.presentation_revision).toBe(2);
  });

  it('cancels a stale thread request and clears prior state for a root draft', () => {
    service.selectThread('thread-a');
    const staleRequest = http.expectOne((req) => req.url.includes('/thread-a/canvases/main'));

    service.selectThread('thread-b');
    expect(staleRequest.cancelled).toBe(true);
    expect(service.state()).toBeNull();
    http.expectOne((req) => req.url.includes('/thread-b/canvases/main')).flush(canvasState(4));
    expect(service.state()?.presentation_revision).toBe(4);

    service.selectThread(null);
    expect(service.threadId()).toBeNull();
    expect(service.state()).toBeNull();
    expect(service.stateEtag()).toBeNull();
    expect(service.loadStatus()).toBe('idle');
  });

  it('ignores invalidations belonging to another thread', () => {
    service.selectThread('thread-1');
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(1));

    transport.forwardEvent('thread-2', {
      method: 'canvas.cleared',
      params: {canvas_id: 'main', presentation_revision: 2},
    });

    http.expectNone((req) => req.url.includes('/canvases/main'));
    expect(service.state()?.presentation_revision).toBe(1);
  });

  it('preserves the last authoritative state when a reconciliation fails', () => {
    service.selectThread('thread-1');
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(
      canvasState(1),
      {headers: {ETag: '"canvas:1:stable"'}},
    );

    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 2},
    });
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(
      {detail: {code: 'canvas_temporarily_unavailable'}},
      {status: 503, statusText: 'Service Unavailable'},
    );

    expect(service.state()?.presentation_revision).toBe(1);
    expect(service.stateEtag()).toBe('"canvas:1:stable"');
    expect(service.loadStatus()).toBe('error');
    expect(service.requestError()).toEqual({
      status: 503,
      code: 'canvas_temporarily_unavailable',
    });
  });

  it.each([401, 403, 404])(
    'clears state and ETag immediately on terminal HTTP %s',
    (status) => {
      service.selectThread('thread-1');
      http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(
        canvasState(1),
        {headers: {ETag: '"canvas:1:authorized"'}},
      );
      expect(service.state()).not.toBeNull();

      transport.forwardEvent('thread-1', {
        method: 'canvas.updated',
        params: {canvas_id: 'main', presentation_revision: 2},
      });
      http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(
        {detail: {code: 'canvas_access_ended'}},
        {status, statusText: 'Terminal Canvas state failure'},
      );

      expect(service.state()).toBeNull();
      expect(service.stateEtag()).toBeNull();
      expect(service.loadStatus()).toBe('error');
      expect(service.requestError()).toEqual({status, code: 'canvas_access_ended'});
    },
  );

  it('reconciles on focus only after the bounded staleness interval', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-13T10:00:00Z'));
    service.selectThread('thread-1');
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(1));

    window.dispatchEvent(new Event('focus'));
    http.expectNone((req) => req.url.includes('/thread-1/canvases/main'));

    vi.advanceTimersByTime(30_001);
    window.dispatchEvent(new Event('focus'));
    http.expectOne((req) => req.url.includes('/thread-1/canvases/main')).flush(canvasState(1));
  });

  it('saves source bytes with both preconditions and applies the response immediately', () => {
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      canvasState(2, {status: 'ready', editable: true}),
      {headers: {ETag: '"canvas:2:state"'}},
    );
    const sender = vi.fn().mockReturnValue(true);
    transport.attachControlSender(sender);
    let result: unknown;

    service.saveContent({
      contentUrl: '/api/persistent/threads/thread-1/canvases/main/content' +
        '?presentation_revision=2&source_fingerprint=sha256%3Afp' +
        '&source_version=sha256%3Aabc&ngsw-bypass=true',
      contentEtag: '"sha256:abc"',
      presentationRevision: 2,
      content: '# Edited report',
    }).subscribe(value => result = value);

    const save = http.expectOne(req => req.method === 'PUT');
    expect(save.request.headers.get('If-Match')).toBe('"sha256:abc"');
    expect(save.request.headers.get('X-Canvas-Presentation-Revision')).toBe('2');
    expect(save.request.headers.get('Content-Type')).toBe('text/plain; charset=utf-8');
    expect(save.request.body).toBe('# Edited report');
    save.flush(canvasState(3, {
      status: 'ready',
      editable: true,
      source_version: 'sha256:def',
    }), {
      headers: {
        ETag: '"canvas:3:state"',
        'X-Canvas-Content-ETag': '"sha256:def"',
      },
    });

    expect(result).toEqual(expect.objectContaining({
      stateEtag: '"canvas:3:state"',
      contentEtag: '"sha256:def"',
    }));
    expect(service.state()?.presentation_revision).toBe(3);
    expect(service.stateEtag()).toBe('"canvas:3:state"');
    expect(sender).toHaveBeenCalledWith('thread-1', {
      method: 'canvas.source_updated',
      canvas_id: 'main',
      path: 'output/report.md',
      presentation_revision: 3,
      source_version: 'sha256:def',
    });
  });

  it('reconciles a WOPI save and invalidates the agent read cache once per revision', async () => {
    const office = canvasState(1, {
      source: {type: 'workspace_file', path: 'output/report.docx'},
      renderer: 'office',
      editable: true,
      status: 'ready',
      capabilities: {
        can_edit: true,
        can_pop_out: true,
        can_take_control: false,
        can_view_office: true,
      },
    });
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      office,
      {headers: {ETag: '"canvas:1:office"'}},
    );
    const sender = vi.fn().mockReturnValue(true);
    transport.attachControlSender(sender);

    const first = service.reconcileOfficeSave();
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      {
        ...office,
        presentation_revision: 2,
        source_version: 'sha256:office-two',
      },
      {headers: {ETag: '"canvas:2:office"'}},
    );
    await expect(first).resolves.toBe(2);
    expect(sender).toHaveBeenCalledWith('thread-1', {
      method: 'canvas.source_updated',
      canvas_id: 'main',
      path: 'output/report.docx',
      presentation_revision: 2,
      source_version: 'sha256:office-two',
    });

    const duplicate = service.reconcileOfficeSave();
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      {
        ...office,
        presentation_revision: 2,
        source_version: 'sha256:office-two',
      },
      {headers: {ETag: '"canvas:2:office"'}},
    );
    await expect(duplicate).resolves.toBe(2);
    expect(sender).toHaveBeenCalledTimes(1);

    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 3},
    });
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      {
        ...office,
        presentation_revision: 3,
        source_version: 'sha256:agent-three',
      },
      {headers: {ETag: '"canvas:3:office"'}},
    );
    sender.mockClear();

    const stale = service.reconcileOfficeSave();
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      {
        ...office,
        presentation_revision: 2,
        source_version: 'sha256:office-two',
      },
      {headers: {ETag: '"canvas:2:office"'}},
    );
    await expect(stale).resolves.toBe(2);
    expect(service.state()?.presentation_revision).toBe(3);
    expect(sender).not.toHaveBeenCalled();
  });

  it('conditionally adopts current workspace bytes and invalidates the runtime cache', () => {
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      canvasState(4, {status: 'source_changed'}),
      {headers: {ETag: '"canvas:4:state"'}},
    );
    const sender = vi.fn().mockReturnValue(true);
    transport.attachControlSender(sender);

    service.refreshSource().subscribe();
    const refresh = http.expectOne(req => req.url.endsWith('/canvases/main/refresh'));
    expect(refresh.request.method).toBe('POST');
    expect(refresh.request.headers.get('If-Match')).toBe('"canvas:4:state"');
    refresh.flush(canvasState(5, {
      status: 'ready',
      source_version: 'sha256:fresh',
    }), {
      headers: {
        ETag: '"canvas:5:state"',
        'X-Canvas-Content-ETag': '"sha256:fresh"',
      },
    });

    expect(service.state()?.source_version).toBe('sha256:fresh');
    expect(sender).toHaveBeenCalledWith('thread-1', expect.objectContaining({
      method: 'canvas.source_updated',
      presentation_revision: 5,
      source_version: 'sha256:fresh',
    }));
  });

  it('conditionally rotates a live app origin without requiring a content ETag', () => {
    const app = canvasState(8, {
      source: {type: 'workspace_app', entry_path: '/demo'},
      renderer: 'auto',
      source_version: null,
      status: 'ready',
      capabilities: {
        can_edit: false,
        can_pop_out: true,
        can_take_control: false,
        can_create_viewer_session: true,
      },
    });
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(app, {
      headers: {ETag: '"canvas:8:app"'},
    });
    const sender = vi.fn().mockReturnValue(true);
    transport.attachControlSender(sender);

    let result: unknown;
    service.resetOrigin('"canvas:8:app"').subscribe(value => result = value);
    const reset = http.expectOne(req => req.url.endsWith('/canvases/main/reset-origin'));
    expect(reset.request.method).toBe('POST');
    expect(reset.request.body).toBeNull();
    expect(reset.request.headers.get('If-Match')).toBe('"canvas:8:app"');
    reset.flush({...app, presentation_revision: 9}, {
      headers: {ETag: '"canvas:9:fresh-origin"'},
    });

    expect(result).toEqual({
      state: expect.objectContaining({presentation_revision: 9}),
      stateEtag: '"canvas:9:fresh-origin"',
    });
    expect(service.state()?.presentation_revision).toBe(9);
    expect(service.stateEtag()).toBe('"canvas:9:fresh-origin"');
    expect(sender).toHaveBeenCalledWith('thread-1', {
      method: 'canvas.presentation_updated',
      canvas_id: 'main',
      presentation_revision: 9,
    });
  });

  it('discovers a strict browser capability and treats a missing endpoint as dark', () => {
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush({
      ...browserCapability(),
      private_generation: 'must-not-be-accepted',
    });

    expect(service.browserCapability()).toBeNull();
    expect(service.browserCapabilityStatus()).toBe('error');

    service.selectThread('thread-2');
    http.expectOne(req => req.url.includes('/thread-2/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-2/browser/capability')).flush(
      {detail: 'Not Found'},
      {status: 404, statusText: 'Not Found'},
    );

    expect(service.browserCapability()).toEqual(browserCapability({
      feature_enabled: false,
      can_open_browser: false,
      workspace_ready: false,
      reason: 'feature_disabled',
    }));
    expect(service.browserCapabilityStatus()).toBe('ready');

    service.selectThread('thread-3');
    http.expectOne(req => req.url.includes('/thread-3/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-3/browser/capability')).flush(
      {detail: {code: 'forbidden'}},
      {status: 403, statusText: 'Forbidden'},
    );
    expect(service.browserCapability()).toBeNull();
    expect(service.browserCapabilityStatus()).toBe('error');
  });

  it('moves through workspace and browser startup before applying the open response', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-22T10:00:00Z'));
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      canvasState(4, {status: 'ready'}),
      {headers: {ETag: '"canvas:4:file"'}},
    );
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability({workspace_ready: false}),
    );
    const sender = vi.fn().mockReturnValue(true);
    transport.attachControlSender(sender);

    service.openBrowser('Research browser', 4);
    service.openBrowser('Duplicate click');
    expect(service.browserOpenStatus()).toBe('workspace');
    const provisioning = http.expectOne(req => req.url.endsWith('/thread-1/browser/open'));
    expect(provisioning.request.method).toBe('POST');
    expect(provisioning.request.body).toEqual({
      title: 'Research browser',
      expected_presentation_revision: 4,
    });
    provisioning.flush(
      {status: 'provisioning'},
      {status: 202, statusText: 'Accepted', headers: {'Retry-After': '2'}},
    );

    vi.advanceTimersByTime(1_999);
    http.expectNone(req => req.url.endsWith('/thread-1/browser/capability'));
    vi.advanceTimersByTime(1);
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability({workspace_ready: true}),
    );
    expect(service.browserOpenStatus()).toBe('browser');

    const opened = http.expectOne(req => req.url.endsWith('/thread-1/browser/open'));
    expect(opened.request.body).toEqual({
      title: 'Research browser',
      expected_presentation_revision: 4,
    });
    opened.flush(canvasState(5, {
      source: {type: 'browser'},
      title: 'Research browser',
      renderer: 'auto',
      source_version: null,
      status: 'ready',
      capabilities: {
        can_edit: false,
        can_pop_out: true,
        can_take_control: true,
        can_stream_browser: true,
      },
    }), {
      headers: {
        ETag: '"canvas:5:browser"',
        'X-Canvas-Mutation-Changed': 'true',
      },
    });

    expect(service.browserOpenStatus()).toBe('idle');
    expect(service.browserOpenError()).toBeNull();
    expect(service.state()?.source).toEqual({type: 'browser'});
    expect(service.stateEtag()).toBe('"canvas:5:browser"');
    expect(sender).toHaveBeenCalledWith('thread-1', {
      method: 'canvas.presentation_updated',
      canvas_id: 'main',
      presentation_revision: 5,
    });
  });

  it('applies an idempotent browser open without duplicate runtime or tab invalidation', () => {
    const browser = canvasState(5, {
      source: {type: 'browser'},
      renderer: 'auto',
      source_version: null,
      status: 'ready',
      capabilities: {
        can_edit: false,
        can_pop_out: true,
        can_take_control: true,
        can_stream_browser: true,
      },
    });
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      canvasState(4, {status: 'ready'}),
      {headers: {ETag: '"canvas:4:file"'}},
    );
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability(),
    );
    const sender = vi.fn().mockReturnValue(true);
    const postMessage = vi.fn();
    transport.attachControlSender(sender);
    (service as unknown as {broadcastChannel: {
      postMessage: ReturnType<typeof vi.fn>;
      removeEventListener: ReturnType<typeof vi.fn>;
      close: ReturnType<typeof vi.fn>;
    }}).broadcastChannel = {
      postMessage,
      removeEventListener: vi.fn(),
      close: vi.fn(),
    };

    service.openBrowser();
    http.expectOne(req => req.url.endsWith('/thread-1/browser/open')).flush(browser, {
      headers: {
        ETag: '"canvas:5:browser"',
        'X-Canvas-Mutation-Changed': 'true',
      },
    });
    expect(service.state()?.presentation_revision).toBe(5);
    expect(sender).toHaveBeenCalledOnce();
    expect(postMessage).toHaveBeenCalledOnce();

    sender.mockClear();
    postMessage.mockClear();
    service.openBrowser();
    http.expectOne(req => req.url.endsWith('/thread-1/browser/open')).flush(browser, {
      headers: {
        ETag: '"canvas:5:browser-refreshed"',
        'X-Canvas-Mutation-Changed': 'false',
      },
    });

    expect(service.stateEtag()).toBe('"canvas:5:browser-refreshed"');
    expect(sender).not.toHaveBeenCalled();
    expect(postMessage).not.toHaveBeenCalled();
  });

  it('fails closed on missing open mutation metadata and permits a retry', () => {
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability(),
    );

    service.openBrowser();
    http.expectOne(req => req.url.endsWith('/thread-1/browser/open')).flush(
      canvasState(1, {
        source: {type: 'browser'},
        renderer: 'auto',
        source_version: null,
        status: 'ready',
      }),
      {headers: {ETag: '"canvas:1:browser"'}},
    );

    expect(service.state()).toBeNull();
    expect(service.browserOpenStatus()).toBe('error');
    expect(service.browserOpenError()).toBe('invalid_browser_open_response');

    service.retryOpenBrowser();
    expect(service.browserOpenStatus()).toBe('browser');
    http.expectOne(req => req.url.endsWith('/thread-1/browser/open')).flush(
      {detail: {code: 'browser_gone'}},
      {status: 503, statusText: 'Unavailable'},
    );
    expect(service.browserOpenError()).toBe('browser_gone');
  });

  it('reconciles the winning Canvas after a conditional browser-open conflict', () => {
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      canvasState(4, {status: 'ready'}),
      {headers: {ETag: '"canvas:4:file"'}},
    );
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability(),
    );

    service.openBrowser(undefined, 4);
    const open = http.expectOne(req => req.url.endsWith('/thread-1/browser/open'));
    expect(open.request.body).toEqual({
      title: null,
      expected_presentation_revision: 4,
    });
    open.flush(
      {detail: {code: 'canvas_presentation_changed'}},
      {status: 409, statusText: 'Conflict'},
    );

    expect(service.browserOpenStatus()).toBe('error');
    expect(service.browserOpenError()).toBe('canvas_presentation_changed');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      canvasState(5, {title: 'New agent presentation', status: 'ready'}),
      {headers: {ETag: '"canvas:5:new"'}},
    );
    expect(service.state()?.presentation_revision).toBe(5);
    expect(service.state()?.title).toBe('New agent presentation');
  });

  it('times out cold provisioning after five minutes with a retryable error', () => {
    vi.useFakeTimers();
    const started = new Date('2026-07-22T10:00:00Z');
    vi.setSystemTime(started);
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability({workspace_ready: false}),
    );

    service.openBrowser();
    http.expectOne(req => req.url.endsWith('/thread-1/browser/open')).flush(
      {status: 'provisioning'},
      {status: 202, statusText: 'Accepted', headers: {'Retry-After': '1'}},
    );
    vi.setSystemTime(new Date(started.getTime() + BROWSER_OPEN_TIMEOUT_MS));
    vi.advanceTimersByTime(1_000);

    expect(service.browserOpenStatus()).toBe('error');
    expect(service.browserOpenError()).toBe('browser_open_timeout');
    http.expectNone(req => req.url.endsWith('/thread-1/browser/capability'));
  });

  it('cancels stale capability and open work when the selected thread changes', () => {
    service.selectThread('thread-a');
    http.expectOne(req => req.url.includes('/thread-a/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-a/browser/capability')).flush(
      browserCapability(),
    );
    service.openBrowser();
    const staleOpen = http.expectOne(req => req.url.endsWith('/thread-a/browser/open'));

    service.selectThread('thread-b');
    expect(staleOpen.cancelled).toBe(true);
    expect(service.browserCapability()).toBeNull();
    expect(service.browserCapabilityStatus()).toBe('loading');
    expect(service.browserOpenStatus()).toBe('idle');
    expect(service.browserOpenError()).toBeNull();
    http.expectOne(req => req.url.includes('/thread-b/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-b/browser/capability')).flush(
      browserCapability(),
    );
    expect(service.state()).toBeNull();
  });

  it('asks again while the workspace is not yet attested, and stops once it can open', () => {
    vi.useFakeTimers();
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability({can_open_browser: false, workspace_ready: false, reason: 'workspace_unattested'}),
    );
    expect(service.browserCapability()?.can_open_browser).toBe(false);

    vi.advanceTimersByTime(BROWSER_CAPABILITY_RECHECK_MS - 1);
    http.expectNone(req => req.url.endsWith('/thread-1/browser/capability'));
    vi.advanceTimersByTime(1);
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability(),
    );
    expect(service.browserCapability()?.can_open_browser).toBe(true);

    vi.advanceTimersByTime(BROWSER_CAPABILITY_RECHECK_MS * 3);
    http.expectNone(req => req.url.endsWith('/thread-1/browser/capability'));
  });

  it('does not poll a workspace that needs an upgrade; refresh and the upgrade ask again', () => {
    vi.useFakeTimers();
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability({can_open_browser: false, workspace_ready: false, reason: 'workspace_required'}),
    );
    vi.advanceTimersByTime(BROWSER_CAPABILITY_RECHECK_MS * 3);
    http.expectNone(req => req.url.endsWith('/thread-1/browser/capability'));

    // The Canvas refresh button asks again while the browser cannot open.
    service.reconcile();
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability({can_open_browser: false, workspace_ready: false, reason: 'workspace_required'}),
    );

    // A finished workspace upgrade asks again (PersistentChatService calls this).
    service.refreshBrowserCapability();
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability(),
    );
    expect(service.browserCapability()?.can_open_browser).toBe(true);
  });

  it('stops asking again when the thread changes', () => {
    vi.useFakeTimers();
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability({can_open_browser: false, workspace_ready: false, reason: 'workspace_unattested'}),
    );
    service.selectThread('thread-2');
    http.expectOne(req => req.url.includes('/thread-2/canvases/main')).flush(null, {
      status: 204,
      statusText: 'No Content',
    });
    http.expectOne(req => req.url.endsWith('/thread-2/browser/capability')).flush(
      browserCapability(),
    );
    vi.advanceTimersByTime(BROWSER_CAPABILITY_RECHECK_MS * 2);
    http.expectNone(req => req.url.endsWith('/thread-1/browser/capability'));
  });

  it('refuses to reset app storage without an authoritative state ETag', () => {
    let error: unknown;
    service.resetOrigin().subscribe({error: value => error = value});

    expect(error).toBeInstanceOf(Error);
    http.expectNone(req => req.url.includes('/reset-origin'));
  });

  it('does not notify the runtime from a mutation response older than current state', () => {
    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(
      canvasState(3, {status: 'ready', editable: true}),
      {headers: {ETag: '"canvas:3:state"'}},
    );
    const sender = vi.fn().mockReturnValue(true);
    transport.attachControlSender(sender);
    const contentUrl = '/api/persistent/threads/thread-1/canvases/main/content' +
      '?presentation_revision=3&source_fingerprint=sha256%3Afp' +
      '&source_version=sha256%3Aabc&ngsw-bypass=true';

    service.saveContent({
      contentUrl,
      contentEtag: '"sha256:abc"',
      presentationRevision: 3,
      content: '# Older completion',
    }).subscribe();
    service.saveContent({
      contentUrl,
      contentEtag: '"sha256:abc"',
      presentationRevision: 3,
      content: '# Newer completion',
    }).subscribe();

    const [older, newer] = http.match(req => req.method === 'PUT');
    newer.flush(canvasState(5, {
      status: 'ready',
      editable: true,
      source_version: 'sha256:newer',
    }), {
      headers: {
        ETag: '"canvas:5:state"',
        'X-Canvas-Content-ETag': '"sha256:newer"',
      },
    });
    older.flush(canvasState(4, {
      status: 'ready',
      editable: true,
      source_version: 'sha256:older',
    }), {
      headers: {
        ETag: '"canvas:4:state"',
        'X-Canvas-Content-ETag': '"sha256:older"',
      },
    });

    expect(service.state()?.presentation_revision).toBe(5);
    expect(sender).toHaveBeenCalledTimes(1);
    expect(sender).toHaveBeenCalledWith('thread-1', expect.objectContaining({
      presentation_revision: 5,
      source_version: 'sha256:newer',
    }));
  });
});

describe('CanvasService cross-tab invalidation', () => {
  it('accepts only a bounded state pointer and never a Canvas state payload', () => {
    expect(parseCanvasBroadcastInvalidation({
      type: 'canvas.presentation_invalidated',
      threadId: 'thread-1',
      canvasId: 'main',
      presentationRevision: 9,
      state: {title: 'must not be trusted'},
    })).toEqual({
      type: 'canvas.presentation_invalidated',
      threadId: 'thread-1',
      canvasId: 'main',
      presentationRevision: 9,
    });
    expect(parseCanvasBroadcastInvalidation({
      type: 'canvas.presentation_invalidated',
      threadId: 'thread-1',
      canvasId: 'other',
      presentationRevision: 9,
    })).toBeNull();
    expect(parseCanvasBroadcastInvalidation({
      type: 'canvas.presentation_invalidated',
      threadId: 'thread-1',
      canvasId: 'main',
      presentationRevision: -1,
    })).toBeNull();
  });

  it('rebroadcasts transport pointers and reconciles received pointers without looping', () => {
    class FakeBroadcastChannel {
      static instances: FakeBroadcastChannel[] = [];
      readonly postMessage = vi.fn();
      readonly close = vi.fn();
      private listener: ((event: MessageEvent<unknown>) => void) | null = null;

      constructor(readonly name: string) {
        FakeBroadcastChannel.instances.push(this);
      }

      addEventListener(_type: string, listener: EventListenerOrEventListenerObject): void {
        if (typeof listener === 'function') {
          this.listener = listener as (event: MessageEvent<unknown>) => void;
        }
      }

      removeEventListener(): void {
        this.listener = null;
      }

      emit(data: unknown): void {
        this.listener?.(new MessageEvent('message', {data}));
      }
    }

    const original = Object.getOwnPropertyDescriptor(window, 'BroadcastChannel');
    Object.defineProperty(window, 'BroadcastChannel', {
      configurable: true,
      value: FakeBroadcastChannel as unknown as typeof BroadcastChannel,
    });
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        CanvasService,
        PersistentThreadTransportBridge,
        provideHttpClient(),
        provideHttpClientTesting(),
      ],
    });
    const service = TestBed.inject(CanvasService);
    const transport = TestBed.inject(PersistentThreadTransportBridge);
    const http = TestBed.inject(HttpTestingController);
    const channel = FakeBroadcastChannel.instances[0]!;

    service.selectThread('thread-1');
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(canvasState(1));
    http.expectOne(req => req.url.endsWith('/thread-1/browser/capability')).flush(
      browserCapability({
        feature_enabled: false,
        can_open_browser: false,
        workspace_ready: false,
        reason: 'feature_disabled',
      }),
    );
    transport.forwardEvent('thread-1', {
      method: 'canvas.updated',
      params: {canvas_id: 'main', presentation_revision: 2},
    });
    expect(channel.postMessage).toHaveBeenCalledWith({
      type: 'canvas.presentation_invalidated',
      threadId: 'thread-1',
      canvasId: 'main',
      presentationRevision: 2,
    });
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(canvasState(2));

    channel.postMessage.mockClear();
    channel.emit({
      type: 'canvas.presentation_invalidated',
      threadId: 'thread-1',
      canvasId: 'main',
      presentationRevision: 3,
      state: {title: 'ignored'},
    });
    expect(service.state()?.presentation_revision).toBe(2);
    expect(channel.postMessage).not.toHaveBeenCalled();
    http.expectOne(req => req.url.includes('/thread-1/canvases/main')).flush(canvasState(3));
    expect(service.state()?.presentation_revision).toBe(3);

    http.verify();
    TestBed.resetTestingModule();
    if (original) Object.defineProperty(window, 'BroadcastChannel', original);
    else Reflect.deleteProperty(window, 'BroadcastChannel');
  });
});
