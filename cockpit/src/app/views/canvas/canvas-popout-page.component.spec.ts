import {provideHttpClient} from '@angular/common/http';
import {signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {ActivatedRoute, convertToParamMap, Router} from '@angular/router';
import {BehaviorSubject, EMPTY} from 'rxjs';
import {afterEach, describe, expect, it, vi} from 'vitest';
import {routes} from '../../app.routes';
import {authGuard} from '../../core/guards/auth.guard';
import {CanvasState} from '../../core/models/canvas.model';
import {CanvasService} from '../../core/services/canvas.service';
import {PersistentThreadTransportBridge} from '../../core/services/persistent-thread-transport-bridge.service';
import {TranslocoService} from '@jsverse/transloco';
import {
  CANVAS_BROWSER_BITMAP_FACTORY,
  CANVAS_BROWSER_SOCKET_FACTORY,
  CANVAS_BROWSER_VISIBILITY,
  CanvasBrowserController,
} from './canvas-browser.controller';
import {CanvasPaneComponent} from './canvas-pane.component';
import {
  CANVAS_POPOUT_RECONCILE_MS,
  CanvasPopoutPageComponent,
} from './canvas-popout-page.component';

describe('Canvas authenticated pop-out wrapper', () => {
  afterEach(() => {
    TestBed.resetTestingModule();
    vi.useRealTimers();
  });

  it('is reachable only through the authenticated session route', async () => {
    const route = routes.find(candidate => candidate.path === 'sessions/:threadId/canvas');

    expect(await route?.loadComponent?.()).toBe(CanvasPopoutPageComponent);
    expect(route?.canActivate).toContain(authGuard);
    expect(route?.data?.['canvasPopout']).toBe(true);
  });

  it('selects the route thread and follows Angular param reuse', () => {
    const params = new BehaviorSubject(convertToParamMap({threadId: 'thread-1'}));
    const canvas = {selectThread: vi.fn(), threadId: vi.fn(() => 'thread-1')};
    const router = {navigate: vi.fn().mockResolvedValue(true)};
    TestBed.configureTestingModule({
      providers: [
        CanvasPopoutPageComponent,
        {provide: ActivatedRoute, useValue: {paramMap: params.asObservable()}},
        {provide: Router, useValue: router},
        {provide: CanvasService, useValue: canvas},
      ],
    });
    const component = TestBed.inject(CanvasPopoutPageComponent);

    component.ngOnInit();
    expect(canvas.selectThread).toHaveBeenLastCalledWith('thread-1');

    params.next(convertToParamMap({threadId: 'thread-2'}));
    expect(canvas.selectThread).toHaveBeenLastCalledWith('thread-2');
    expect(router.navigate).not.toHaveBeenCalled();
  });

  it('polls authoritative state only while visible and stops on destroy', () => {
    vi.useFakeTimers();
    const originalVisibility = Object.getOwnPropertyDescriptor(document, 'visibilityState');
    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      value: 'visible',
    });
    const params = new BehaviorSubject(convertToParamMap({threadId: 'thread-1'}));
    const canvas = {
      selectThread: vi.fn(),
      threadId: vi.fn(() => 'thread-1'),
      reconcile: vi.fn(),
    };
    TestBed.configureTestingModule({
      providers: [
        CanvasPopoutPageComponent,
        {provide: ActivatedRoute, useValue: {paramMap: params.asObservable()}},
        {provide: Router, useValue: {navigate: vi.fn().mockResolvedValue(true)}},
        {provide: CanvasService, useValue: canvas},
      ],
    });
    TestBed.inject(CanvasPopoutPageComponent).ngOnInit();

    vi.advanceTimersByTime(CANVAS_POPOUT_RECONCILE_MS);
    expect(canvas.reconcile).toHaveBeenCalledOnce();

    TestBed.resetTestingModule();
    vi.advanceTimersByTime(CANVAS_POPOUT_RECONCILE_MS * 2);
    expect(canvas.reconcile).toHaveBeenCalledOnce();
    if (originalVisibility) {
      Object.defineProperty(document, 'visibilityState', originalVisibility);
    } else {
      Reflect.deleteProperty(document, 'visibilityState');
    }
  });

  it('owns a second pane-local browser socket and detaches it independently', async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));

    class FakeSocket {
      binaryType: BinaryType = 'blob';
      readyState = WebSocket.CONNECTING;
      onmessage: ((event: MessageEvent<unknown>) => void) | null = null;
      onclose: ((event: CloseEvent) => void) | null = null;
      onerror: ((event: Event) => void) | null = null;
      readonly close = vi.fn(() => { this.readyState = WebSocket.CLOSED; });
      readonly send = vi.fn();
    }

    const browserState: CanvasState = {
      canvas_id: 'main',
      source: {type: 'browser'},
      title: 'Shared browser',
      renderer: 'auto',
      editable: false,
      alt_text: null,
      presentation_revision: 7,
      source_version: null,
      status: 'ready',
      capabilities: {
        can_edit: false,
        can_pop_out: true,
        can_take_control: true,
        can_stream_browser: true,
      },
      updated_at: '2026-07-22T12:00:00Z',
    };
    const canvas = {
      threadId: signal<string | null>('thread-1'),
      state: signal<CanvasState | null>(browserState),
      stateEtag: signal<string | null>('"canvas:7:fixture"'),
      loadStatus: signal<'ready'>('ready'),
      requestError: signal(null),
      browserCapability: signal({
        feature_enabled: true,
        can_open_browser: true,
        workspace_ready: true,
        reason: null,
      }),
      browserCapabilityStatus: signal<'ready'>('ready'),
      browserOpenStatus: signal<'idle'>('idle'),
      browserOpenError: signal<string | null>(null),
      lastSuccessfulSyncAt: signal<number | null>(null),
      selectThread: vi.fn(),
      reconcile: vi.fn(),
    };
    const sockets: FakeSocket[] = [];
    TestBed.configureTestingModule({
      imports: [CanvasPaneComponent],
      providers: [
        provideHttpClient(),
        {
          provide: Router,
          useValue: {
            navigate: vi.fn().mockResolvedValue(true),
            serializeUrl: vi.fn(value => String(value)),
            createUrlTree: vi.fn(value => value),
          },
        },
        {provide: CanvasService, useValue: canvas},
        {provide: TranslocoService, useValue: {translate: (key: string) => key}},
        {
          provide: PersistentThreadTransportBridge,
          useValue: {canvasAwareness$: EMPTY},
        },
        {
          provide: CANVAS_BROWSER_SOCKET_FACTORY,
          useValue: () => {
            const socket = new FakeSocket();
            sockets.push(socket);
            return socket as unknown as WebSocket;
          },
        },
        {
          provide: CANVAS_BROWSER_BITMAP_FACTORY,
          useValue: vi.fn(),
        },
        {provide: CANVAS_BROWSER_VISIBILITY, useValue: null},
      ],
    });
    TestBed.overrideComponent(CanvasPaneComponent, {
      set: {
        template: '',
        styleUrl: undefined,
        styleUrls: [],
        styles: [''],
      },
    });
    await ɵresolveComponentResources(() => Promise.resolve(''));
    await TestBed.compileComponents();

    const main = TestBed.createComponent(CanvasPaneComponent);
    main.detectChanges();
    // CanvasPopoutPageComponent's authenticated route mounts the same
    // standalone pane type; a second fixture exercises its component-scoped
    // providers without sharing the first pane's controller.
    const popout = TestBed.createComponent(CanvasPaneComponent);
    popout.detectChanges();
    TestBed.flushEffects();

    const mainController = main.debugElement.injector.get(CanvasBrowserController);
    const popoutController = popout.debugElement.injector.get(CanvasBrowserController);
    expect(popoutController).not.toBe(mainController);
    expect(sockets).toHaveLength(2);

    popout.destroy();
    expect(sockets[1].close).toHaveBeenCalledOnce();
    expect(sockets[0].close).not.toHaveBeenCalled();

    main.destroy();
    expect(sockets[0].close).toHaveBeenCalledOnce();
  });
});
