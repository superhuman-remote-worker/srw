import { ComponentFixture, TestBed } from '@angular/core/testing';
import { readFileSync } from 'node:fs';
import { TranslocoPipe, TranslocoTestingModule } from '@jsverse/transloco';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  Component,
  Directive,
  EventEmitter,
  Input,
  Output,
  signal,
  ɵresolveComponentResources,
} from '@angular/core';
import { BrowserPageState } from './canvas-browser-protocol';
import { CanvasService } from '../../core/services/canvas.service';
import { PersistentThreadTransportBridge } from '../../core/services/persistent-thread-transport-bridge.service';
import {
  BrowserViewportSize,
  CanvasBrowserConnectionStatus,
  CanvasBrowserController,
} from './canvas-browser.controller';
import {
  BROWSER_VIEWPORT_DEBOUNCE_MS,
  CanvasBrowserRendererComponent,
  fitBrowserFrame,
} from './canvas-browser-renderer.component';

@Directive({selector: '[appTooltip]', standalone: true})
class TooltipStubDirective {
  @Input() appTooltip = '';
}

@Component({
  selector: 'app-icon-button',
  standalone: true,
  template: '<button type="button" [attr.aria-label]="ariaLabel" [disabled]="disabled" (click)="clicked.emit($event)"><ng-content /></button>',
})
class IconButtonStubComponent {
  @Input() size = '';
  @Input() ariaLabel = '';
  @Input() tooltip = '';
  @Input() disabled = false;
  @Output() readonly clicked = new EventEmitter<MouseEvent>();
}

@Component({selector: 'app-button', standalone: true, template: '<ng-content />'})
class ButtonStubComponent {
  @Input() size = '';
  @Input() variant = '';
  @Input() disabled = false;
  @Input() loading = false;
  @Output() readonly clicked = new EventEmitter<MouseEvent>();
}

@Component({selector: 'app-icon', standalone: true, template: '<ng-content />'})
class IconStubComponent {
  @Input() size = '';
}

@Component({selector: 'app-spinner', standalone: true, template: ''})
class SpinnerStubComponent {
  @Input() size = '';
  @Input() tone = '';
}

const translations = {
  canvas: {
    browser: {
      untitled: 'Untitled page',
      noUrl: 'No page URL',
      loading: 'Page loading…',
      surfaceLabel: 'Shared browser page',
      retryConnection: 'Retry connection',
      restart: 'Restart browser',
      baton: {
        agent: 'Agent is driving',
        user: "You're driving",
        take: 'Take control',
        release: 'Release control',
        pending: 'Waiting for browser…',
      },
      toolbar: {
        label: 'Shared browser controls',
        back: 'Back',
        reload: 'Reload',
        address: 'Address',
        navigationRejected: 'Navigation blocked:',
      },
      size: {
        label: 'Page size',
        fit: 'Fit to pane',
        phone: 'Phone ({{width}} × {{height}})',
        tablet: 'Tablet ({{width}} × {{height}})',
        desktop: 'Desktop ({{width}} × {{height}})',
      },
      status: {
        connecting: 'Connecting',
        ready: 'Connected',
        reconnecting: 'Reconnecting',
        ended: 'Ended',
        viewerLimit: 'Viewer limit',
        unauthorized: 'Unauthorized',
        unavailable: 'Unavailable',
        disabled: 'Disabled',
        error: 'Protocol error',
      },
      open: {
        phase: {
          workspace: 'Starting workspace…',
          browser: 'Starting browser…',
        },
        error: { failed: 'Browser open failed' },
      },
    },
  },
};

function page(overrides: Partial<BrowserPageState> = {}): BrowserPageState {
  return {
    baton: 'agent',
    viewport: { width: 1280, height: 720 },
    url: 'https://example.test/form',
    title: 'Example form',
    loading: false,
    ...overrides,
  };
}

function bitmap(width: number, height: number): ImageBitmap {
  return { width, height, close: vi.fn() } as unknown as ImageBitmap;
}

describe('Canvas shared-browser renderer', () => {
  let fixture: ComponentFixture<CanvasBrowserRendererComponent>;
  let controller: {
    connectionStatus: ReturnType<typeof signal<CanvasBrowserConnectionStatus>>;
    pageState: ReturnType<typeof signal<BrowserPageState | null>>;
    frame: ReturnType<typeof signal<ImageBitmap | null>>;
    errorCode: ReturnType<typeof signal<string | null>>;
    errorMessage: ReturnType<typeof signal<string | null>>;
    pendingBaton: ReturnType<typeof signal<'agent' | 'user' | null>>;
    viewportControl: ReturnType<typeof signal<boolean>>;
    frameSize: ReturnType<typeof signal<BrowserViewportSize | null>>;
    retry: ReturnType<typeof vi.fn>;
    sendControl: ReturnType<typeof vi.fn>;
    sendInput: ReturnType<typeof vi.fn>;
    requestViewport: ReturnType<typeof vi.fn>;
  };
  let resizeCallbacks: ResizeObserverCallback[];
  let observed: Element[];
  let canvas: {
    browserCapability: ReturnType<typeof signal<{
      feature_enabled: boolean;
      can_open_browser: boolean;
      workspace_ready: boolean;
      reason: null;
    } | null>>;
    browserOpenStatus: ReturnType<typeof signal<'idle' | 'workspace' | 'browser' | 'error'>>;
    openBrowser: ReturnType<typeof vi.fn>;
  };
  let context: {
    clearRect: ReturnType<typeof vi.fn>;
    drawImage: ReturnType<typeof vi.fn>;
  };
  let getContext: ReturnType<typeof vi.spyOn>;

  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(async () => {
    controller = {
      connectionStatus: signal<CanvasBrowserConnectionStatus>('connecting'),
      pageState: signal<BrowserPageState | null>(page()),
      frame: signal<ImageBitmap | null>(null),
      errorCode: signal<string | null>(null),
      errorMessage: signal<string | null>(null),
      pendingBaton: signal<'agent' | 'user' | null>(null),
      viewportControl: signal(false),
      frameSize: signal<BrowserViewportSize | null>(null),
      retry: vi.fn(),
      sendControl: vi.fn(() => true),
      sendInput: vi.fn(() => true),
      requestViewport: vi.fn(() => true),
    };
    resizeCallbacks = [];
    observed = [];
    vi.stubGlobal('ResizeObserver', class {
      constructor(callback: ResizeObserverCallback) {
        resizeCallbacks.push(callback);
      }
      observe(element: Element): void {
        observed.push(element);
      }
      disconnect(): void {
        observed = [];
      }
    });
    canvas = {
      browserCapability: signal({
        feature_enabled: true,
        can_open_browser: true,
        workspace_ready: true,
        reason: null,
      }),
      browserOpenStatus: signal<'idle' | 'workspace' | 'browser' | 'error'>('idle'),
      openBrowser: vi.fn(),
    };
    context = { clearRect: vi.fn(), drawImage: vi.fn() };
    getContext = vi
      .spyOn(HTMLCanvasElement.prototype, 'getContext')
      .mockReturnValue(context as unknown as CanvasRenderingContext2D);
    TestBed.configureTestingModule({
      imports: [
        CanvasBrowserRendererComponent,
        TranslocoTestingModule.forRoot({
          langs: { en: translations },
          translocoConfig: { availableLangs: ['en'], defaultLang: 'en' },
        }),
      ],
      providers: [
        { provide: CanvasBrowserController, useValue: controller },
        { provide: CanvasService, useValue: canvas },
      ],
    });
    TestBed.overrideComponent(CanvasBrowserRendererComponent, {
      set: {
        styleUrl: undefined,
        styleUrls: [],
        styles: [''],
        imports: [
          ButtonStubComponent,
          IconButtonStubComponent,
          IconStubComponent,
          SpinnerStubComponent,
          TooltipStubDirective,
          TranslocoPipe,
        ],
      },
    });
    await ɵresolveComponentResources(() => Promise.resolve(''));
    await TestBed.compileComponents();
    fixture = TestBed.createComponent(CanvasBrowserRendererComponent);
    fixture.detectChanges();
    TestBed.flushEffects();
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture?.destroy();
    getContext?.mockRestore();
    vi.unstubAllGlobals();
    TestBed.resetTestingModule();
  });

  it('keeps keyboard focus visible and disables reconnect motion on request', () => {
    const styles = readFileSync(
      'src/app/views/canvas/canvas-browser-renderer.component.scss',
      'utf8',
    );

    expect(styles).toContain('.browser-surface:focus-visible');
    expect(styles).toContain('@media (prefers-reduced-motion: reduce)');
    expect(styles).toContain('animation: none !important');
    expect(styles).toContain('transition: none !important');
  });

  it('renders only trusted chrome and one focusable bitmap canvas', () => {
    const root = fixture.nativeElement as HTMLElement;
    const surface = root.querySelector('canvas') as HTMLCanvasElement;

    expect(root.textContent).toContain('Example form');
    expect(root.textContent).toContain('Agent is driving');
    expect(root.textContent).toContain('Take control');
    expect(root.textContent).toContain('Connecting');
    expect((root.querySelector('input') as HTMLInputElement).value).toBe(
      'https://example.test/form',
    );
    expect(surface.tabIndex).toBe(0);
    expect(surface.getAttribute('aria-label')).toBe('Shared browser page');
    expect(root.querySelectorAll('canvas')).toHaveLength(1);
    expect(root.querySelector('iframe, img, object, embed')).toBeNull();
  });

  it('uses decoded bitmap dimensions for backing pixels despite metadata mismatch', async () => {
    const decoded = bitmap(640, 480);
    controller.frame.set(decoded);
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    const surface = fixture.nativeElement.querySelector('canvas') as HTMLCanvasElement;

    expect(getContext).toHaveBeenCalled();
    expect(surface.width).toBe(640);
    expect(surface.height).toBe(480);
    // Displayed at the page's CSS size (STATE viewport), not the bitmap's.
    expect(surface.style.width).toBe('1280px');
    expect(surface.style.height).toBe('720px');
    expect(context.drawImage).toHaveBeenLastCalledWith(decoded, 0, 0);

    controller.frame.set(null);
    fixture.detectChanges();
    await fixture.whenStable();
    expect(context.clearRect).toHaveBeenLastCalledWith(0, 0, 640, 480);
  });

  it('shows page loading and the authoritative user baton', () => {
    controller.connectionStatus.set('ready');
    controller.pageState.set(page({ baton: 'user', loading: true }));
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).toContain("You're driving");
    expect(fixture.nativeElement.textContent).toContain('Page loading…');
    expect(
      (fixture.nativeElement.querySelector('.browser-baton') as HTMLElement).dataset['baton'],
    ).toBe('user');
    expect(
      (fixture.nativeElement.querySelector('.browser-baton-action') as HTMLButtonElement)
        .getAttribute('aria-pressed'),
    ).toBe('true');
  });

  it('keeps URL edits local and sends navigation and baton controls without optimistic labels', () => {
    const component = fixture.componentInstance;
    controller.connectionStatus.set('ready');
    fixture.detectChanges();

    expect(component.canDrive()).toBe(false);
    expect(component.batonDisabled()).toBe(false);
    component.toggleBaton();
    expect(controller.sendControl).toHaveBeenLastCalledWith({op: 'take_baton'});
    expect(fixture.nativeElement.textContent).toContain('Agent is driving');

    controller.pendingBaton.set('user');
    fixture.detectChanges();
    component.toggleBaton();
    expect(controller.sendControl).toHaveBeenCalledOnce();
    expect(fixture.nativeElement.textContent).toContain('Agent is driving');

    controller.pendingBaton.set(null);
    controller.pageState.set(page({baton: 'user'}));
    fixture.detectChanges();
    expect(component.canDrive()).toBe(true);
    expect(fixture.nativeElement.textContent).toContain("You're driving");
    expect(fixture.nativeElement.textContent).toContain('Release control');

    component.urlEditing.set(true);
    const input = fixture.nativeElement.querySelector('input') as HTMLInputElement;
    input.value = 'https://typed.example/path';
    component.onUrlInput({target: input} as unknown as Event);
    controller.pageState.set(page({baton: 'user', url: 'https://state.example/new'}));
    TestBed.flushEffects();
    expect(component.urlValue()).toBe('https://typed.example/path');

    const submit = {preventDefault: vi.fn()} as unknown as Event;
    component.navigate(submit);
    component.goBack();
    component.reload();
    expect(controller.sendControl.mock.calls.slice(-3).map(call => call[0])).toEqual([
      {op: 'navigate', url: 'https://typed.example/path'},
      {op: 'back'},
      {op: 'reload'},
    ]);

    component.urlEditing.set(false);
    TestBed.flushEffects();
    expect(component.urlValue()).toBe('https://state.example/new');
    component.toggleBaton();
    expect(controller.sendControl).toHaveBeenLastCalledWith({op: 'release_baton'});
  });

  it('maps and throttles pointer input on the exact displayed canvas', () => {
    const component = fixture.componentInstance;
    const surface = fixture.nativeElement.querySelector('canvas') as HTMLCanvasElement;
    controller.connectionStatus.set('ready');
    controller.pageState.set(page({baton: 'user', viewport: {width: 1280, height: 720}}));
    fixture.detectChanges();
    surface.focus();
    vi.spyOn(surface, 'getBoundingClientRect').mockReturnValue(
      {left: 10, top: 20, width: 200, height: 100} as DOMRect,
    );
    const setPointerCapture = vi.fn();
    const releasePointerCapture = vi.fn();
    Object.defineProperty(surface, 'setPointerCapture', {value: setPointerCapture});
    Object.defineProperty(surface, 'releasePointerCapture', {value: releasePointerCapture});
    let animationFrame: FrameRequestCallback | null = null;
    vi.stubGlobal('requestAnimationFrame', vi.fn((callback: FrameRequestCallback) => {
      animationFrame = callback;
      return 17;
    }));
    vi.stubGlobal('cancelAnimationFrame', vi.fn());

    const pointer = (overrides: Record<string, unknown> = {}) => ({
      clientX: 60,
      clientY: 45,
      pointerId: 3,
      isPrimary: true,
      button: 0,
      buttons: 1,
      detail: 1,
      altKey: false,
      ctrlKey: false,
      metaKey: false,
      shiftKey: false,
      preventDefault: vi.fn(),
      ...overrides,
    }) as unknown as PointerEvent;

    component.onPointerMove(pointer({clientX: 60, clientY: 45}));
    component.onPointerMove(pointer({clientX: 110, clientY: 70}));
    expect(controller.sendInput).not.toHaveBeenCalled();
    expect(animationFrame).not.toBeNull();
    animationFrame!(0);
    expect(controller.sendInput).toHaveBeenLastCalledWith({
      kind: 'mouse',
      params: {
        type: 'mouseMoved',
        x: 640,
        y: 360,
        buttons: 1,
        modifiers: 0,
      },
    });

    controller.sendInput.mockClear();
    const down = pointer();
    component.onPointerDown(down);
    component.onPointerUp(pointer({clientX: 500, clientY: 500, buttons: 0}));
    expect(controller.sendInput.mock.calls.map(call => call[0])).toEqual([
      {
        kind: 'mouse',
        params: {
          type: 'mousePressed',
          x: 320,
          y: 180,
          button: 'left',
          buttons: 1,
          modifiers: 0,
          clickCount: 1,
        },
      },
      {
        kind: 'mouse',
        params: {
          type: 'mouseReleased',
          x: 320,
          y: 180,
          button: 'left',
          buttons: 0,
          modifiers: 0,
          clickCount: 1,
        },
      },
    ]);
    expect(down.preventDefault).toHaveBeenCalledOnce();
    expect(setPointerCapture).toHaveBeenCalledWith(3);
    expect(releasePointerCapture).toHaveBeenCalledWith(3);

    controller.sendInput.mockClear();
    component.onPointerDown(pointer({button: 4}));
    component.onPointerDown(pointer({clientX: 9}));
    expect(controller.sendInput).not.toHaveBeenCalled();

    component.onPointerDown(pointer());
    component.onPointerCancel(pointer({buttons: 0}));
    expect(controller.sendInput).toHaveBeenLastCalledWith({
      kind: 'mouse',
      params: {
        type: 'mouseReleased',
        x: 320,
        y: 180,
        button: 'left',
        buttons: 0,
        modifiers: 0,
        clickCount: 1,
      },
    });
  });

  it('normalizes focused wheel and keyboard input and ignores composition or agent baton', () => {
    const component = fixture.componentInstance;
    const surface = fixture.nativeElement.querySelector('canvas') as HTMLCanvasElement;
    controller.connectionStatus.set('ready');
    controller.pageState.set(page({baton: 'user', viewport: {width: 1280, height: 720}}));
    fixture.detectChanges();
    surface.focus();
    vi.spyOn(surface, 'getBoundingClientRect').mockReturnValue(
      {left: 10, top: 20, width: 200, height: 100} as DOMRect,
    );
    const base = {
      altKey: false,
      ctrlKey: false,
      metaKey: false,
      shiftKey: false,
      preventDefault: vi.fn(),
    };
    const wheel = {
      ...base,
      clientX: 110,
      clientY: 70,
      deltaX: 1,
      deltaY: -2,
      deltaMode: 1,
    } as unknown as WheelEvent;
    component.onWheel(wheel);
    expect(controller.sendInput).toHaveBeenLastCalledWith({
      kind: 'wheel',
      params: {x: 640, y: 360, deltaX: 102.4, deltaY: -230.4, modifiers: 0},
    });
    expect(base.preventDefault).toHaveBeenCalledOnce();

    const keyDown = {
      ...base,
      key: 'A',
      code: 'KeyA',
      location: 0,
      repeat: false,
      isComposing: false,
      preventDefault: vi.fn(),
    } as unknown as KeyboardEvent;
    component.onKeyDown(keyDown);
    expect(controller.sendInput).toHaveBeenLastCalledWith({
      kind: 'key',
      params: {
        type: 'keyDown',
        key: 'A',
        code: 'KeyA',
        location: 0,
        autoRepeat: false,
        modifiers: 0,
        windowsVirtualKeyCode: 65,
        nativeVirtualKeyCode: 65,
        text: 'A',
      },
    });
    component.onKeyUp({
      ...keyDown,
      preventDefault: vi.fn(),
    } as unknown as KeyboardEvent);
    expect(controller.sendInput).toHaveBeenLastCalledWith({
      kind: 'key',
      params: {
        type: 'keyUp',
        key: 'A',
        code: 'KeyA',
        location: 0,
        modifiers: 0,
        windowsVirtualKeyCode: 65,
        nativeVirtualKeyCode: 65,
      },
    });

    controller.sendInput.mockClear();
    component.composing = true;
    component.onKeyDown(keyDown);
    expect(controller.sendInput).not.toHaveBeenCalled();
    component.composing = false;
    controller.pageState.set(page({baton: 'agent'}));
    fixture.detectChanges();
    TestBed.flushEffects();
    component.onWheel(wheel);
    component.onKeyDown(keyDown);
    expect(controller.sendInput).not.toHaveBeenCalled();
  });

  it('pastes the local clipboard as a remote text insertion', async () => {
    const component = fixture.componentInstance;
    const surface = fixture.nativeElement.querySelector('canvas') as HTMLCanvasElement;
    controller.connectionStatus.set('ready');
    controller.pageState.set(page({baton: 'user'}));
    fixture.detectChanges();
    surface.focus();

    const readText = vi.fn().mockResolvedValue('hunter2');
    Object.defineProperty(navigator, 'clipboard', {
      value: {readText},
      configurable: true,
    });
    try {
      const chord = {
        altKey: false,
        ctrlKey: true,
        metaKey: false,
        shiftKey: false,
        key: 'v',
        code: 'KeyV',
        location: 0,
        repeat: false,
        isComposing: false,
        preventDefault: vi.fn(),
      } as unknown as KeyboardEvent;
      component.onKeyDown(chord);
      expect(chord.preventDefault).toHaveBeenCalledOnce();
      await Promise.resolve();
      await Promise.resolve();
      expect(controller.sendInput).toHaveBeenLastCalledWith({
        kind: 'insertText',
        params: {text: 'hunter2'},
      });
      // The chord itself must never be forwarded as a remote key event.
      expect(
        controller.sendInput.mock.calls.every(call => call[0].kind !== 'key'),
      ).toBe(true);

      controller.sendInput.mockClear();
      const menuPaste = {
        clipboardData: {getData: vi.fn(() => 'from-menu')},
        preventDefault: vi.fn(),
      } as unknown as ClipboardEvent;
      component.onPaste(menuPaste);
      expect(controller.sendInput).toHaveBeenLastCalledWith({
        kind: 'insertText',
        params: {text: 'from-menu'},
      });
      expect(menuPaste.preventDefault).toHaveBeenCalledOnce();
    } finally {
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        configurable: true,
      });
    }
  });

  it('automates the baton across agent turn boundaries', () => {
    const bridge = TestBed.inject(PersistentThreadTransportBridge);
    controller.connectionStatus.set('ready');
    controller.pageState.set(page({baton: 'user'}));
    fixture.detectChanges();
    TestBed.flushEffects();
    // The first observation only records the baseline.
    expect(controller.sendControl).not.toHaveBeenCalled();

    // Agent turn starts (user sent a message) -> control returns to the agent.
    bridge.setAgentTurnActive(true);
    TestBed.flushEffects();
    expect(controller.sendControl).toHaveBeenLastCalledWith({op: 'release_baton'});

    // Agent turn completes while connected -> the user gets the baton.
    controller.sendControl.mockClear();
    controller.pageState.set(page({baton: 'agent'}));
    bridge.setAgentTurnActive(false);
    TestBed.flushEffects();
    expect(controller.sendControl).toHaveBeenLastCalledWith({op: 'take_baton'});

    // A turn ending while the surface is not connected takes nothing.
    controller.sendControl.mockClear();
    bridge.setAgentTurnActive(true);
    TestBed.flushEffects();
    controller.sendControl.mockClear();
    controller.connectionStatus.set('ended');
    bridge.setAgentTurnActive(false);
    TestBed.flushEffects();
    expect(controller.sendControl).not.toHaveBeenCalledWith({op: 'take_baton'});
  });

  it('best-effort releases held pointer and keys on blur or authoritative baton loss', () => {
    const component = fixture.componentInstance;
    const surface = fixture.nativeElement.querySelector('canvas') as HTMLCanvasElement;
    controller.connectionStatus.set('ready');
    controller.pageState.set(page({baton: 'user'}));
    fixture.detectChanges();
    surface.focus();
    vi.spyOn(surface, 'getBoundingClientRect').mockReturnValue(
      {left: 0, top: 0, width: 1280, height: 720} as DOMRect,
    );
    Object.defineProperty(surface, 'setPointerCapture', {value: vi.fn()});
    Object.defineProperty(surface, 'releasePointerCapture', {value: vi.fn()});
    const modifiers = {
      altKey: false,
      ctrlKey: false,
      metaKey: false,
      shiftKey: false,
    };
    component.onPointerDown({
      ...modifiers,
      clientX: 10,
      clientY: 20,
      pointerId: 1,
      isPrimary: true,
      button: 0,
      buttons: 1,
      detail: 1,
      preventDefault: vi.fn(),
    } as unknown as PointerEvent);
    component.onKeyDown({
      ...modifiers,
      key: 'Control',
      code: 'ControlLeft',
      location: 1,
      repeat: false,
      isComposing: false,
      preventDefault: vi.fn(),
    } as unknown as KeyboardEvent);

    controller.sendInput.mockClear();
    component.releaseHeldInput();
    expect(controller.sendInput.mock.calls.map(call => call[0])).toEqual([
      {
        kind: 'mouse',
        params: {
          type: 'mouseReleased',
          x: 10,
          y: 20,
          button: 'left',
          buttons: 0,
          modifiers: 0,
          clickCount: 1,
        },
      },
      {
        kind: 'key',
        params: {
          type: 'keyUp',
          key: 'Control',
          code: 'ControlLeft',
          location: 1,
          modifiers: 0,
          windowsVirtualKeyCode: 17,
          nativeVirtualKeyCode: 17,
        },
      },
    ]);

    controller.sendInput.mockClear();
    component.onKeyDown({
      ...modifiers,
      key: 'Shift',
      code: 'ShiftLeft',
      location: 1,
      repeat: false,
      isComposing: false,
      preventDefault: vi.fn(),
    } as unknown as KeyboardEvent);
    controller.sendInput.mockClear();
    controller.pageState.set(page({baton: 'agent'}));
    fixture.detectChanges();
    TestBed.flushEffects();
    expect(controller.sendInput).toHaveBeenCalledWith({
      kind: 'key',
      params: {
        type: 'keyUp',
        key: 'Shift',
        code: 'ShiftLeft',
        location: 1,
        modifiers: 0,
        windowsVirtualKeyCode: 16,
        nativeVirtualKeyCode: 16,
      },
    });
  });

  function resizeStage(width: number, height: number): void {
    const entry = {contentRect: {width, height}} as unknown as ResizeObserverEntry;
    for (const callback of resizeCallbacks) callback([entry], {} as ResizeObserver);
    TestBed.flushEffects();
    fixture.detectChanges();
  }

  function drive(viewportControl = true): void {
    controller.connectionStatus.set('ready');
    controller.viewportControl.set(viewportControl);
    controller.pageState.set(page({baton: 'user'}));
    TestBed.flushEffects();
    fixture.detectChanges();
  }

  it('places the page 1:1, scales it down only to fit, and centres it on whole pixels', () => {
    expect(fitBrowserFrame({width: 585, height: 480}, {width: 585.6, height: 480.2})).toEqual({
      width: 585, height: 480, left: 0, top: 0,
    });
    expect(fitBrowserFrame({width: 768, height: 1024}, {width: 585, height: 480})).toEqual({
      width: 360, height: 480, left: 112, top: 0,
    });
    expect(fitBrowserFrame({width: 390, height: 844}, {width: 2001, height: 900})).toEqual({
      width: 390, height: 844, left: 805, top: 28,
    });
    expect(fitBrowserFrame({width: 1919, height: 992}, null)).toEqual({
      width: 1919, height: 992, left: 0, top: 0,
    });
    expect(fitBrowserFrame({width: 1919, height: 992}, {width: 0, height: 0}).width).toBe(1919);

    expect(observed).toEqual([fixture.nativeElement.querySelector('.browser-stage')]);
    controller.frameSize.set({width: 768, height: 1024});
    resizeStage(585, 480);
    const surface = fixture.nativeElement.querySelector('canvas') as HTMLCanvasElement;
    expect([surface.style.width, surface.style.height, surface.style.left, surface.style.top])
      .toEqual(['360px', '480px', '112px', '0px']);
  });

  it('hides the size switch when the daemon cannot size its page', () => {
    drive(false);
    resizeStage(585, 480);

    expect(fixture.nativeElement.querySelector('.browser-size')).toBeNull();
    expect(controller.requestViewport).not.toHaveBeenCalled();
  });

  it('sizes the page to the pane in Fit mode once the pane holds still', () => {
    vi.useFakeTimers();
    try {
      resizeStage(585.6, 480.2);
      drive();
      expect(controller.requestViewport).toHaveBeenCalledExactlyOnceWith(585, 480);

      resizeStage(700, 500);
      resizeStage(720, 510);
      expect(controller.requestViewport).toHaveBeenCalledOnce();
      vi.advanceTimersByTime(BROWSER_VIEWPORT_DEBOUNCE_MS);
      TestBed.flushEffects();
      expect(controller.requestViewport).toHaveBeenCalledTimes(2);
      expect(controller.requestViewport).toHaveBeenLastCalledWith(720, 510);

      // STATE updates (title, URL, the viewport itself) never resend.
      controller.pageState.set(page({baton: 'user', viewport: {width: 720, height: 510}}));
      controller.pageState.set(page({baton: 'user', title: 'Other'}));
      TestBed.flushEffects();
      expect(controller.requestViewport).toHaveBeenCalledTimes(2);

      // A hidden pane measures 0x0 and asks for nothing.
      resizeStage(0, 0);
      vi.advanceTimersByTime(BROWSER_VIEWPORT_DEBOUNCE_MS);
      TestBed.flushEffects();
      expect(controller.requestViewport).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('sends the size only while this viewer drives, and again when it takes the baton', () => {
    resizeStage(585, 480);
    controller.connectionStatus.set('ready');
    controller.viewportControl.set(true);
    TestBed.flushEffects();
    fixture.detectChanges();
    expect(controller.requestViewport).not.toHaveBeenCalled();
    const fit = fixture.nativeElement.querySelector('[data-mode="fit"]') as HTMLButtonElement;
    expect(fit.disabled).toBe(true);

    controller.pendingBaton.set('user');
    TestBed.flushEffects();
    expect(controller.requestViewport).not.toHaveBeenCalled();
    controller.pendingBaton.set(null);
    controller.pageState.set(page({baton: 'user'}));
    TestBed.flushEffects();
    expect(controller.requestViewport).toHaveBeenCalledExactlyOnceWith(585, 480);
  });

  it('switches between Fit and the phone, tablet and desktop presets', () => {
    resizeStage(585, 480);
    drive();
    const root = fixture.nativeElement as HTMLElement;
    const group = root.querySelector('.browser-size') as HTMLElement;
    const buttons = [...group.querySelectorAll<HTMLButtonElement>('button')];
    expect(group.getAttribute('role')).toBe('group');
    expect(group.getAttribute('aria-label')).toBe('Page size');
    expect(buttons.map((button) => button.getAttribute('aria-label'))).toEqual([
      'Fit to pane',
      'Phone (390 × 844)',
      'Tablet (768 × 1024)',
      'Desktop (1440 × 900)',
    ]);
    expect(buttons.map((button) => button.getAttribute('aria-pressed'))).toEqual([
      'true', 'false', 'false', 'false',
    ]);

    for (const [index, size] of [[1, [390, 844]], [2, [768, 1024]], [3, [1440, 900]]] as const) {
      buttons[index].click();
      TestBed.flushEffects();
      fixture.detectChanges();
      expect(controller.requestViewport).toHaveBeenLastCalledWith(...size);
      expect(buttons[index].getAttribute('aria-pressed')).toBe('true');
    }
    // A preset ignores pane resizes; Fit goes back to the pane.
    const calls = controller.requestViewport.mock.calls.length;
    resizeStage(600, 500);
    expect(controller.requestViewport).toHaveBeenCalledTimes(calls);
    buttons[0].click();
    TestBed.flushEffects();
    expect(controller.requestViewport).toHaveBeenLastCalledWith(585, 480);
  });

  it('maps input onto the CSS size of the frame on screen, scrollbar included', () => {
    drive(false);
    controller.pageState.set(page({baton: 'user', viewport: {width: 1904, height: 992}}));
    controller.frameSize.set({width: 1919, height: 992});
    fixture.detectChanges();
    const surface = fixture.nativeElement.querySelector('canvas') as HTMLCanvasElement;
    vi.spyOn(surface, 'getBoundingClientRect').mockReturnValue(
      {left: 0, top: 0, width: 1919, height: 992} as DOMRect,
    );

    fixture.componentInstance.onPointerDown(
      {clientX: 1910, clientY: 5, button: 0, buttons: 1, detail: 1, isPrimary: true, pointerId: 1,
       altKey: false, ctrlKey: false, metaKey: false, shiftKey: false,
       preventDefault: vi.fn()} as unknown as PointerEvent,
    );

    expect(controller.sendInput.mock.calls.at(-1)?.[0].params).toMatchObject({x: 1910, y: 5});
  });

  it('gives a typed loopback address http:// before navigating', () => {
    drive(false);
    const component = fixture.componentInstance;
    component.urlValue.set('localhost:4200');
    component.navigate({preventDefault: vi.fn()} as unknown as Event);
    component.urlValue.set('example.com');
    component.navigate({preventDefault: vi.fn()} as unknown as Event);

    expect(controller.sendControl.mock.calls.map((call) => call[0])).toEqual([
      {op: 'navigate', url: 'http://localhost:4200'},
      {op: 'navigate', url: 'https://example.com'},
    ]);
  });

  it('keeps the full page title and URL available when the toolbar truncates them', () => {
    drive(false);
    controller.pageState.set(page({
      baton: 'user',
      title: 'A very long application title that does not fit',
      url: 'http://127.0.0.1:8000/entries?sort=date',
    }));
    TestBed.flushEffects();
    fixture.detectChanges();
    const root = fixture.nativeElement as HTMLElement;

    expect(root.querySelector('.browser-title')?.getAttribute('title')).toBe(
      'A very long application title that does not fit',
    );
    expect(root.querySelector('input')?.getAttribute('title')).toBe(
      'http://127.0.0.1:8000/entries?sort=date',
    );
    // Connected is the normal case; only other states take toolbar room.
    expect(root.querySelector('.browser-connection')).toBeNull();
  });

  it('renders the bounded navigation rejection without detaching the stream', () => {
    controller.connectionStatus.set('ready');
    controller.pageState.set(page({baton: 'user'}));
    controller.errorCode.set('navigation_rejected');
    controller.errorMessage.set('Blocked hostname');
    fixture.detectChanges();

    const alert = fixture.nativeElement.querySelector('.browser-navigation-error') as HTMLElement;
    expect(alert.getAttribute('role')).toBe('alert');
    expect(alert.textContent).toContain('Navigation blocked:');
    expect(alert.textContent).toContain('Blocked hostname');
    expect(controller.connectionStatus()).toBe('ready');
  });

  it('exposes labelled keyboard controls and polite authoritative status', () => {
    controller.connectionStatus.set('ready');
    controller.pageState.set(page({baton: 'user'}));
    fixture.detectChanges();

    const root = fixture.nativeElement as HTMLElement;
    const toolbar = root.querySelector('[role="toolbar"]') as HTMLElement;
    const focusables = [...toolbar.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled)')];
    expect(toolbar.getAttribute('aria-label')).toBe('Shared browser controls');
    expect(focusables.map(element => element.getAttribute('aria-label'))).toEqual([
      'Back',
      'Reload',
      'Release control',
      null,
    ]);
    expect((focusables[3] as HTMLInputElement).labels?.[0]?.textContent).toContain('Address');
    expect(focusables.every(element => element.tabIndex === 0)).toBe(true);
    expect(root.querySelector('.browser-baton')?.getAttribute('aria-live')).toBe('polite');
    expect(root.querySelector('canvas')?.getAttribute('aria-label')).toBe('Shared browser page');
  });

  it('offers manual retry for viewer limits and unavailable streams', () => {
    const component = fixture.componentInstance;
    controller.connectionStatus.set('viewer_limit');
    controller.errorCode.set('viewer_limit');
    fixture.detectChanges();

    let state = fixture.nativeElement.querySelector('.browser-empty-state') as HTMLElement;
    expect(state.getAttribute('role')).toBe('alert');
    expect(state.getAttribute('aria-live')).toBe('polite');
    expect(state.textContent).toContain('Retry connection');
    component.retryConnection();
    expect(controller.retry).toHaveBeenCalledOnce();

    controller.connectionStatus.set('unavailable');
    controller.errorCode.set('browser_workspace_unavailable');
    fixture.detectChanges();
    state = fixture.nativeElement.querySelector('.browser-empty-state') as HTMLElement;
    expect(state.textContent).toContain('Retry connection');
    expect(state.textContent).toContain('Restart browser');
  });

  it('restarts ended generations through the ordinary bounded open workflow', () => {
    const component = fixture.componentInstance;
    controller.connectionStatus.set('ended');
    controller.errorCode.set('browser_generation_ended');
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).toContain('Restart browser');
    component.restartBrowser();
    expect(canvas.openBrowser).toHaveBeenCalledOnce();

    canvas.browserOpenStatus.set('workspace');
    fixture.detectChanges();
    expect(component.restartPending()).toBe(true);
    expect(fixture.nativeElement.textContent).toContain('Ended');
    component.restartBrowser();
    expect(canvas.openBrowser).toHaveBeenCalledOnce();
  });

  it.each([
    ['unavailable', 'shared_browser_disabled', 'Disabled'],
    ['unauthorized', 'browser_unauthorized', 'Unauthorized'],
    ['error', 'invalid_browser_protocol', 'Protocol error'],
  ] as const)('renders terminal %s failures without retry actions', (status, code, copy) => {
    controller.connectionStatus.set(status);
    controller.errorCode.set(code);
    fixture.detectChanges();

    const state = fixture.nativeElement.querySelector('.browser-empty-state') as HTMLElement;
    expect(state.getAttribute('role')).toBe('alert');
    expect(state.textContent).toContain(copy);
    expect(state.querySelector('.browser-lifecycle-actions')).toBeNull();
  });

  it.each([
    ['connecting', 'Connecting'],
    ['reconnecting', 'Reconnecting'],
    ['unavailable', 'Unavailable'],
    ['viewer_limit', 'Viewer limit'],
    ['error', 'Protocol error'],
    ['ended', 'Ended'],
  ] as const)('renders the %s non-frame state as text', (status, copy) => {
    controller.connectionStatus.set(status);
    controller.frame.set(null);
    fixture.detectChanges();

    const state = fixture.nativeElement.querySelector('.browser-empty-state') as HTMLElement;
    expect(state.getAttribute('role')).toBe(
      status === 'connecting' || status === 'reconnecting' ? 'status' : 'alert',
    );
    expect(state.textContent).toContain(copy);
    expect(fixture.nativeElement.querySelector('.browser-renderer').dataset.connectionStatus).toBe(
      status,
    );
  });
});
