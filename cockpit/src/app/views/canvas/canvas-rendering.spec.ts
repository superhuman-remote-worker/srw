import {describe, expect, it} from 'vitest';
import {CanvasState} from '../../core/models/canvas.model';
import {
  CANVAS_FRAME_WRAPPER_MAX_OUTPUT_CHARS,
  CANVAS_MARKDOWN_MAX_SOURCE_CHARS,
  CANVAS_MARKDOWN_MAX_TOKENS,
  CANVAS_MATH_MAX_EXPRESSIONS,
  CANVAS_RENDER_MAX_NODES,
  CANVAS_RENDER_MAX_OUTPUT_CHARS,
  CANVAS_STATIC_MAX_TOTAL_STYLE_CHARS,
  INTERACTIVE_HTML_CSP,
  STATIC_HTML_CSP,
  buildCanvasFrameWrapper,
  canvasSourceKey,
  declaredCanvasRenderer,
  renderCanvasInteractiveHtml,
  renderCanvasMarkdown,
  renderCanvasStaticHtml,
  resolveCanvasContentUrl,
  resolveCanvasViewerBootstrapUrl,
  selectCanvasChromeState,
  selectCanvasRenderer,
  shouldResetCanvasImageZoom,
} from './canvas-rendering';

function state(renderer: CanvasState['renderer'], sourceType = 'workspace_file'): CanvasState {
  return {
    canvas_id: 'main',
    source: sourceType === 'workspace_file'
      ? {type: 'workspace_file', path: 'output/report.md'}
      : {type: sourceType},
    title: 'Report',
    renderer,
    editable: false,
    alt_text: null,
    presentation_revision: 1,
    source_version: 'sha256:one',
    status: 'ready',
    capabilities: {can_edit: false, can_pop_out: false, can_take_control: false},
    updated_at: '2026-07-13T10:00:00Z',
  };
}

describe('Dynamic Canvas rendering trust boundary', () => {
  it.each(['markdown', 'text', 'html', 'html-interactive', 'image'] as const)(
    'selects the trusted %s renderer only for workspace files',
    renderer => {
      expect(selectCanvasRenderer(state(renderer))).toBe(renderer);
      expect(selectCanvasRenderer(state(renderer, 'workspace_app'))).toBe('unsupported');
    },
  );

  it('fails closed when the backend leaves a presented renderer as auto', () => {
    expect(selectCanvasRenderer(state('auto'))).toBe('unsupported');
  });

  it('selects Office only with the positive view capability', () => {
    const unavailable = state('office');
    const available: CanvasState = {
      ...unavailable,
      capabilities: {
        ...unavailable.capabilities,
        can_view_office: true,
      },
    };

    expect(selectCanvasRenderer(unavailable)).toBe('unsupported');
    expect(selectCanvasRenderer({
      ...available,
      capabilities: {...available.capabilities, can_view_office: false},
    })).toBe('unsupported');
    expect(selectCanvasRenderer(available)).toBe('office');
    expect(selectCanvasRenderer({...available, source: {type: 'workspace_app'}}))
      .toBe('unsupported');
  });

  it('selects a live app only with the positive viewer-session capability', () => {
    const unavailable = state('auto', 'workspace_app');
    const available: CanvasState = {
      ...unavailable,
      capabilities: {
        ...unavailable.capabilities,
        can_create_viewer_session: true,
      },
    };

    expect(selectCanvasRenderer(unavailable)).toBe('unsupported');
    expect(selectCanvasRenderer(available)).toBe('app');
    expect(selectCanvasRenderer({...available, renderer: 'html'})).toBe('unsupported');
  });

  it('selects the browser renderer only with source, auto, and stream capability', () => {
    const unavailable = state('auto', 'browser');
    const available: CanvasState = {
      ...unavailable,
      capabilities: {...unavailable.capabilities, can_stream_browser: true},
    };

    expect(selectCanvasRenderer(unavailable)).toBe('unsupported');
    expect(selectCanvasRenderer({
      ...available,
      capabilities: {...available.capabilities, can_stream_browser: false},
    })).toBe('unsupported');
    expect(selectCanvasRenderer({...available, renderer: 'html'})).toBe('unsupported');
    expect(selectCanvasRenderer(available)).toBe('browser');
  });

  it('keys browser lifecycle to the logical presentation revision', () => {
    const opened = state('auto', 'browser');
    const idempotent = {...opened, title: 'Same generation'};
    const restarted = {...opened, presentation_revision: 2};

    expect(canvasSourceKey(opened)).toBe('browser:1');
    expect(canvasSourceKey(idempotent)).toBe(canvasSourceKey(opened));
    expect(canvasSourceKey(restarted)).toBe('browser:2');
  });

  it('preserves image zoom for a same-source refresh and resets on replacement', () => {
    expect(shouldResetCanvasImageZoom(null, 'workspace_file:chart.png')).toBe(false);
    expect(shouldResetCanvasImageZoom('workspace_file:chart.png', 'workspace_file:chart.png'))
      .toBe(false);
    expect(shouldResetCanvasImageZoom('workspace_file:chart.png', 'workspace_file:other.png'))
      .toBe(true);
  });

  it('keeps preserved edit chrome attached to the original source after replacement', () => {
    const original = state('markdown');
    const replacement = {
      ...state('text'),
      source: {type: 'workspace_file', path: 'output/replacement.txt'},
      title: 'Replacement',
      presentation_revision: 2,
    } as CanvasState;

    expect(selectCanvasChromeState(replacement, original, true)).toBe(original);
    expect(selectCanvasChromeState(replacement, original, false)).toBe(replacement);
  });

  it('accepts only complete, versioned server URLs on the configured API origin', () => {
    const pointer =
      '/api/persistent/threads/t-1/canvases/main/content' +
      '?presentation_revision=7&source_fingerprint=fp&source_version=sha256%3Aone&ngsw-bypass=true';
    expect(resolveCanvasContentUrl(pointer, 'https://api.example.test/api', 'https://cockpit.test/'))
      .toBe(`https://api.example.test${pointer}`);

    expect(resolveCanvasContentUrl('https://evil.test/file', 'https://api.example.test/api')).toBeNull();
    expect(resolveCanvasContentUrl('//evil.test/file', 'https://api.example.test/api')).toBeNull();
    expect(resolveCanvasContentUrl('/api/%255c%255cevil.test/file?presentation_revision=1&source_fingerprint=x&source_version=y&ngsw-bypass=true', 'https://api.example.test/api')).toBeNull();
    expect(resolveCanvasContentUrl('/api/file?ngsw-bypass=true', 'https://api.example.test/api')).toBeNull();
  });

  it('accepts only an exact isolated Canvas bootstrap origin and path', () => {
    const origin = 'https://7f2640cb-8584-4ab1-a68e-95b2c9274419.canvas.userland.test';
    const attachmentId = '54f4fd56-69d8-46c8-8ab7-a3349af0d784';
    const bootstrap = `${origin}/_canvas/bootstrap?attachment_id=${attachmentId}`;
    expect(resolveCanvasViewerBootstrapUrl(
      bootstrap,
      origin,
      attachmentId,
      '.canvas.userland.test',
      'https://cockpit.platform.test/sessions/t-1',
    )).toBe(bootstrap);

    const rejected = [
      [`http://7f2640cb.canvas.userland.test/_canvas/bootstrap?attachment_id=${attachmentId}`,
        'http://7f2640cb.canvas.userland.test'],
      [`${origin}/app?attachment_id=${attachmentId}`, origin],
      [`${origin}/_canvas/bootstrap?attachment_id=${attachmentId}#leak`, origin],
      [`${origin}/_canvas/bootstrap?attachment_id=${attachmentId}&extra=1`, origin],
      [`${origin}/_canvas/bootstrap?token=${'t'.repeat(43)}`, origin],
      [`https://user@7f2640cb.canvas.userland.test/_canvas/bootstrap?attachment_id=${attachmentId}`, origin],
      [`https://7f2640cb.canvas.userland.test.evil.test/_canvas/bootstrap?attachment_id=${attachmentId}`,
        'https://7f2640cb.canvas.userland.test.evil.test'],
      [`https://nested.label.canvas.userland.test/_canvas/bootstrap?attachment_id=${attachmentId}`,
        'https://nested.label.canvas.userland.test'],
      [`https://7f2640cb-8584-4ab1-a68e-95b2c9274419.canvas.userland.test:444/_canvas/bootstrap?attachment_id=${attachmentId}`,
        'https://7f2640cb-8584-4ab1-a68e-95b2c9274419.canvas.userland.test:444'],
      [bootstrap, 'https://other.canvas.userland.test'],
      [`https://cockpit.platform.test/_canvas/bootstrap?attachment_id=${attachmentId}`,
        'https://cockpit.platform.test'],
    ] as const;
    for (const [url, expectedOrigin] of rejected) {
      expect(resolveCanvasViewerBootstrapUrl(
        url,
        expectedOrigin,
        attachmentId,
        '.canvas.userland.test',
        'https://cockpit.platform.test/',
      )).toBeNull();
    }
    expect(resolveCanvasViewerBootstrapUrl(bootstrap, origin, attachmentId, null)).toBeNull();
    expect(resolveCanvasViewerBootstrapUrl(
      bootstrap, origin, attachmentId, 'canvas.userland.test',
    )).toBeNull();
    expect(resolveCanvasViewerBootstrapUrl(
      bootstrap,
      origin,
      '5ce49cf9-72e4-4e50-aa4f-f972f63e934d',
      '.canvas.userland.test',
    )).toBeNull();
  });

  it('uses isolated Markdown semantics with bounded math and inert untrusted resources', () => {
    const result = renderCanvasMarkdown(
      '# Report\n\n$E=mc^2$\n\n' +
      '<script>alert(1)</script>\n\n' +
      '![tracking pixel](https://tracker.test/pixel.png)\n\n' +
      '[external](https://example.test/path) [relative](./other.md) [1]',
    );
    expect(result.errorCode).toBeNull();
    const html = result.html;

    expect(html).toContain('class="katex"');
    expect(html).toContain('&lt;script&gt;');
    expect(html).not.toContain('<script');
    expect(html).not.toContain('<img');
    expect(html).not.toContain('tracker.test');
    expect(html).toContain('aria-label="tracking pixel"');
    expect(html).toContain('data-canvas-external="https://example.test/path"');
    expect(html).not.toContain('href="./other.md"');
    expect(html).not.toContain('citation-ref');
    expect(html).toContain('[1]');
  });

  it('bounds adversarial KaTeX work and falls back to escaped text', () => {
    const manyExpressions = Array.from(
      {length: CANVAS_MATH_MAX_EXPRESSIONS + 20},
      (_, index) => `$x_${index}$`,
    ).join(' ');
    const manyResult = renderCanvasMarkdown(manyExpressions);
    expect(manyResult.errorCode).toBeNull();
    const manyHtml = manyResult.html;
    expect((manyHtml.match(/class="katex"/g) ?? []).length).toBe(CANVAS_MATH_MAX_EXPRESSIONS);
    expect(manyHtml).toContain('canvas-math-fallback');

    const oversizedResult = renderCanvasMarkdown(`$${'<script>'.repeat(700)}$`);
    expect(oversizedResult.errorCode).toBeNull();
    const oversized = oversizedResult.html;
    expect(oversized).toContain('canvas-math-fallback');
    expect(oversized).not.toContain('<script>');
    expect(oversized).toContain('&lt;script&gt;');
  });

  it('makes static HTML script-free, non-navigating, and opaque-srcdoc ready', () => {
    const result = renderCanvasStaticHtml(`
      <base href="https://evil.test/">
      <script>parent.location='https://evil.test/'</script>
      <form action="https://evil.test/"><input name="secret"><button>Send</button></form>
      <a id="outside" href="https://evil.test/" onclick="steal()">outside</a>
      <a id="inside" href="#section">inside</a>
      <a id="dangling" href="#missing">missing</a>
      <section id="section">target</section>
      <img id="remote" src="https://tracker.test/pixel.png" onerror="steal()">
      <img id="inline" alt="chart" src="data:image/png;base64,iVBORw0KGgo=">
      <style>@import 'https://evil.test/x.css'; .x { background:url(https://evil.test/x) }</style>
      <div class="x" style="color:red;animation:spin 1s;filter:blur(2px);background-image:url(https://evil.test/y)">content</div>
    `);
    expect(result.errorCode).toBeNull();
    const srcdoc = result.html;

    expect(srcdoc).toContain(STATIC_HTML_CSP.replace(/'/g, '&#39;'));
    expect(srcdoc).not.toMatch(/<script|<form|<input|<button/i);
    expect(srcdoc).not.toMatch(/onerror|onclick|https:\/\/evil\.test|tracker\.test|@import/i);
    const parsed = new DOMParser().parseFromString(srcdoc, 'text/html');
    const headElements = Array.from(parsed.head.children);
    expect(headElements[0]?.getAttribute('http-equiv')).toBe('Content-Security-Policy');
    expect(headElements[1]?.tagName).toBe('BASE');
    expect(headElements[1]?.getAttribute('href')).toBe('about:srcdoc');
    expect(headElements[1]?.getAttribute('target')).toBe('_self');
    expect(parsed.querySelectorAll('base')).toHaveLength(1);
    const outside = parsed.querySelector('#user-content-outside');
    const inside = parsed.querySelector('#user-content-inside');
    const missing = parsed.querySelector('#user-content-dangling');
    const section = parsed.querySelector('#user-content-section');
    const remote = parsed.querySelector('#user-content-remote');
    const inline = parsed.querySelector('#user-content-inline');
    expect(outside?.hasAttribute('href')).toBe(false);
    expect(inside?.getAttribute('href')).toBe('#user-content-section');
    expect(missing?.hasAttribute('href')).toBe(false);
    expect(section?.textContent).toBe('target');
    expect(remote?.hasAttribute('src')).toBe(false);
    expect(inline?.hasAttribute('src')).toBe(false);
    const content = parsed.querySelector('.x');
    expect(parsed.querySelector('style')).toBeNull();
    expect(content?.getAttribute('style')).toBe('color: red;');
    expect(srcdoc).not.toMatch(/animation|filter|background-image/i);
  });

  it('preserves self-contained interactive HTML behind the injected no-network CSP', () => {
    const result = renderCanvasInteractiveHtml(`
      <!doctype html>
      <html>
        <head>
          <base href="https://evil.test/" target="_top">
          <style>:root { --accent: green } .card { color: var(--accent) }</style>
        </head>
        <body>
          <a href="#dashboard">Dashboard</a>
          <section id="dashboard" class="card">Ready</section>
          <script>document.body.dataset.ready = 'true'</script>
        </body>
      </html>
    `);
    expect(result.errorCode).toBeNull();
    const parsed = new DOMParser().parseFromString(result.html, 'text/html');
    const firstHeadElement = parsed.head.firstElementChild;
    expect(firstHeadElement?.getAttribute('http-equiv')).toBe('Content-Security-Policy');
    expect(firstHeadElement?.getAttribute('content')).toBe(INTERACTIVE_HTML_CSP);
    const bases = parsed.querySelectorAll('base');
    expect(bases).toHaveLength(2);
    expect(bases[0].getAttribute('href')).toBe('about:srcdoc');
    expect(bases[0].getAttribute('target')).toBe('_self');
    expect(bases[1].getAttribute('href')).toBe('https://evil.test/');
    expect(parsed.querySelector('style')?.textContent).toContain('var(--accent)');
    expect(parsed.querySelector('script')?.textContent).toContain('dataset.ready');
    expect(parsed.querySelector('a')?.getAttribute('href')).toBe('#dashboard');
  });

  it('nests Canvas srcdoc documents inside renderer-specific navigation ceilings', () => {
    const staticContent = '<!doctype html><html><body>Static</body></html>';
    const staticResult = buildCanvasFrameWrapper(staticContent, 'Static preview', false);
    expect(staticResult.errorCode).toBeNull();
    const staticWrapper = new DOMParser().parseFromString(staticResult.html, 'text/html');
    const staticFrame = staticWrapper.querySelector('iframe');
    expect(staticFrame?.hasAttribute('src')).toBe(false);
    expect(staticFrame?.getAttribute('srcdoc')).toBe(staticContent);
    expect(staticFrame?.getAttribute('sandbox')).toBe('');
    expect(staticWrapper.querySelector('meta[http-equiv="Content-Security-Policy"]')
      ?.getAttribute('content')).toBe(STATIC_HTML_CSP);

    const interactiveContent =
      '<!doctype html><html><body><button onclick="run()">Run</button></body></html>';
    const interactiveResult = buildCanvasFrameWrapper(
      interactiveContent,
      'Interactive preview',
      true,
    );
    expect(interactiveResult.errorCode).toBeNull();
    const interactiveWrapper = new DOMParser().parseFromString(
      interactiveResult.html,
      'text/html',
    );
    const interactiveFrame = interactiveWrapper.querySelector('iframe');
    expect(interactiveFrame?.getAttribute('srcdoc')).toBe(interactiveContent);
    expect(interactiveFrame?.getAttribute('sandbox')).toBe('allow-scripts');
    expect(interactiveFrame?.getAttribute('allow')).toContain("camera 'none'");
    expect(interactiveWrapper.querySelector('meta[http-equiv="Content-Security-Policy"]')
      ?.getAttribute('content')).toBe(INTERACTIVE_HTML_CSP);
    expect(interactiveResult.html.length).toBeLessThan(
      CANVAS_FRAME_WRAPPER_MAX_OUTPUT_CHARS,
    );
  });

  it('attribute-serializes adversarial srcdoc bytes without escaping the wrapper', () => {
    const content =
      '<!doctype html><html><body data-value="&quot;">' +
      '</iframe><script>document.body.dataset.ready="yes"</script>&amp;</body></html>';
    const result = buildCanvasFrameWrapper(content, '"<& preview', true);
    expect(result.errorCode).toBeNull();

    const wrapper = new DOMParser().parseFromString(result.html, 'text/html');
    const frames = wrapper.querySelectorAll('iframe');
    expect(frames).toHaveLength(1);
    expect(frames[0].getAttribute('srcdoc')).toBe(content);
    expect(frames[0].getAttribute('title')).toBe('"<& preview');
    expect(wrapper.body.querySelector('script')).toBeNull();
  });

  it('rejects a wrapper input that bypasses the inner-document output ceiling', () => {
    expect(buildCanvasFrameWrapper(
      'x'.repeat(CANVAS_RENDER_MAX_OUTPUT_CHARS + 1),
      'Oversized',
      true,
    )).toEqual({html: '', errorCode: 'canvas_content_too_complex'});
  });

  it('rejects Markdown token bombs before lexing while accepting plain text at the file ceiling', () => {
    const listBomb = '- x\n'.repeat(Math.floor(CANVAS_MARKDOWN_MAX_SOURCE_CHARS / 4));
    expect(listBomb.length).toBe(CANVAS_MARKDOWN_MAX_SOURCE_CHARS);
    expect(renderCanvasMarkdown(listBomb)).toEqual({
      html: '',
      errorCode: 'canvas_content_too_complex',
    });

    const plainText = 'x'.repeat(CANVAS_MARKDOWN_MAX_SOURCE_CHARS - 1);
    const accepted = renderCanvasMarkdown(plainText);
    expect(accepted.errorCode).toBeNull();
    expect(accepted.html.length).toBeGreaterThanOrEqual(plainText.length);
    expect(accepted.html.length).toBeLessThan(CANVAS_RENDER_MAX_OUTPUT_CHARS);
  });

  it('allows only bounded inert inline CSS and rejects an aggregate style bomb', () => {
    const unsafe = renderCanvasStaticHtml(
      `<div style="position:fixed;color:blue;transform:scale(100);width:var(--secret)">safe</div>`,
    );
    expect(unsafe.errorCode).toBeNull();
    const parsed = new DOMParser().parseFromString(unsafe.html, 'text/html');
    expect(parsed.querySelector('div')?.getAttribute('style')).toBe('color: blue;');

    const style = `font-family:${'a'.repeat(500)}`;
    const elementCount = Math.ceil(CANVAS_STATIC_MAX_TOTAL_STYLE_CHARS / style.length) + 2;
    const styleBomb = `<span style="${style}">x</span>`.repeat(elementCount);
    expect(renderCanvasStaticHtml(styleBomb)).toEqual({
      html: '',
      errorCode: 'canvas_content_too_complex',
    });
  });

  // Builds ~10k-20k spans; the default 5 s timeout flakes under full-suite load.
  it('accepts the exact sanitized node ceiling and rejects one additional text node', () => {
    const exact = '<span>x</span>'.repeat(CANVAS_RENDER_MAX_NODES / 2);
    const accepted = renderCanvasStaticHtml(exact);
    expect(accepted.errorCode).toBeNull();
    expect(accepted.html.length).toBeGreaterThan(0);

    const rejected = renderCanvasStaticHtml(`${exact}x`);
    expect(rejected).toEqual({html: '', errorCode: 'canvas_content_too_complex'});
  }, 30_000);

  it('allows a near-2 MiB text node without multiplying it into DOM nodes', () => {
    const source = 'x'.repeat(2 * 1024 * 1024 - 1);
    const result = renderCanvasStaticHtml(source);
    expect(result.errorCode).toBeNull();
    expect(result.html.length).toBeGreaterThanOrEqual(source.length);
    expect(result.html.length).toBeLessThan(CANVAS_RENDER_MAX_OUTPUT_CHARS);
  });

  // Builds ~10k-20k spans; the default 5 s timeout flakes under full-suite load.
  it('rejects pathological Markdown and HTML with a fixed, source-free result', () => {
    const marker = '<script>do-not-reflect-me</script>';
    const markdown = Array.from(
      {length: CANVAS_MARKDOWN_MAX_TOKENS + 1},
      (_, index) => `- item ${index}`,
    ).join('\n') + marker;
    expect(renderCanvasMarkdown(markdown)).toEqual({
      html: '',
      errorCode: 'canvas_content_too_complex',
    });

    const html = '<span>x</span>'.repeat(CANVAS_RENDER_MAX_NODES + 1) + marker;
    const rejected = renderCanvasStaticHtml(html);
    expect(rejected).toEqual({html: '', errorCode: 'canvas_content_too_complex'});
    expect(rejected.html).not.toContain('do-not-reflect-me');
  }, 30_000);
});

describe('declared renderer labelling', () => {
  it.each(['markdown', 'text', 'html', 'html-interactive', 'image', 'office'] as const)(
    'reports %s regardless of runtime capability or loaded content',
    renderer => {
      // selectCanvasRenderer answers "what can mount right now" and degrades to
      // unsupported without capabilities; the label must not inherit that.
      expect(declaredCanvasRenderer(state(renderer))).toBe(renderer);
    },
  );

  it('names live-app and browser sources by their kind', () => {
    expect(declaredCanvasRenderer(state('auto', 'workspace_app'))).toBe('app');
    expect(declaredCanvasRenderer(state('auto', 'browser'))).toBe('browser');
  });

  it('reserves unsupported for a genuinely unknown source or renderer', () => {
    expect(declaredCanvasRenderer(null)).toBe('unsupported');
    expect(declaredCanvasRenderer({...state('markdown'), source: null})).toBe('unsupported');
    expect(declaredCanvasRenderer(state('mystery' as CanvasState['renderer'])))
      .toBe('unsupported');
  });
});
