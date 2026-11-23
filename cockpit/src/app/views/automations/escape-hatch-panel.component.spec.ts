import {describe, expect, it} from 'vitest';
import {Injector, runInInjectionContext} from '@angular/core';
import {EscapeHatchPanelComponent} from './escape-hatch-panel.component';
import {environment} from '../../core/environment';

/** Construct the component without compiling its template so the spec
 *  stays focused on the small bit of logic worth testing: the API docs
 *  URL derivation from environment.apiUrl. The template itself is pure
 *  router/translation links that get exercised by Playwright. */
function createPanel() {
  const injector = Injector.create({providers: []});
  return runInInjectionContext(injector, () => new EscapeHatchPanelComponent());
}

describe('EscapeHatchPanelComponent', () => {
  it('strips the trailing /api before appending /api/docs (FastAPI Swagger UI path)', () => {
    const panel = createPanel();
    const expected = `${environment.apiUrl.replace(/\/api$/, '')}/api/docs`;
    expect(panel.apiDocsUrl).toBe(expected);
  });

  it('the API docs URL ends with /api/docs', () => {
    const panel = createPanel();
    expect(panel.apiDocsUrl.endsWith('/api/docs')).toBe(true);
  });

  it('the API docs URL never double-suffixes /api/api/docs', () => {
    const panel = createPanel();
    expect(panel.apiDocsUrl).not.toMatch(/\/api\/api\/docs$/);
  });

  it('the guide link opens the public API page, not a moved repository file', () => {
    const panel = createPanel();
    expect(panel.guideUrl).toBe('https://docs.superhuman-remote-worker.com/api/');
  });
});
