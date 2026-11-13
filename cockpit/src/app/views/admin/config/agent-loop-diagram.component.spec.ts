import {afterEach, beforeEach, describe, expect, it} from 'vitest';
import {TestBed} from '@angular/core/testing';
import {AgentLoopDiagramComponent} from './agent-loop-diagram.component';

/**
 * The diagram is purely presentational, so the value here is verifying the SVG
 * template compiles and renders the labels that explain which prompt runs in
 * which agent context. (Page-level rendering is otherwise covered by Playwright
 * on the dev cluster; this is cheap because the component has no deps.)
 */
describe('AgentLoopDiagramComponent', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({imports: [AgentLoopDiagramComponent]});
  });
  afterEach(() => {
    TestBed.resetTestingModule();
  });

  function render(): HTMLElement {
    const fixture = TestBed.createComponent(AgentLoopDiagramComponent);
    fixture.detectChanges();
    return fixture.nativeElement as HTMLElement;
  }

  it('renders an SVG diagram', () => {
    expect(render().querySelector('svg')).toBeTruthy();
  });

  it('shows both agent loops with the worker phase split', () => {
    const text = render().textContent ?? '';
    // worker (job) loop alternates strategic + tactical phases
    expect(text).toContain('WORKER');
    expect(text).toContain('STRATEGIC');
    expect(text).toContain('TACTICAL');
    // persistent (session) loop is a single interactive turn loop
    expect(text).toContain('PERSISTENT');
    expect(text).toContain('LLM');
  });

  it('labels the prompt used in each context', () => {
    const text = render().textContent ?? '';
    // one phase-agnostic worker prompt: no strategic/tactical prompt files
    expect(text).toContain('PROMPT · EVERY LLM CALL');
    expect(text).toContain('system · persona');
    expect(text).not.toContain('strategic.txt');
    expect(text).not.toContain('tactical.txt');
    expect(text).toContain('PROMPT · EVERY TURN');
    expect(text).toContain('interactive');
  });

  it('shows instruction files and their triggers per context', () => {
    const text = render().textContent ?? '';
    expect(text).toContain('INSTRUCTION FILES');
    // worker: each phase opens with its phase skill
    expect(text).toContain('PHASE SKILLS · AT PHASE START');
    expect(text).toContain('strategic-phase');
    expect(text).toContain('tactical-phase');
    // persistent: injected once on session setup
    expect(text).toContain('design_guide.md');
  });
});
