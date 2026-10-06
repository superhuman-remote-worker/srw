import {beforeAll, describe, expect, it} from 'vitest';
import {Component, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {AppExpanderComponent} from './expander.component';

/**
 * Signal inputs cannot be bound in this pipeline (no ngtsc for vitest, so
 * `ɵcmp.inputs` is empty — see tools-group.render.spec.ts). Inputs are stubbed
 * on the instance; projection is tested through a host that binds nothing.
 */
function mount(inputs: Record<string, unknown> = {}) {
  TestBed.configureTestingModule({imports: [AppExpanderComponent]});
  const fixture = TestBed.createComponent(AppExpanderComponent);
  for (const [name, value] of Object.entries({
    heading: 'Expert',
    question: 'Who should your AI be?',
    summary: 'Developer · Opus 5.5',
    chip: '2 changed',
    ...inputs,
  })) {
    Object.defineProperty(fixture.componentInstance, name, {value: () => value});
  }
  fixture.detectChanges();
  const el = fixture.nativeElement as HTMLElement;
  return {fixture, el, trigger: el.querySelector('button.expander-trigger') as HTMLButtonElement};
}

@Component({
  standalone: true,
  imports: [AppExpanderComponent],
  template: `<app-expander><input id="probe" /></app-expander>`,
})
class ProjectingHostComponent {}

describe('AppExpanderComponent', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('shows title, question, summary and chip while collapsed', () => {
    const {el, trigger} = mount();
    expect(trigger.getAttribute('aria-expanded')).toBe('false');
    expect(el.querySelector('.expander-title')?.textContent).toContain('Expert');
    expect(el.querySelector('.expander-question')?.textContent).toContain('Who should your AI be?');
    expect(el.querySelector('.expander-summary')?.textContent).toContain('Developer · Opus 5.5');
    expect(el.querySelector('app-badge')?.textContent).toContain('2 changed');
  });

  it('omits the chip and summary rows when they are empty', () => {
    const {el} = mount({chip: '', summary: ''});
    expect(el.querySelector('app-badge')).toBeNull();
    expect(el.querySelector('.expander-summary')).toBeNull();
  });

  it('toggles and ties the trigger to the region it controls', () => {
    const {fixture, el, trigger} = mount();
    const body = el.querySelector('.expander-body') as HTMLElement;
    expect(body.hidden).toBe(true);
    expect(trigger.getAttribute('aria-controls')).toBe(body.id);
    expect(body.getAttribute('role')).toBe('region');
    trigger.click();
    fixture.detectChanges();
    expect(fixture.componentInstance.expanded()).toBe(true);
    expect(trigger.getAttribute('aria-expanded')).toBe('true');
    expect(body.hidden).toBe(false);
  });

  it('keeps collapsed content mounted, so form state survives a collapse', () => {
    TestBed.configureTestingModule({imports: [ProjectingHostComponent]});
    const fixture = TestBed.createComponent(ProjectingHostComponent);
    fixture.detectChanges();
    const el = fixture.nativeElement as HTMLElement;
    const body = el.querySelector('.expander-body') as HTMLElement;
    const probe = el.querySelector('#probe') as HTMLInputElement;
    expect(body.hidden).toBe(true);
    expect(body.contains(probe)).toBe(true);
    probe.value = 'typed';
    const trigger = el.querySelector('button.expander-trigger') as HTMLButtonElement;
    trigger.click();
    fixture.detectChanges();
    trigger.click();
    fixture.detectChanges();
    expect(el.querySelector('#probe')).toBe(probe);
    expect(probe.value).toBe('typed');
  });
});
