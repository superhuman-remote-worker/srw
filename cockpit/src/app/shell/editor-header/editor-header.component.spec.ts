import {Component, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {provideRouter} from '@angular/router';
import {beforeAll, describe, expect, it, vi} from 'vitest';
import {SidebarService} from '../../core/services/sidebar.service';
import {EditorHeaderComponent} from './editor-header.component';

const providers = [
  provideRouter([]),
  // Expanded: the toggle the header carries for phones renders nothing.
  {provide: SidebarService, useValue: {collapsed: signal(false), expand: vi.fn()}},
];

/**
 * Signal inputs cannot be bound in this pipeline (no ngtsc for vitest, so
 * `ɵcmp.inputs` is empty — see expander.component.spec.ts). Inputs are stubbed
 * on the instance; projection is tested through a host that binds nothing.
 */
function mount() {
  TestBed.configureTestingModule({imports: [EditorHeaderComponent], providers});
  const fixture = TestBed.createComponent(EditorHeaderComponent);
  for (const [name, value] of Object.entries({heading: 'Developer', backLabel: 'Experts', backLink: '/experts'})) {
    Object.defineProperty(fixture.componentInstance, name, {value: () => value});
  }
  fixture.detectChanges();
  return fixture.nativeElement as HTMLElement;
}

@Component({
  standalone: true,
  imports: [EditorHeaderComponent],
  template: `
    <app-editor-header>
      <span editorIcon class="icon">i</span>
      <span editorBadges class="badge">worker</span>
      <div editorActions><button type="button">Save</button></div>
      @if (error()) {
        <div editorAlert role="alert">{{ error() }}</div>
      }
    </app-editor-header>
  `,
})
class HostComponent {
  readonly error = signal('');
}

describe('EditorHeaderComponent', () => {
  // The header's icon and sidebar toggle load their styles from files.
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('titles the page with what is being edited and links back to its list', () => {
    const el = mount();

    expect(el.querySelector('h1')?.textContent?.trim()).toBe('Developer');
    const back = el.querySelector('a.eh-back') as HTMLAnchorElement;
    expect(back.textContent).toContain('Experts');
    expect(back.getAttribute('href')).toBe('/experts');
  });

  it('places the icon before the title, the badges after it and the actions at the edge', () => {
    TestBed.configureTestingModule({imports: [HostComponent], providers});
    const fixture = TestBed.createComponent(HostComponent);
    fixture.detectChanges();
    const el = fixture.nativeElement as HTMLElement;
    const titleRow = el.querySelector('.eh-title-row') as HTMLElement;

    expect(Array.from(titleRow.children).map((c) => c.className || c.tagName.toLowerCase())).toEqual([
      'icon',
      'eh-title',
      'badge',
    ]);
    expect(el.querySelector('.eh-actions button')?.textContent).toBe('Save');
  });

  it('shows an alert inside the header, so it stays in view with it', () => {
    TestBed.configureTestingModule({imports: [HostComponent], providers});
    const fixture = TestBed.createComponent(HostComponent);
    fixture.detectChanges();
    const el = fixture.nativeElement as HTMLElement;
    expect(el.querySelector('header [role="alert"]')).toBeNull();

    fixture.componentInstance.error.set('Save failed');
    fixture.detectChanges();
    expect(el.querySelector('header [role="alert"]')?.textContent).toBe('Save failed');
  });
});
