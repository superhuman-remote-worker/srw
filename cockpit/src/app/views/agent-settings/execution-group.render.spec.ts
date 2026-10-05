import {beforeAll, describe, expect, it} from 'vitest';
import {signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import en from '../../../assets/i18n/en.json';
import {ExecutionGroupComponent} from './execution-group.component';
import type {TierReachability} from './agent-settings.types';
import {UserService} from '../../core/services/user.service';

/**
 * Where the workspace backend actually SITS.
 *
 * The control was moved out of Advanced → Workspace to be a level-1 setting in
 * the EXECUTION group, between Permission Mode (Autonomy, in job mode) and
 * Image Quality. Placement is the deliverable, so these mount the component and
 * assert against rendered label order rather than component state — a spec that
 * only reads signals would pass with the row anywhere on the page, or nowhere.
 */
function mount(options: {
  mode?: string;
  canUseVm?: boolean;
  liveTier?: string | null;
  tierReachability?: Record<string, TierReachability>;
  upgradeInProgress?: {tier: string; elapsed?: number} | null;
  showWorkspaceBackend?: boolean;
  workspaceBackendLabel?: string;
} = {}) {
  TestBed.configureTestingModule({
    imports: [
      ExecutionGroupComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
      }),
    ],
    providers: [
      {
        provide: UserService,
        useValue: {currentUser: signal(options.canUseVm ? {is_admin: true} : {is_admin: false})},
      },
    ],
  });
  const fixture = TestBed.createComponent(ExecutionGroupComponent);
  // Signal inputs cannot be set through setInput() in this pipeline; see
  // tools-group.render.spec.ts for the reason.
  const stub = (name: string, value: unknown) =>
    Object.defineProperty(fixture.componentInstance, name, {value: () => value});
  stub('mode', options.mode ?? 'session');
  stub('liveTier', options.liveTier ?? null);
  stub('tierReachability', options.tierReachability ?? {});
  stub('upgradeInProgress', options.upgradeInProgress ?? null);
  if (options.showWorkspaceBackend !== undefined) stub('showWorkspaceBackend', options.showWorkspaceBackend);
  if (options.workspaceBackendLabel !== undefined) stub('workspaceBackendLabel', options.workspaceBackendLabel);
  fixture.detectChanges();
  return fixture;
}

function labels(fixture: {nativeElement: unknown}): string[] {
  return Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('.field-label'))
    .map((el) => (el.textContent ?? '').trim());
}

function rowFor(fixture: {nativeElement: unknown}, labelText: string): HTMLElement {
  const row = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('.field-row'))
    .find((el) => (el.querySelector('.field-label')?.textContent ?? '').trim() === labelText);
  if (!row) throw new Error(`no row labelled ${labelText}; saw: ${labels(fixture).join(', ')}`);
  return row as HTMLElement;
}

function selectFor(fixture: {nativeElement: unknown}, labelText: string): HTMLSelectElement {
  const select = rowFor(fixture, labelText).querySelector('select');
  if (!select) throw new Error(`row ${labelText} renders no select`);
  return select as HTMLSelectElement;
}

function optionValues(fixture: {nativeElement: unknown}, labelText: string): string[] {
  return options(fixture, labelText).map((o) => o.value);
}

function options(fixture: {nativeElement: unknown}, labelText: string) {
  return Array.from(rowFor(fixture, labelText).querySelectorAll('option')).map((el) => {
    const o = el as HTMLOptionElement;
    return {value: o.value, label: o.textContent?.trim() ?? '', disabled: o.disabled, selected: o.selected};
  });
}

describe('ExecutionGroupComponent — workspace backend placement', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('sits between Permission Mode and Image Quality in session mode', () => {
    const seen = labels(mount({mode: 'session'}));
    expect(seen).toContain('Workspace');
    expect(seen.indexOf('Workspace')).toBeGreaterThan(seen.indexOf('Permission Mode'));
    expect(seen.indexOf('Workspace')).toBeLessThan(seen.indexOf('Image Quality'));
  });

  it('sits between Autonomy and Image Quality in job mode', () => {
    const seen = labels(mount({mode: 'job'}));
    expect(seen.indexOf('Workspace')).toBeGreaterThan(seen.indexOf('Autonomy'));
    expect(seen.indexOf('Workspace')).toBeLessThan(seen.indexOf('Image Quality'));
  });

  it('is absent in live mode until the running tier is known', () => {
    expect(labels(mount({mode: 'live', liveTier: null}))).not.toContain('Workspace');
  });

  it('offers the VM tier to a user who may run one', () => {
    expect(optionValues(mount({canUseVm: true}), 'Workspace'))
      .toEqual(['sandbox', 'vm', 'virtual', 'none']);
  });

  it('withholds the VM tier from a user who may not', () => {
    expect(optionValues(mount({canUseVm: false}), 'Workspace'))
      .toEqual(['sandbox', 'virtual', 'none']);
  });

  it('is absent when the picker owns the workspace', () => {
    const fixture = mount({mode: 'job', showWorkspaceBackend: false});
    expect(labels(fixture)).not.toContain('Workspace');
  });

  it('can carry the Expert editor label', () => {
    const fixture = mount({mode: 'job', workspaceBackendLabel: 'agentSettings.execution.workspaceRecommended'});
    expect(labels(fixture)).toContain('Recommended workspace');
  });

  it('renders resolved copy, never a raw translation key', () => {
    const text = ((mount().nativeElement as HTMLElement).textContent ?? '');
    expect(text).not.toMatch(/agentSettings\.|advanced\./);
    expect(text).toContain('Container');
  });
});

/**
 * On a running session the row stops being a setting and becomes a launcher
 * for the upgrade verb. What matters is that the refusal is legible BEFORE the
 * click: an unreachable tier renders disabled, carrying its reason.
 */
describe('ExecutionGroupComponent — live tier row', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const LIVE = {
    mode: 'live',
    liveTier: 'virtual',
    tierReachability: {sandbox: 'ok', vm: 'needsApproval', none: 'downgrade'} as Record<string, TierReachability>,
  };

  it('marks the running tier and leaves it selected', () => {
    const opt = options(mount(LIVE), 'Workspace').find((o) => o.value === 'virtual')!;
    expect(opt.label).toContain('current');
    expect(opt.selected).toBe(true);
    expect(opt.disabled).toBe(false);
  });

  it('disables unreachable tiers and says why in the option itself', () => {
    const byValue = Object.fromEntries(
      options(mount(LIVE), 'Workspace').map((o) => [o.value, o]),
    );
    expect(byValue['sandbox'].disabled).toBe(false);
    expect(byValue['vm'].disabled).toBe(true);
    expect(byValue['vm'].label).toContain('needs approval');
    expect(byValue['none'].disabled).toBe(true);
    expect(byValue['none'].label).toContain("can't move down a tier");
  });

  it('emits the pick and snaps straight back to the running tier', () => {
    const fixture = mount(LIVE);
    const emitted: string[] = [];
    fixture.componentInstance.tierChangeRequested.subscribe((t: string) => emitted.push(t));

    const select = selectFor(fixture, 'Workspace');
    select.value = 'sandbox';
    select.dispatchEvent(new Event('change'));

    expect(emitted).toEqual(['sandbox']);
    // The tier moves only when the upgrade lands, so a cancelled confirmation
    // must not leave the control showing a tier the session is not on.
    expect(select.value).toBe('virtual');
  });

  it('re-picking the running tier emits nothing', () => {
    const fixture = mount(LIVE);
    const emitted: string[] = [];
    fixture.componentInstance.tierChangeRequested.subscribe((t: string) => emitted.push(t));

    const select = selectFor(fixture, 'Workspace');
    select.value = 'virtual';
    select.dispatchEvent(new Event('change'));

    expect(emitted).toEqual([]);
  });

  it('locks the control while an upgrade is running, and says so', () => {
    const fixture = mount({...LIVE, upgradeInProgress: {tier: 'sandbox', elapsed: 62}});
    expect(selectFor(fixture, 'Workspace').disabled).toBe(true);
    const text = ((fixture.nativeElement as HTMLElement).textContent ?? '');
    expect(text).toContain('Provisioning Container');
    expect(text).toContain('62');
  });

  it('falls back to static text when nothing is reachable', () => {
    // A `none` session: every option would be refused, so offer no select.
    const fixture = mount({mode: 'live', liveTier: 'none', tierReachability: {}});
    const row = rowFor(fixture, 'Workspace');
    expect(row.querySelector('select')).toBeNull();
    expect(row.textContent).toContain('None (no workspace)');
  });

  it('keeps an unrecognised running tier visible rather than mislabelling it', () => {
    const fixture = mount({mode: 'live', liveTier: 'remote', tierReachability: {sandbox: 'ok'}});
    const opt = options(fixture, 'Workspace').find((o) => o.value === 'remote');
    expect(opt?.selected).toBe(true);
  });

  it('never contributes the tier to the config_override payload', () => {
    // The tier moves through the upgrade verb; letting it ride the pane's
    // debounced config.update would be a second, silent writer.
    const fixture = mount(LIVE);
    fixture.componentInstance.workspaceBackend.set('sandbox');
    expect(fixture.componentInstance.getOverrides()['workspace']).toBeUndefined();
  });
});
