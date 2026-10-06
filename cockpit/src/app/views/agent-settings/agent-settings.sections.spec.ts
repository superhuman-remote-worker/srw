import {beforeAll, describe, expect, it} from 'vitest';
import {Component, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import en from '../../../assets/i18n/en.json';
import {AgentSettingsComponent, changedLeaves} from './agent-settings.component';
import {ModelService} from '../../core/services/model.service';
import {UserService} from '../../core/services/user.service';
import {ApiService} from '../../core/services/api.service';
import {of} from 'rxjs';

describe('changedLeaves', () => {
  it('counts only leaves that differ from the template config', () => {
    const config = {llm: {model: 'm1', reasoning_level: 'high', temperature: 0.3}, image_quality: 'standard'};
    // A prefill that pinned the template's own values is not a change.
    expect(changedLeaves({llm: {reasoning_level: 'high', temperature: 0.3}}, config)).toBe(0);
    expect(changedLeaves({llm: {model: 'm2', reasoning_level: 'high'}}, config)).toBe(1);
    expect(changedLeaves({image_quality: 'high', llm: {top_p: 0.9}}, config)).toBe(2);
  });

  it('compares arrays by value', () => {
    const config = {tools: {shell: ['run_command']}};
    expect(changedLeaves({tools: {shell: ['run_command']}}, config)).toBe(0);
    expect(changedLeaves({tools: {shell: []}}, config)).toBe(1);
  });
});

/** A host that projects into every slot and binds no inputs: signal inputs
 *  cannot be bound in this pipeline (see tools-group.render.spec.ts). */
@Component({
  standalone: true,
  imports: [AgentSettingsComponent],
  template: `
    <app-agent-settings>
      <div settingsTop id="top-probe">task fields</div>
      <div expertPicker id="picker-probe">template grid</div>
      <div workspacePicker id="workspace-probe">workspace picker</div>
      <div connectorsExtra id="connectors-probe">cloud access</div>
    </app-agent-settings>
  `,
})
class ProjectingHostComponent {}

const PROVIDERS = [
  {provide: ModelService, useValue: {models: signal([]), reasoningByModel: signal({})}},
  {provide: UserService, useValue: {currentUser: signal({is_admin: true})}},
  {provide: ApiService, useValue: {getDatasourceIndexStatus: () => of(null)}},
];

function configure(component: unknown) {
  TestBed.configureTestingModule({
    imports: [
      component as never,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
      }),
    ],
    providers: PROVIDERS,
  });
}

function mountProjecting() {
  configure(ProjectingHostComponent);
  const fixture = TestBed.createComponent(ProjectingHostComponent);
  fixture.detectChanges();
  return fixture.nativeElement as HTMLElement;
}

function mount(inputs: Record<string, unknown>) {
  configure(AgentSettingsComponent);
  const fixture = TestBed.createComponent(AgentSettingsComponent);
  for (const [name, value] of Object.entries(inputs)) {
    Object.defineProperty(fixture.componentInstance, name, {value: () => value});
  }
  fixture.detectChanges();
  fixture.detectChanges();
  return {el: fixture.nativeElement as HTMLElement, host: fixture.componentInstance};
}

const LIVE = {mode: 'live', expertName: 'Developer', projectName: 'SRW Platform', liveTier: 'virtual'};

/** Sections by position: Expert, Workspace, Connectors. The host's bindings
 *  into AppExpanderComponent's signal inputs (title, summary, chip) are inert
 *  in this pipeline, so those are asserted on the host's own signals. */
function section(el: HTMLElement, index: 0 | 1 | 2): HTMLElement {
  const expanders = Array.from(el.querySelectorAll('app-expander')) as HTMLElement[];
  expect(expanders).toHaveLength(3);
  return expanders[index];
}
const EXPERT = 0, WORKSPACE = 1, CONNECTORS = 2;

describe('AgentSettingsComponent — three sections', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('renders the task block first, then three sections', () => {
    const el = mountProjecting();
    const block = el.querySelector('.es-block') as HTMLElement;
    expect(block.querySelector('#top-probe')).not.toBeNull();
    const order = Array.from(el.querySelector('.exec-settings')!.children).map((c) => c.tagName.toLowerCase());
    expect(order.slice(0, 4)).toEqual(['section', 'app-expander', 'app-expander', 'app-expander']);
  });

  it('puts each projected piece in its own section', () => {
    const el = mountProjecting();
    expect(section(el, EXPERT).querySelector('#picker-probe')).not.toBeNull();
    expect(section(el, WORKSPACE).querySelector('#workspace-probe')).not.toBeNull();
    expect(section(el, CONNECTORS).querySelector('#connectors-probe')).not.toBeNull();
  });

  it('starts collapsed on the create forms, with the summary and source readable', () => {
    const {host} = mount({mode: 'job', expertName: 'Developer', expertSource: 'project', workspacePicker: true});
    expect(host.expertOpen()).toBe(false);
    expect(host.workspaceOpen()).toBe(false);
    expect(host.connectorsOpen()).toBe(false);
    expect(host.expertSummary()).toContain('Developer');
    expect(host.expertChip()).toEqual({label: 'Project default', tone: 'info'});
    expect(host.connectorsChip().label).toBe('Defaults');
  });

  it('live: Expert opens by default and shows the Expert and project locked, never hidden', () => {
    const {el, host} = mount(LIVE);
    expect(host.expertOpen()).toBe(true);
    expect(host.expertChip().label).toBe('Some settings locked');
    const locked = Array.from(el.querySelectorAll('.locked-field')).map((f) => f.textContent ?? '');
    expect(locked.some((t) => t.includes('Developer') && t.includes('Fixed at creation'))).toBe(true);
    expect(locked.some((t) => t.includes('SRW Platform'))).toBe(true);
  });

  it('live: the workspace section holds the tier row and names the tier', () => {
    const {el, host} = mount(LIVE);
    expect(section(el, WORKSPACE).querySelector('app-execution-group')).not.toBeNull();
    expect(host.workspaceSummary()).toBe('Virtual (cloud files)');
    expect(host.workspaceChip().label).toBe('Upgrade only');
  });

  it('live without the frozen config says so instead of showing fallback values', () => {
    const {el} = mount(LIVE);
    const expert = section(el, EXPERT);
    expect(expert.querySelector('.locked-banner')?.textContent).toContain('could not be loaded');
    expect(expert.querySelector('app-advanced-accordion')).toBeNull();
  });

  it('live with the frozen config shows More locked, from that config', () => {
    const {el} = mount({...LIVE, lockedConfig: {image_quality: 'high'}});
    const expert = section(el, EXPERT);
    expect(expert.querySelector('.locked-banner')?.textContent).toContain('fixed when the session started');
    expect(expert.querySelector('app-advanced-accordion')).not.toBeNull();
  });
});
