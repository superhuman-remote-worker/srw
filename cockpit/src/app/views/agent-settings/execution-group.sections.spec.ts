import {beforeAll, describe, expect, it} from 'vitest';
import {signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import en from '../../../assets/i18n/en.json';
import {ExecutionGroupComponent, ExecutionSection} from './execution-group.component';
import {UserService} from '../../core/services/user.service';

/**
 * The three-section host mounts one execution group per section. Each
 * instance must render and WRITE only its own rows: a second instance that
 * still emitted `autonomy` or `image_quality` would duplicate a field in the
 * merged config_override, and the last writer would silently win.
 */
function mount(section: ExecutionSection, mode = 'job') {
  TestBed.configureTestingModule({
    imports: [
      ExecutionGroupComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
      }),
    ],
    providers: [{provide: UserService, useValue: {currentUser: signal({is_admin: true})}}],
  });
  const fixture = TestBed.createComponent(ExecutionGroupComponent);
  const stub = (name: string, value: unknown) =>
    Object.defineProperty(fixture.componentInstance, name, {value: () => value});
  stub('mode', mode);
  stub('section', section);
  stub('showProjectMemory', true);
  fixture.detectChanges();
  return fixture;
}

function pinEverything(c: ExecutionGroupComponent): void {
  c.autonomy.set('full');
  c.permissionMode.set('autonomous');
  c.narrationMode.set('verbose');
  c.scholar.set(false);
  c.critic.set(true);
  c.criticRounds.set(2);
  c.projectMemory.set(false);
  c.imageQuality.set('high');
}

function labels(fixture: {nativeElement: unknown}): string[] {
  return Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('.field-label, .toggle-label'))
    .map((el) => (el.textContent ?? '').trim());
}

describe('ExecutionGroupComponent sections', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('task (job): autonomy only', () => {
    const fixture = mount('task');
    pinEverything(fixture.componentInstance);
    expect(fixture.componentInstance.getOverrides()).toEqual({autonomy: 'full'});
    expect(fixture.componentInstance.modifiedCount()).toBe(1);
  });

  it('taskMore (job): scholar, critic and project memory', () => {
    const fixture = mount('taskMore');
    pinEverything(fixture.componentInstance);
    expect(fixture.componentInstance.getOverrides()).toEqual({
      scholar: {enabled: false},
      verification: {enabled: true, max_rounds: 2},
      memory: {project_scoped: false},
    });
  });

  it('task (session): permission mode, no image quality', () => {
    const fixture = mount('task', 'session');
    pinEverything(fixture.componentInstance);
    expect(fixture.componentInstance.getOverrides()).toEqual({
      interactive: {permission_mode: 'autonomous', narration_mode: 'verbose'},
    });
  });

  it('expert: image quality only, and only outside live mode', () => {
    const create = mount('expert', 'session');
    pinEverything(create.componentInstance);
    expect(create.componentInstance.getOverrides()).toEqual({image_quality: 'high'});

    TestBed.resetTestingModule();
    const live = mount('expert', 'live');
    pinEverything(live.componentInstance);
    expect(live.componentInstance.getOverrides()).toEqual({});
  });

  it('live expert section shows image quality locked instead of hiding it', async () => {
    const fixture = mount('expert', 'live');
    // ngModel applies [disabled] asynchronously.
    await fixture.whenStable();
    fixture.detectChanges();
    const select = (fixture.nativeElement as HTMLElement).querySelector('select') as HTMLSelectElement;
    expect(select).not.toBeNull();
    expect(select.disabled).toBe(true);
    expect(labels(fixture).join(' ')).toContain('Fixed at creation');
  });

  it('a sectioned instance renders no group label; "all" keeps it', () => {
    const sectioned = mount('task');
    expect((sectioned.nativeElement as HTMLElement).querySelector('.group-label')).toBeNull();
    TestBed.resetTestingModule();
    const all = mount('all');
    expect((all.nativeElement as HTMLElement).querySelector('.group-label')).not.toBeNull();
  });

  it('"all" keeps the single-group behaviour the Expert editor relies on', () => {
    const fixture = mount('all');
    pinEverything(fixture.componentInstance);
    expect(fixture.componentInstance.getOverrides()).toEqual({
      autonomy: 'full',
      scholar: {enabled: false},
      verification: {enabled: true, max_rounds: 2},
      memory: {project_scoped: false},
      image_quality: 'high',
    });
  });
});
