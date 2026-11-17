import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {TestBed} from '@angular/core/testing';
import {ActivatedRoute, convertToParamMap, Router} from '@angular/router';
import {TranslocoPipe, TranslocoTestingModule} from '@jsverse/transloco';
import {of, throwError} from 'rxjs';
import {beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import en from '../../../assets/i18n/en.json';
import {ApiService} from '../../core/services/api.service';
import {ModelService} from '../../core/services/model.service';
import {ExpertEditorComponent} from './expert-editor.component';

// A copy of a bundled expert: export refuses it (409, installed asset), the
// source read does not. The editor used to prefill from export and stayed blank.
const SOURCE = {
  name: 'designer-interactive-copy',
  display_name: 'Design Studio (copy)',
  description: 'Interactive design collaborator',
  icon: 'palette',
  color: '#f5c2e7',
  expert_type: 'session',
  tags: ['design', 'session'],
  config: {llm: {reasoning_level: 'high'}, instruction_files: []},
  prompts: {persona: '<role>Design collaborator</role>'},
};

const api = {
  getExpertSource: vi.fn(),
  exportExpert: vi.fn(),
  updateExpert: vi.fn(),
  duplicateExpert: vi.fn(),
  getMyCapabilities: vi.fn(() => of(null)),
  getExpertDetail: vi.fn(() => of(null)),
  previewToolGroups: vi.fn(() => of(null)),
};
const router = {navigate: vi.fn(), getCurrentNavigation: () => null};

function create(data: Record<string, unknown> = {}) {
  TestBed.configureTestingModule({
    imports: [
      ExpertEditorComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
        preloadLangs: true,
      }),
    ],
    providers: [
      {provide: ApiService, useValue: api},
      {provide: ModelService, useValue: {models: signal([]), load: vi.fn()}},
      {provide: Router, useValue: router},
      {
        provide: ActivatedRoute,
        useValue: {snapshot: {paramMap: convertToParamMap({id: 'copy-id'}), data}},
      },
    ],
  });
  TestBed.overrideComponent(ExpertEditorComponent, {
    set: {imports: [TranslocoPipe, FormsModule], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
  });
  const fixture = TestBed.createComponent(ExpertEditorComponent);
  fixture.detectChanges();
  return fixture;
}

describe('ExpertEditorComponent page shell', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    vi.clearAllMocks();
    api.getExpertSource.mockReturnValue(of(structuredClone(SOURCE)));
  });

  it('offers Cancel and Save in the header on an editable expert, and titles the page with its name', () => {
    const fixture = create();
    fixture.detectChanges();
    const el = fixture.nativeElement as HTMLElement;

    expect(fixture.componentInstance.heading()).toBe('Design Studio (copy)');
    const buttons = Array.from(el.querySelectorAll('[editorActions] app-button')).map((b) => b.textContent?.trim());
    expect(buttons).toEqual(['Cancel', 'Save']);
    expect(el.querySelector('footer')).toBeNull();
  });

  it('shows a save error in the header, which stays in view, not at the foot of the form', () => {
    const fixture = create();
    const editor = fixture.componentInstance;
    editor.form.configText = '{nope';
    editor.save();
    fixture.detectChanges();

    const alert = (fixture.nativeElement as HTMLElement).querySelector('app-editor-header [editorAlert]');
    expect(alert?.textContent).toContain('Config:');
    expect(alert?.getAttribute('role')).toBe('alert');
    expect(api.updateExpert).not.toHaveBeenCalled();
  });

  it('keeps every section mounted while it is closed, so save still reads its controls', () => {
    const fixture = create();
    fixture.detectChanges();
    const el = fixture.nativeElement as HTMLElement;

    // Six sections, one of them open; the groups sit inside the closed ones.
    expect(el.querySelectorAll('app-expander').length).toBe(6);
    for (const tag of ['app-tools-group', 'app-subagents-editor', 'app-execution-group', 'app-advanced-accordion']) {
      expect(el.querySelector(`app-expander ${tag}`), tag).not.toBeNull();
    }
  });

  it('marks expert instructions as deprecated and points to Skills', () => {
    api.getExpertSource.mockReturnValue(of({...structuredClone(SOURCE), expert_type: 'worker', tags: ['worker']}));
    const fixture = create();
    fixture.detectChanges();
    const note = (fixture.nativeElement as HTMLElement).querySelector('.deprecation');

    expect(note?.textContent).toContain('Deprecated');
    expect(note?.textContent).toContain('will be removed in a later release');
    expect(note?.textContent).not.toContain("Sessions don't read");
    expect(note?.querySelector('a')?.getAttribute('routerLink')).toBe('/skills');
  });

  it('tells a session expert that sessions never read its instructions', () => {
    const fixture = create(); // SOURCE is a session expert
    fixture.detectChanges();
    const note = (fixture.nativeElement as HTMLElement).querySelector('.deprecation');

    expect(note?.textContent).toContain("Sessions don't read these instructions");
  });

  it('summarises the prompts for the closed section', () => {
    const editor = create().componentInstance;

    expect(editor.personaSummary()).toBe('Persona, 2 words');
    editor.form.instructions = 'Always write the test first.';
    editor.form.summarization = 'Keep decisions.';
    expect(editor.personaSummary()).toBe('Persona, 2 words · instructions, 5 words · own summarization prompt');
    editor.form.persona = '';
    editor.form.instructions = '';
    editor.form.summarization = '';
    expect(editor.personaSummary()).toBe('Not written yet');
  });

  it('counts the keys only the JSON field carries as settings the expert sets', () => {
    const editor = create().componentInstance;

    // SOURCE: instruction_files is not owned by a control, so it is in the JSON field.
    expect(editor.advancedSummary()).toBe('1 set by this expert');
    editor.form.configText = '';
    expect(editor.advancedSummary()).toBe('Framework defaults');
  });
});

describe('ExpertEditorComponent loading a stored expert', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    vi.clearAllMocks();
    api.getExpertSource.mockReturnValue(of(structuredClone(SOURCE)));
  });

  it('prefills an asset-backed copy from its source, never the export', () => {
    const editor = create().componentInstance;

    expect(api.getExpertSource).toHaveBeenCalledWith('copy-id');
    expect(api.exportExpert).not.toHaveBeenCalled();
    expect(editor.form.display_name).toBe('Design Studio (copy)');
    expect(editor.form.name).toBe('designer-interactive-copy');
    expect(editor.form.expert_type).toBe('session');
    expect(editor.form.persona).toBe('<role>Design collaborator</role>');
    expect(editor.form.tags).toBe('design');
    expect(editor.form.configText).toContain('instruction_files');
    expect(editor.loadError()).toBe('');
  });

  it('says why a failed read left the form empty, and will not save it', () => {
    api.getExpertSource.mockReturnValue(throwError(() => ({error: {detail: 'Expert not found'}})));
    const fixture = create();
    fixture.detectChanges();

    expect(fixture.componentInstance.loadError()).toBe("Couldn't load this expert: Expert not found");
    expect((fixture.nativeElement as HTMLElement).querySelector('.banner.err')?.textContent).toContain(
      'Expert not found',
    );
    fixture.componentInstance.save();
    expect(api.updateExpert).not.toHaveBeenCalled();
  });

  it('the view route shows the expert read-only and duplicates to edit', () => {
    api.duplicateExpert.mockReturnValue(of({id: 'new-copy', dropped: ['shell_tools']}));
    const fixture = create({readOnly: true});
    fixture.detectChanges();
    const el = fixture.nativeElement as HTMLElement;
    const editor = fixture.componentInstance;

    // The page is titled with the expert's own name; the header's back link
    // is the way out, so the only action is the one that makes it editable.
    expect(editor.heading()).toBe('Design Studio (copy)');
    expect(el.querySelector('[editorBadges]')?.textContent).toContain('Read-only');
    expect(editor.form.persona).toBe('<role>Design collaborator</role>');
    const buttons = Array.from(el.querySelectorAll('[editorActions] app-button')).map((b) => b.textContent?.trim());
    expect(buttons).toEqual(['Duplicate to edit']);

    editor.save();
    expect(api.updateExpert).not.toHaveBeenCalled();

    editor.duplicate();
    expect(api.duplicateExpert).toHaveBeenCalledWith('copy-id');
    expect(router.navigate).toHaveBeenCalledWith(['/experts', 'new-copy', 'edit'], {
      state: {dropped: ['shell_tools'], duplicated: true},
    });
  });
});

describe('ExpertEditorComponent "Plans in phases"', () => {
  const WORKER = {
    ...SOURCE,
    name: 'reviewer',
    display_name: 'Reviewer',
    expert_type: 'worker',
    tags: ['worker'],
    config: {llm: {reasoning_level: 'high'}, instruction_files: []},
  };

  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    vi.clearAllMocks();
    api.updateExpert.mockReturnValue(of({}));
  });

  function load(config: Record<string, unknown>, expertType = 'worker', data: Record<string, unknown> = {}) {
    api.getExpertSource.mockReturnValue(of(structuredClone({...WORKER, expert_type: expertType, config})));
    const fixture = create(data);
    fixture.detectChanges();
    const el = fixture.nativeElement as HTMLElement;
    return {
      fixture,
      editor: fixture.componentInstance,
      toggle: () => el.querySelector('[data-testid="expert-phases"]') as (HTMLElement & {checked: boolean; disabled: boolean}) | null,
      savedConfig: () => {
        fixture.componentInstance.save();
        return api.updateExpert.mock.calls.at(-1)![1].config as Record<string, unknown>;
      },
    };
  }

  it('shows the toggle off, labelled and explained, for a worker expert without the key', () => {
    const {toggle} = load(WORKER.config);
    const el = toggle();
    expect(el).not.toBeNull();
    expect(el!.checked).toBe(false);
    expect(el!.disabled).toBe(false);
    expect(el!.textContent?.trim()).toBe('Plans in phases');
    expect((el!.closest('.phases-field') as unknown as {hint: string}).hint).toBe(en.experts.editor.phasesHint);
    expect(en.experts.editor.phasesHint).toContain('Applies to jobs, not to sessions.');
  });

  it('reads phases: true as on, and keeps the key out of the JSON field', () => {
    const {editor, toggle} = load({...WORKER.config, phases: true});
    expect(editor.phases()).toBe(true);
    expect(toggle()!.checked).toBe(true);
    expect(editor.form.configText).not.toContain('phases');
    expect(editor.form.configText).toContain('instruction_files');
  });

  it('reads the server\'s "on" as on and false as off', () => {
    expect(load({phases: 'on'}).editor.phases()).toBe(true);
    TestBed.resetTestingModule();
    expect(load({phases: false}).editor.phases()).toBe(false);
  });

  it('saves no key while off for an expert that has none', () => {
    const {savedConfig} = load(WORKER.config);
    const config = savedConfig();
    expect(config).not.toHaveProperty('phases');
    expect(config['instruction_files']).toEqual([]);
  });

  it('saves phases: true when turned on, and keeps every other key', () => {
    const {editor, fixture, toggle, savedConfig} = load(WORKER.config);
    editor.phases.set(true);
    fixture.detectChanges();
    expect(toggle()!.checked).toBe(true);
    const config = savedConfig();
    expect(config['phases']).toBe(true);
    expect(config['llm']).toEqual({reasoning_level: 'high'});
    expect(config['instruction_files']).toEqual([]);
  });

  it('round-trips a stored value it was not asked to change', () => {
    expect(load({phases: true}).savedConfig()['phases']).toBe(true);
    TestBed.resetTestingModule();
    expect(load({phases: false}).savedConfig()['phases']).toBe(false);
  });

  it('turning a stored phases off writes false', () => {
    const {editor, savedConfig} = load({...WORKER.config, phases: true});
    editor.phases.set(false);
    expect(savedConfig()['phases']).toBe(false);
  });

  it('a phases key typed into the JSON field is saved', () => {
    const {editor, savedConfig} = load(WORKER.config);
    editor.form.configText = '{"instruction_files": [], "phases": true}';
    expect(savedConfig()['phases']).toBe(true);
  });

  it('names the setting in the closed section\'s summary while on', () => {
    const {editor} = load(WORKER.config);
    // The execution group is not mounted here (its children are stubs); the
    // summary reads only these from it.
    const exec = {
      autonomy: () => null, resolvedAutonomy: () => 'review', tierLabel: () => 'Sandbox',
      effectiveBackend: () => 'sandbox', scholar: () => false, resolvedScholar: () => false,
      effectiveCritic: () => false,
    };
    Object.defineProperty(editor, 'execGroup', {value: () => exec});
    expect(editor.defaultsSummary()).not.toContain(en.experts.editor.summary.phases);
    editor.phases.set(true);
    expect(editor.defaultsSummary()).toContain(` · ${en.experts.editor.summary.phases}`);
  });

  it('is disabled on the read-only view', () => {
    const {toggle} = load({phases: true}, 'worker', {readOnly: true});
    expect(toggle()!.disabled).toBe(true);
    expect(toggle()!.checked).toBe(true);
  });

  it('offers no toggle on a session expert, and keeps its stored value', () => {
    const {toggle, savedConfig} = load({...WORKER.config, phases: true}, 'session');
    expect(toggle()).toBeNull();
    expect(savedConfig()['phases']).toBe(true);
  });
});
