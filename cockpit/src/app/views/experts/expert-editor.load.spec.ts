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

    expect(el.querySelector('h1')?.textContent?.trim()).toBe('View');
    expect(editor.form.persona).toBe('<role>Design collaborator</role>');
    const buttons = Array.from(el.querySelectorAll('footer app-button')).map((b) => b.textContent?.trim());
    expect(buttons).toEqual(['Back', 'Duplicate to edit']);

    editor.save();
    expect(api.updateExpert).not.toHaveBeenCalled();

    editor.duplicate();
    expect(api.duplicateExpert).toHaveBeenCalledWith('copy-id');
    expect(router.navigate).toHaveBeenCalledWith(['/experts', 'new-copy', 'edit'], {
      state: {dropped: ['shell_tools'], duplicated: true},
    });
  });
});
