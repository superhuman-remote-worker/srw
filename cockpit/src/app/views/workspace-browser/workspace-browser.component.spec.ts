import {describe, expect, it, beforeAll, beforeEach, vi} from 'vitest';
import {TestBed} from '@angular/core/testing';
import {CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources} from '@angular/core';
import {of} from 'rxjs';
import {WorkspaceBrowserComponent} from './workspace-browser.component';
import {ApiService, RepoFile} from '../../core/services/api.service';
import {DataService} from '../../core/services/data.service';

describe('WorkspaceBrowserComponent file view', () => {
  let api: {listRepoContents: ReturnType<typeof vi.fn>; getRepoFile: ReturnType<typeof vi.fn>};

  beforeAll(async () => {
    // The icon button's external styleUrl is not inlined by the vitest JIT;
    // drain the resource queue so the original imports compile.
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    api = {
      listRepoContents: vi.fn(() => of([])),
      getRepoFile: vi.fn(),
    };
    TestBed.configureTestingModule({
      imports: [WorkspaceBrowserComponent],
      providers: [
        {provide: ApiService, useValue: api},
        {provide: DataService, useValue: {currentJobId: signal<string | null>('job-1')}},
      ],
    });
    // The vitest JIT does not see signal inputs, so the real icon button's
    // required `ariaLabel` throws; the header button is not under test.
    TestBed.overrideComponent(WorkspaceBrowserComponent, {
      set: {imports: [], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
    });
  });

  function render(file: RepoFile) {
    api.getRepoFile.mockReturnValue(of(file));
    const fixture = TestBed.createComponent(WorkspaceBrowserComponent);
    fixture.detectChanges();
    fixture.componentInstance.loadFile(file.path);
    fixture.detectChanges();
    return fixture;
  }

  it('shows a binary file as a note with its size, not as base64 text', () => {
    const fixture = render({
      path: 'output/report.pdf',
      content: 'JVBERi0xLjcK4uPP0w==',
      encoding: 'base64',
      size: 2048,
    });
    const text = (fixture.nativeElement as HTMLElement).textContent ?? '';

    expect(text).toContain('Binary file, no text preview');
    expect(text).toContain('2.0 KB');
    expect(text).not.toContain('JVBERi0');
    expect(text).not.toContain('Failed to load file');
  });

  it('shows a text file as its content', () => {
    const fixture = render({path: 'plan.md', content: '# Plan', encoding: 'utf-8', size: 6});
    const code = (fixture.nativeElement as HTMLElement).querySelector('pre code');

    expect(code?.textContent).toBe('# Plan');
  });

  it('treats a response without encoding as text', () => {
    const fixture = render({path: 'a.txt', content: 'hello', size: 5});
    const code = (fixture.nativeElement as HTMLElement).querySelector('pre code');

    expect(code?.textContent).toBe('hello');
  });
});
