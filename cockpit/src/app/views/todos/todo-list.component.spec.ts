import {CUSTOM_ELEMENTS_SCHEMA, computed, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoPipe, TranslocoService, TranslocoTestingModule} from '@jsverse/transloco';
import {afterEach, beforeAll, describe, expect, it, vi} from 'vitest';
import en from '../../../assets/i18n/en.json';
import {JobSummary} from '../../core/models/audit.model';
import {DataService} from '../../core/services/data.service';
import {TodoService} from '../../core/services/todo.service';
import {TodoListComponent} from './todo-list.component';

/**
 * The empty state of the to-do view. A job that does not plan in phases
 * (`phases: false`) has no plan, so its current list may stay empty (or
 * absent) for the whole run; the view says so instead of suggesting the agent
 * is still planning.
 *
 * The `ui/` children bind signal inputs, which this JIT pipeline cannot
 * property-bind (see job-list.component.spec.ts), so they render as plain
 * custom elements; the assertions read the component's own markup.
 */

function todoService(state: {current?: {todos: unknown[]} | null; selectedArchive?: string | null} = {}) {
  const currentTodos = signal(state.current === undefined ? {todos: []} : state.current);
  const selectedArchive = signal<string | null>(state.selectedArchive ?? null);
  const selectedArchiveData = signal<{todos: unknown[]; summary?: unknown} | null>(
    state.selectedArchive ? {todos: []} : null,
  );
  return {
    isLoading: signal(false),
    error: signal<string | null>(null),
    currentTodos,
    archives: signal([]),
    selectedArchive,
    selectedArchiveData,
    hasWorkspace: signal(true),
    displayTodos: computed(() => selectedArchiveData()?.todos ?? currentTodos()?.todos ?? []),
    isShowingCurrent: computed(() => selectedArchive() === null && currentTodos() !== null),
    progress: computed(() => ({total: 0, completed: 0, inProgress: 0, pending: 0, percentage: 0})),
    failureNote: computed(() => null),
    loadTodos: vi.fn(),
    clear: vi.fn(),
    refresh: vi.fn(),
    showCurrent: vi.fn(),
    selectArchive: vi.fn(),
  };
}

function mount(jobs: Partial<JobSummary>[], todo = todoService(), currentJobId: string | null = 'job-1') {
  TestBed.configureTestingModule({
    imports: [
      TodoListComponent,
      TranslocoTestingModule.forRoot({
        langs: {en},
        translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
      }),
    ],
    providers: [
      {provide: TodoService, useValue: todo},
      {provide: DataService, useValue: {currentJobId: signal(currentJobId), jobs: signal(jobs as JobSummary[])}},
    ],
  });
  TestBed.overrideComponent(TodoListComponent, {
    set: {imports: [TranslocoPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA]},
  });
  const transloco = TestBed.inject(TranslocoService);
  transloco.setTranslation(en, 'en');
  transloco.setActiveLang('en');
  const fixture = TestBed.createComponent(TodoListComponent);
  fixture.detectChanges();
  const root = fixture.nativeElement as HTMLElement;
  return {
    fixture,
    component: fixture.componentInstance,
    hint: () => root.querySelector('[data-testid="todo-empty-hint"]')?.textContent?.trim() ?? null,
  };
}

const job = (id: string, over: Partial<JobSummary> = {}): Partial<JobSummary> => ({
  id, description: id, status: 'processing', created_at: '2026-11-16T08:00:00Z', ...over,
});

describe('TodoListComponent empty state', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  afterEach(() => TestBed.resetTestingModule());

  it('a job without phases and with an empty current list says it has no todo list', () => {
    const {hint} = mount([job('job-1', {phases: false})]);
    expect(hint()).toBe(en.todoList.empty.directTaskHint);
    expect(hint()).not.toBe(en.todoList.empty.planningHint);
  });

  it('says the same when a job without phases has no current list at all', () => {
    const {hint} = mount([job('job-1', {phases: false})], todoService({current: null}));
    expect(hint()).toBe(en.todoList.empty.directTaskHint);
  });

  it('a job that plans in phases keeps the planning hint', () => {
    const {hint} = mount([job('job-1', {phases: true})]);
    expect(hint()).toBe(en.todoList.empty.planningHint);
  });

  it('a job whose value is unknown keeps the planning hint', () => {
    expect(mount([job('job-1', {phases: null})]).hint()).toBe(en.todoList.empty.planningHint);
    TestBed.resetTestingModule();
    expect(mount([job('job-1')]).hint()).toBe(en.todoList.empty.planningHint);
  });

  it('reads the value of the selected job, not of another row', () => {
    const {component} = mount([job('job-1', {phases: true}), job('job-2', {phases: false})]);
    expect(component.currentJobWorksWithoutPhases()).toBe(false);
  });

  it('an empty archive keeps the archive hint, with or without phases', () => {
    const {hint} = mount([job('job-1', {phases: false})], todoService({selectedArchive: 'todos_phase_0.yaml'}));
    expect(hint()).toBe(en.todoList.empty.archiveHint);
  });

  it('with no job selected nothing counts as a job without phases', () => {
    const {component} = mount([job('job-1', {phases: false})], todoService(), null);
    expect(component.currentJobWorksWithoutPhases()).toBe(false);
  });
});
