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
 * The empty state of the to-do view. A job in agent mode has no plan and no
 * phases, so its current list stays empty (or absent) for the whole run; the
 * view says so instead of suggesting the agent is still planning.
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

  it('an agent-mode job with an empty current list says it has no todo list', () => {
    const {hint} = mount([job('job-1', {job_mode: 'agent'})]);
    expect(hint()).toBe(en.todoList.empty.agentModeHint);
    expect(hint()).not.toBe(en.todoList.empty.planningHint);
  });

  it('says the same when an agent-mode job has no current list at all', () => {
    const {hint} = mount([job('job-1', {job_mode: 'agent'})], todoService({current: null}));
    expect(hint()).toBe(en.todoList.empty.agentModeHint);
  });

  it('a worker job keeps the planning hint', () => {
    const {hint} = mount([job('job-1', {job_mode: 'worker'})]);
    expect(hint()).toBe(en.todoList.empty.planningHint);
  });

  it('a row without the field is a worker job', () => {
    const {hint} = mount([job('job-1')]);
    expect(hint()).toBe(en.todoList.empty.planningHint);
  });

  it('reads the mode of the selected job, not of another row', () => {
    const {component} = mount([job('job-1'), job('job-2', {job_mode: 'agent'})]);
    expect(component.currentJobIsAgentMode()).toBe(false);
  });

  it('an empty archive keeps the archive hint, in either mode', () => {
    const {hint} = mount([job('job-1', {job_mode: 'agent'})], todoService({selectedArchive: 'todos_phase_0.yaml'}));
    expect(hint()).toBe(en.todoList.empty.archiveHint);
  });

  it('with no job selected it is not an agent-mode job', () => {
    const {component} = mount([job('job-1', {job_mode: 'agent'})], todoService(), null);
    expect(component.currentJobIsAgentMode()).toBe(false);
  });
});
