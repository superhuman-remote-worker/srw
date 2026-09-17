import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {TestBed} from '@angular/core/testing';
import {HttpErrorResponse, HttpEventType, provideHttpClient} from '@angular/common/http';
import {
  HttpTestingController,
  provideHttpClientTesting,
} from '@angular/common/http/testing';
import {TranslocoService} from '@jsverse/transloco';
import {firstValueFrom} from 'rxjs';
import {ApiService, SESSION_TOOL_GROUPS_TIMEOUT_MS} from './api.service';
import {AppToastService} from '../../ui/toast';
import {ErrorMessageService} from './error-message.service';
import type {ThreadUploadedFile, ThreadUploadEvent} from '../models/file.model';
import type {Job, JobCreateRequest} from '../models/api.model';
import jobListPage from '../models/fixtures/job-list-page.json';

describe('ApiService.createJob public wire contract', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({providers: [
      ApiService, provideHttpClient(), provideHttpClientTesting(),
      {provide: AppToastService, useValue: {success: vi.fn()}},
      {provide: TranslocoService, useValue: {translate: (key: string) => key}},
      {provide: ErrorMessageService, useValue: {}},
    ]});
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it.each([
    {representation: 'object', override: {llm: {model: 'gpt-5-mini'}}},
    {representation: 'JSON text', override: '{"llm":{"model":"gpt-5-mini"}}'},
    {representation: 'null', override: null},
  ])('posts modern selection fields once and preserves $representation overrides', async ({override}) => {
    const body: JobCreateRequest = {
      description: 'Create a report', expert: 'developer',
      required_deliverables: ['output/report.txt'], use_datasource_defaults: true,
    };
    const pending = firstValueFrom(api.createJob(body));
    const request = httpMock.expectOne((item) => item.url.endsWith('/jobs'));
    expect(request.request.method).toBe('POST');
    expect(request.request.body).toEqual(body);
    expect(request.request.body).not.toHaveProperty('datasource_ids');
    const response: Job & {existing_extension: {nullable: null}} = {
      id: 'job-1', description: body.description, config_name: 'developer', status: 'created',
      created_at: '2026-09-06T08:00:00Z', assigned_agent_id: null,
      config_override: override, existing_extension: {nullable: null},
    };
    request.flush(response);
    await expect(pending).resolves.toEqual(response);
  });

  it('propagates structured refusal details without retrying or replacing them', async () => {
    const detail = {code: 'contract_conflict', message: 'Review the contract', paths: ['output/report.txt']};
    const pending = firstValueFrom(api.createJob({description: 'Rejected draft', datasource_ids: []}));
    const rejected = expect(pending).rejects.toMatchObject({status: 409, error: {detail}});
    const request = httpMock.expectOne((item) => item.url.endsWith('/jobs'));
    expect(request.request.body.datasource_ids).toEqual([]);
    request.flush({detail}, {status: 409, statusText: 'Conflict'});
    await rejected;
    httpMock.expectNone((item) => item.url.endsWith('/jobs'));
  });
});

describe('ApiService.retryWorkspaceRecovery', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;
  const toast = {danger: vi.fn(), success: vi.fn()};

  beforeEach(() => {
    toast.danger.mockClear();
    toast.success.mockClear();
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: toast},
        {provide: TranslocoService, useValue: {translate: (key: string) => key}},
        {
          provide: ErrorMessageService,
          useValue: {translate: (_e: unknown, key: string) => key},
        },
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('sends the caller-owned request id unchanged and surfaces a 409 as ambiguous', async () => {
    const pending = firstValueFrom(
      api.retryWorkspaceRecovery(
        'job-1',
        '22222222-bbbb-4222-8222-222222222222',
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      ),
    );
    const request = httpMock.expectOne((item) =>
      item.url.endsWith('/jobs/job-1/workspace-recovery/retry'),
    );
    expect(request.request.body).toEqual({
      operation_id: '22222222-bbbb-4222-8222-222222222222',
      request_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    });

    request.flush(
      {detail: {code: 'recovery_superseded', message: 'Read back current state'}},
      {status: 409, statusText: 'Conflict'},
    );

    await expect(pending).resolves.toBeNull();
    expect(toast.danger).toHaveBeenCalledOnce();
  });
});

describe('ApiService.getJobPullRequestStatus', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('reads the server-resolved job PR without sending connector data', async () => {
    const pending = firstValueFrom(api.getJobPullRequestStatus('job-1'));
    const request = httpMock.expectOne((item) =>
      item.url.endsWith('/jobs/job-1/pull-request'),
    );
    expect(request.request.method).toBe('GET');
    expect(request.request.body).toBeNull();
    request.flush({
      forge: 'github',
      repo: 'acme/widget',
      number: 9,
      url: 'https://github.com/acme/widget/pull/9',
      state: 'open',
      head: 'feature/review',
      base: 'main',
      draft: false,
    });

    await expect(pending).resolves.toMatchObject({number: 9, state: 'open'});
  });

  it('creates a review session by naming only the job', async () => {
    const pending = firstValueFrom(api.createJobReviewSession('job-1'));
    const request = httpMock.expectOne((item) =>
      item.url.endsWith('/jobs/job-1/review-session'),
    );
    expect(request.request.method).toBe('POST');
    expect(request.request.body).toEqual({});
    request.flush({job_id: 'job-1', thread_id: 'thread-review', status: 'created'});

    await expect(pending).resolves.toEqual({
      job_id: 'job-1',
      thread_id: 'thread-review',
      status: 'created',
    });
  });
});

describe('ApiService.transcribeVoice', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('POSTs the audio as multipart FormData to the thread transcribe endpoint', async () => {
    const file = new File([new Uint8Array([1, 2, 3])], 'voice.webm', {
      type: 'audio/webm',
    });
    const pending = firstValueFrom(api.transcribeVoice('thread-1', file));

    const req = httpMock.expectOne((r) =>
      r.url.endsWith('/persistent/threads/thread-1/transcribe'),
    );
    expect(req.request.method).toBe('POST');
    expect(req.request.body instanceof FormData).toBe(true);

    req.flush({text: 'hello world'});
    expect(await pending).toEqual({text: 'hello world'});
  });

  it('maps a 204 response to "unavailable"', async () => {
    const blob = new Blob([new Uint8Array([1])], {type: 'audio/webm'});
    const pending = firstValueFrom(api.transcribeVoice('t', blob));

    const req = httpMock.expectOne((r) => r.url.endsWith('/transcribe'));
    req.flush(null, {status: 204, statusText: 'No Content'});
    expect(await pending).toBe('unavailable');
  });

  it('returns null on transport error', async () => {
    const blob = new Blob([new Uint8Array([1])], {type: 'audio/webm'});
    const pending = firstValueFrom(api.transcribeVoice('t', blob));

    const req = httpMock.expectOne((r) => r.url.endsWith('/transcribe'));
    req.flush('error', {status: 500, statusText: 'Server Error'});
    expect(await pending).toBeNull();
  });
});

describe('ApiService datasource policy endpoints', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('requests the filtered cursor catalog', async () => {
    const pending = firstValueFrom(api.getDatasourceCatalog({
      q: 'prod', availability: 'projects', auto_attach: true, ownership: 'mine', limit: 25,
    }));
    const req = httpMock.expectOne(r => r.url.endsWith('/datasources/catalog'));
    expect(req.request.params.get('q')).toBe('prod');
    expect(req.request.params.get('availability')).toBe('projects');
    expect(req.request.params.get('auto_attach')).toBe('true');
    expect(req.request.params.get('ownership')).toBe('mine');
    req.flush({items: [], next_cursor: 'next'});
    await expect(pending).resolves.toEqual({items: [], next_cursor: 'next'});
  });

  it('preserves repeated project ids and lets eligibility failures propagate', async () => {
    const pending = firstValueFrom(api.getEligibleDatasources(['p1', 'p2']));
    const req = httpMock.expectOne(r => r.url.endsWith('/datasources/eligible'));
    expect(req.request.params.getAll('project_id')).toEqual(['p1', 'p2']);
    req.flush({detail: 'unavailable'}, {status: 503, statusText: 'Unavailable'});
    await expect(pending).rejects.toBeTruthy();
  });

  it('loads retained project targets and propagates policy-write conflicts', async () => {
    const targets = firstValueFrom(api.getLinkableDatasourceProjects({
      datasourceId: 'ds1', q: 'app', cursor: 'c1', limit: 20,
    }));
    const read = httpMock.expectOne(r => r.url.endsWith('/projects/linkable-datasource-targets'));
    expect(read.request.params.get('datasource_id')).toBe('ds1');
    expect(read.request.params.get('q')).toBe('app');
    expect(read.request.params.get('cursor')).toBe('c1');
    read.flush({items: [], next_cursor: null});
    await targets;

    const update = firstValueFrom(api.updateDatasource('ds1', {
      scope_mode: 'projects', project_ids: ['p1'], policy_revision: 3,
    }));
    const write = httpMock.expectOne(r => r.url.endsWith('/datasources/ds1'));
    write.flush({detail: 'stale policy'}, {status: 409, statusText: 'Conflict'});
    await expect(update).rejects.toBeTruthy();
  });

  it('requests a cursor page of linkable connectors for one project', async () => {
    const pending = firstValueFrom(api.getLinkableProjectDatasources('project-1', {
      q: 'database', cursor: 'cursor-1', limit: 25,
    }));
    const req = httpMock.expectOne(
      r => r.url.endsWith('/projects/project-1/linkable-datasources'),
    );
    expect(req.request.params.get('q')).toBe('database');
    expect(req.request.params.get('cursor')).toBe('cursor-1');
    expect(req.request.params.get('limit')).toBe('25');
    req.flush({items: [], next_cursor: 'cursor-2'});
    await expect(pending).resolves.toEqual({items: [], next_cursor: 'cursor-2'});
  });
});

describe('ApiService.generateTTS', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('POSTs the content and decodes the base64 audio into an MP3 Blob', async () => {
    const pending = firstValueFrom(
      api.generateTTS('thread-1', 'hello there', {language: 'en'}),
    );

    const req = httpMock.expectOne((r) =>
      r.url.endsWith('/persistent/threads/thread-1/tts'),
    );
    expect(req.request.method).toBe('POST');
    expect(req.request.body.content).toBe('hello there');
    expect(req.request.body.reformulate).toBe(true);

    // base64 of the three bytes [1, 2, 3].
    req.flush({text: 'spoken', audio: btoa('\x01\x02\x03')});

    const result = await pending;
    if (result === null || result === 'unavailable') {
      throw new Error('expected a {text, audio} result');
    }
    expect(result.text).toBe('spoken');
    expect(result.audio).toBeInstanceOf(Blob);
    expect(result.audio.type).toBe('audio/mpeg');
    expect(result.audio.size).toBe(3);
  });

  it('maps a 204 response to "unavailable"', async () => {
    const pending = firstValueFrom(api.generateTTS('t', 'x'));
    const req = httpMock.expectOne((r) => r.url.endsWith('/tts'));
    req.flush(null, {status: 204, statusText: 'No Content'});
    expect(await pending).toBe('unavailable');
  });

  it('returns null on a 502 synthesis failure', async () => {
    const pending = firstValueFrom(api.generateTTS('t', 'x'));
    const req = httpMock.expectOne((r) => r.url.endsWith('/tts'));
    req.flush('synthesis failed', {status: 502, statusText: 'Bad Gateway'});
    expect(await pending).toBeNull();
  });
});

describe('ApiService.planTTS', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('POSTs the content and returns the chunks + rewritten flag', async () => {
    const pending = firstValueFrom(api.planTTS('thread-1', 'a long message'));
    const req = httpMock.expectOne((r) =>
      r.url.endsWith('/persistent/threads/thread-1/tts/plan'),
    );
    expect(req.request.method).toBe('POST');
    expect(req.request.body.content).toBe('a long message');
    req.flush({chunks: ['part one', 'part two'], rewritten: true});
    expect(await pending).toEqual({chunks: ['part one', 'part two'], rewritten: true});
  });

  it('defaults rewritten to false when the field is absent', async () => {
    const pending = firstValueFrom(api.planTTS('t', 'x'));
    const req = httpMock.expectOne((r) => r.url.endsWith('/tts/plan'));
    req.flush({chunks: ['only part']});
    expect(await pending).toEqual({chunks: ['only part'], rewritten: false});
  });

  it('maps a 204 response to "unavailable"', async () => {
    const pending = firstValueFrom(api.planTTS('t', 'x'));
    const req = httpMock.expectOne((r) => r.url.endsWith('/tts/plan'));
    req.flush(null, {status: 204, statusText: 'No Content'});
    expect(await pending).toBe('unavailable');
  });

  it('returns null on error', async () => {
    const pending = firstValueFrom(api.planTTS('t', 'x'));
    const req = httpMock.expectOne((r) => r.url.endsWith('/tts/plan'));
    req.flush('boom', {status: 502, statusText: 'Bad Gateway'});
    expect(await pending).toBeNull();
  });
});

describe('ApiService audit id normalization (Mongo _id / Postgres id)', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('coerces a Postgres integer id + nested request_id to strings (audit)', async () => {
    const pending = firstValueFrom(api.getJobAudit('job-1'));
    const req = httpMock.expectOne((r) => r.url.endsWith('/jobs/job-1/audit'));
    req.flush({
      entries: [{id: 4271, step_type: 'llm', llm: {request_id: 99}}],
      total: 1,
      page: 1,
      pageSize: 50,
      hasMore: false,
    });
    const res = await pending;
    expect(res.entries[0]._id).toBe('4271');
    expect(typeof res.entries[0]._id).toBe('string');
    expect(res.entries[0].llm!.request_id).toBe('99');
  });

  it('leaves a Mongo ObjectId _id untouched (audit)', async () => {
    const pending = firstValueFrom(api.getJobAudit('job-1'));
    const req = httpMock.expectOne((r) => r.url.endsWith('/jobs/job-1/audit'));
    req.flush({
      entries: [{_id: '507f1f77bcf86cd799439011', step_type: 'tool'}],
      total: 1,
      page: 1,
      pageSize: 50,
      hasMore: false,
    });
    const res = await pending;
    expect(res.entries[0]._id).toBe('507f1f77bcf86cd799439011');
  });

  it('coerces chat id + request_id to strings', async () => {
    const pending = firstValueFrom(api.getChatHistory('job-1'));
    const req = httpMock.expectOne((r) => r.url.endsWith('/jobs/job-1/chat'));
    req.flush({
      entries: [{id: 5, request_id: 88}],
      total: 1,
      page: 1,
      pageSize: 50,
      hasMore: false,
    });
    const res = await pending;
    expect(res.entries[0]._id).toBe('5');
    expect(res.entries[0].request_id).toBe('88');
  });

  it('coerces a single LLM request id to string', async () => {
    const pending = firstValueFrom(api.getRequest('7'));
    const req = httpMock.expectOne((r) => r.url.endsWith('/requests/7'));
    req.flush({id: 7, job_id: 'job-1'});
    const res = await pending;
    expect(res!._id).toBe('7');
  });
});

describe('ApiService OKF Knowledge Base datasource index endpoints', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('GETs credential-free datasource index status', async () => {
    const pending = firstValueFrom(api.getDatasourceIndexStatus('kb-1'));
    const req = httpMock.expectOne((r) =>
      r.url.endsWith('/datasources/kb-1/index-status'),
    );
    expect(req.request.method).toBe('GET');
    req.flush({
      datasource_id: 'kb-1',
      status: 'ready',
      indexed_commit: 'abc123',
      last_success_at: '2026-07-11T12:00:00Z',
    });
    expect((await pending)?.indexed_commit).toBe('abc123');
  });

  it('POSTs incremental and full datasource reindex requests', async () => {
    const incremental = firstValueFrom(api.reindexDatasource('kb-1'));
    const req1 = httpMock.expectOne((r) =>
      r.url.endsWith('/datasources/kb-1/reindex') && r.params.get('full') === 'false',
    );
    expect(req1.request.method).toBe('POST');
    req1.flush({status: 'ready', full: false, indexed_commit: 'abc123'});
    expect((await incremental)?.full).toBe(false);

    const full = firstValueFrom(api.reindexDatasource('kb-1', true));
    const req2 = httpMock.expectOne((r) => r.params.get('full') === 'true');
    req2.flush({status: 'ready', full: true, indexed_commit: 'def456'});
    expect((await full)?.full).toBe(true);
  });

  it('persists read-only access when linking an external KB to a project', async () => {
    const pending = firstValueFrom(
      api.linkProjectDatasource('project-1', 'kb-1', {read_only: true}),
    );
    const req = httpMock.expectOne((r) =>
      r.url.endsWith('/projects/project-1/datasources/kb-1'),
    );
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({read_only: true});
    req.flush({status: 'linked'});
    expect(await pending).toEqual({status: 'linked'});
  });
});

describe('ApiService.getSessionToolGroups', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    vi.useFakeTimers();
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('returns the WHOLE answer, not just the four booleans', async () => {
    // `tool_groups` was all the old transport carried, which is why the live
    // pane could show four of twenty-five categories and say nothing about the
    // rest. The provenance, the per-category reasons and the write vocabulary
    // all live in the fields it dropped.
    const body = {
      thread_id: 'thread-1',
      source: 'resolved',
      origin: 'agent_partial',
      observed_at: null,
      degraded_reason: 'this agent image predates GET /session/toolset',
      enumerate_only: {shell: ['run_command']},
      tool_groups: {canvas: true},
      categories: {
        canvas: {state: 'on', settable: true, reason: null, decided_by: 'base', tools: ['get_canvas']},
      },
    };
    const pending = firstValueFrom(api.getSessionToolGroups('thread-1'));
    httpMock
      .expectOne((r) => r.url.endsWith('/persistent/threads/thread-1/tool-groups'))
      .flush(body);
    await expect(pending).resolves.toEqual(body);
    httpMock.verify();
  });

  it('gives up after a deadline instead of hanging the settings pane', async () => {
    // The server now probes the session pod for its real bound toolset, so
    // this request can hang on a wedged agent. `loadThread` forkJoins it and
    // anchors `lastApplied` only once both arms settle — without a deadline
    // the baseline stays unanchored and every later edit is swallowed.
    const pending = firstValueFrom(api.getSessionToolGroups('thread-1'));
    const req = httpMock.expectOne((r) =>
      r.url.endsWith('/persistent/threads/thread-1/tool-groups'),
    );

    expect(req.cancelled).toBe(false);

    await vi.advanceTimersByTimeAsync(SESSION_TOOL_GROUPS_TIMEOUT_MS + 1);

    // Resolves (to the null fallback) rather than never settling, and the
    // in-flight request is actually aborted rather than left dangling.
    await expect(pending).resolves.toBeNull();
    expect(req.cancelled).toBe(true);
    httpMock.verify();
  });

  it('sits above the server-side probe budget so a slow-but-live agent wins', () => {
    expect(SESSION_TOOL_GROUPS_TIMEOUT_MS).toBeGreaterThan(3000);
  });

  it('previewToolGroups POSTs the selection and returns the forecast', async () => {
    const body = {
      source: 'resolved',
      origin: 'prediction',
      observed_at: null,
      prediction_reason: 'no agent exists for an unsaved session',
      enumerate_only: {shell: ['run_command']},
      tool_groups: {canvas: true},
      categories: {
        canvas: {state: 'on', settable: true, reason: null, decided_by: 'base', tools: ['get_canvas']},
      },
    };
    const pending = firstValueFrom(
      api.previewToolGroups({config_name: 'session_base', project_id: 'p1'}),
    );
    const req = httpMock.expectOne((r) => r.url.endsWith('/persistent/tool-groups/preview'));
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({config_name: 'session_base', project_id: 'p1'});
    req.flush(body);
    await expect(pending).resolves.toEqual(body);
    httpMock.verify();
  });

  it('previewToolGroups is deadline-bounded and fails silently to null', async () => {
    // The creation form blocks nothing on this, but a hung request that never
    // settles would leave the tool switches anchored to the config forever.
    const pending = firstValueFrom(api.previewToolGroups({config_name: 'session_base'}));
    const req = httpMock.expectOne((r) => r.url.endsWith('/persistent/tool-groups/preview'));
    await vi.advanceTimersByTimeAsync(SESSION_TOOL_GROUPS_TIMEOUT_MS + 1);
    await expect(pending).resolves.toBeNull();
    expect(req.cancelled).toBe(true);
    httpMock.verify();
  });
});

describe('uploadOneToThread', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  /** The one `done` event, or undefined if the stream never produced one. */
  function doneFiles(events: readonly ThreadUploadEvent[]): ThreadUploadedFile[] | undefined {
    const done = events.find((e) => e.kind === 'done');
    return done?.kind === 'done' ? done.files : undefined;
  }

  it('posts a single file as multipart and returns its server entries', () => {
    const file = new File(['abc'], 'report.pdf', {type: 'application/pdf'});
    const events: ThreadUploadEvent[] = [];

    api.uploadOneToThread('t1', file).subscribe((e) => events.push(e));

    const req = httpMock.expectOne((r) => r.url.endsWith('/persistent/threads/t1/uploads'));
    expect(req.request.method).toBe('POST');
    const body = req.request.body as FormData;
    expect(body.getAll('files').length).toBe(1);

    req.flush({
      thread_id: 't1',
      files: [{name: 'report.pdf', size: 3, mime_type: 'application/pdf', path: 'uploads/report.pdf'}],
    });

    expect(doneFiles(events)).toEqual([
      {name: 'report.pdf', size: 3, mime_type: 'application/pdf', path: 'uploads/report.pdf'},
    ]);
  });

  it('emits fractional progress then the final files', () => {
    // The whole reason app.config.ts drops withFetch(): FetchBackend emits no
    // UploadProgress events, so `reportProgress` there is a silent no-op and a
    // 90MB PDF would sit at 0% and jump to 100%.
    const events: unknown[] = [];
    api.uploadOneToThread('t1', new File(['abc'], 'a.pdf')).subscribe((e) => events.push(e));

    const req = httpMock.expectOne((r) => r.url.endsWith('/persistent/threads/t1/uploads'));
    expect(req.request.reportProgress).toBe(true);

    req.event({type: HttpEventType.UploadProgress, loaded: 50, total: 100});
    // `total` absent — the gotcha. HttpUploadProgressEvent.total is optional
    // (the body length is not always computable) and a consumer that divides
    // by it unguarded renders NaN.
    req.event({type: HttpEventType.UploadProgress, loaded: 100});
    req.flush({thread_id: 't1', files: []});

    expect(events).toEqual([
      {kind: 'progress', loaded: 50, total: 100},
      {kind: 'progress', loaded: 100, total: null},
      {kind: 'done', files: []},
    ]);
  });

  it('returns every extracted member when the file is an archive', () => {
    const zip = new File(['x'], 'bundle.zip', {type: 'application/zip'});
    const events: ThreadUploadEvent[] = [];

    api.uploadOneToThread('t1', zip).subscribe((e) => events.push(e));

    httpMock.expectOne((r) => r.url.endsWith('/persistent/threads/t1/uploads')).flush({
      thread_id: 't1',
      files: [
        {name: 'bundle/a.txt', size: 1, mime_type: 'text/plain', path: 'uploads/bundle/a.txt'},
        {name: 'bundle/b.txt', size: 1, mime_type: 'text/plain', path: 'uploads/bundle/b.txt'},
      ],
    });

    expect(doneFiles(events)?.length).toBe(2);
  });

  it('rethrows the HttpErrorResponse so the caller can read status and detail', () => {
    let err: unknown;
    api.uploadOneToThread('t1', new File([''], 'a.pdf')).subscribe({error: (e) => (err = e)});

    httpMock
      .expectOne((r) => r.url.endsWith('/persistent/threads/t1/uploads'))
      .flush({detail: "File 'a.pdf' exceeds 100MB"}, {status: 413, statusText: 'Payload Too Large'});

    expect((err as HttpErrorResponse).status).toBe(413);
    expect(api.humanizeUploadError(err)).toBe("File 'a.pdf' exceeds 100MB");
  });

  it('unsubscribing cancels the upload without surfacing an error', () => {
    // Chip removal cancels by unsubscribing (§5.4). If that surfaced as an
    // error the caller could not tell it from an outage — Angular reports both
    // as status 0 — which is why cancellation is tracked as explicit intent.
    let errored = false;
    const sub = api
      .uploadOneToThread('t1', new File(['abc'], 'a.pdf'))
      .subscribe({error: () => (errored = true)});
    const req = httpMock.expectOne((r) => r.url.endsWith('/persistent/threads/t1/uploads'));

    sub.unsubscribe();

    expect(req.cancelled).toBe(true);
    expect(errored).toBe(false);
  });

  it('deletes by the uploads-RELATIVE name, encoding each segment', () => {
    // The {path:path} segment is UploadedFile.name, never its `path` field:
    // `uploads/bundle/a.txt` would resolve to uploads/uploads/… and 404.
    // Encoding is per segment so a zip member keeps its separators while a `#`
    // (which would otherwise truncate the URL at the fragment) does not.
    api.deleteThreadUpload('t1', 'bundle/sub/re#port ?.txt').subscribe();

    const req = httpMock.expectOne((r) =>
      r.url.endsWith(
        '/persistent/threads/t1/uploads/bundle/sub/re%23port%20%3F.txt',
      ),
    );
    expect(req.request.method).toBe('DELETE');
    req.flush({thread_id: 't1', path: 'uploads/bundle/sub/re#port ?.txt', deleted: true});
  });
});

describe('ApiService officer post endpoints (knowledge-base/knowledge/features/officer_post.md)', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('reads the post and degrades to null only on transport failure', async () => {
    const pending = firstValueFrom(api.getOfficerPost('p-1'));
    const request = httpMock.expectOne((r) => r.url.endsWith('/projects/p-1/officer'));
    expect(request.request.method).toBe('GET');
    request.flush('boom', {status: 500, statusText: 'Server Error'});
    await expect(pending).resolves.toBeNull();
  });

  it('commissions with the optional partial config body', async () => {
    const body = {slots: {line: {count: 2, backend: 'sandbox'}}};
    const pending = firstValueFrom(api.commissionOfficer('p-1', body));
    const request = httpMock.expectOne((r) =>
      r.url.endsWith('/projects/p-1/officer/commission'),
    );
    expect(request.request.method).toBe('POST');
    expect(request.request.body).toEqual(body);
    request.flush({thread_id: 't-1', status: 'commissioned'});
    await expect(pending).resolves.toMatchObject({thread_id: 't-1'});
  });

  it('decommission sends {} by default and {force: true} only when forced', async () => {
    const first = firstValueFrom(api.decommissionOfficer('p-1'));
    const warn = httpMock.expectOne((r) =>
      r.url.endsWith('/projects/p-1/officer/decommission'),
    );
    expect(warn.request.body).toEqual({});
    warn.flush({warning: 'jobs in flight', in_flight_jobs: [{job_id: 'j-1'}]});
    await expect(first).resolves.toMatchObject({warning: 'jobs in flight'});

    const second = firstValueFrom(api.decommissionOfficer('p-1', true));
    const forced = httpMock.expectOne((r) =>
      r.url.endsWith('/projects/p-1/officer/decommission'),
    );
    expect(forced.request.body).toEqual({force: true});
    forced.flush({status: 'decommissioned'});
    await expect(second).resolves.toMatchObject({status: 'decommissioned'});
  });

  it('hold sends the trimmed note, or an empty body without one', async () => {
    const noted = firstValueFrom(api.holdOfficer('p-1', '  migration window  '));
    const withNote = httpMock.expectOne((r) =>
      r.url.endsWith('/projects/p-1/officer/hold'),
    );
    expect(withNote.request.body).toEqual({note: 'migration window'});
    withNote.flush({status: 'held'});
    await noted;

    const bare = firstValueFrom(api.holdOfficer('p-1'));
    const blank = httpMock.expectOne((r) =>
      r.url.endsWith('/projects/p-1/officer/hold'),
    );
    expect(blank.request.body).toEqual({});
    blank.flush({status: 'held'});
    await bare;
  });

  it('release POSTs an empty body', async () => {
    const pending = firstValueFrom(api.releaseOfficer('p-1'));
    const request = httpMock.expectOne((r) =>
      r.url.endsWith('/projects/p-1/officer/release'),
    );
    expect(request.request.method).toBe('POST');
    expect(request.request.body).toEqual({});
    request.flush({status: 'released'});
    await pending;
  });

  it('recycle POSTs the supported project-scoped operation', async () => {
    const pending = firstValueFrom(api.recycleOfficer('p-1'));
    const request = httpMock.expectOne((r) =>
      r.url.endsWith('/projects/p-1/officer/recycle'),
    );
    expect(request.request.method).toBe('POST');
    expect(request.request.body).toEqual({});
    request.flush({state: 'recycling', phase: 'awaiting_old_pod_exit'});
    await pending;
  });

  it('PATCHes partial kit fields and the row-only communication_policy', async () => {
    const body = {
      daily_token_ceiling: null,
      sleep_min_minutes: 10,
      communication_policy: {worker_messages: 'officer_first' as const},
    };
    const pending = firstValueFrom(api.updateOfficerPost('p-1', body));
    const request = httpMock.expectOne((r) => r.url.endsWith('/projects/p-1/officer'));
    expect(request.request.method).toBe('PATCH');
    expect(request.request.body).toEqual(body);
    request.flush({status: 'updated'});
    await pending;
  });

  it('lifecycle errors propagate their FastAPI detail — the card owns messaging', async () => {
    const pending = firstValueFrom(api.commissionOfficer('p-1'));
    const request = httpMock.expectOne((r) =>
      r.url.endsWith('/projects/p-1/officer/commission'),
    );
    request.flush(
      {detail: 'post already commissioned'},
      {status: 409, statusText: 'Conflict'},
    );
    await expect(pending).rejects.toMatchObject({
      status: 409,
      error: {detail: 'post already commissioned'},
    });
  });
});

describe('ApiService.getJobProgress (honest liveness, officer_supervision_surface E1/E3)', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('surfaces state/reasons/last_activity_at and tolerates the honest null percent', async () => {
    const pending = firstValueFrom(api.getJobProgress('job-1'));
    const request = httpMock.expectOne((r) => r.url.endsWith('/jobs/job-1/progress'));
    expect(request.request.method).toBe('GET');
    // The exact producer payload: liveness verdict plus null-percent shape
    // compatibility fields. No consumer may re-fabricate a percent from it.
    request.flush({
      job_id: 'job-1',
      status: 'processing',
      state: 'suspected_stuck',
      reasons: ['no audit movement for 41 minutes'],
      last_activity_at: '2026-08-14T11:19:00+00:00',
      observed_at: '2026-08-14T12:00:00+00:00',
      threshold_minutes: 30,
      threshold_source: 'deployment_default',
      sources: [
        {name: 'control_db', status: 'fresh', as_of: '2026-08-14T12:00:00+00:00'},
        {name: 'audit_db', status: 'unavailable', reason: 'timeout'},
      ],
      progress_percent: null,
      elapsed_seconds: 512.5,
      eta_seconds: null,
      created_at: '2026-08-14T11:00:00+00:00',
      updated_at: '2026-08-14T11:50:00+00:00',
      completed_at: null,
    });

    const progress = await pending;
    expect(progress).not.toBeNull();
    expect(progress!.state).toBe('suspected_stuck');
    expect(progress!.reasons).toEqual(['no audit movement for 41 minutes']);
    expect(progress!.last_activity_at).toBe('2026-08-14T11:19:00+00:00');
    expect(progress!.threshold_source).toBe('deployment_default');
    expect(progress!.progress_percent).toBeNull();
    expect(progress!.eta_seconds).toBeNull();
  });

  it('resolves null on transport failure instead of throwing into the view', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      const pending = firstValueFrom(api.getJobProgress('job-1'));
      const request = httpMock.expectOne((r) => r.url.endsWith('/jobs/job-1/progress'));
      request.flush({detail: 'boom'}, {status: 500, statusText: 'Server Error'});
      await expect(pending).resolves.toBeNull();
    } finally {
      consoleError.mockRestore();
    }
  });
});

describe('ApiService.getStuckJobs (server-owned OC-08 policy)', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('omits a caller default and preserves the deployment policy verdict', async () => {
    const pending = firstValueFrom(api.getStuckJobs());
    const request = httpMock.expectOne((r) => r.url.endsWith('/stats/stuck'));
    expect(request.request.params.has('threshold_minutes')).toBe(false);
    request.flush({
      jobs: [{id: 'job-1', status: 'processing', stuck_reason: '41 minutes', stuck_component: 'job'}],
      threshold_minutes: 30,
      threshold_source: 'deployment_default',
    });
    await expect(pending).resolves.toMatchObject({
      threshold_minutes: 30,
      threshold_source: 'deployment_default',
      jobs: [{id: 'job-1'}],
    });
  });

  it('passes an explicit override and reports it without reclassification', async () => {
    const pending = firstValueFrom(api.getStuckJobs(60));
    const request = httpMock.expectOne((r) => r.url.endsWith('/stats/stuck'));
    expect(request.request.params.get('threshold_minutes')).toBe('60');
    request.flush({jobs: [], threshold_minutes: 60, threshold_source: 'request_override'});
    await expect(pending).resolves.toEqual({
      jobs: [],
      threshold_minutes: 60,
      threshold_source: 'request_override',
    });
  });

  it('does not fabricate a deployment threshold when the server is unavailable', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      const pending = firstValueFrom(api.getStuckJobs());
      const request = httpMock.expectOne((r) => r.url.endsWith('/stats/stuck'));
      request.flush({detail: 'boom'}, {status: 500, statusText: 'Server Error'});
      await expect(pending).resolves.toEqual({
        jobs: [],
        threshold_minutes: null,
        threshold_source: 'unavailable',
      });
    } finally {
      consoleError.mockRestore();
    }
  });
});

describe('ApiService project lifecycle', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('sends `status` as a repeatable param, and omits it when unasked', async () => {
    // The server defaults to `active`; asking for both is how a caller says
    // "everything", and there is deliberately no `include_archived` flag.
    const both = firstValueFrom(api.getProjects('user-1', ['active', 'archived']));
    const req = httpMock.expectOne((r) => r.url.endsWith('/projects'));
    expect(req.request.params.getAll('status')).toEqual(['active', 'archived']);
    expect(req.request.params.get('user_id')).toBe('user-1');
    req.flush([]);
    await both;

    const bare = firstValueFrom(api.getProjects());
    const plain = httpMock.expectOne((r) => r.url.endsWith('/projects'));
    expect(plain.request.params.has('status')).toBe(false);
    plain.flush([]);
    await bare;
  });

  it('propagates a failed project list instead of resolving to an empty one', async () => {
    // The old `catchError(() => of([]))` made a 500 indistinguishable from an
    // account with no projects.
    const pending = firstValueFrom(api.getProjects('user-1'));
    httpMock
      .expectOne((r) => r.url.endsWith('/projects'))
      .flush({detail: 'boom'}, {status: 500, statusText: 'Server Error'});

    await expect(pending).rejects.toBeInstanceOf(HttpErrorResponse);
  });

  it('patches status alone, and surfaces the archived refusal', async () => {
    // Status-only body: on an archived project the server accepts nothing else,
    // and refuses the whole request if another field rides along.
    const archiving = firstValueFrom(api.setProjectStatus('p-1', 'archived'));
    const write = httpMock.expectOne((r) => r.url.endsWith('/projects/p-1'));
    expect(write.request.method).toBe('PATCH');
    expect(write.request.body).toEqual({status: 'archived'});
    write.flush({archived: true, loop_paused: true, officer_held: false, jobs_parked: 3});
    await expect(archiving).resolves.toMatchObject({archived: true, jobs_parked: 3});

    const refused = firstValueFrom(api.setProjectStatus('p-1', 'active'));
    httpMock.expectOne((r) => r.url.endsWith('/projects/p-1')).flush(
      {detail: 'This project is archived. Unarchive it before creating new work.'},
      {status: 409, statusText: 'Conflict'},
    );
    await expect(refused).rejects.toBeInstanceOf(HttpErrorResponse);
  });

  it('propagates a refused field edit, where `updateProject` swallows it', async () => {
    // Same endpoint, same body, opposite error contract. `updateProject` maps
    // every failure to `null`, so the archived-project 409 — whose sentence is
    // the whole point — reaches nobody. A sibling rather than a change to it:
    // `null`-on-error is a published shape, and this pins both halves.
    const saved = firstValueFrom(api.updateProjectFields('p-1', {name: 'Renamed'}));
    const write = httpMock.expectOne((r) => r.url.endsWith('/projects/p-1'));
    expect(write.request.method).toBe('PATCH');
    expect(write.request.body).toEqual({name: 'Renamed'});
    write.flush({status: 'updated'});
    await expect(saved).resolves.toEqual({status: 'updated'});

    const detail =
      'This project is archived and is read-only apart from its status. ' +
      'Unarchive it before editing anything else.';
    const refused = firstValueFrom(api.updateProjectFields('p-1', {name: 'Renamed'}));
    httpMock
      .expectOne((r) => r.url.endsWith('/projects/p-1'))
      .flush({detail}, {status: 409, statusText: 'Conflict'});
    await expect(refused).rejects.toBeInstanceOf(HttpErrorResponse);

    const swallowed = firstValueFrom(api.updateProject('p-1', {name: 'Renamed'}));
    httpMock
      .expectOne((r) => r.url.endsWith('/projects/p-1'))
      .flush({detail}, {status: 409, statusText: 'Conflict'});
    await expect(swallowed).resolves.toBeNull();
  });
});

describe('ApiService job list envelope', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('unwraps the envelope so existing callers still receive an array', async () => {
    const pending = firstValueFrom(api.getJobs());
    const request = httpMock.expectOne((item) => item.url.endsWith('/jobs'));
    request.flush({
      jobs: [{id: 'job-1', description: 'x', status: 'completed', created_at: 'now'}],
      total: 806,
      total_is_capped: false,
      has_more: true,
      limit: 100,
      offset: 0,
    });

    const jobs = await pending;
    expect(Array.isArray(jobs)).toBe(true);
    expect(jobs.map((job) => job.id)).toEqual(['job-1']);
  });

  it('keeps the counts a paging UI needs on getJobsPage', async () => {
    const pending = firstValueFrom(api.getJobsPage({limit: 25, offset: 25}));
    const request = httpMock.expectOne((item) => item.url.endsWith('/jobs'));
    expect(request.request.params.get('limit')).toBe('25');
    expect(request.request.params.get('offset')).toBe('25');
    request.flush({
      jobs: [],
      total: 806,
      total_is_capped: false,
      has_more: true,
      limit: 25,
      offset: 25,
      as_of: '2026-08-20T12:00:00Z',
      filters: {include_archived_projects: false},
    });

    const page = await pending;
    expect(page.total).toBe(806);
    expect(page.has_more).toBe(true);
    expect(page.filters?.include_archived_projects).toBe(false);
  });

  it('repeats status, origin and project_id rather than joining them', async () => {
    // Wire shape, not a camelCase options object: job-filters.ts owns the
    // filter-state → params mapping, and a second mapping here would be a
    // second thing to keep in step.
    const pending = firstValueFrom(
      api.getJobsPage({status: ['failed', 'paused'], origin: ['user', 'session'], project_id: ['p1', 'p2']}),
    );
    const request = httpMock.expectOne((item) => item.url.endsWith('/jobs'));
    expect(request.request.params.getAll('status')).toEqual(['failed', 'paused']);
    expect(request.request.params.getAll('origin')).toEqual(['user', 'session']);
    expect(request.request.params.getAll('project_id')).toEqual(['p1', 'p2']);
    request.flush({
      jobs: [],
      total: 0,
      total_is_capped: false,
      has_more: false,
      limit: 100,
      offset: 0,
    });
    await pending;
  });

  it('consumes the backend wire fixture without flattening roots, nulls or filter metadata', async () => {
    // This JSON is checked against the mounted FastAPI route by test_job_list_wire.py.
    const pending = firstValueFrom(api.getJobsPage({
      status: ['completed', 'completed'],
      origin: ['user', 'subjob', 'user'],
      project_id: jobListPage.filters.project_id,
      as_of: jobListPage.as_of,
      limit: 1,
      offset: 2,
    }));
    const request = httpMock.expectOne((item) => item.url.endsWith('/jobs'));
    expect(request.request.params.getAll('origin')).toEqual(['user', 'subjob', 'user']);
    expect(request.request.params.get('as_of')).toBe(jobListPage.as_of);
    request.flush(jobListPage);

    const page = await pending;
    expect(page).toEqual(jobListPage);
    expect(page.jobs.length).toBeGreaterThan(page.limit);
    expect(page.jobs[0].config_name).toBeNull();
    expect(page.jobs[0].audit_count).toBeNull();
    expect(page.filters?.origin).toEqual(['user', 'subjob']);
    expect(page.total).toBe(10_000);
    expect(page.total_is_capped).toBe(true);
  });

  it('preserves false/empty query values, drops absent filters and accepts a skipped total', async () => {
    const stamp = '2026-09-06T11:00:00+02:00';
    const pending = firstValueFrom(api.getJobsPage({
      include_total: false, has_project: false, search: '', origin: [],
      user_id: null, status: undefined, as_of: stamp, offset: 25,
    }));
    const request = httpMock.expectOne((item) => item.url.endsWith('/jobs'));
    expect(request.request.params.get('include_total')).toBe('false');
    expect(request.request.params.get('has_project')).toBe('false');
    expect(request.request.params.get('search')).toBe('');
    for (const key of ['origin', 'user_id', 'status']) {
      expect(request.request.params.has(key)).toBe(false);
    }
    // The '+' in an offset must survive actual URL query encoding.
    expect(new URL(request.request.urlWithParams, 'http://list.test').searchParams.get('as_of')).toBe(stamp);
    const skipped = {...jobListPage, total: null, total_is_capped: false, as_of: stamp};
    request.flush(skipped);
    await expect(pending).resolves.toEqual(skipped);
  });

  it.each([
    [400, {detail: 'Offset exceeds the maximum'}],
    [401, {detail: 'Authentication required'}],
    [403, {detail: 'Access denied by MCP token scope'}],
    [422, {detail: 'Unknown job origin'}],
    [422, {detail: [{loc: ['query', 'limit'], msg: 'Invalid limit', type: 'greater_than_equal'}]}],
    [500, {detail: 'boom'}],
  ])('keeps the synthetic empty fallback for HTTP %s', async (status, body) => {
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    const pending = firstValueFrom(api.getJobsPage());
    const request = httpMock.expectOne((item) => item.url.endsWith('/jobs'));
    request.flush(body, {status: status as number, statusText: 'Request failed'});

    const page = await pending;
    expect(page).toEqual({jobs: [], total: 0, total_is_capped: false, has_more: false, limit: 0, offset: 0});
    expect(page.as_of).toBeUndefined();
    expect(page.filters).toBeUndefined();
    log.mockRestore();
  });
});

describe('ApiService — protected cloud diff outcomes', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;
  const toast = {danger: vi.fn(), info: vi.fn(), success: vi.fn(), warning: vi.fn()};

  beforeEach(() => {
    toast.danger.mockClear();
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: toast},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {translate: (_e: unknown, k: string) => k}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  async function failWith(
    call: Promise<unknown>,
    match: (url: string) => boolean,
    status: number,
    body: unknown,
  ): Promise<unknown> {
    const request = httpMock.expectOne((item) => match(item.url));
    request.flush(body, {status, statusText: 'error'});
    return call;
  }

  it('separates a permission failure from "no changes"', async () => {
    const pending = firstValueFrom(api.getThreadCloudDiffOutcome('t1'));
    await expect(
      failWith(pending, (u) => u.endsWith('/threads/t1/cloud-diff'), 403, {detail: 'nope'}),
    ).resolves.toEqual({kind: 'forbidden'});
  });

  it('reports an offline browser as a network error, not a bare HTTP string', async () => {
    const pending = firstValueFrom(api.getThreadCloudDiffOutcome('t1'));
    const request = httpMock.expectOne((item) => item.url.endsWith('/threads/t1/cloud-diff'));
    request.error(new ProgressEvent('error'), {status: 0, statusText: 'Unknown Error'});
    await expect(pending).resolves.toEqual({
      kind: 'error',
      status: 0,
      detail: 'errors.network',
    });
  });

  it('carries the backend reason behind a per-file 404', async () => {
    // The endpoint 404s for three different situations and the review UI has
    // to explain which; without the code it guessed "the session re-staged".
    const pending = firstValueFrom(api.getThreadCloudDiffFileOutcome('t1', 'a/b.txt'));
    await expect(
      failWith(pending, (u) => u.includes('/cloud-diff/a/b.txt'), 404, {
        detail: {code: 'staged_content_unreadable', message: 'unreadable'},
      }),
    ).resolves.toEqual({kind: 'missing', code: 'staged_content_unreadable'});
  });

  it('still reports a per-file 404 from an older orchestrator', async () => {
    const pending = firstValueFrom(api.getThreadCloudDiffFileOutcome('t1', 'a/b.txt'));
    await expect(
      failWith(pending, (u) => u.includes('/cloud-diff/a/b.txt'), 404, {
        detail: "Path 'a/b.txt' is not in the staged diff.",
      }),
    ).resolves.toEqual({kind: 'missing', code: undefined});
  });

  it('tags a stale reject instead of returning a bare null', async () => {
    const pending = firstValueFrom(api.rejectThreadCloudDiff('t1', 5));
    await expect(
      failWith(pending, (u) => u.endsWith('/cloud-diff/reject'), 409, {
        detail: {code: 'epoch_stale', staged_epoch: 9},
      }),
    ).resolves.toEqual({kind: 'stale', staged_epoch: 9});
    // No toast from the service: the surface renders the outcome itself.
    expect(toast.danger).not.toHaveBeenCalled();
  });

  it('tags an already-resolved reject as nothing_staged', async () => {
    const pending = firstValueFrom(api.rejectThreadCloudDiff('t1', 5));
    await expect(
      failWith(pending, (u) => u.endsWith('/cloud-diff/reject'), 409, {
        detail: {code: 'nothing_staged'},
      }),
    ).resolves.toEqual({kind: 'nothing_staged'});
  });

  it('tags an invalid epoch pin as an actionable error', async () => {
    const pending = firstValueFrom(api.rejectThreadCloudDiff('t1', 5));
    await expect(
      failWith(pending, (u) => u.endsWith('/cloud-diff/reject'), 422, {
        detail: {code: 'invalid_epoch'},
      }),
    ).resolves.toMatchObject({kind: 'error', status: 422});
  });

  it('returns the reject body on success', async () => {
    const pending = firstValueFrom(api.rejectThreadCloudDiff('t1', 5));
    const request = httpMock.expectOne((item) => item.url.endsWith('/cloud-diff/reject'));
    expect(request.request.body).toEqual({epoch: 5});
    request.flush({thread_id: 't1', rejected: true, epoch: 6, overlay_reset: true});
    await expect(pending).resolves.toMatchObject({kind: 'ok'});
  });
});

describe('ApiService.getThreadQueue — the durable queue block', () => {
  let api: ApiService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        ApiService,
        provideHttpClient(),
        provideHttpClientTesting(),
        {provide: AppToastService, useValue: {danger: vi.fn(), info: vi.fn(), success: vi.fn()}},
        {provide: TranslocoService, useValue: {translate: (k: string) => k}},
        {provide: ErrorMessageService, useValue: {translate: (_e: unknown, k: string) => k}},
      ],
    });
    api = TestBed.inject(ApiService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  const block = {
    state: 'done',
    park_reason: null,
    parked_at: null,
    retryable: false,
    attempts: 1,
    pending_input: false,
  };

  // GET …/queue wraps the block, unlike /input and /connection which carry it
  // inline. Without the unwrap every poll resolved to an envelope whose
  // `state` is undefined and the caller dropped it — the poll never applied
  // anything it read.
  it('unwraps the {thread_id, queue} envelope the endpoint actually returns', async () => {
    const pending = firstValueFrom(api.getThreadQueue('t1'));
    const request = httpMock.expectOne((item) => item.url.endsWith('/persistent/threads/t1/queue'));
    expect(request.request.method).toBe('GET');
    request.flush({thread_id: 't1', queue: block});
    await expect(pending).resolves.toEqual(block);
  });

  it('still accepts a bare block, for a peer that answers the inline shape', async () => {
    const pending = firstValueFrom(api.getThreadQueue('t1'));
    httpMock.expectOne((item) => item.url.endsWith('/persistent/threads/t1/queue')).flush(block);
    await expect(pending).resolves.toEqual(block);
  });

  it('reports an unreadable block as null rather than a shapeless object', async () => {
    const pending = firstValueFrom(api.getThreadQueue('t1'));
    httpMock
      .expectOne((item) => item.url.endsWith('/persistent/threads/t1/queue'))
      .flush({thread_id: 't1'});
    await expect(pending).resolves.toBeNull();
  });
});
