/**
 * MCP server questions in a Session
 * (knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md,
 * "C2 Sessions"): `question.request`, the `session.state` snapshot's
 * `pending_questions`, `question.resolved`, and the decision POST each card
 * answer sends.
 *
 * Frames go straight into the service's event handler (the path both the
 * SSE stream and the pinned socket feed), so this harness skips the
 * EventSource plumbing persistent-chat.service.spec.ts drives.
 */
import {describe, expect, it, vi} from 'vitest';
import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {HttpClient} from '@angular/common/http';
import {Observable, of, throwError} from 'rxjs';
import {TranslocoService} from '@jsverse/transloco';
import {PersistentChatService} from './persistent-chat.service';
import {ApiService} from './api.service';
import {CapabilitiesService} from './capabilities.service';
import {IndexedDbService} from './indexed-db.service';
import {NotificationService} from './notification.service';
import {AppToastService} from '../../ui/toast';
import {isAssistantTurn, isSystemTurn, isToolCall} from '../models/turn.model';

function createService() {
  const mockHttp: any = {
    get: vi.fn().mockReturnValue(of({status: 'active', total_turns: 0, messages: [], total: 0})),
    post: vi.fn().mockReturnValue(of({})),
    patch: vi.fn().mockReturnValue(of({status: 'updated'})),
    delete: vi.fn().mockReturnValue(of({})),
  };
  const mockCache: any = {
    getThreadCursor: vi.fn().mockResolvedValue(null),
    setThreadCursor: vi.fn().mockResolvedValue(undefined),
    deleteThreadCursor: vi.fn().mockResolvedValue(undefined),
    getThreadMessages: vi.fn().mockResolvedValue([]),
    getNewestCachedCreatedAt: vi.fn().mockResolvedValue(null),
    upsertThreadMessages: vi.fn().mockResolvedValue(undefined),
    clearThreadMessages: vi.fn().mockResolvedValue(undefined),
  };
  const toast: any = {
    show: vi.fn(),
    info: vi.fn(),
    success: vi.fn(),
    warning: vi.fn(),
    danger: vi.fn(),
    dismiss: vi.fn(),
    dismissAll: vi.fn(),
  };
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({
    providers: [
      {provide: HttpClient, useValue: mockHttp},
      {provide: ApiService, useValue: {}},
      {
        provide: CapabilitiesService,
        useValue: {
          datasourceScopeAutoAttachAvailable: () => true,
          datasourceScopeAutoAttachAvailability$: of(true),
        },
      },
      {provide: IndexedDbService, useValue: mockCache},
      {provide: AppToastService, useValue: toast},
      {
        provide: NotificationService,
        useValue: {
          isConnected: signal(false),
          lifecycleEvent: signal(null),
          cloudDiffStagedEvent: signal(null),
        },
      },
      {
        provide: TranslocoService,
        useValue: {
          translate: (key: string, params?: Record<string, unknown>) =>
            params && Object.keys(params).length ? `${key}:${JSON.stringify(params)}` : key,
        },
      },
      PersistentChatService,
    ],
  });
  const service = TestBed.inject(PersistentChatService);
  service.threadId.set('thread-q');
  const frame = (method: string, params: Record<string, unknown>, covered = false) =>
    (service as any)._handleEvent({method, params}, true, covered);
  return {service, mockHttp, frame};
}

const ASK = {
  id: 'call-1',
  approval_id: 'ask-1',
  server: 'Civitai',
  server_slug: 'civitai',
  tool: 'mcp__civitai__generate',
  mode: 'form',
  message: 'Spend 12 Buzz to run imageGen?',
  schema: {type: 'object', properties: {}},
  url: null,
  confirmation: {confirm: true},
};

const PERMISSION = {id: 'call-0', approval_id: 'perm-0', tool: 'run_command', args: {command: 'ls'}};

/** A turn with the MCP call running, as the stream has it when its server asks. */
function runningCall(frame: ReturnType<typeof createService>['frame']) {
  frame('turn.started', {turn_id: 1});
  frame('tool.started', {id: 'call-1', tool: 'mcp__civitai__generate', args: {}});
}

function callOf(service: PersistentChatService, id = 'call-1') {
  for (const turn of service.turns()) {
    if (!isAssistantTurn(turn)) continue;
    const call = turn.events.find((e) => isToolCall(e) && e.id === id);
    if (call && isToolCall(call)) return call;
  }
  return undefined;
}

describe('PersistentChatService — MCP server questions', () => {
  it('lists a question.request as a card, beside (not among) the pending approvals', () => {
    const {service, frame} = createService();
    frame('permission.request', PERMISSION);
    frame('question.request', ASK);
    expect(service.pendingQuestions()).toEqual([
      {
        id: 'call-1',
        approvalId: 'ask-1',
        server: 'Civitai',
        serverSlug: 'civitai',
        tool: 'mcp__civitai__generate',
        mode: 'form',
        message: 'Spend 12 Buzz to run imageGen?',
        schema: {type: 'object', properties: {}},
        url: null,
        confirmation: {confirm: true},
      },
    ]);
    expect(service.pendingPermissions().map((p) => p.id)).toEqual(['call-0']);
  });

  it('replaces a re-announced question of the same call with its newest request id', () => {
    const {service, frame} = createService();
    frame('question.request', ASK);
    frame('question.request', {...ASK, approval_id: 'ask-1b', message: 'Again?'});
    expect(service.pendingQuestions().map((q) => [q.approvalId, q.message])).toEqual([
      ['ask-1b', 'Again?'],
    ]);
  });

  it('re-shows the open questions from session.state, presence authoritative', () => {
    const {service, frame} = createService();
    frame('session.state', {
      pending_permissions: [PERMISSION],
      pending_questions: [ASK, {...ASK, id: 'call-2', approval_id: 'ask-2', mode: 'url', url: 'https://civitai.com/'}],
    });
    expect(service.pendingQuestions().map((q) => q.approvalId)).toEqual(['ask-1', 'ask-2']);
    expect(service.pendingQuestions()[1].mode).toBe('url');
    // The approval rows stay their own list.
    expect(service.pendingPermissions().map((p) => p.approvalId)).toEqual(['perm-0']);

    // A snapshot without the key (an older peer) leaves the cards alone...
    frame('session.state', {pending_permissions: []});
    expect(service.pendingQuestions()).toHaveLength(2);
    // ...and an explicit empty list clears them.
    frame('session.state', {pending_permissions: [], pending_questions: []});
    expect(service.pendingQuestions()).toEqual([]);
  });

  it('keeps the snapshot list when a covered frame replays behind it', () => {
    const {service, frame} = createService();
    frame('session.state', {pending_questions: []});
    frame('question.request', ASK, true);
    expect(service.pendingQuestions()).toEqual([]);
  });

  it.each([
    ['approved', 'accepted'],
    ['denied', 'declined'],
    ['expired', 'unanswered'],
    ['interrupted', 'unanswered'],
    ['unavailable', 'unanswered'],
  ])('retires the card on question.resolved %s and marks its call %s', (decision, outcome) => {
    const {service, frame} = createService();
    runningCall(frame);
    frame('question.request', ASK);
    frame('question.resolved', {id: 'call-1', approval_id: 'ask-1', decision});
    expect(service.pendingQuestions()).toEqual([]);
    const call = callOf(service)!;
    expect(call.question).toEqual({server: 'Civitai', outcome});
    // The question is not the tool's approval: the call keeps running.
    expect(call.status).toBe('running');
    expect(call.decision).toBeUndefined();
  });

  it('names the server on a replayed resolution whose card the snapshot already dropped', () => {
    const {service, frame} = createService();
    runningCall(frame);
    frame('session.state', {pending_questions: []});
    frame('question.request', ASK, true);
    frame('question.resolved', {id: 'call-1', approval_id: 'ask-1', decision: 'denied'}, true);
    expect(callOf(service)!.question).toEqual({server: 'Civitai', outcome: 'declined'});
  });

  it('removes only the settled request, not a later question on the same call', () => {
    const {service, frame} = createService();
    frame('question.request', {...ASK, approval_id: 'ask-2'});
    frame('question.resolved', {id: 'call-1', approval_id: 'ask-1', decision: 'expired'});
    expect(service.pendingQuestions().map((q) => q.approvalId)).toEqual(['ask-2']);
  });

  it('writes a system line when no card holds the call', () => {
    const {service, frame} = createService();
    frame('question.request', ASK);
    frame('question.resolved', {id: 'call-1', approval_id: 'ask-1', decision: 'approved'});
    const line = service.turns().find(isSystemTurn)!;
    expect(line.id).toBe('question:call-1');
    expect(line.content).toBe('chat.question.outcome.accepted:{"server":"Civitai"}');
    // A replay replaces the line rather than adding a second.
    frame('question.resolved', {id: 'call-1', approval_id: 'ask-1', decision: 'approved'});
    expect(service.turns().filter(isSystemTurn)).toHaveLength(1);
  });

  it('says "the MCP server" when no frame named it', () => {
    const {service, frame} = createService();
    frame('question.resolved', {id: 'call-9', approval_id: 'ask-9', decision: 'expired'});
    expect(service.turns().find(isSystemTurn)!.content).toBe(
      'chat.question.outcomeGeneric.unanswered:{"server":""}',
    );
  });

  it('a permission.resolved for the same call leaves the question card alone', () => {
    const {service, frame} = createService();
    frame('question.request', ASK);
    frame('permission.resolved', {id: 'call-1', approval_id: 'perm-x', decision: 'approved'});
    expect(service.pendingQuestions()).toHaveLength(1);
  });

  describe('answering', () => {
    function asked() {
      const ctx = createService();
      ctx.frame('question.request', ASK);
      return {...ctx, question: ctx.service.pendingQuestions()[0]};
    }

    const URL = '/persistent/threads/thread-q/approve/ask-1';

    it.each([
      ['accept', {decision: 'approve', content: {confirm: true}}, {decision: 'approve', content: {confirm: true}}],
      [
        'always accept',
        {decision: 'approve', content: {confirm: true}, alwaysAccept: true},
        {decision: 'approve', content: {confirm: true}, always_accept: true},
      ],
      ['a form', {decision: 'approve', content: {prompt: 'fox', count: 2}}, {decision: 'approve', content: {prompt: 'fox', count: 2}}],
      ['URL done', {decision: 'approve'}, {decision: 'approve'}],
      ['decline', {decision: 'deny'}, {decision: 'deny'}],
      ['dismiss', {decision: 'cancel'}, {decision: 'cancel'}],
    ] as const)('posts %s to the approval endpoint', async (_name, answer, body) => {
      const {service, mockHttp, question} = asked();
      await service.answerQuestion(question, answer as any);
      expect(mockHttp.post).toHaveBeenCalledTimes(1);
      const [url, sent] = mockHttp.post.mock.calls[0];
      expect(String(url).endsWith(URL)).toBe(true);
      expect(sent).toEqual(body);
      // Ack-driven: the card waits for question.resolved.
      expect(service.pendingQuestions()).toHaveLength(1);
      expect(service.questionUi()['ask-1']).toEqual({sent: true});
    });

    it('blocks a second answer while the first is on its way', () => {
      const {service, mockHttp, question} = asked();
      mockHttp.post.mockReturnValue(new Observable(() => undefined));
      void service.answerQuestion(question, {decision: 'deny'});
      expect(service.questionUi()['ask-1']).toEqual({busy: true, sent: false, error: null});
      void service.answerQuestion(question, {decision: 'cancel'});
      expect(mockHttp.post).toHaveBeenCalledTimes(1);
    });

    it('shows a 422 detail on the card and keeps it open', async () => {
      const {service, mockHttp, question} = asked();
      mockHttp.post.mockReturnValue(
        throwError(() => ({status: 422, error: {detail: 'count must be at most 4'}})),
      );
      await service.answerQuestion(question, {decision: 'approve', content: {count: 9}});
      expect(service.pendingQuestions()).toHaveLength(1);
      expect(service.questionUi()['ask-1']).toEqual({error: 'count must be at most 4'});
    });

    it('falls back to its own words for a 422 without a detail, and for a failed send', async () => {
      const {service, mockHttp, question} = asked();
      mockHttp.post.mockReturnValue(throwError(() => ({status: 422, error: {}})));
      await service.answerQuestion(question, {decision: 'approve', content: {}});
      expect(service.questionUi()['ask-1'].error).toBe(
        'chat.question.invalidAnswer:{"server":"Civitai"}',
      );
      mockHttp.post.mockReturnValue(throwError(() => ({status: 503})));
      await service.answerQuestion(question, {decision: 'deny'});
      expect(service.questionUi()['ask-1'].error).toBe(
        'chat.question.sendFailed:{"server":"Civitai"}',
      );
      expect(service.pendingQuestions()).toHaveLength(1);
    });

    it.each([
      [409, 'chat.question.alreadyAnswered'],
      [404, 'chat.question.gone'],
    ])('drops the card on %s with a system line', async (status, message) => {
      const {service, mockHttp, question} = asked();
      mockHttp.post.mockReturnValue(throwError(() => ({status})));
      await service.answerQuestion(question, {decision: 'deny'});
      expect(service.pendingQuestions()).toEqual([]);
      expect(service.questionUi()).toEqual({});
      expect(service.turns().filter(isSystemTurn).map((t) => t.content)).toEqual([message]);
    });

    it('clears the send state once the question settles', async () => {
      const {service, frame, question} = asked();
      await service.answerQuestion(question, {decision: 'deny'});
      frame('question.resolved', {id: 'call-1', approval_id: 'ask-1', decision: 'denied'});
      expect(service.pendingQuestions()).toEqual([]);
      expect(service.questionUi()).toEqual({});
    });
  });
});
