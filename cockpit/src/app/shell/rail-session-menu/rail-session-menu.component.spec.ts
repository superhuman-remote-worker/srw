import {afterEach, describe, expect, it, vi} from 'vitest';
import {ChangeDetectorRef, Injector, runInInjectionContext, signal} from '@angular/core';
import {DOCUMENT, Location} from '@angular/common';
import {HttpClient} from '@angular/common/http';
import {Router} from '@angular/router';
import {TranslocoService} from '@jsverse/transloco';
import {of, throwError} from 'rxjs';
import {RailSessionMenuComponent, sessionMenuActions} from './rail-session-menu.component';
import {PersistentChatService} from '../../core/services/persistent-chat.service';
import {SessionListService} from '../../core/services/session-list.service';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {AppToastService} from '../../ui/toast';
import {environment} from '../../core/environment';
import type {Thread} from '../../core/models/api.model';

function thread(overrides: Partial<Thread> = {}): Thread {
  return {id: 't1', title: 'Quarterly report', status: 'active', ...overrides} as Thread;
}

/**
 * Builds the component directly with stub providers — no TestBed, matching
 * the sidebar's own spec. The actions are plain methods over the target
 * thread; none of them needs the rendered menu.
 */
function create(opts: {url?: string; connectedThreadId?: string | null} = {}) {
  const http = {
    delete: vi.fn((_url: string): unknown => of({status: 'ended'})),
    post: vi.fn((_url: string, _body: unknown): unknown => of({})),
  };
  const router = {
    url: opts.url ?? '/',
    navigate: vi.fn().mockResolvedValue(true),
    createUrlTree: vi.fn((commands: string[]) => commands),
    serializeUrl: vi.fn((commands: string[]) => commands.join('/')),
  };
  const chat = {
    threadId: signal(opts.connectedThreadId ?? null),
    endSession: vi.fn().mockResolvedValue('done'),
    resumeSession: vi.fn().mockResolvedValue(undefined),
  };
  const sessions = {refresh: vi.fn().mockResolvedValue(undefined), removeLocal: vi.fn()};
  const toast = {success: vi.fn(), info: vi.fn(), warning: vi.fn(), danger: vi.fn()};
  const injector = Injector.create({
    providers: [
      {provide: HttpClient, useValue: http},
      {provide: Router, useValue: router},
      {provide: Location, useValue: {prepareExternalUrl: (path: string) => path}},
      {provide: DOCUMENT, useValue: document},
      {provide: ChangeDetectorRef, useValue: {detectChanges: vi.fn()}},
      {provide: PersistentChatService, useValue: chat},
      {provide: SessionListService, useValue: sessions},
      {provide: AppToastService, useValue: toast},
      {provide: ErrorMessageService, useValue: {translate: (_e: unknown, key: string) => key}},
      {provide: TranslocoService, useValue: {translate: (key: string) => key}},
    ],
  });
  const component = runInInjectionContext(injector, () => new RailSessionMenuComponent());
  return {component, http, router, chat, sessions, toast};
}

const busy = (code: string) => throwError(() => ({status: 409, error: {detail: {code}}}));

afterEach(() => vi.restoreAllMocks());

describe('sessionMenuActions', () => {
  it('offers End on a running session', () => {
    expect(sessionMenuActions(thread({status: 'active'}))).toEqual({end: true, resume: false, delete: true});
  });

  it('offers Resume, not End, on an ended session', () => {
    expect(sessionMenuActions(thread({status: 'ended'}))).toEqual({end: false, resume: true, delete: true});
  });

  it('offers nothing while a pinned retirement settles by itself', () => {
    expect(sessionMenuActions(thread({status: 'ending'}))).toEqual({end: false, resume: false, delete: false});
    expect(sessionMenuActions(thread({status: 'active', runtime_retirement_pending: true}))).toEqual({
      end: false,
      resume: false,
      delete: false,
    });
  });

  it('offers End again as the retry of a fenced stateless End', () => {
    const t = thread({status: 'ending', execution_lane: 'stateless', runtime_retirement_pending: true});
    expect(sessionMenuActions(t)).toEqual({end: true, resume: false, delete: true});
  });

  it('offers only Delete as the retry of a fenced stateless Delete', () => {
    const t = thread({
      status: 'ending',
      execution_lane: 'stateless',
      runtime_retirement_pending: true,
      retirement_permanent: true,
    });
    expect(sessionMenuActions(t)).toEqual({end: false, resume: false, delete: true});
  });
});

describe('RailSessionMenuComponent', () => {
  describe('End', () => {
    it('ends a session in the background without force', async () => {
      const {component, http, sessions} = create();
      component.target.set(thread());
      await component.end();
      expect(http.delete).toHaveBeenCalledWith(`${environment.apiUrl}/persistent/threads/t1`);
      expect(sessions.refresh).toHaveBeenCalled();
    });

    it('ends the connected session through the chat service', async () => {
      const {component, http, chat} = create({connectedThreadId: 't1'});
      component.target.set(thread());
      await component.end();
      expect(chat.endSession).toHaveBeenCalled();
      expect(http.delete).not.toHaveBeenCalled();
    });

    it('asks before stopping a turn in flight, then forces', async () => {
      const {component, http} = create();
      http.delete.mockReturnValueOnce(busy('turn_in_flight'));
      component.target.set(thread());
      await component.end();
      expect(component.pendingForce()?.action).toBe('end');
      expect(component.forceMessageKey()).toBe('sessions.confirmEndMidTurn');

      await component.confirmForce();
      expect(component.pendingForce()).toBeNull();
      expect(http.delete).toHaveBeenLastCalledWith(`${environment.apiUrl}/persistent/threads/t1?force=true`);
    });

    it('words the stateless busy refusal as its own question', async () => {
      const {component, http} = create();
      http.delete.mockReturnValueOnce(busy('stateless_end_busy'));
      component.target.set(thread());
      await component.end();
      expect(component.forceMessageKey()).toBe('sessions.confirmEndStatelessBusy');
    });

    it('reports the retirement fence as retryable, not as a failure', async () => {
      const {component, http, toast} = create();
      http.delete.mockReturnValueOnce(throwError(() => ({status: 503})));
      component.target.set(thread());
      await component.end();
      expect(toast.warning).toHaveBeenCalledWith('nav.sessionMenu.endRetryable');
      expect(toast.danger).not.toHaveBeenCalled();
    });
  });

  describe('Delete', () => {
    it('deletes only after the confirmation', async () => {
      const {component, http, sessions} = create();
      component.target.set(thread());
      component.requestDelete();
      expect(component.pendingDelete()?.id).toBe('t1');
      expect(http.delete).not.toHaveBeenCalled();

      await component.confirmDelete();
      expect(http.delete).toHaveBeenCalledWith(`${environment.apiUrl}/persistent/threads/t1?permanent=true`);
      expect(sessions.removeLocal).toHaveBeenCalledWith('t1');
      expect(component.pendingDelete()).toBeNull();
    });

    it('leaves the deleted session for a new chat when it was on screen', async () => {
      const {component, router} = create({url: '/sessions/t1?tab=files'});
      component.target.set(thread());
      component.requestDelete();
      await component.confirmDelete();
      expect(router.navigate).toHaveBeenCalledWith(['/']);
    });

    it('stays put when another session was on screen', async () => {
      const {component, router} = create({url: '/sessions/t10'});
      component.target.set(thread());
      component.requestDelete();
      await component.confirmDelete();
      expect(router.navigate).not.toHaveBeenCalled();
    });

    it('escalates a busy session to a force confirmation', async () => {
      const {component, http, sessions} = create();
      http.delete.mockReturnValueOnce(busy('turn_in_flight'));
      component.target.set(thread());
      component.requestDelete();
      await component.confirmDelete();
      expect(component.pendingForce()?.action).toBe('delete');
      expect(sessions.removeLocal).not.toHaveBeenCalled();

      await component.confirmForce();
      expect(http.delete).toHaveBeenLastCalledWith(
        `${environment.apiUrl}/persistent/threads/t1?permanent=true&force=true`,
      );
      expect(sessions.removeLocal).toHaveBeenCalledWith('t1');
    });

    it('keeps the row when the delete fails', async () => {
      const {component, http, sessions, toast} = create();
      http.delete.mockReturnValueOnce(throwError(() => ({status: 500})));
      component.target.set(thread());
      component.requestDelete();
      await component.confirmDelete();
      expect(toast.danger).toHaveBeenCalledWith('errors.sessions.deleteFailed');
      expect(sessions.removeLocal).not.toHaveBeenCalled();
      expect(sessions.refresh).toHaveBeenCalled();
    });
  });

  describe('Resume', () => {
    it('resumes an ended session and opens it', async () => {
      const {component, http, router} = create();
      component.target.set(thread({status: 'ended'}));
      await component.resume();
      expect(http.post).toHaveBeenCalledWith(`${environment.apiUrl}/persistent/threads/t1/resume`, {});
      expect(router.navigate).toHaveBeenCalledWith(['/sessions', 't1']);
    });

    it('takes config drift to the chat page, whose Resume card resolves it', async () => {
      const {component, http, router, toast} = create();
      http.post.mockReturnValueOnce(
        throwError(() => ({
          status: 428,
          error: {detail: {code: 'config_drift', drift: [{kind: 'connector', id: 'c1', name: 'Drive'}]}},
        })),
      );
      component.target.set(thread({status: 'ended'}));
      await component.resume();
      expect(toast.info).toHaveBeenCalledWith('sessions.configDrift.attentionNeeded');
      expect(router.navigate).toHaveBeenCalledWith(['/sessions', 't1']);
    });

    it('stays put on any other refusal', async () => {
      const {component, http, router, toast} = create();
      http.post.mockReturnValueOnce(throwError(() => ({status: 500})));
      component.target.set(thread({status: 'ended'}));
      await component.resume();
      expect(toast.danger).toHaveBeenCalledWith('errors.sessions.resumeFailed');
      expect(router.navigate).not.toHaveBeenCalled();
    });

    it('resumes the connected session in place', async () => {
      const {component, http, chat} = create({connectedThreadId: 't1'});
      component.target.set(thread({status: 'ended'}));
      await component.resume();
      expect(chat.resumeSession).toHaveBeenCalled();
      expect(http.post).not.toHaveBeenCalled();
    });
  });

  it('asks the row to rename itself', () => {
    const {component} = create();
    const emitted: Thread[] = [];
    component.renameRequested.subscribe((t) => emitted.push(t));
    component.target.set(thread());
    component.rename();
    expect(emitted.map((t) => t.id)).toEqual(['t1']);
  });

  it('opens the session in a new tab', () => {
    const {component} = create();
    const open = vi.spyOn(window, 'open').mockReturnValue(null);
    component.target.set(thread());
    component.openInNewTab();
    expect(open).toHaveBeenCalledWith('/sessions/t1', '_blank', 'noopener');
  });
});
