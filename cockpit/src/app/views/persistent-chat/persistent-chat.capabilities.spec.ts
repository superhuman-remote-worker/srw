import {CommonModule} from '@angular/common';
import {HttpClient} from '@angular/common/http';
import {CUSTOM_ELEMENTS_SCHEMA, Pipe, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {FormsModule} from '@angular/forms';
import {By} from '@angular/platform-browser';
import {Router} from '@angular/router';
import {TranslocoService} from '@jsverse/transloco';
import {Observable, of, Subject} from 'rxjs';
import {beforeAll, describe, expect, it, vi} from 'vitest';
import {environment} from '../../core/environment';
import {WorkspaceLifecycleView} from '../../core/models/api.model';
import {ApiService} from '../../core/services/api.service';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {PersistentChatService} from '../../core/services/persistent-chat.service';
import {ChatPreferencesService} from '../../core/services/chat-preferences.service';
import {DeviceCapabilitiesService} from '../../core/services/device-capabilities.service';
import {ErrorMessageService} from '../../core/services/error-message.service';
import {FileHandlingService} from '../../core/services/file-handling.service';
import {I18nService} from '../../core/services/i18n.service';
import {SessionListService} from '../../core/services/session-list.service';
import {ViewportService} from '../../core/services/viewport.service';
import {VoiceCapabilitiesService} from '../../core/services/voice-capabilities.service';
import {VoiceRecordingService} from '../../core/services/voice-recording.service';
import {AppMenuComponent} from '../../ui/menu/menu.component';
import {AppMenuItemComponent} from '../../ui/menu/menu-item.component';
import {AppToastService} from '../../ui/toast';
import {PersistentChatComponent} from './persistent-chat.component';

@Pipe({name: 'transloco', standalone: true})
class TestTranslocoPipe { transform(key: string): string { return key; } }

// An idle, connected session. Explicit neutral state keeps the real template,
// effects and capability service under test; unrelated child controls are shallow.
function sessionState() {
  const state: Record<string, unknown> = {};
  const workspaceLifecycle = signal<WorkspaceLifecycleView | null>(null);
  for (const key of ['isStartingSession', 'isDraftSession', 'isOfficerThread', 'isParked',
    'isResuming', 'isCreating', 'isAwaitingTurn', 'isStreaming', 'isInterrupting', 'isVmSession',
    'hasOlderTurns', 'cloudDiffPanelOpen', 'cloudSyncDegraded', 'protectedCloud',
    'workspaceUpgradeInProgress', 'continueAfterUpgrade', 'rewindInFlight', 'rewindOutcomeUnknown',
    'rewindPreviewLoading', 'rewindModeAvailable', 'summarizeAvailable', 'outboxStalled',
    'draftDefaultsLoading', 'reconnectGaveUp', 'endRetryAvailable', 'cloudFoldersAgentOutdated',
    'cloudFoldersProtected', 'workspaceUpgradeFailed']) state[key] = signal(false);
  for (const key of ['turns', 'visibleTurns', 'pendingAttachments', 'pendingPermissions', 'tasks',
    'outbox', 'outboxIds', 'draftDatasourceIds', 'runningTools', 'cloudFolderProblems',
    'cloudFolders']) state[key] = signal([]);
  for (const key of ['compaction', 'rewindPrefill', 'rewindPreview', 'pendingWorkspaceOffer',
    'currentUsage', 'queueState', 'verifiedProjectFolder', 'cloudSessionUrl', 'ncSessionFolder',
    'cloudDiffProbe', 'cloudStagedAt', 'attachmentError', 'error', 'endedAt', 'draftDefaultsError',
    'workspaceCreation']) state[key] = signal(null);
  return {...state, isConnected: signal(true), sessionReady: signal(true), threadId: signal('test-thread'),
    threadStatus: signal('active'), sessionTitle: signal('Test session'), connectionState: signal('connected'),
    modelName: signal('Test model'), permissionMode: signal('read_only'), narrationMode: signal('off'),
    workspaceUpgradeFailureText: () => '',
    startupPhase: signal('ready'), controlTransport: signal('websocket'), citationsByCid: signal(new Map()), citationFocus: signal(null),
    awaitingElapsedMs: signal(0), reconnectAttempt: signal(0), agentSilenceSeconds: signal(0), cloudChangesCount: signal(0),
    sshHandle: signal('test-thread'), resetWindow: vi.fn(), refreshPendingRewindReceipt: vi.fn(),
    // The real service answers per command from /connection; here only
    // /rewind is session-dependent, through the same signal the tests flip.
    slashCommandAvailable: (command: string) =>
      command !== '/rewind' || (state['rewindModeAvailable'] as () => boolean)(),
    workspaceLifecycle,
    // These fixtures keep one current view; accept its IDE response and retain
    // any lifecycle projection, including when the response omits that field.
    captureWorkspaceLifecycleUpdate: (_threadId: string) => (view: WorkspaceLifecycleView | null | undefined) => {
      if (view) workspaceLifecycle.set(view);
      return true;
    },
  };
}

/** Mount the real chat component over a stubbed service `chat`. `withMenu`
 *  renders the real header menu; left shallow, its unpopulated JIT view query
 *  cannot throw on destroy (see the panelRef note below). */
async function mountChat(chat: ReturnType<typeof sessionState>, api: Record<string, unknown>, withMenu = true) {
  TestBed.resetTestingModule();
  TestBed.configureTestingModule({imports: [PersistentChatComponent], providers: [
    {provide: PersistentChatService, useValue: chat},
    {provide: ApiService, useValue: api},
    {provide: HttpClient, useValue: {get: () => of([])}},
    {provide: TranslocoService, useValue: {translate: (key: string) => key}},
    {provide: I18nService, useValue: {activeLang: signal('en')}},
    {provide: ViewportService, useValue: {isMobile: signal(false)}},
    {provide: ChatPreferencesService, useValue: {readingWidth: signal('comfortable'), textSize: signal('medium'), officerLensFolded: signal(false)}},
    {provide: DeviceCapabilitiesService, useValue: {getCapabilities: () => of({hasCamera: false, hasAudioInput: false, isMobile: false})}},
    {provide: VoiceRecordingService, useValue: {getRecordingState: () => of({isRecording: false, duration: 0})}},
    {provide: VoiceCapabilitiesService, useValue: {canTranscribe: signal(false)}},
    ...[FileHandlingService, Router, AppToastService, ErrorMessageService, SessionListService].map(provide => ({provide, useValue: {}})),
    CapabilitiesService,
  ]});
  TestBed.overrideComponent(PersistentChatComponent, {set: {
    styles: [], styleUrls: [],
    imports: [CommonModule, FormsModule, TestTranslocoPipe, ...(withMenu ? [AppMenuComponent, AppMenuItemComponent] : [])],
    schemas: [CUSTOM_ELEMENTS_SCHEMA],
  }});
  const fixture = TestBed.createComponent(PersistentChatComponent);
  // JIT does not populate signal view queries. Resolve the actual rendered
  // elements so after-render observers also work when a test switches threads.
  for (const [query, selector] of [
    ['messagesInner', '.messages-inner'], ['chatHeaderEl', '.chat-header'],
    ['headerActionsEl', '.header-right'],
  ]) Object.defineProperty(fixture.componentInstance, query, {
    value: () => ({nativeElement: fixture.nativeElement.querySelector(selector)}),
  });
  fixture.detectChanges();
  await fixture.whenStable();
  fixture.detectChanges();
  return fixture;
}

describe('single-origin session capabilities', () => {
  beforeAll(async () => {
    HTMLElement.prototype.scrollTo = vi.fn();
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });
  it.each([false, true])('retains browser IDE while external setup follows the deployment flag (%s)', async (enabled) => {
    const previous = environment.externalClientsEnabled;
    environment.externalClientsEnabled = enabled;
    const ide = {
      status: 'active',
      code_server_url: 'https://localhost:8443/p/test-thread/ide/',
      gitea_url: 'https://localhost:8443/git/test/repo',
    };
    const api = {
      getThreadIdeStatus: vi.fn(() => of(ide)),
      getMyCapabilities: () => of(null),
      getSshHostKeys: vi.fn(() => of({hostname: 'ssh.example.test', host_keys: [{fingerprint: 'test', key_type: 'ssh-ed25519', public_key: 'test'}]})),
    };
    try {
      const fixture = await mountChat(sessionState(), api);

      const menuDebug = fixture.debugElement.query(By.directive(AppMenuComponent));
      const menu = menuDebug.componentInstance as AppMenuComponent;
      const panel = menuDebug.nativeElement.querySelector('.app-menu__panel') as HTMLElement;
      // Angular's JIT test compiler does not populate the signal view query for
      // this standalone child. Keep the real AppMenu template, projection and
      // open behavior under test while supplying its actual rendered panel.
      Object.defineProperty(menu, 'panelRef', {value: () => ({nativeElement: panel})});
      menu.open(document.createElement('button'));
      fixture.detectChanges();

      const menuText = [...panel.querySelectorAll('app-menu-item')]
        .map((node: any) => node.textContent.trim());
      expect(menuText).toContain('chat.header.gitButton');
      expect(menuText).toContain('chat.header.ideButton');
      expect(menuText.includes('chat.header.sshButton')).toBe(enabled);
      expect(fixture.nativeElement.querySelector('app-ssh-connect-panel')).toBeNull();
      expect(api.getSshHostKeys).toHaveBeenCalledTimes(enabled ? 1 : 0);
      const open = vi.spyOn(window, 'open').mockImplementation(() => null);
      fixture.componentInstance.openCodeServer();
      expect(open).toHaveBeenCalledWith(ide.code_server_url, '_blank');
      open.mockRestore();
      fixture.destroy();
    } finally {
      TestBed.resetTestingModule();
      environment.externalClientsEnabled = previous;
    }
  });
});

describe('pending VM IDE ownership on the mounted component', () => {
  beforeAll(async () => {
    HTMLElement.prototype.scrollTo = vi.fn();
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  it('abandons the original pending lease immediately when the mounted view switches threads', async () => {
    const chat = sessionState();
    const admission = new Subject<any>();
    const api = {
      getThreadIdeStatus: () => of(null), getMyCapabilities: () => of(null),
      getSshHostKeys: () => of({hostname: 'ssh.example.test', host_keys: []}),
      startThreadIdeSession: () => admission,
      closeThreadIdeLease: vi.fn(() => of(void 0)),
    };
    const tab: any = {
      closed: false, opener: {}, document: {title: '', body: {textContent: ''}},
      location: {href: ''}, close: vi.fn(() => { tab.closed = true; }),
    };
    const open = vi.spyOn(window, 'open').mockReturnValue(tab);
    const fixture = await mountChat(chat, api, false);
    try {
      fixture.componentInstance.openVmCodeServer();
      admission.next({status: 'restoring', access_lease_id: 'original-lease'});
      admission.complete();
      chat.threadId.set('successor');
      fixture.detectChanges();
      await fixture.whenStable();
      expect(api.closeThreadIdeLease).toHaveBeenCalledExactlyOnceWith('test-thread', 'original-lease');
      expect(tab.closed).toBe(true);
    } finally {
      admission.complete();
      fixture.destroy();
      TestBed.resetTestingModule();
      open.mockRestore();
    }
  });
});

describe('parked unit composer', () => {
  beforeAll(async () => {
    HTMLElement.prototype.scrollTo = vi.fn();
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const api = {
    getThreadIdeStatus: () => of(null),
    getMyCapabilities: () => of(null),
    getSshHostKeys: () => of({hostname: 'ssh.example.test', host_keys: []}),
  };

  // Input recorded into a parked unit is never claimed until an owner Retry or
  // an operator unpark revives it, so an open composer only swallowed it. The
  // closed box names the way out instead: Retry when the owner may revive the
  // unit, the administrator release when only an operator can.
  it.each([
    ['live', true, 'chat.input.parked'],
    ['live', false, 'chat.input.parkedBlocked'],
    // A reload of a parked session stays "starting" (a park is no readiness
    // evidence); the closed box must still say why, not "type while it starts".
    ['starting', true, 'chat.input.parked'],
    ['starting', false, 'chat.input.parkedBlocked'],
  ])('closes the composer on a %s session and points at the way out (retryable=%s)', async (phase, retryable, placeholder) => {
    const chat = sessionState() as any;
    if (phase === 'starting') {
      chat.sessionReady.set(false);
      chat.isStartingSession.set(true);
    }
    chat.isParked.set(true);
    chat.queueState.set({
      state: 'parked', park_reason: retryable ? 'attach_failed' : 'claim_loss_hold', parked_at: null,
      retryable, attempts: 3, pending_input: true,
    });
    chat.sendMessage = vi.fn(async () => true);
    const fixture = await mountChat(chat, api, false);
    try {
      const el = fixture.nativeElement as HTMLElement;
      const composer = el.querySelector<HTMLTextAreaElement>('[data-testid="chat-composer"]')!;
      expect(composer.disabled).toBe(true);
      expect(composer.placeholder).toBe(placeholder);
      // No half-open composer: an attachment chip could never be sent either.
      expect(el.querySelector<HTMLButtonElement>('.attach-wrap button')!.disabled).toBe(true);
      // The way out stays on screen: Retry only when the owner may revive it.
      expect(el.querySelector('[data-testid="chat-parked"]')).not.toBeNull();
      expect(el.querySelector('[data-testid="chat-parked-retry"]') !== null).toBe(retryable);

      fixture.componentInstance.inputText = 'are you there?';
      expect(fixture.componentInstance.canSend()).toBe(false);
      fixture.componentInstance.send();
      expect(chat.sendMessage).not.toHaveBeenCalled();
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  // End and suspension settle the unit (the End funnel closes a parked one to
  // `done`), so a parked block that outlived an in-tab End is stale: the box
  // stays the resume path it always was.
  it.each([
    ['ended', 'chat.input.endedSendResumes'],
    ['suspended', 'chat.input.suspendedSendResumes'],
  ])('keeps an %s session composable despite a stale parked block', async (status, placeholder) => {
    const chat = sessionState() as any;
    chat.threadStatus.set(status);
    chat.isConnected.set(false);
    chat.connectionState.set('disconnected');
    chat.isParked.set(true);
    chat.queueState.set({state: 'parked', park_reason: 'attach_failed', parked_at: null, retryable: true, attempts: 3, pending_input: true});
    const fixture = await mountChat(chat, api, false);
    try {
      const composer = (fixture.nativeElement as HTMLElement)
        .querySelector<HTMLTextAreaElement>('[data-testid="chat-composer"]')!;
      expect(composer.disabled).toBe(false);
      expect(composer.placeholder).toBe(placeholder);
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('reopens the composer once the unit is no longer parked', async () => {
    const chat = sessionState() as any;
    chat.isParked.set(true);
    chat.queueState.set({state: 'parked', park_reason: 'attach_failed', parked_at: null, retryable: true, attempts: 3, pending_input: true});
    const fixture = await mountChat(chat, api, false);
    try {
      const composer = () =>
        (fixture.nativeElement as HTMLElement).querySelector<HTMLTextAreaElement>('[data-testid="chat-composer"]')!;
      expect(composer().disabled).toBe(true);

      // What a successful Retry (or turn.started after an operator unpark) leaves.
      chat.isParked.set(false);
      chat.queueState.set({state: 'queued', park_reason: null, parked_at: null, retryable: false, attempts: 0, pending_input: true});
      fixture.detectChanges();
      await fixture.whenStable();
      fixture.detectChanges();
      expect(composer().disabled).toBe(false);
      expect(composer().placeholder).toBe('chat.input.default');
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });
});

/** The landing draft (New chat): an open composer with no session behind it. */
function draftState() {
  const chat = sessionState() as any;
  chat.isDraftSession.set(true);
  chat.isConnected.set(false);
  chat.sessionReady.set(false);
  chat.threadId.set(null);
  chat.threadStatus.set(null);
  chat.connectionState.set('disconnected');
  chat.startupPhase.set(null);
  chat.draftDatasourceIds.set([]);
  chat.draftConnectorsEnabled = signal(true);
  return chat;
}

// New chat lands on a draft with no session behind it. A chip rides the first
// send's outbox item into the thread that send creates, exactly like the text,
// so Attach and drop follow the composer rather than the connection.
describe('attachments follow the composer', () => {
  beforeAll(async () => {
    HTMLElement.prototype.scrollTo = vi.fn();
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const api = {
    getThreadIdeStatus: () => of(null),
    getMyCapabilities: () => of(null),
    getSshHostKeys: () => of({hostname: 'ssh.example.test', host_keys: []}),
  };

  function dropEvent(file: File): DragEvent {
    return {dataTransfer: {types: ['Files'], files: [file]}, preventDefault: vi.fn()} as unknown as DragEvent;
  }

  async function mountWithPreviews(chat: any) {
    chat.addAttachments = vi.fn();
    const fixture = await mountChat(chat, api, false);
    const preview = {id: 'p1', name: 'notes.pdf', size: 3, mimeType: 'application/pdf'};
    Object.assign(TestBed.inject(FileHandlingService), {
      createFilePreviews: vi.fn(async () => ({previews: [preview], rejected: []})),
    });
    return {fixture, preview};
  }

  it('attaches on the landing draft before any session exists', async () => {
    const chat = draftState();
    const {fixture, preview} = await mountWithPreviews(chat);
    try {
      const el = fixture.nativeElement as HTMLElement;
      expect(el.querySelector<HTMLButtonElement>('.attach-wrap button')!.disabled).toBe(false);

      await fixture.componentInstance.onDrop(dropEvent(new File(['pdf'], 'notes.pdf')));
      expect(chat.addAttachments).toHaveBeenCalledExactlyOnceWith([preview]);
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('refuses Attach and drop during a mid-session reconnect', async () => {
    const chat = sessionState() as any;
    chat.isConnected.set(false);
    chat.connectionState.set('connecting');
    const {fixture} = await mountWithPreviews(chat);
    try {
      const el = fixture.nativeElement as HTMLElement;
      expect(el.querySelector<HTMLButtonElement>('.attach-wrap button')!.disabled).toBe(true);

      await fixture.componentInstance.onDrop(dropEvent(new File(['pdf'], 'notes.pdf')));
      expect(chat.addAttachments).not.toHaveBeenCalled();
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });
});

// Dictation on the landing draft (knowledge-base/knowledge/features/draft_session_dictation.md):
// the mic follows the composer, the draft transcribes through the account
// route, and a result that outlives its composer is dropped whole.
describe('dictation follows the composer', () => {
  beforeAll(async () => {
    HTMLElement.prototype.scrollTo = vi.fn();
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const clip = new File(['ogg'], 'voice.webm', {type: 'audio/webm'});
  const preview = {id: 'a1', file: clip, name: 'voice.webm', size: 3, mimeType: 'audio/webm', type: 'audio'};

  /** Mount with a usable mic and a stopped recording ready to hand back. */
  async function mountWithMic(chat: any, transcription: Observable<unknown>) {
    chat.addAttachments = vi.fn();
    const api = {
      getThreadIdeStatus: () => of(null),
      getMyCapabilities: () => of(null),
      getSshHostKeys: () => of({hostname: 'ssh.example.test', host_keys: []}),
      transcribeVoice: vi.fn(() => transcription),
    };
    const fixture = await mountChat(chat, api, false);
    Object.assign(TestBed.inject(VoiceCapabilitiesService), {stt: signal(true)});
    Object.assign(TestBed.inject(VoiceRecordingService), {
      stopRecording: vi.fn(async () => ({blob: clip, duration: 3, mimeType: 'audio/webm'})),
      cancelRecording: vi.fn(),
    });
    Object.assign(TestBed.inject(FileHandlingService), {
      createAudioFilePreview: vi.fn(async () => preview),
    });
    fixture.componentInstance.hasAudioInput.set(true);
    fixture.detectChanges();
    return {fixture, api};
  }

  const micButton = (fixture: {nativeElement: HTMLElement}) =>
    fixture.nativeElement.querySelector<HTMLButtonElement>('button.send.mic')!;

  it('records and transcribes on the landing draft through the account route', async () => {
    const chat = draftState();
    const {fixture, api} = await mountWithMic(chat, of({text: 'plan my week'}));
    try {
      expect(micButton(fixture).disabled).toBe(false);

      const component = fixture.componentInstance;
      component.isRecording.set(true);
      await component.stopRecording();

      expect(api.transcribeVoice).toHaveBeenCalledExactlyOnceWith(null, clip);
      expect(component.inputText).toBe('plan my week');
      // The clip rides the first send like any attachment.
      expect(chat.addAttachments).toHaveBeenCalledExactlyOnceWith([preview]);
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('closes the mic during a mid-session reconnect', async () => {
    const chat = sessionState() as any;
    chat.isConnected.set(false);
    chat.connectionState.set('connecting');
    const {fixture} = await mountWithMic(chat, of({text: ''}));
    try {
      expect(micButton(fixture).disabled).toBe(true);
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('refuses Send while the transcript is still on its way', async () => {
    const chat = draftState();
    chat.sendMessage = vi.fn(async () => true);
    const transcription = new Subject<unknown>();
    const {fixture} = await mountWithMic(chat, transcription);
    try {
      const component = fixture.componentInstance;
      component.isRecording.set(true);
      const stopped = component.stopRecording();
      await vi.waitFor(() => expect(component.isTranscribing()).toBe(true));

      component.inputText = 'typed meanwhile';
      expect(component.canSend()).toBe(false);
      component.send();
      expect(chat.sendMessage).not.toHaveBeenCalled();

      transcription.next({text: 'and dictated'});
      transcription.complete();
      await stopped;
      expect(component.inputText).toBe('typed meanwhile\n\nand dictated');
      expect(component.canSend()).toBe(true);
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('drops a transcription that lands after the user switched sessions', async () => {
    const chat = sessionState() as any;
    const transcription = new Subject<unknown>();
    const {fixture} = await mountWithMic(chat, transcription);
    try {
      const component = fixture.componentInstance;
      component.isRecording.set(true);
      const stopped = component.stopRecording();
      await vi.waitFor(() => expect(component.isTranscribing()).toBe(true));

      // The reused component now shows another session: the first one's
      // transcription neither shows here nor holds this composer's Send.
      chat.threadId.set('another-session');
      expect(component.isTranscribing()).toBe(false);
      component.inputText = 'a message for this session';
      expect(component.canSend()).toBe(true);

      transcription.next({text: 'meant for the first session'});
      transcription.complete();
      await stopped;

      expect(component.inputText).toBe('a message for this session');
      expect(chat.addAttachments).not.toHaveBeenCalled();
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it("an earlier session's late transcription leaves a newer one running", async () => {
    const chat = sessionState() as any;
    const first = new Subject<unknown>();
    const second = new Subject<unknown>();
    const {fixture, api} = await mountWithMic(chat, first);
    api.transcribeVoice.mockReturnValueOnce(first).mockReturnValueOnce(second);
    try {
      const component = fixture.componentInstance;
      component.isRecording.set(true);
      const firstStopped = component.stopRecording();
      await vi.waitFor(() => expect(component.isTranscribing()).toBe(true));

      chat.threadId.set('another-session');
      component.isRecording.set(true);
      const secondStopped = component.stopRecording();
      await vi.waitFor(() => expect(component.isTranscribing()).toBe(true));

      first.next({text: 'late'});
      first.complete();
      await firstStopped;
      expect(component.isTranscribing()).toBe(true);

      second.next({text: 'current'});
      second.complete();
      await secondStopped;
      expect(component.isTranscribing()).toBe(false);
      expect(component.inputText).toBe('current');
      expect(chat.addAttachments).toHaveBeenCalledOnce();
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('drops a transcription that lands after its draft composer is gone', async () => {
    const chat = draftState();
    const transcription = new Subject<unknown>();
    const {fixture} = await mountWithMic(chat, transcription);
    const component = fixture.componentInstance;
    component.isRecording.set(true);
    const stopped = component.stopRecording();
    await vi.waitFor(() => expect(component.isTranscribing()).toBe(true));

    fixture.destroy();
    transcription.next({text: 'too late'});
    transcription.complete();
    await stopped;

    expect(chat.addAttachments).not.toHaveBeenCalled();
    TestBed.resetTestingModule();
  });
});

// R1 follow-up: a stateless End fenced by a retryable 503 stays `ending` until
// End is pressed again — nothing on the server finishes it — so the header
// keeps End (`chat.header.disconnect`) there. A pinned `ending` session, which
// the server settles by itself, shows no End.
describe('header End on a pending stateless retirement', () => {
  beforeAll(async () => {
    HTMLElement.prototype.scrollTo = vi.fn();
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const api = {
    getThreadIdeStatus: () => of(null),
    getMyCapabilities: () => of(null),
    getSshHostKeys: () => of({hostname: 'ssh.example.test', host_keys: []}),
  };

  function endButtons(host: HTMLElement): HTMLElement[] {
    return [...host.querySelectorAll<HTMLElement>('.header-right app-button')]
      .filter(button => button.textContent?.trim() === 'chat.header.disconnect');
  }

  it.each([
    ['offers End as the retry', true],
    ['shows no End on an ending session End cannot retry', false],
  ])('%s', async (_name, retry) => {
    const chat = sessionState() as any;
    chat.threadStatus.set('ending');
    chat.isConnected.set(false);
    chat.connectionState.set('disconnected');
    chat.endRetryAvailable.set(retry);
    // `kept`: the page stays put, so no router/toast is reached.
    chat.endSession = vi.fn(async () => 'kept');
    const fixture = await mountChat(chat, api, false);
    try {
      const buttons = endButtons(fixture.nativeElement as HTMLElement);
      expect(buttons).toHaveLength(retry ? 1 : 0);
      if (retry) {
        buttons[0].dispatchEvent(new CustomEvent('clicked'));
        await fixture.whenStable();
        expect(chat.endSession).toHaveBeenCalledTimes(1);
      }
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });
});

describe('the slash menu completes the command word, never its argument', () => {
  beforeAll(async () => {
    HTMLElement.prototype.scrollTo = vi.fn();
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const api = {
    getThreadIdeStatus: () => of(null),
    getMyCapabilities: () => of(null),
    getSshHostKeys: () => of({hostname: 'ssh.example.test', host_keys: []}),
  };

  const enter = () => new KeyboardEvent('keydown', {key: 'Enter'});

  it('closes once an argument starts, so Enter sends the whole command', async () => {
    const chat = sessionState() as any;
    chat.sendMessage = vi.fn(async () => true);
    const fixture = await mountChat(chat, api, false);
    try {
      const view = fixture.componentInstance;
      view.onInputChange('/comp');
      expect(view.showSlashMenu()).toBe(true);

      view.inputText = '/compact the pricing decisions';
      view.onInputChange(view.inputText);
      expect(view.showSlashMenu()).toBe(false);

      view.onKeydown(enter());
      expect(chat.sendMessage).toHaveBeenCalledWith('/compact the pricing decisions');
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('Enter on a command typed in full runs it instead of completing it again', async () => {
    const chat = sessionState() as any;
    chat.sendMessage = vi.fn(async () => true);
    const fixture = await mountChat(chat, api, false);
    try {
      const view = fixture.componentInstance;
      view.inputText = '/compact';
      view.onInputChange(view.inputText);
      expect(view.showSlashMenu()).toBe(true);

      view.onKeydown(enter());
      expect(view.showSlashMenu()).toBe(false);
      expect(chat.sendMessage).toHaveBeenCalledWith('/compact');
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('Enter on a partial command still completes it', async () => {
    const chat = sessionState() as any;
    chat.sendMessage = vi.fn(async () => true);
    const fixture = await mountChat(chat, api, false);
    try {
      const view = fixture.componentInstance;
      view.inputText = '/comp';
      view.onInputChange(view.inputText);

      view.onKeydown(enter());
      expect(view.inputText).toBe('/compact ');
      expect(chat.sendMessage).not.toHaveBeenCalled();
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });
});

describe('permission mode surfaces', () => {
  beforeAll(async () => {
    HTMLElement.prototype.scrollTo = vi.fn();
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const api = {
    getThreadIdeStatus: () => of(null),
    getMyCapabilities: () => of(null),
    getSshHostKeys: () => of({hostname: 'ssh.example.test', host_keys: []}),
  };

  it('offers /readonly but never lists the hidden /supervised alias', async () => {
    const fixture = await mountChat(sessionState() as any, api, false);
    try {
      const view = fixture.componentInstance;
      view.onInputChange('/');
      const offered = view.filteredCommands().map((c) => c.command);
      expect(offered).toContain('/readonly');
      expect(offered).toContain('/auto');
      expect(offered).toContain('/autonomous');
      expect(offered).not.toContain('/supervised');
      view.onInputChange('/sup');
      expect(view.filteredCommands()).toEqual([]);
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('labels the header badge from i18n, never the raw value', async () => {
    const chat = sessionState() as any;
    const fixture = await mountChat(chat, api, false);
    try {
      const view = fixture.componentInstance;
      expect(view.permissionModeLabelKey()).toBe('chat.settings.modeReadOnly');
      fixture.detectChanges();
      const text = (fixture.nativeElement as HTMLElement).textContent ?? '';
      expect(text).toContain('chat.settings.modeReadOnly');
      expect(text).not.toMatch(/read_only/i);

      chat.permissionMode.set('auto_accept');
      expect(view.permissionModeLabelKey()).toBe('chat.settings.modeAutoAccept');
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });
});

describe('rewind affordances follow the declared controls', () => {
  beforeAll(async () => {
    HTMLElement.prototype.scrollTo = vi.fn();
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const api = {
    getThreadIdeStatus: () => of(null),
    getMyCapabilities: () => of(null),
    getSshHostKeys: () => of({hostname: 'ssh.example.test', host_keys: []}),
  };

  it('neither offers /rewind nor opens the picker where the session has no rewind', async () => {
    const chat = sessionState() as any;
    chat.sendMessage = vi.fn(async () => true);
    const fixture = await mountChat(chat, api, false);
    try {
      const view = fixture.componentInstance;
      view.onInputChange('/re');
      expect(view.filteredCommands().map((c) => c.command)).not.toContain('/rewind');

      view.openRewindPicker();
      expect(view.rewindPickerOpen()).toBe(false);

      // Typed out in full it is refused out loud — it used to vanish without a
      // word, and is never handed to the agent as chat.
      view.inputText = '/rewind';
      view.send();
      expect(view.rewindPickerOpen()).toBe(false);
      expect(chat.error()).toBe('chat.rewind.unavailable');
      expect(chat.sendMessage).not.toHaveBeenCalled();
      expect(view.inputText).toBe('/rewind');
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('offers /rewind and opens the picker once the session declares it', async () => {
    const chat = sessionState() as any;
    chat.rewindModeAvailable.set(true);
    chat.sendMessage = vi.fn(async () => true);
    const fixture = await mountChat(chat, api, false);
    try {
      const view = fixture.componentInstance;
      view.onInputChange('/re');
      expect(view.filteredCommands().map((c) => c.command)).toContain('/rewind');

      // The picker defers its initial focus; run that timer here rather than
      // let it fire after this file's DOM is gone.
      vi.useFakeTimers();
      view.inputText = '/rewind';
      view.send();
      vi.runOnlyPendingTimers();
      expect(view.rewindPickerOpen()).toBe(true);
      expect(chat.error()).toBeNull();
      expect(chat.sendMessage).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });
});

describe('the continuation a delegation-batch recovery wrote', () => {
  beforeAll(async () => {
    HTMLElement.prototype.scrollTo = vi.fn();
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  const api = {
    getThreadIdeStatus: () => of(null),
    getMyCapabilities: () => of(null),
    getSshHostKeys: () => of({hostname: 'ssh.example.test', host_keys: []}),
  };
  const MODEL_TEXT = '[subagent recovery] This turn was resumed after the process running it was replaced.';

  // The muted line every injected event gets, stating the counts from the
  // row's marker in the viewer's language; the model's text stays on hover.
  // The test pipe renders keys, so the assertion reads which parts show.
  it('states the turn counts it carries, and only the non-zero ones', async () => {
    const chat = sessionState() as any;
    chat.visibleTurns.set([
      {kind: 'system', id: 'continuation', content: MODEL_TEXT, timestamp: 0,
        subagentRecovery: {calls: 4, finished: 1, interrupted: 1, notStarted: 2, declined: 0, retired: 0}},
    ]);
    const fixture = await mountChat(chat, api, false);
    try {
      const line = fixture.nativeElement.querySelector('[data-testid="subagent-recovery-notice"]') as HTMLElement;
      expect(line.getAttribute('title')).toBe(MODEL_TEXT);
      expect(line.textContent!.replace(/\s+/g, ' ').trim()).toBe(
        'info chat.system.subagentRecovery.resumed: chat.system.subagentRecovery.finished' +
          ' · chat.system.subagentRecovery.interrupted · chat.system.subagentRecovery.notStarted',
      );
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });

  it('leaves an event without the marker as the plain line it always was', async () => {
    const chat = sessionState() as any;
    chat.visibleTurns.set([{kind: 'system', id: 'legacy', content: 'report of the orphaned child', timestamp: 0}]);
    const fixture = await mountChat(chat, api, false);
    try {
      expect(fixture.nativeElement.querySelector('[data-testid="subagent-recovery-notice"]')).toBeNull();
      const line = fixture.nativeElement.querySelector('.message-system .system-message') as HTMLElement;
      expect(line.textContent!.replace(/\s+/g, ' ').trim()).toBe('info report of the orphaned child');
      expect(line.hasAttribute('title')).toBe(false);
    } finally {
      fixture.destroy();
      TestBed.resetTestingModule();
    }
  });
});
