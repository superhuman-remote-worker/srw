import {CommonModule} from '@angular/common';
import {HttpClient} from '@angular/common/http';
import {CUSTOM_ELEMENTS_SCHEMA, Pipe, signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {FormsModule} from '@angular/forms';
import {By} from '@angular/platform-browser';
import {Router} from '@angular/router';
import {TranslocoService} from '@jsverse/transloco';
import {of} from 'rxjs';
import {beforeAll, describe, expect, it, vi} from 'vitest';
import {environment} from '../../core/environment';
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
  for (const key of ['isStartingSession', 'isDraftSession', 'isOfficerThread', 'isParked',
    'isResuming', 'isCreating', 'isAwaitingTurn', 'isStreaming', 'isInterrupting', 'isVmSession',
    'hasOlderTurns', 'cloudDiffPanelOpen', 'cloudSyncDegraded', 'protectedCloud',
    'workspaceUpgradeInProgress', 'continueAfterUpgrade', 'rewindInFlight', 'rewindOutcomeUnknown',
    'rewindPreviewLoading', 'rewindModeAvailable', 'summarizeAvailable', 'outboxStalled',
    'draftDefaultsLoading', 'reconnectGaveUp']) state[key] = signal(false);
  for (const key of ['turns', 'visibleTurns', 'pendingAttachments', 'pendingPermissions', 'tasks',
    'outbox', 'outboxIds', 'draftDatasourceIds']) state[key] = signal([]);
  for (const key of ['compaction', 'rewindPrefill', 'rewindPreview', 'runningTool', 'pendingWorkspaceOffer',
    'currentUsage', 'queueState', 'verifiedProjectFolder', 'cloudSessionUrl', 'ncSessionFolder',
    'cloudDiffProbe', 'cloudStagedAt', 'attachmentError', 'error', 'endedAt', 'draftDefaultsError',
    'workspaceLifecycle']) state[key] = signal(null);
  return {...state, isConnected: signal(true), sessionReady: signal(true), threadId: signal('test-thread'),
    threadStatus: signal('active'), sessionTitle: signal('Test session'), connectionState: signal('connected'),
    modelName: signal('Test model'), permissionMode: signal('supervised'), narrationMode: signal('off'),
    startupPhase: signal('ready'), controlTransport: signal('websocket'), citationsByCid: signal(new Map()),
    awaitingElapsedMs: signal(0), reconnectAttempt: signal(0), agentSilenceSeconds: signal(0), cloudChangesCount: signal(0),
    sshHandle: signal('test-thread'), resetWindow: vi.fn(), refreshPendingRewindReceipt: vi.fn(),
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
