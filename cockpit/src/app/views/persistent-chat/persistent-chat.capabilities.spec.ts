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
    'cloudDiffProbe', 'cloudStagedAt', 'attachmentError', 'error', 'endedAt', 'draftDefaultsError']) state[key] = signal(null);
  return {...state, isConnected: signal(true), sessionReady: signal(true), threadId: signal('test-thread'),
    threadStatus: signal('active'), sessionTitle: signal('Test session'), connectionState: signal('connected'),
    modelName: signal('Test model'), permissionMode: signal('supervised'), narrationMode: signal('off'),
    startupPhase: signal('ready'), controlTransport: signal('websocket'), citationsByCid: signal(new Map()),
    awaitingElapsedMs: signal(0), reconnectAttempt: signal(0), agentSilenceSeconds: signal(0), cloudChangesCount: signal(0),
    sshHandle: signal('test-thread'), resetWindow: vi.fn(), refreshPendingRewindReceipt: vi.fn(),
  };
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
      TestBed.resetTestingModule();
      TestBed.configureTestingModule({imports: [PersistentChatComponent], providers: [
        {provide: PersistentChatService, useValue: sessionState()},
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
        imports: [CommonModule, FormsModule, TestTranslocoPipe, AppMenuComponent, AppMenuItemComponent], schemas: [CUSTOM_ELEMENTS_SCHEMA],
      }});
      const fixture = TestBed.createComponent(PersistentChatComponent);
      fixture.detectChanges();
      await fixture.whenStable();
      fixture.detectChanges();

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
