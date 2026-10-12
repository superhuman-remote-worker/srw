import {DestroyRef, inject, Injectable, signal} from '@angular/core';
import {HttpErrorResponse} from '@angular/common/http';
import {ApiService} from './api.service';
import {CloudRootStatus} from '../models/api.model';

/** How often the state is read while the user signs in to their cloud: the
 *  server asks the cloud as often (`POLL_SECONDS`). */
export const CLOUD_ROOT_POLL_MS = 3000;

/**
 * "My cloud": the user's own cloud root (main cloud slice 4), connected by
 * the user through the cloud's own sign-in ("Connect your cloud", Nextcloud
 * Login Flow v2). Connect opens the cloud's page in a new tab and reads the
 * state every few seconds until it leaves `connecting` or the flow
 * expires; nothing here ever holds the credential.
 *
 * Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md
 * (slice 4).
 */
@Injectable({providedIn: 'root'})
export class CloudRootService {
  private readonly api = inject(ApiService);

  /** The last state read; null until the first answer. */
  readonly status = signal<CloudRootStatus | null>(null);
  readonly loadFailed = signal(false);
  /** An action (connect, cancel, disconnect) is on its way. */
  readonly busy = signal(false);
  /** The translation key of the last action's failure, or null. */
  readonly actionError = signal<string | null>(null);
  /** A disconnect SRW could not have the cloud revoke: the credential
   *  stays in the account's device list until the user removes it. */
  readonly revokeIncomplete = signal(false);

  private pollTimer: ReturnType<typeof setTimeout> | null = null;

  constructor() {
    inject(DestroyRef).onDestroy(() => this.stopPolling());
  }

  load(): void {
    this.api.getCloudRoot().subscribe({
      next: (status) => this.apply(status),
      error: () => this.loadFailed.set(true),
    });
  }

  /** Start "Connect your cloud". Call it from the click itself: the tab is
   *  opened before the request, since the answer would outlive the
   *  browser's user activation and a later open would be blocked. */
  connect(): void {
    if (this.busy()) return;
    const tab = openBlankTab();
    this.busy.set(true);
    this.actionError.set(null);
    this.revokeIncomplete.set(false);
    this.api.connectCloudRoot().subscribe({
      next: (start) => {
        this.busy.set(false);
        if (!isWebPage(start.login_url)) {
          // Never send the tab anywhere but a web page (the server checks
          // the host; this keeps any other scheme out of the tab).
          tab?.close();
          this.actionError.set('cloudRoot.errors.startFailed');
          this.load();
          return;
        }
        const before = this.status();
        this.apply({
          ...(before ?? emptyStatus()),
          supported: true,
          reason: null,
          state: 'connecting',
          connector_id: start.connector_id,
          failure: null,
          connecting: {login_url: start.login_url, expires_at: start.expires_at},
        });
        if (tab) tab.location.href = start.login_url;
      },
      error: (err: HttpErrorResponse) => {
        tab?.close();
        this.busy.set(false);
        this.actionError.set(
          err?.status === 409 ? 'cloudRoot.errors.unsupported' : 'cloudRoot.errors.startFailed',
        );
        if (err?.status === 409) this.load();
      },
    });
  }

  cancel(): void {
    if (this.busy()) return;
    this.busy.set(true);
    this.actionError.set(null);
    this.api.cancelCloudRootConnect().subscribe({
      next: (status) => {
        this.busy.set(false);
        this.apply(status);
      },
      error: () => {
        this.busy.set(false);
        this.actionError.set('cloudRoot.errors.cancelFailed');
      },
    });
  }

  disconnect(): void {
    if (this.busy()) return;
    this.busy.set(true);
    this.actionError.set(null);
    this.api.disconnectCloudRoot().subscribe({
      next: (status) => {
        this.busy.set(false);
        this.revokeIncomplete.set(status.provider_revoked === false);
        this.apply(status);
      },
      error: () => {
        this.busy.set(false);
        this.actionError.set('cloudRoot.errors.disconnectFailed');
      },
    });
  }

  private apply(status: CloudRootStatus): void {
    this.loadFailed.set(false);
    this.status.set(status);
    if (status.state === 'connecting') this.schedulePoll();
    else this.stopPolling();
  }

  /** Read the state again in a few seconds while the flow lives; once it
   *  expired, one last read says how it ended. */
  private schedulePoll(): void {
    this.stopPolling();
    const expiresAt = Date.parse(this.status()?.connecting?.expires_at ?? '');
    const expired = Number.isFinite(expiresAt) && Date.now() >= expiresAt;
    this.pollTimer = setTimeout(() => {
      this.pollTimer = null;
      this.api.getCloudRoot().subscribe({
        next: (status) => {
          if (expired && status.state === 'connecting') {
            // The server still holds an expired flow: stop asking.
            this.status.set(status);
            return;
          }
          this.apply(status);
        },
        // A failed read keeps polling until the flow expires.
        error: () => {
          if (!expired) this.schedulePoll();
        },
      });
    }, CLOUD_ROOT_POLL_MS);
  }

  private stopPolling(): void {
    if (this.pollTimer !== null) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
  }
}

/** Whether `url` is an absolute http(s) URL: the only kind of page the
 *  cloud's sign-in may be. */
export function isWebPage(url: unknown): url is string {
  if (typeof url !== 'string') return false;
  try {
    const {protocol} = new URL(url);
    return protocol === 'https:' || protocol === 'http:';
  } catch {
    return false;
  }
}

/** A blank tab opened inside the click, cut from this page before it goes
 *  anywhere (the cloud's page must not reach back through `opener`). */
function openBlankTab(): Window | null {
  try {
    const tab = window.open('', '_blank');
    if (tab) tab.opener = null;
    return tab;
  } catch {
    return null;
  }
}

function emptyStatus(): CloudRootStatus {
  return {
    supported: true,
    reason: null,
    state: 'not_connected',
    connector_id: null,
    name: 'My cloud',
    account_id: null,
    granted_at: null,
    client_name: null,
    revoked_at: null,
    revoked_reason: null,
    failure: null,
    connecting: null,
  };
}
