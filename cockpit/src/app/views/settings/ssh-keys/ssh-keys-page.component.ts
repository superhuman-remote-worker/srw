import {ChangeDetectionStrategy, Component, OnInit, inject, signal} from '@angular/core';
import {Router} from '@angular/router';
import {TranslocoPipe} from '@jsverse/transloco';
import {SshKey, SshKeyChallenge, SshKeysService} from '../../../core/services/ssh-keys.service';
import {SidebarToggleComponent} from '../../../shell/sidebar-toggle/sidebar-toggle.component';
import {AppButtonComponent} from '../../../ui/button';
import {AppInputComponent} from '../../../ui/input';
import {AppTextareaComponent} from '../../../ui/textarea';
import {environment} from '../../../core/environment';

/** The signature namespace every registration challenge is minted under.
 *  Mirrors `SIGNATURE_NAMESPACE` in `orchestrator/main.py` — kept here only
 *  as a display fallback; the command always prefers the namespace the
 *  server actually issued on the challenge response. */
const FALLBACK_NAMESPACE = 'srw-ssh-key-registration';

/**
 * Escapes `value` for interpolation inside a single-quoted POSIX shell
 * string: close the quote, emit a literal escaped quote, reopen the quote.
 *
 * `challenge.challenge` (`_mint_ssh_key_challenge` in `orchestrator/main.py`)
 * embeds a human-readable identity label — the user's own
 * `preferred_username` or `email` — sanitized server-side for
 * whitespace/printability/ASCII/length but NOT for shell metacharacters. An
 * apostrophe in that label (a user named `o'brien`) survives every one of
 * those checks and would otherwise close the single-quoted string early,
 * handing the user a broken command (self-targeting — the value is always
 * the signer's own identity, never attacker-controlled). Standard technique;
 * see e.g. the POSIX shell FAQ on quoting a single quote inside single quotes.
 */
function shellSingleQuote(value: string): string {
  return `'${value.replace(/'/g, "'\\''")}'`;
}

interface SshKeyForm {
  name: string;
  publicKey: string;
  signature: string;
}

function emptyForm(): SshKeyForm {
  return {name: '', publicKey: '', signature: ''};
}

/**
 * Settings → SSH Keys.
 *
 * Registers the public key that authenticates a user's SSH connections into
 * their session workspaces (knowledge-base/knowledge/features/
 * workspace_ssh_access.md). Modeled on `api-keys-page.component.ts`, minus
 * the reveal-once banner: a public key is not a secret, so there is nothing
 * to hide after creation.
 *
 * Registration is two steps because possession of the *private* key must be
 * proven before the server accepts a public key claim (see
 * `orchestrator/main.py:create_ssh_key_challenge`):
 *   1. `startRegistration()` mints a nonce and renders the exact
 *      `ssh-keygen -Y sign` command to run locally.
 *   2. The user pastes back the public key and the resulting signature;
 *      `submit()` sends both to the server with the original challenge.
 */
@Component({
  selector: 'app-ssh-keys-page',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    SidebarToggleComponent,
    TranslocoPipe,
    AppButtonComponent,
    AppInputComponent,
    AppTextareaComponent,
  ],
  template: `
    @if (externalClientsEnabled) {
    <div class="ssh-keys-page">
      <div class="ssh-keys-container">
        <div class="page-header">
          <app-sidebar-toggle />
          <h1 class="page-title">{{ 'settings.sshKeys.title' | transloco }}</h1>
        </div>

        <p class="page-desc">{{ 'settings.sshKeys.desc' | transloco }}</p>

        @if (error(); as err) {
          <div class="error-banner" role="alert">{{ err }}</div>
        }

        <!-- Existing keys table -->
        <section class="keys-section">
          <h2 class="section-title">{{ 'settings.sshKeys.existingTitle' | transloco }}</h2>
          @if (keys().length > 0) {
            <div class="keys-table">
              <div class="keys-header">
                <span class="col-name">{{ 'settings.sshKeys.colName' | transloco }}</span>
                <span class="col-type">{{ 'settings.sshKeys.colType' | transloco }}</span>
                <span class="col-fingerprint">{{ 'settings.sshKeys.colFingerprint' | transloco }}</span>
                <span class="col-created">{{ 'settings.sshKeys.colCreated' | transloco }}</span>
                <span class="col-used">{{ 'settings.sshKeys.colLastUsed' | transloco }}</span>
                <span class="col-action"></span>
              </div>
              @for (key of keys(); track key.id) {
                <div class="keys-row" [class.disabled]="key.disabled">
                  <span class="col-name">
                    {{ key.name }}
                    @if (key.disabled) {
                      <span class="disabled-badge">{{ 'settings.sshKeys.disabledBadge' | transloco }}</span>
                    }
                  </span>
                  <span class="col-type mono">{{ key.key_type }}</span>
                  <span class="col-fingerprint mono">{{ key.fingerprint }}</span>
                  <span class="col-created">{{ formatDate(key.created_at) }}</span>
                  <span class="col-used">
                    {{ key.last_used_at ? formatDate(key.last_used_at) : ('common.never' | transloco) }}
                  </span>
                  <span class="col-action">
                    <app-button variant="danger" size="sm" (clicked)="remove(key)">
                      {{ 'common.delete' | transloco }}
                    </app-button>
                  </span>
                </div>
              }
            </div>
          } @else if (!keysLoading()) {
            <p class="empty-state">{{ 'settings.sshKeys.empty' | transloco }}</p>
          }
        </section>

        <!-- Registration flow -->
        <section class="create-section">
          <h2 class="section-title">{{ 'settings.sshKeys.addTitle' | transloco }}</h2>

          @if (!registering()) {
            <p class="section-desc">{{ 'settings.sshKeys.addDesc' | transloco }}</p>
            <app-button
              variant="primary"
              size="md"
              [loading]="requestingChallenge()"
              [disabled]="requestingChallenge()"
              (clicked)="startRegistration()"
            >
              {{
                requestingChallenge()
                  ? ('settings.sshKeys.requestingChallenge' | transloco)
                  : ('settings.sshKeys.addButton' | transloco)
              }}
            </app-button>
          } @else {
            <div class="registration-flow">
              <div class="step">
                <h3 class="step-title">{{ 'settings.sshKeys.step1Title' | transloco }}</h3>
                <p class="section-desc">{{ 'settings.sshKeys.step1Desc' | transloco }}</p>
                <div class="command-row">
                  <pre #commandBlock class="code-block mono">{{ signCommand() }}</pre>
                  <app-button variant="secondary" size="sm" (clicked)="copyCommand(commandBlock)">
                    {{ copied() ? ('common.copied' | transloco) : ('common.copy' | transloco) }}
                  </app-button>
                </div>
              </div>

              <div class="step">
                <h3 class="step-title">{{ 'settings.sshKeys.step2Title' | transloco }}</h3>
                <div class="create-form">
                  <div class="form-row">
                    <app-input
                      [value]="form.name"
                      [placeholder]="'settings.sshKeys.namePlaceholder' | transloco"
                      [disabled]="submitting()"
                      (changed)="form.name = $event"
                    />
                  </div>
                  <div class="form-row">
                    <app-textarea
                      [value]="form.publicKey"
                      [placeholder]="'settings.sshKeys.publicKeyPlaceholder' | transloco"
                      [disabled]="submitting()"
                      [rows]="2"
                      (changed)="form.publicKey = $event"
                    />
                  </div>
                  <div class="form-row">
                    <app-textarea
                      [value]="form.signature"
                      [placeholder]="'settings.sshKeys.signaturePlaceholder' | transloco"
                      [disabled]="submitting()"
                      [rows]="4"
                      (changed)="form.signature = $event"
                    />
                  </div>
                  <div class="form-actions">
                    <app-button
                      variant="primary"
                      size="md"
                      [loading]="submitting()"
                      [disabled]="submitting()"
                      (clicked)="submit()"
                    >
                      {{ submitting() ? ('settings.sshKeys.submitting' | transloco) : ('settings.sshKeys.submit' | transloco) }}
                    </app-button>
                    <app-button variant="ghost" size="md" [disabled]="submitting()" (clicked)="cancelRegistration()">
                      {{ 'common.cancel' | transloco }}
                    </app-button>
                  </div>
                </div>
              </div>
            </div>
          }
        </section>
      </div>
    </div>
    }
  `,
  styleUrl: './ssh-keys-page.component.scss',
})
export class SshKeysPageComponent implements OnInit {
  readonly externalClientsEnabled = environment.externalClientsEnabled;
  readonly service = inject(SshKeysService);
  private readonly router = inject(Router);

  // ── key list state ──
  // Owned here rather than read off `service.keys()`: the service's
  // methods are Promise-returning (ruling P-4 — see ssh-keys.service.ts),
  // so the page awaits them and holds its own presentation-layer signal
  // instead of subscribing to a push-updated one on the service.
  readonly keys = signal<SshKey[]>([]);
  readonly keysLoading = signal(false);

  readonly error = signal<string | null>(null);

  // ── registration flow state ──
  readonly registering = signal(false);
  readonly requestingChallenge = signal(false);
  readonly submitting = signal(false);
  readonly challenge = signal<SshKeyChallenge | null>(null);
  readonly signCommand = signal('');
  readonly copied = signal(false);

  form: SshKeyForm = emptyForm();

  ngOnInit(): void {
    if (!this.externalClientsEnabled) {
      void this.router.navigateByUrl('/settings/general');
      return;
    }
    void this.refreshKeys();
  }

  /** The sole owner of list refresh (fix round 1, minor "double fetch per
   *  mutation" — the service no longer calls `loadKeys()` itself). Errors
   *  surface via `error()` rather than rendering as an empty list: the
   *  service's `loadKeys()` no longer swallows failures either (fix round
   *  1, minor: a user who sees "no keys yet" on a failed request may
   *  re-register a key they believe is gone). */
  private async refreshKeys(): Promise<void> {
    this.keysLoading.set(true);
    try {
      const keys = await this.service.loadKeys();
      this.keys.set(keys);
      this.error.set(null);
    } catch (err) {
      this.error.set(this.extractError(err, 'Could not load your SSH keys.'));
    } finally {
      this.keysLoading.set(false);
    }
  }

  /** Step 1: mint a possession challenge and render the sign command. */
  async startRegistration(): Promise<void> {
    this.error.set(null);
    this.requestingChallenge.set(true);
    try {
      const challenge = await this.service.requestChallenge();
      this.challenge.set(challenge);
      this.signCommand.set(this.buildSignCommand(challenge));
      this.registering.set(true);
    } catch (err) {
      this.error.set(this.extractError(err, 'Could not request a challenge.'));
    } finally {
      this.requestingChallenge.set(false);
    }
  }

  cancelRegistration(): void {
    this.registering.set(false);
    this.requestingChallenge.set(false);
    this.submitting.set(false);
    this.challenge.set(null);
    this.signCommand.set('');
    this.form = emptyForm();
  }

  /** Step 2: send the pasted public key + signature back with the original
   *  challenge. */
  async submit(): Promise<void> {
    const challenge = this.challenge();
    if (!challenge) return;
    const name = this.form.name.trim();
    const publicKey = this.form.publicKey.trim();
    const signature = this.form.signature.trim();
    if (!name || !publicKey || !signature) {
      this.error.set('Give the key a name, and paste both the public key and the signature.');
      return;
    }
    this.error.set(null);
    this.submitting.set(true);
    try {
      await this.service.createKey({
        name,
        publicKey,
        challenge: challenge.challenge,
        signature,
      });
      this.cancelRegistration();
      await this.refreshKeys();
    } catch (err) {
      this.error.set(this.extractError(err, 'Could not register this key.'));
    } finally {
      this.submitting.set(false);
    }
  }

  async remove(key: SshKey): Promise<void> {
    if (
      !window.confirm(
        `Delete the SSH key "${key.name}"? Anything currently connected with it will lose access immediately.`,
      )
    ) {
      return;
    }
    this.error.set(null);
    try {
      await this.service.deleteKey(key.id);
      await this.refreshKeys();
    } catch (err) {
      this.error.set(this.extractError(err, 'Could not delete this key.'));
    }
  }

  copyCommand(el: HTMLElement): void {
    void navigator.clipboard.writeText(this.signCommand()).then(() => {
      this.copied.set(true);
      setTimeout(() => this.copied.set(false), 2000);
    });
    // Selecting the text gives non-clipboard-API browsers a fallback.
    const selection = window.getSelection();
    if (selection) {
      const range = document.createRange();
      range.selectNodeContents(el);
      selection.removeAllRanges();
      selection.addRange(range);
    }
  }

  formatDate(iso: string): string {
    try {
      return new Date(iso).toLocaleString();
    } catch {
      return iso;
    }
  }

  /** `mktemp` rather than a fixed `/tmp/srw` path (M-5): a pre-planted
   *  symlink at a predictable, world-guessable path in a command users are
   *  told to paste turns into a file clobber on a shared machine. The
   *  challenge token is escaped with `shellSingleQuote` (M-6) rather than
   *  interpolated raw between hand-written quotes. */
  private buildSignCommand(challenge: SshKeyChallenge): string {
    const namespace = challenge.namespace || FALLBACK_NAMESPACE;
    return (
      `f=$(mktemp) && echo -n ${shellSingleQuote(challenge.challenge)} > "$f" && ` +
      `ssh-keygen -Y sign -f ~/.ssh/id_ed25519 -n ${namespace} "$f" && ` +
      `cat "$f.sig"`
    );
  }

  private extractError(err: unknown, fallback: string): string {
    const detail = (err as {error?: {detail?: string}} | undefined)?.error?.detail;
    return detail ?? fallback;
  }
}
