import {ChangeDetectionStrategy, Component, OnDestroy, computed, input, signal} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {AppCopyFieldComponent, copyText} from '../../../ui/copy-field';
import {HANDLE_PATTERN, buildJetBrainsCommand, buildSshConfig} from '../../../core/util/ssh-config';

/** How long the "Copied" label stays visible after a successful copy —
 *  mirrors `AppCopyFieldComponent`'s own COPIED_RESET_MS. */
const COPIED_RESET_MS = 2500;

/** ssh-access.md on the public repo. Linked from
 *  the prerequisites line below (I-1): the PAT and the helper are both
 *  documented there and nowhere else the product surfaces. */
const SSH_DOCS_URL =
    'https://github.com/superhuman-remote-worker/srw/blob/main/ssh-access.md';

/**
 * Session view → "Connect over SSH". Renders the `~/.ssh/config` block for
 * this session's workspace, the gateway host key fingerprint for
 * first-connect verification, the JetBrains Gateway listener command, and
 * the documented concurrency seams (D4) — see
 * knowledge-base/knowledge/features/workspace_ssh_access.md §5.1/§5.4.
 *
 * Own component and stylesheet, deliberately: `persistent-chat.component.scss`
 * is already close to its 48 kB per-component compiled-CSS budget, and this
 * panel is mounted through an `@defer` block so neither it nor its
 * stylesheet lands in the initial bundle (ruling P-11 — the initial bundle
 * itself is within ~20 kB of its own 2.75 MB hard-fail budget).
 *
 * `origin` is read from `window.location.origin` rather than taken as an
 * input (ruling P-8): the gateway's Origin allow-list has no default and
 * fails closed, so the only origin that is ever correct to send is the one
 * the cockpit is actually being served from.
 */
@Component({
    selector: 'app-ssh-connect-panel',
    standalone: true,
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [TranslocoPipe, AppCopyFieldComponent],
    template: `
    @if (available()) {
      <div class="ssh-connect-panel">
        <p class="ssh-connect-panel__intro">{{ 'chat.ssh.intro' | transloco }}</p>

        <p class="ssh-connect-panel__hint">
          {{ 'chat.ssh.prereqs' | transloco }}
          <a
            class="ssh-connect-panel__docs-link"
            [href]="sshDocsUrl"
            target="_blank"
            rel="noopener"
          >{{ 'chat.ssh.prereqsLink' | transloco }}</a>
        </p>

        <section class="ssh-connect-panel__section">
          <h3 class="ssh-connect-panel__heading">{{ 'chat.ssh.configTitle' | transloco }}</h3>
          <div class="ssh-connect-panel__block-row">
            <pre class="ssh-connect-panel__block">{{ configBlock() }}</pre>
            <button
              type="button"
              class="ssh-connect-panel__copy-btn"
              (click)="copyConfig()"
            >
              {{ (copiedConfig() ? 'common.copied' : 'common.copy') | transloco }}
            </button>
          </div>
          <p class="ssh-connect-panel__hint">{{ 'chat.ssh.configHint' | transloco }}</p>
          <p class="ssh-connect-panel__hint">{{ 'chat.ssh.identityHint' | transloco }}</p>
        </section>

        <section class="ssh-connect-panel__section">
          <app-copy-field
            [label]="'chat.ssh.handleLabel' | transloco"
            [value]="handle() ?? ''"
          />
          <app-copy-field
            [label]="'chat.ssh.hostKeyLabel' | transloco"
            [value]="hostKeyFingerprint()"
          />
          <p class="ssh-connect-panel__hint">{{ 'chat.ssh.hostKeyHint' | transloco }}</p>
        </section>

        <section class="ssh-connect-panel__section">
          <h3 class="ssh-connect-panel__heading">{{ 'chat.ssh.jetBrainsTitle' | transloco }}</h3>
          <p class="ssh-connect-panel__hint">{{ 'chat.ssh.jetBrainsDesc' | transloco }}</p>
          <div class="ssh-connect-panel__block-row">
            <pre class="ssh-connect-panel__block">{{ jetBrainsCommand() }}</pre>
            <button
              type="button"
              class="ssh-connect-panel__copy-btn"
              (click)="copyJetBrains()"
            >
              {{ (copiedJetBrains() ? 'common.copied' : 'common.copy') | transloco }}
            </button>
          </div>
        </section>

        <section class="ssh-connect-panel__section ssh-connect-panel__seams">
          <p class="ssh-connect-panel__hint">{{ 'chat.ssh.seams.rewindWarning' | transloco }}</p>
          <p class="ssh-connect-panel__hint">{{ 'chat.ssh.seams.refused' | transloco }}</p>
        </section>
      </div>
    } @else {
      <p class="ssh-connect-panel__unavailable">{{ 'chat.ssh.unavailable' | transloco }}</p>
    }
  `,
    styleUrl: './ssh-connect-panel.component.scss',
})
export class SshConnectPanelComponent implements OnDestroy {
    readonly handle = input<string | null>(null);
    readonly apiHost = input<string>('');
    readonly sshHost = input<string>('');
    readonly hostKeyFingerprint = input<string>('');

    /** Read once — the panel is per-mount, and the origin cannot change
     *  without a full page reload. */
    private readonly origin = typeof window !== 'undefined' ? window.location.origin : '';

    /** Bound into the template's docs link (I-1). */
    readonly sshDocsUrl = SSH_DOCS_URL;

    /** False when there is nothing sane to render yet: no handle minted, a
     *  handle that fails validation (never trusted, even server-supplied),
     *  or a deployment with no configured gateway. */
    readonly available = computed(() => {
        const handle = this.handle();
        if (!handle || !HANDLE_PATTERN.test(handle)) return false;
        return this.sshHost().length > 0;
    });

    /** `buildSshConfig` throws on anything it doesn't trust; degrade to an
     *  empty block rather than let a malformed value crash the panel. */
    readonly configBlock = computed(() => {
        try {
            return buildSshConfig({
                handle: this.handle() ?? '',
                apiHost: this.apiHost(),
                sshHost: this.sshHost(),
                origin: this.origin,
            });
        } catch {
            return '';
        }
    });

    /** Same degrade-don't-throw posture as `configBlock`. Carries `origin`
     *  too (ruling I-2): the JetBrains listener command is the one client
     *  surface that previously had no way to override srw-ssh-proxy's wrong
     *  default guess on this chart's own topology, and this panel already
     *  holds the correct value. */
    readonly jetBrainsCommand = computed(() => {
        try {
            return buildJetBrainsCommand({apiHost: this.apiHost(), origin: this.origin});
        } catch {
            return '';
        }
    });

    readonly copiedConfig = signal(false);
    readonly copiedJetBrains = signal(false);
    private configResetTimer: ReturnType<typeof setTimeout> | null = null;
    private jetBrainsResetTimer: ReturnType<typeof setTimeout> | null = null;

    async copyConfig(): Promise<void> {
        if (!(await copyText(this.configBlock()))) return;
        this.copiedConfig.set(true);
        if (this.configResetTimer) clearTimeout(this.configResetTimer);
        this.configResetTimer = setTimeout(() => this.copiedConfig.set(false), COPIED_RESET_MS);
    }

    async copyJetBrains(): Promise<void> {
        if (!(await copyText(this.jetBrainsCommand()))) return;
        this.copiedJetBrains.set(true);
        if (this.jetBrainsResetTimer) clearTimeout(this.jetBrainsResetTimer);
        this.jetBrainsResetTimer = setTimeout(() => this.copiedJetBrains.set(false), COPIED_RESET_MS);
    }

    ngOnDestroy(): void {
        if (this.configResetTimer) clearTimeout(this.configResetTimer);
        if (this.jetBrainsResetTimer) clearTimeout(this.jetBrainsResetTimer);
    }
}
