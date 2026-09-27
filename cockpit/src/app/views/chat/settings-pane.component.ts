import {
    Component,
    DestroyRef,
    ElementRef,
    computed,
    effect,
    inject,
    input,
    output,
    signal,
    viewChild,
} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {forkJoin} from 'rxjs';
import {AgentSettingsComponent} from '../agent-settings/agent-settings.component';
import {readConfigPath, TierReachability} from '../agent-settings/agent-settings.types';
import {
    enabledCategoryKeys,
    toolsFragment,
} from '../agent-settings/resolved-toolset';
import {deepMergeConfig} from '../agent-settings/config-merge';
import {
    ConfigUpdateOutcome,
    PersistentChatService,
    NarrationMode,
    PermissionMode,
} from '../../core/services/persistent-chat.service';
import {ApiService, SessionToolGroupsResponse} from '../../core/services/api.service';
import {CapabilitiesService} from '../../core/services/capabilities.service';
import {ModelService} from '../../core/services/model.service';
import {Datasource} from '../../core/models/api.model';
import {AppIconComponent} from '../../ui/icon';
import {AppIconButtonComponent} from '../../ui/icon-button';
import {AppButtonComponent} from '../../ui/button';
import {AppDialogComponent} from '../../ui/dialog';

/** Paths tracked by the desired-state diff. Everything the live path honors
 *  except tool groups (which diff as enabled-flags) and workspace (its own
 *  upgrade verb). */
const TRACKED_LLM_PATHS = ['llm.model', 'llm.temperature', 'llm.reasoning_level'] as const;

/** Tracked-state prefix for a locked-on category's requested additions.
 *
 *  Deliberately NOT a real config path — nothing reads `tools+<c>` out of a
 *  config, and it must not collide with the `tools.<c>` boolean it sits beside,
 *  which tracks a different fact about the same category (whether it is on, not
 *  whether the user asked to complete it). */
const TOOL_ADDITIONS_PREFIX = 'tools+';

/** How long to coalesce control changes before applying. One batch = one
 *  config.update = one prompt-cache invalidation, however many toggles the
 *  user flips in quick succession (live_session_settings.md, principle 4).
 *  Also debounces the per-tick temperature slider. */
const APPLY_DEBOUNCE_MS = 400;

/**
 * Live session settings pane (live_session_settings.md, Slice A).
 *
 * Hosts the shared AgentSettingsComponent in `live` mode next to the chat
 * (content-switched with the canvas pane). Every control edit is collected
 * through the sub-groups' existing override state, then this host diffs the
 * desired state against what was last applied and dispatches only the delta:
 * permission/narration via their dedicated verbs (they broadcast + persist),
 * everything else via one coalesced `config.update`. Pin-only — live mode
 * renders no reset-to-default affordances (principle 6).
 */
@Component({
    selector: 'app-settings-pane',
    standalone: true,
    imports: [
        AgentSettingsComponent,
        TranslocoPipe,
        AppIconComponent,
        AppIconButtonComponent,
        AppButtonComponent,
        AppDialogComponent,
    ],
    template: `
      <div class="pane-root" #paneRoot>
        <div class="pane-header">
          <app-icon size="sm">tune</app-icon>
          <span class="pane-title">{{ 'chat.settingsPane.title' | transloco }}</span>
          <span class="pane-spacer"></span>
          <app-icon-button size="sm"
                           [ariaLabel]="'chat.settingsPane.close' | transloco"
                           (clicked)="closeRequested.emit()">
            <app-icon size="sm">close</app-icon>
          </app-icon-button>
        </div>

        <div class="pane-body">
          <app-agent-settings
            mode="live"
            [config]="liveConfig()"
            [resolvedToolset]="resolvedToolset()"
            [readsResolvedToolset]="true"
            [disabled]="!chat.isConnected()"
            [gatedCapabilities]="capabilities.grants() ?? null"
            [datasources]="pickerDatasources()"
            [loadingDatasources]="loadingDatasources()"
            [datasourceLoadError]="datasourceLoadError()"
            [datasourceContextKey]="'live:' + (chat.threadId() ?? '')"
            [datasourceDefaultsEnabled]="capabilities.datasourceScopeAutoAttachAvailable()"
            [initialDatasourceIds]="attachedIds()"
            [lockedDatasourceIds]="lockedDatasourceIds()"
            [liteBackend]="isLiteBackend()"
            [liveTier]="workspaceTier()"
            [tierReachability]="tierReachability()"
            [upgradeInProgress]="chat.workspaceUpgradeInProgress()"
            (change)="onSettingsChange()"
            (retryDatasources)="retryDatasourceLoad()"
            (tierChangeRequested)="onTierPicked($event)"
          />

          <!-- Set-at-creation surfaces, shown for honesty (criterion 7) -->
          <p class="fixed-note">{{ 'chat.settingsPane.fixedNote' | transloco }}</p>
        </div>
      </div>

      <!-- Tier moves are one-way and expensive (a VM is a cold image import of
           several minutes), so unlike every other control in this pane they
           confirm before dispatch. The agent's offer card and
           /upgrade-workspace stay direct: both already carry explicit intent. -->
      <app-dialog
        [open]="pendingTier() !== null"
        [title]="pendingTierCopy() + '.title' | transloco"
        size="sm"
        (closed)="pendingTier.set(null)"
      >
        <p class="upgrade-body">
          {{ pendingTierCopy() + '.body' | transloco }}
        </p>
        <p class="upgrade-body irreversible">
          {{ 'chat.settingsPane.upgradeConfirm.irreversible' | transloco }}
        </p>
        <ng-container appDialogActions>
          <app-button variant="secondary" size="sm" (clicked)="pendingTier.set(null)">
            {{ 'common.cancel' | transloco }}
          </app-button>
          <app-button variant="primary" size="sm" (clicked)="confirmUpgrade()">
            {{ 'chat.settingsPane.upgradeConfirm.confirm' | transloco }}
          </app-button>
        </ng-container>
      </app-dialog>
    `,
    styles: `
      .pane-root {
        display: flex;
        flex-direction: column;
        width: 100%;
        height: 100%;
        min-width: 0;
        background: var(--panel-bg);
      }
      .pane-header {
        display: flex;
        align-items: center;
        gap: 8px;
        flex: 0 0 auto;
        padding: 8px 12px;
        border-bottom: 1px solid var(--border-color);
        color: var(--text-primary);
      }
      .pane-title { font-weight: 600; font-size: 13px; }
      .pane-spacer { flex: 1; }
      .pane-body {
        flex: 1;
        overflow-y: auto;
        padding: 12px;
        min-height: 0;
      }
      .fixed-note {
        display: block;
        margin-top: 16px;
        font-size: 11px;
        line-height: 1.4;
        color: var(--text-muted);
      }
      .upgrade-body {
        margin: 0 0 10px;
        font-size: 13px;
        line-height: 1.5;
        color: var(--text-primary);
      }
      .upgrade-body.irreversible {
        margin-bottom: 0;
        color: var(--text-muted);
      }
    `,
})
export class SettingsPaneComponent {
    readonly chat = inject(PersistentChatService);
    readonly capabilities = inject(CapabilitiesService);
    private readonly api = inject(ApiService);
    private readonly modelService = inject(ModelService);
    private readonly destroyRef = inject(DestroyRef);

    /** Whether the pane is currently shown (drives the lazy thread fetch). */
    readonly active = input(false);
    readonly closeRequested = output<void>();

    private readonly settings = viewChild(AgentSettingsComponent);
    private readonly paneRoot = viewChild<unknown, ElementRef<HTMLElement>>('paneRoot', {read: ElementRef});

    /** Redacted config_override from thread metadata (fetched on open). */
    private readonly threadOverride = signal<Record<string, unknown>>({});
    /** The session's resolved toolset — the agent's own answer where there is
     *  one. null = not fetched yet, older orchestrator, or the request failed;
     *  the tools surface then degrades to its static list. Always set BEFORE
     *  prefill/anchor — see loadThread. */
    readonly resolvedToolset = signal<SessionToolGroupsResponse | null>(null);
    /** The categories the answer says are ON — the diff baseline, and the one
     *  definition of "enabled" this pane uses. */
    private readonly resolvedCategories = computed(
        () => this.resolvedToolset()?.categories ?? null,
    );
    readonly loadingThread = signal(false);
    /** The session's currently attached datasource ids (durable selection;
     *  optimistically advanced when the pane dispatches a change). */
    readonly attachedIds = signal<string[]>([]);
    /** Eligible datasources shown in the picker: the create-flow eligible
     *  union, minus unattached kb entries (not addable live — v1). */
    readonly pickerDatasources = signal<Datasource[]>([]);
    readonly loadingDatasources = signal(false);
    readonly datasourceLoadError = signal(false);
    private datasourceProjectIds: string[] = [];
    private datasourceRequestSerial = 0;
    /** kb entries render frozen: knowledge bindings only rewire on attach. */
    readonly lockedDatasourceIds = computed(() =>
        this.pickerDatasources().filter((ds) => ds.type === 'kb').map((ds) => ds.id),
    );
    /** Thread id the pane last prefilled for (re-prefill on session switch). */
    private prefilledThread: string | null = null;
    /** Desired state actually dispatched last — the diff baseline. */
    private lastApplied: Record<string, unknown> | null = null;
    private applyTimer: ReturnType<typeof setTimeout> | null = null;

    /** The session's current effective config: the durable overrides overlaid
     *  with the live signals (which win — they track config.changed acks, so
     *  the sub-groups' "resolved default" is always the running state).
     *
     *  Tool groups are NOT in here any more. They used to ride a synthesised
     *  defaults layer, because a config-shaped fragment was the only way to
     *  tell the tools control which groups were on. The resolved read answers
     *  that directly and answers it better — it sees the runtime injection
     *  layer, the backend capability gate and the grants, none of which any
     *  config layer can express. */
    readonly liveConfig = computed(() => {
        const live: Record<string, unknown> = {
            llm: {
                ...(this.chat.modelName() ? {model: this.chat.modelName()} : {}),
                ...(this.chat.temperature() != null ? {temperature: this.chat.temperature()} : {}),
            },
            interactive: {
                permission_mode: this.chat.permissionMode(),
                narration_mode: this.chat.narrationMode(),
            },
        };
        return deepMergeConfig(this.threadOverride(), live);
    });

    readonly workspaceTier = computed(() =>
        this.chat.workspaceTier()
        ?? (readConfigPath(this.threadOverride(), 'workspace.backend') as string)
        ?? 'virtual',
    );
    readonly isLiteBackend = computed(() =>
        ['virtual', 'none'].includes(this.workspaceTier()),
    );
    /** Whether this session may move to a VM. Fail closed while loading; null
     *  = admin/unrestricted. The PDP key is `vm_workspace`
     *  (src/core/capability_grants.py) — the server-side upgrade gate enforces
     *  the same grant either way. */
    private readonly vmGranted = computed(() => {
        const g = this.capabilities.grants();
        return g === null ? true : g?.['vm_workspace'] === true;
    });

    /**
     * Which tiers this session can move to, and why not when it can't.
     *
     * The ladder is one-way and partial: `virtual → sandbox|vm`, `sandbox → vm`.
     * Downgrade is an explicit non-goal (workspace_tier_upgrade.md §"Non-goals")
     * and `none` has no durable anchor to seed from, so a `none` session can
     * reach nothing at all — the row renders static text in that case.
     *
     * Every refusal carries a reason so it renders in the option itself rather
     * than arriving as a rejected click.
     */
    readonly tierReachability = computed<Record<string, TierReachability>>(() => {
        const vm: TierReachability = this.vmGranted() ? 'ok' : 'needsApproval';
        const map: Record<string, TierReachability> = {};
        switch (this.workspaceTier()) {
            case 'virtual':
                map['sandbox'] = 'ok';
                map['vm'] = vm;
                map['none'] = 'downgrade';
                break;
            case 'sandbox':
                map['vm'] = vm;
                map['virtual'] = 'downgrade';
                map['none'] = 'downgrade';
                break;
            case 'vm':
                map['sandbox'] = 'downgrade';
                map['virtual'] = 'downgrade';
                map['none'] = 'downgrade';
                break;
            // `none`, or a tier this build does not know: nothing reachable,
            // so the row falls back to static text.
        }
        // A session with no upgrade transport (the stateless lane) can move
        // nowhere, whatever the ladder allows: say so in the option instead
        // of refusing after the confirmation dialog.
        if (this.chat.controlTransport('upgrade-to-workspace') !== 'websocket') {
            for (const [tier, state] of Object.entries(map)) {
                if (state === 'ok' || state === 'needsApproval') map[tier] = 'sessionUnsupported';
            }
        }
        return map;
    });

    /** The tier awaiting confirmation; non-null opens the dialog. */
    readonly pendingTier = signal<string | null>(null);
    /** Copy namespace for the open dialog. Only `sandbox`/`vm` are ever
     *  reachable, so only those two have confirmation copy. */
    readonly pendingTierCopy = computed(() =>
        `chat.settingsPane.upgradeConfirm.${this.pendingTier() ?? 'sandbox'}`,
    );

    /** A tier the user picked in the settings row. Re-checked here rather than
     *  trusted: the row is a view, this is the gate. */
    onTierPicked(tier: string): void {
        if (this.tierReachability()[tier] !== 'ok') return;
        if (this.chat.workspaceUpgradeInProgress()) return;
        this.pendingTier.set(tier);
    }

    confirmUpgrade(): void {
        const tier = this.pendingTier();
        this.pendingTier.set(null);
        if (tier === 'sandbox' || tier === 'vm') this.chat.upgradeWorkspace(tier);
    }

    constructor() {
        // The model picker needs the catalog; the chat page never loads it
        // (the retired popover's host did).
        this.modelService.load();

        effect(() => {
            const threadId = this.chat.threadId();
            if (!this.active() || !threadId) return;
            if (this.prefilledThread === threadId) return;
            this.prefilledThread = threadId;
            this.loadThread(threadId);
        });

        this.destroyRef.onDestroy(() => {
            if (this.applyTimer) clearTimeout(this.applyTimer);
        });
    }

    /** Scroll the model control into view (status-chip entry point). */
    scrollToModel(): void {
        this.paneRoot()?.nativeElement
            .querySelector('app-model-group')
            ?.scrollIntoView({block: 'start', behavior: 'smooth'});
    }

    onSettingsChange(): void {
        if (this.applyTimer) clearTimeout(this.applyTimer);
        this.applyTimer = setTimeout(() => {
            this.applyTimer = null;
            this.applyChanges();
        }, APPLY_DEBOUNCE_MS);
    }

    private loadThread(threadId: string): void {
        this.loadingThread.set(true);
        this.loadingDatasources.set(true);
        this.datasourceLoadError.set(false);
        this.datasourceRequestSerial += 1;
        // EVERY per-thread anchor is dropped here, before the fetch, and the
        // baseline most of all. `lastApplied` used to survive a thread switch:
        // for the whole request window `applyChanges` would diff the NEW
        // session's desired state against the OLD session's baseline and
        // dispatch the difference — into the new session. `threadOverride`
        // had the same shape of bug, feeding liveConfig() the previous
        // thread's durable config while the pane already pointed elsewhere.
        // The window was sub-second until the read grew a pod probe; it is now
        // up to 8s. Clearing the baseline is not merely safer, it is what
        // makes applyChanges' "no baseline ⇒ re-arm, never absorb" guard
        // actually cover a session switch.
        this.lastApplied = null;
        this.threadOverride.set({});
        this.attachedIds.set([]);
        this.pickerDatasources.set([]);
        // Honest "unknown" while loading: the tools surface degrades to its
        // static list until the server answers, so nothing is shown as
        // measured that has not been measured.
        this.resolvedToolset.set(null);
        // forkJoin, NOT two independent subscribes: the tool-group answer must
        // land BEFORE prefillFromConfig and BEFORE the lastApplied anchor. If
        // it arrived after, liveConfig() would shift under a stale baseline and
        // the next change would dispatch a config.update disabling three tool
        // groups that the user never touched. Both observables are
        // catchError → of(null) in ApiService, so neither can starve the join.
        forkJoin({
            thread: this.api.getPersistentThread(threadId),
            toolGroups: this.api.getSessionToolGroups(threadId),
        }).subscribe(({thread, toolGroups}) => {
            // prefilledThread is claimed synchronously before the fetch, so a
            // late response for a superseded thread would clobber the pane the
            // user is actually looking at.
            if (this.chat.threadId() !== threadId) return;
            this.loadingThread.set(false);
            this.resolvedToolset.set(toolGroups);
            const metadata = (thread?.['metadata'] ?? {}) as Record<string, unknown>;
            const override = (metadata['config_override'] ?? {}) as Record<string, unknown>;
            this.threadOverride.set(override);
            if (this.chat.workspaceTier() === null) {
                const tier = readConfigPath(override, 'workspace.backend') as string | null;
                if (tier) this.chat.workspaceTier.set(tier);
            }

            const ids = ((metadata['datasource_ids'] ?? []) as string[]).map(String);
            this.attachedIds.set(ids);

            // Prefill the sub-groups' baselines from the effective config
            // (this also clears any pins made while the fetch was in flight —
            // the datasource picker gets the same reset), then anchor the diff
            // baseline to the CONFIG-ONLY state — never to getOverrides(),
            // which would silently absorb a pending pin.
            this.settings()?.prefillFromConfig(this.liveConfig());
            // Tool groups anchor to the AGENT's answer, not to the config: the
            // config says what was asked for and the answer says what is held,
            // and on a real session those differed by 28 names.
            const categories = this.resolvedCategories();
            if (categories) this.settings()?.prefillFromResolvedToolset(categories);
            this.settings()?.resetDatasourceSelection();
            this.lastApplied = this.desiredState({});

            // Eligible list is loaded separately so a failed context read is a
            // visible retry state, never a believable empty picker. Attached
            // ids missing from the picker remain preserved invisibly in every
            // dispatched set — see canonicalDatasourceIds. Unattached kb
            // entries are hidden: knowledge bindings don't rewire live (v1).
            const projectIds = ((thread?.['project_ids'] ?? []) as string[]).map(String);
            this.datasourceProjectIds = projectIds;
            this.loadEligibleDatasources(threadId, ids, projectIds);
        });
    }

    retryDatasourceLoad(): void {
        const threadId = this.chat.threadId();
        if (!threadId) return;
        this.loadEligibleDatasources(threadId, this.attachedIds(), this.datasourceProjectIds);
    }

    private loadEligibleDatasources(
        threadId: string,
        attachedIds: string[],
        projectIds: string[],
    ): void {
        const serial = ++this.datasourceRequestSerial;
        this.loadingDatasources.set(true);
        this.datasourceLoadError.set(false);
        this.api.getEligibleDatasources(projectIds).subscribe({
            next: (eligible) => {
                if (serial !== this.datasourceRequestSerial || this.chat.threadId() !== threadId) return;
                const shown = eligible.filter(
                    ds => ds.type !== 'kb' || attachedIds.includes(ds.id),
                );
                const visibleIds = new Set(shown.map(ds => ds.id));
                // A stored attachment that fell out of eligibility is a live
                // fail-closed conflict, not something to silently preserve
                // forever. Render a non-enumerating placeholder so the user
                // can explicitly detach it without leaking its former name or
                // project association.
                const unavailable: Datasource[] = attachedIds
                    .filter(id => !visibleIds.has(id))
                    .map(id => ({
                        id,
                        name: id,
                        description: null,
                        type: 'generic',
                        connection_url: null,
                        cli_hint: null,
                        default_branch: null,
                        job_id: null,
                        created_at: '',
                        updated_at: '',
                        unavailable: true,
                    }));
                this.pickerDatasources.set([...shown, ...unavailable]);
                this.loadingDatasources.set(false);
            },
            error: () => {
                if (serial !== this.datasourceRequestSerial || this.chat.threadId() !== threadId) return;
                this.pickerDatasources.set([]);
                this.datasourceLoadError.set(true);
                this.loadingDatasources.set(false);
            },
        });
    }

    /** The full desired selection: the picker's checked set plus attached ids
     *  the picker doesn't show (kb hidden entries, revoked-visibility rows,
     *  eligible-fetch failures) — those must survive every dispatch, or an
     *  unrelated toggle would silently detach them. Sorted for stable
     *  comparison. */
    private canonicalDatasourceIds(): string[] {
        const groupIds = this.settings()?.getSelectedDatasourceIds() ?? [];
        const pickerIds = new Set(this.pickerDatasources().map((ds) => ds.id));
        const hidden = this.attachedIds().filter((id) => !pickerIds.has(id));
        return [...new Set([...groupIds, ...hidden])].sort();
    }

    /** Flatten the pane's tracked surface into path → value. Tool groups
     *  normalize to booleans so list-content differences never masquerade as
     *  changes; dispatch re-expands them through the vocabulary mirror. */
    private currentDesiredState(): Record<string, unknown> {
        return this.desiredState(this.settings()?.getOverrides() ?? {});
    }

    private desiredState(overrides: Record<string, unknown>): Record<string, unknown> {
        const config = this.liveConfig();
        const state: Record<string, unknown> = {};
        for (const path of TRACKED_LLM_PATHS) {
            state[path] = readConfigPath(overrides, path) ?? readConfigPath(config, path);
        }
        state['interactive.permission_mode'] =
            readConfigPath(overrides, 'interactive.permission_mode')
            ?? readConfigPath(config, 'interactive.permission_mode');
        state['interactive.narration_mode'] =
            readConfigPath(overrides, 'interactive.narration_mode')
            ?? readConfigPath(config, 'interactive.narration_mode');
        // Every category the server answered for, not a hand-picked four. A
        // pinned value normalises to a boolean so a policy spelling (`true`,
        // `{only: [...]}`, a name list) never masquerades as a change; the
        // unpinned value comes from the resolved answer, which is the only
        // thing that knows whether the agent actually holds the category.
        const categories = this.resolvedCategories();
        if (categories) {
            const on = enabledCategoryKeys(categories);
            for (const key of Object.keys(categories)) {
                const pinned = readConfigPath(overrides, `tools.${key}`);
                state[`tools.${key}`] =
                    pinned !== null && pinned !== undefined
                        ? !(Array.isArray(pinned) && pinned.length === 0) && pinned !== false
                        : on.has(key);
            }
            // A LOCKED-ON category is on before the click and on after it, so
            // the boolean above cannot carry "add the tools config may still
            // grant here" — the request is structurally invisible to a diff
            // over switch positions. It gets its own tracked path, holding the
            // requested enumeration, which means the ordinary diff dispatches
            // it exactly once and the debounce, the `!previous ⇒ re-arm` guard
            // and the thread-switch reset all apply to it unchanged. Empty for
            // every session that never uses the affordance, so the baseline is
            // stable.
            const additions = this.settings()?.getToolAdditions() ?? {};
            for (const key of Object.keys(categories)) {
                state[`${TOOL_ADDITIONS_PREFIX}${key}`] = (additions[key] ?? []).join(',');
            }
        }
        // The delegation concurrency knob rides beside the Delegation row and
        // is the only non-tools key the tools group writes (besides the gate,
        // which is derived at dispatch from the row's own switch position).
        state['delegation.max_concurrent'] =
            readConfigPath(overrides, 'delegation.max_concurrent')
            ?? readConfigPath(config, 'delegation.max_concurrent')
            ?? null;
        // Canonical joined form so the diff is a plain string compare. The
        // picker's untouched default IS the attached set, so this holds the
        // baseline value until the user actually toggles a datasource.
        state['datasource_ids'] = this.canonicalDatasourceIds().join(',');
        return state;
    }

    private applyChanges(): void {
        if (!this.chat.isConnected()) return;
        const previous = this.lastApplied;
        if (!previous) {
            // Baseline not anchored yet (thread fetch in flight) — retry
            // instead of absorbing the edit into the baseline.
            this.onSettingsChange();
            return;
        }
        const desired = this.currentDesiredState();

        const fragment: Record<string, unknown> = {};
        const llm: Record<string, unknown> = {};
        const tools: Record<string, unknown> = {};

        for (const path of TRACKED_LLM_PATHS) {
            if (desired[path] !== previous[path] && desired[path] != null) {
                llm[path.split('.')[1]] = desired[path];
            }
        }
        const categories = this.resolvedCategories();
        if (categories) {
            const unsettable = new Set(
                Object.keys(categories).filter((key) => categories[key].settable === false),
            );
            // Symmetric, and that is the guarantee: an unsettable category is
            // never emitted in either direction, so "a locked category cannot
            // be switched off" holds here by construction rather than by a
            // downstream pass that strips an off-write someone else built.
            Object.assign(tools, toolsFragment(Object.keys(categories), {
                disabled: new Set(
                    Object.keys(categories).filter((key) => !desired[`tools.${key}`]),
                ),
                baselineOn: new Set(
                    Object.keys(categories).filter((key) => !!previous[`tools.${key}`]),
                ),
                unsettable,
                enumerateOnly: this.resolvedToolset()?.enumerate_only ?? null,
            }));
            // ...and the other direction, which the diff above cannot see: a
            // locked-ON category may still GAIN the tools config grants. This
            // channel can only ever write an enumeration — there is no shape of
            // `desired`/`previous` that makes it emit `[]` — which is what lets
            // the affordance exist without reopening the off half.
            for (const key of Object.keys(categories)) {
                const path = `${TOOL_ADDITIONS_PREFIX}${key}`;
                const requested = desired[path] as string;
                if (requested && requested !== previous[path]) {
                    tools[key] = {only: requested.split(',')};
                }
            }
        }
        if (Object.keys(llm).length) fragment['llm'] = llm;
        if (Object.keys(tools).length) fragment['tools'] = tools;
        // The Delegation row is ONE switch over TWO config facts: the names
        // in `tools.delegation` (membership) and `delegation.enabled` (the
        // explicit-grant gate the factory checks). The tools group already
        // emits both from getOverrides(); this pane used to forward only the
        // names, so a ticked box produced five tools the agent refused to
        // bind. The gate follows the membership decision made just above.
        if ('delegation' in tools) {
            fragment['delegation'] = {enabled: !!desired['tools.delegation']};
        }
        const maxConcurrent = desired['delegation.max_concurrent'];
        if (
            maxConcurrent !== previous['delegation.max_concurrent']
            && typeof maxConcurrent === 'number'
        ) {
            fragment['delegation'] = {
                ...((fragment['delegation'] as Record<string, unknown>) ?? {}),
                max_concurrent: maxConcurrent,
            };
        }

        // Permission + narration ride their dedicated verbs — they broadcast
        // mode.changed/narration.changed and persist server-side; duplicating
        // them through config.update would double-apply.
        const pm = desired['interactive.permission_mode'];
        if (pm !== previous['interactive.permission_mode'] && typeof pm === 'string') {
            this.chat.setMode(pm as PermissionMode);
        }
        const nm = desired['interactive.narration_mode'];
        if (nm !== previous['interactive.narration_mode'] && typeof nm === 'string') {
            this.chat.setNarrationMode(nm as NarrationMode);
        }

        // Datasources ride the same coalesced frame as a sibling key — the
        // desired FULL selection (never a delta; matches create semantics).
        let datasourceIds: string[] | undefined;
        const dsDesired = desired['datasource_ids'] as string;
        if (dsDesired !== previous['datasource_ids']) {
            datasourceIds = dsDesired === '' ? [] : dsDesired.split(',');
        }

        // Speculative apply, authoritative settle (the Replicache shape):
        // the baseline and the attached-ids mirror advance NOW so a second
        // edit during the round trip diffs against what was asked for, and
        // the outcome either confirms that or rolls it back. The old code
        // advanced the baseline and never looked again, which is how a
        // queue-served session showed values it never took.
        const previousAttached = this.attachedIds();
        const threadId = this.chat.threadId();
        let outcome: Promise<ConfigUpdateOutcome> | null = null;
        if (datasourceIds !== undefined) {
            outcome = this.chat.updateConfig(fragment, datasourceIds);
            this.attachedIds.set(datasourceIds);
        } else if (Object.keys(fragment).length) {
            outcome = this.chat.updateConfig(fragment);
        }
        this.lastApplied = desired;
        if (outcome && threadId) {
            void outcome.then((result) =>
                this.settleConfigUpdate(result, threadId, previous, previousAttached),
            );
        }
    }

    /** Confirm or revert one dispatched batch once the session has answered.
     *
     *  Rejected (or unanswered) edits are rolled back to the state the diff
     *  was taken against — baseline, attached-ids mirror AND the sub-groups'
     *  pins, re-prefilled from the effective config so the controls show what
     *  is actually in effect. A REST-confirmed edit folds the accepted
     *  fragment into the durable overlay: there is no `config.changed` ack to
     *  move the live signals for it, and the next pane open would otherwise
     *  read stale thread metadata until the claim lands. */
    private settleConfigUpdate(
        outcome: ConfigUpdateOutcome,
        threadId: string,
        previous: Record<string, unknown>,
        previousAttached: string[],
    ): void {
        // A late answer for a session the pane has already left must not
        // touch the pane now showing another session.
        if (this.chat.threadId() !== threadId || this.prefilledThread !== threadId) return;
        if (outcome.ok) {
            if (outcome.transport === 'rest' && Object.keys(outcome.applied).length) {
                this.threadOverride.update((current) =>
                    deepMergeConfig(current, outcome.applied),
                );
            }
            return;
        }
        this.attachedIds.set(previousAttached);
        this.settings()?.prefillFromConfig(this.liveConfig());
        const categories = this.resolvedCategories();
        if (categories) this.settings()?.prefillFromResolvedToolset(categories);
        this.settings()?.resetDatasourceSelection();
        // Re-anchor to the config-only state (as loadThread does) rather than
        // to `previous`: a concurrent ack may have moved the live signals in
        // between, and the sub-groups were just reset to exactly this.
        this.lastApplied = this.desiredState({});
        void previous;
    }
}
