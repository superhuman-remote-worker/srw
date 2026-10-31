import { Component, OnDestroy, OnInit, computed, inject, input, signal } from '@angular/core';
import { DatePipe, DecimalPipe } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { Router, RouterLink } from '@angular/router';
import { firstValueFrom } from 'rxjs';
import { TranslocoPipe, TranslocoService } from '@jsverse/transloco';

import { environment } from '../../core/environment';
import { ApiService } from '../../core/services/api.service';
import { NotificationService } from '../../core/services/notification.service';
import { buildConferenceThreadCreateBody } from '../../core/officer/conference';
import type { Notification } from '../../core/models/notification.model';
import { chatModelLabels, ModelService } from '../../core/services/model.service';
import type {
  OfficerBrainSpec,
  OfficerDecommissionResult,
  OfficerHold,
  OfficerKitSlot,
  OfficerPost,
  OfficerPostPatch,
  OfficerSlotSpec,
  OfficerVacantLedger,
  WorkerMessagesPolicy,
} from '../../core/models/api.model';
import { AppButtonComponent } from '../../ui/button';
import { AppInputComponent } from '../../ui/input';
import { AppSelectComponent } from '../../ui/select';
import { AppFormFieldComponent } from '../../ui/form-field';
import { AppSpinnerComponent } from '../../ui/spinner';
import { AppCheckboxComponent } from '../../ui/checkbox';

/** Editable roster row in the kit editor. */
export interface SlotDraft {
  name: string;
  count: number;
  model: string;
  backend: string;
  /** '' = not a pool (officer-directed only); otherwise a work category. */
  category: string;
  /** Preserved from the durable roster; BP-01 owns any future editor control. */
  spendCeilingDaily?: number | null;
}

/**
 * The categories a slot may be a pool for. Mirrors WORK_CATEGORIES in
 * orchestrator/services/work_categories.py — the server validates against its
 * own copy at provision time, so a drift here costs a 400, never a bad kit.
 */
export const WORK_CATEGORY_OPTIONS = [
  { value: '', labelKey: 'officerCard.category.none' },
  { value: 'researcher', labelKey: 'officerCard.category.researcher' },
  { value: 'tester', labelKey: 'officerCard.category.tester' },
  { value: 'executor', labelKey: 'officerCard.category.executor' },
] as const;

export type OfficerTranslate = (key: string, params?: Record<string, unknown>) => string;

/** The whole kit editor as plain strings ('' = unset / server default). */
export interface OfficerEditorDraft {
  slots: SlotDraft[];
  autoPull: boolean;
  workerSpendCeilingDaily: string;
  brainModel: string;
  reasoning: string;
  sleepMin: string;
  sleepMax: string;
  tokenCeiling: string;
  maxActions: string;
  maxWorkers: string;
}

/** The card's one state machine (officer_post.md §8). */
export type OfficerPostState = 'vacant' | 'commissioned' | 'held';

/** §11 Q2 (decided: keep): the draft a never-kitted vacant post starts from. */
export const STARTER_SLOT_DRAFT: SlotDraft = {
  name: 'line',
  count: 2,
  model: '',
  backend: 'sandbox',
  // No category by default: a starter kit must not silently arm auto-pull on
  // a century whose officer has never triaged a backlog.
  category: '',
  spendCeilingDaily: null,
};

/** Vacant / commissioned / held — held is commissioned-and-standing-down. */
export function postStateOf(post: OfficerPost | null): OfficerPostState {
  if (!post?.commissioned) return 'vacant';
  return post.held ? 'held' : 'commissioned';
}

/**
 * "held — maintenance" / "held — conference" from the hold's kind — replaces
 * the old hardcoded "held — conference in progress", which was wrong for
 * maintenance holds. The note renders separately.
 */
export function holdBadgeLabel(
  held: OfficerHold | null | undefined,
  translate: OfficerTranslate,
): string {
  const kind = held?.kind?.trim();
  if (!kind) return translate('officerCard.status.held');
  const knownKind = ['maintenance', 'conference'].includes(kind)
    ? translate(`officerCard.holdKind.${kind}`)
    : kind;
  return translate('officerCard.status.heldKind', { kind: knownKind });
}

/** Editor fields grouped by when a live edit actually lands (§7's table). */
export type OfficerEditField =
  | 'slots'
  | 'auto_pull'
  | 'worker_spend_ceiling_daily'
  | 'max_concurrent_workers'
  | 'daily_token_ceiling'
  | 'sleep'
  | 'max_actions_per_wake'
  | 'brain';

/** Per-field honesty: what the §7 table promises, verbatim in the UI. */
export function immediacyLabel(field: OfficerEditField, translate: OfficerTranslate): string {
  switch (field) {
    case 'slots':
    case 'auto_pull':
    case 'worker_spend_ceiling_daily':
    case 'max_concurrent_workers':
      return translate('officerCard.immediacy.nextDispatch');
    case 'daily_token_ceiling':
      return translate('officerCard.immediacy.nextDelivery');
    case 'sleep':
      return translate('officerCard.immediacy.nextSleep');
    case 'max_actions_per_wake':
    case 'brain':
      return translate('officerCard.immediacy.nextRespawn');
  }
}

/**
 * Shrinking a slot below its in-flight count is drain semantics (§7, decided):
 * new dispatches 409, running jobs are untouched. The hint says so.
 */
export function drainHint(
  inFlight: number | null | undefined,
  newCount: number,
  translate: OfficerTranslate,
): string | null {
  if (!inFlight || newCount >= inFlight) return null;
  return translate('officerCard.roster.drainHint', { inFlight, count: newCount });
}

/**
 * Utilization chip per kit slot: `line 1/2 · MiniMax-M3 · vm` (×N without live
 * data). Pools carry two more facts, because utilization alone answers the
 * wrong question — an idle slot with a healthy queue is slack, and slack is
 * fine. What matters is whether the QUEUE is starved, so a pool shows its ready
 * depth and is flagged when it sits below its floor (= its slot count). An open
 * breaker wins the flag: an idle pool that is idle because it is BROKEN must
 * not read as merely quiet.
 *
 * `ready_depth` absent means the knowledge base could not be read. It renders
 * as nothing rather than as `ready 0`, which would be an unmeasured claim about
 * a starved queue.
 */
export function kitChips(
  kit: Record<string, OfficerKitSlot> | null | undefined,
  translate: OfficerTranslate,
  breakers: Record<string, { until?: string }> | null | undefined = null,
  now: Date = new Date(),
): { name: string; label: string; alert: boolean }[] {
  if (!kit) return [];
  return Object.entries(kit).map(([name, s]) => {
    const alloc = s.in_flight != null ? `${s.in_flight}/${s.count}` : `×${s.count}`;
    const parts = [`${name} ${alloc}`];
    if (s.category) parts.push(s.category);
    if (s.model) parts.push(s.model);
    if (s.backend) parts.push(s.backend);
    if (s.spend_ceiling_daily != null) {
      parts.push(
        translate('officerCard.kit.spendCeiling', {
          amount: s.spend_ceiling_daily,
        }),
      );
    }
    if (s.ready_depth != null) {
      parts.push(
        translate(s.below_floor ? 'officerCard.kit.readyBelowFloor' : 'officerCard.kit.ready', {
          count: s.ready_depth,
        }),
      );
    }
    const until = breakers?.[name]?.until;
    const broken = !!until && new Date(until).getTime() > now.getTime();
    if (broken) parts.push(translate('officerCard.kit.breakerOpen'));
    return { name, label: parts.join(' · '), alert: broken || !!s.below_floor };
  });
}

/**
 * Seed the editor from the post. A never-kitted VACANT post gets the starter
 * draft (§11 Q2: keep it); an established post without slots is genuinely
 * flat-cap — no starter is invented for a commissioned officer or a vacant
 * post with prior incarnations. Vacant posts expose only `kit` in the O1–O4
 * contract, so the non-kit fields seed to '' until commissioned.
 */
export function draftFromPost(post: OfficerPost | null): OfficerEditorDraft {
  const kit = post?.kit ?? null;
  const kitRows: SlotDraft[] = kit
    ? Object.entries(kit).map(([name, s]) => ({
        name,
        count: s.count,
        model: s.model ?? '',
        backend: s.backend ?? '',
        category: s.category ?? '',
        spendCeilingDaily: s.spend_ceiling_daily ?? null,
      }))
    : [];
  const establishedPost = post?.commissioned === true || (post?.incarnations?.length ?? 0) > 0;
  const slots = kitRows.length ? kitRows : establishedPost ? [] : [{ ...STARTER_SLOT_DRAFT }];
  const o = post?.officer ?? null;
  const num = (v: number | null | undefined): string => (v == null ? '' : String(v));
  return {
    slots,
    autoPull: post?.backlog?.auto_pull === true,
    workerSpendCeilingDaily: num(post?.backlog?.worker_spend_ceiling_daily),
    brainModel: o?.model ?? '',
    reasoning: o?.reasoning_level ?? '',
    sleepMin: num(o?.sleep_minutes?.min),
    sleepMax: num(o?.sleep_minutes?.max),
    tokenCeiling: num(o?.token_ceiling?.daily),
    maxActions: num(o?.max_actions_per_wake),
    maxWorkers: num(o?.max_concurrent_workers),
  };
}

/** Assemble the `officer.slots` spec from form rows; null = no roster (flat cap). */
export function buildSlotsSpec(rows: SlotDraft[]): Record<string, OfficerSlotSpec> | null {
  const spec: Record<string, OfficerSlotSpec> = {};
  for (const row of rows) {
    const name = row.name.trim().toLowerCase();
    if (!name) throw new Error('blank_slot_name');
    if (Object.hasOwn(spec, name)) throw new Error('duplicate_slot_name');
    const entry: OfficerSlotSpec = {
      count: Math.min(20, Math.max(0, Math.floor(row.count))),
    };
    if (row.model.trim()) entry.model = row.model.trim();
    if (row.backend.trim()) entry.backend = row.backend.trim();
    // `?? ''` because this field is newer than the type: a draft assembled
    // before it existed (or by a caller that skips the type) must degrade to
    // "not a pool" rather than throwing mid-save.
    const category = (row.category ?? '').trim();
    if (category) entry.category = category.toLowerCase();
    if (row.spendCeilingDaily != null) {
      entry.spend_ceiling_daily = row.spendCeilingDaily;
    }
    spec[name] = entry;
  }
  return Object.keys(spec).length ? spec : null;
}

export function rosterValidationIssue(
  rows: SlotDraft[],
): { key: string; params?: Record<string, unknown> } | null {
  const seen = new Set<string>();
  for (const row of rows) {
    const name = row.name.trim().toLowerCase();
    if (!name) return { key: 'officerCard.validation.blankSlot' };
    if (seen.has(name)) {
      return { key: 'officerCard.validation.duplicateSlot', params: { name } };
    }
    seen.add(name);
  }
  return null;
}

/**
 * Full request body from the editor — the commission body, and the base both
 * sides of the PATCH diff are computed from. The roster is always explicit
 * because the editor sees the whole map (`null` means flat-cap); other blank
 * fields are omitted so commission cannot clear row state the editor never saw.
 */
export function buildOfficerConfig(draft: OfficerEditorDraft): OfficerPostPatch {
  const body: OfficerPostPatch = {
    slots: buildSlotsSpec(draft.slots),
    auto_pull: draft.autoPull,
  };
  const brain: OfficerBrainSpec = {};
  if (draft.brainModel.trim()) brain.model = draft.brainModel.trim();
  if (draft.reasoning) brain.reasoning_level = draft.reasoning;
  if (Object.keys(brain).length) body.brain = brain;
  const num = (v: string): number | undefined => {
    const n = parseInt(v, 10);
    return Number.isNaN(n) ? undefined : n;
  };
  const sleepMin = num(draft.sleepMin);
  if (sleepMin !== undefined) body.sleep_min_minutes = sleepMin;
  const sleepMax = num(draft.sleepMax);
  if (sleepMax !== undefined) body.sleep_max_minutes = sleepMax;
  const ceiling = num(draft.tokenCeiling);
  if (ceiling !== undefined) body.daily_token_ceiling = ceiling;
  const actions = num(draft.maxActions);
  if (actions !== undefined) body.max_actions_per_wake = actions;
  const workers = num(draft.maxWorkers);
  if (workers !== undefined) body.max_concurrent_workers = workers;
  const workerSpendCeiling = Number(draft.workerSpendCeilingDaily);
  if (draft.workerSpendCeilingDaily.trim() && Number.isFinite(workerSpendCeiling)) {
    body.worker_spend_ceiling_daily = workerSpendCeiling;
  }
  return body;
}

const PATCH_FIELDS: (keyof OfficerPostPatch)[] = [
  'slots',
  'auto_pull',
  'worker_spend_ceiling_daily',
  'brain',
  'sleep_min_minutes',
  'sleep_max_minutes',
  'daily_token_ceiling',
  'max_actions_per_wake',
  'max_concurrent_workers',
];

/**
 * Field-wise diff against the post's last known state: only what actually
 * changed goes on the wire (the server injects a notice per edit — don't cry
 * wolf). A field the user cleared PATCHes as explicit null. Deliberately
 * never emits `communication_policy` — the routing control PATCHes that
 * alone (§7: a row-only, user-owned field).
 */
export function buildOfficerPatch(
  baseline: OfficerEditorDraft,
  draft: OfficerEditorDraft,
): OfficerPostPatch {
  const before = buildOfficerConfig(baseline);
  const after = buildOfficerConfig(draft);
  const patch: OfficerPostPatch = {};
  for (const field of PATCH_FIELDS) {
    const a = before[field];
    const b = after[field];
    if (JSON.stringify(a ?? null) !== JSON.stringify(b ?? null)) {
      (patch as Record<string, unknown>)[field] = b ?? null;
    }
  }
  return patch;
}

/**
 * Normalize the while-vacant ledger. The O3 shape is `{entries, dropped}`
 * (ring, cap 20, drop-oldest); a bare array is tolerated so the card renders
 * either way while the backend contract settles.
 */
export function vacantLedgerOf(
  post: OfficerPost | null,
): { entries: NonNullable<OfficerVacantLedger['entries']>; dropped: number } | null {
  const raw = post?.while_vacant as
    OfficerVacantLedger | OfficerVacantLedger['entries'] | null | undefined;
  if (!raw) return null;
  const entries = Array.isArray(raw) ? raw : (raw.entries ?? []);
  const dropped = Array.isArray(raw) ? 0 : (raw.dropped ?? 0);
  if (!entries.length && !dropped) return null;
  return { entries, dropped };
}

/** "in 42 min" / "overdue 3 min" — the next-wake label. */
export function nextWakeLabel(
  fireAt: string | null | undefined,
  translate: OfficerTranslate,
): string {
  if (!fireAt) return translate('officerCard.wake.eventDriven');
  const delta = (new Date(fireAt).getTime() - Date.now()) / 60000;
  if (Number.isNaN(delta)) return String(fireAt);
  if (delta >= 1) {
    return translate('officerCard.wake.inMinutes', { count: Math.round(delta) });
  }
  if (delta > -2) return translate('officerCard.wake.dueNow');
  return translate('officerCard.wake.overdueMinutes', {
    count: Math.round(-delta),
  });
}

/**
 * Project Centurion tab — the officer's POST (officer_post.md §8).
 *
 * One card, one state machine: vacant / commissioned / held. The kit editor
 * is the card body in every state — seeded from the row while vacant (last
 * real kit, else the starter draft) and populated live once commissioned,
 * with per-slot utilization, per-field immediacy labels (§7), spend against
 * the ceiling, and the lifecycle: commission, hold/release, decommission
 * (warning on in-flight jobs — they are left running, never cancelled). The
 * log IS his session transcript — the card links there rather than
 * duplicating it. Worker-question routing (officer_message_routing.md §6) is
 * the one row-only policy and PATCHes alone.
 */
@Component({
  selector: 'app-project-officer',
  standalone: true,
  imports: [
    DatePipe,
    DecimalPipe,
    TranslocoPipe,
    RouterLink,
    AppButtonComponent,
    AppInputComponent,
    AppSelectComponent,
    AppFormFieldComponent,
    AppSpinnerComponent,
    AppCheckboxComponent,
  ],
  template: `
    <div class="officer-tab">
      <div class="officer-intro">
        <h3>{{ 'officerCard.title' | transloco }}</h3>
        <p>{{ 'officerCard.intro' | transloco }}</p>
      </div>

      @if (loading()) {
        <div
          class="officer-loading"
          role="status"
          [attr.aria-label]="'officerCard.a11y.loading' | transloco"
        >
          <app-spinner size="md" tone="accent" />
        </div>
      } @else {
        <div
          class="officer-card"
          data-testid="officer-card"
          role="region"
          [attr.aria-label]="'officerCard.a11y.region' | transloco"
          [attr.data-state]="postState()"
        >
          @if (postState() === 'vacant') {
            <p class="officer-hint">{{ 'officerCard.vacant.hint' | transloco }}</p>

            @if (vacantLedger(); as ledger) {
              <div class="officer-ledger" data-testid="officer-vacant-ledger">
                <div class="officer-section-title">
                  {{ 'officerCard.vacant.ledgerTitle' | transloco }}
                </div>
                @for (e of ledger.entries; track $index) {
                  <div class="officer-ledger-item">
                    @if (e.at) {
                      <span class="dim">{{ e.at | date: 'short' }}</span>
                    }
                    <span>{{ e.title || e.job_id }}</span>
                    @if (e.status) {
                      <span class="officer-ledger-status">{{ statusLabel(e.status) }}</span>
                    }
                  </div>
                }
                @if (ledger.dropped) {
                  <div class="dim">
                    {{ 'officerCard.vacant.dropped' | transloco: { count: ledger.dropped } }}
                  </div>
                }
              </div>
            }
          } @else {
            @if (post()?.officer; as o) {
              <div class="officer-status-row">
                <span class="officer-badge" [attr.data-status]="o.status">{{
                  statusLabel(o.status)
                }}</span>
                @if (post()?.held; as h) {
                  <span class="officer-hold" data-testid="officer-hold">{{ holdLabel() }}</span>
                  @if (h.note) {
                    <span class="officer-hold-note" data-testid="officer-hold-note">{{
                      h.note
                    }}</span>
                  }
                }
                <span class="officer-title">{{
                  o.title || ('officerCard.title' | transloco)
                }}</span>
              </div>

              @if (post()?.runtime_authorization?.status === 'unavailable') {
                <div
                  class="officer-slots"
                  data-testid="officer-runtime-authorization"
                  role="alert"
                  [attr.aria-label]="'officerCard.runtimeAuthorization.a11y' | transloco"
                >
                  <span class="k">{{ 'officerCard.runtimeAuthorization.label' | transloco }}</span>
                  <span class="v officer-warn">
                    {{ 'officerCard.runtimeAuthorization.unavailable' | transloco }}
                  </span>
                </div>
              }

              @if (post()?.runtime_lifecycle; as lifecycle) {
                <div class="officer-slots" data-testid="officer-runtime-lifecycle">
                  <span class="k">{{ 'officerCard.runtimeLifecycle.label' | transloco }}</span>
                  <span class="v" [class.officer-warn]="lifecycle.drift_state !== 'current'">
                    {{
                      'officerCard.runtimeLifecycle.drift.' + (lifecycle.drift_state || 'unknown')
                        | transloco
                    }}
                    ·
                    {{
                      'officerCard.runtimeLifecycle.phase.' +
                        (lifecycle.recycle_phase || 'unknown').replaceAll('_', '-') | transloco
                    }}
                    @if (lifecycle.observed_build_sha || lifecycle.expected_build_sha) {
                      ·
                      {{
                        lifecycle.observed_build_sha ||
                          ('officerCard.runtimeLifecycle.missing' | transloco)
                      }}
                      →
                      {{
                        lifecycle.expected_build_sha ||
                          ('officerCard.runtimeLifecycle.unpinned' | transloco)
                      }}
                    }
                    @if (lifecycle.last_failure) {
                      · {{ lifecycle.last_failure }}
                    }
                    ·
                    {{
                      (lifecycle.automatic_reconciliation_enabled
                        ? 'officerCard.runtimeLifecycle.automatic.enabled'
                        : 'officerCard.runtimeLifecycle.automatic.disabled'
                      ) | transloco
                    }}
                  </span>
                </div>
              }

              <div class="officer-meta">
                <div>
                  <span class="k">{{ 'officerCard.summary.nextWake' | transloco }}</span>
                  <span class="v" data-testid="next-wake">{{ wakeLabel() }}</span>
                </div>
                <div>
                  <span class="k">{{ 'officerCard.summary.queuedEvents' | transloco }}</span>
                  <span class="v">{{ o.pending_events ?? 0 }}</span>
                </div>
                @if (post()?.spend_today; as spend) {
                  <div>
                    <span class="k">{{ 'officerCard.summary.spendToday' | transloco }}</span>
                    <span class="v" data-testid="officer-spend">
                      {{ spend.tokens ?? 0 | number }}
                      @if (spendCeiling() != null) {
                        / {{ spendCeiling() | number }}
                      }
                      {{ 'officerCard.summary.tokens' | transloco }}
                      @if (o.token_ceiling?.deferred_today) {
                        <span class="officer-warn">{{
                          'officerCard.summary.ceilingReached' | transloco
                        }}</span>
                      }
                    </span>
                  </div>
                }
                <div>
                  <span class="k">{{ 'officerCard.summary.model' | transloco }}</span>
                  <span class="v" data-testid="officer-model">
                    {{ o.model || ('officerCard.defaults.session' | transloco) }}
                    @if (o.reasoning_level) {
                      · {{ o.reasoning_level }}
                    }
                  </span>
                </div>
              </div>

              @if (kitRows().length) {
                <div class="officer-slots" data-testid="officer-slots">
                  <span class="k">{{ 'officerCard.sections.kit' | transloco }}</span>
                  @for (s of kitRows(); track s.name) {
                    <span class="officer-slot-chip" [class.officer-slot-chip-alert]="s.alert">{{
                      s.label
                    }}</span>
                  }
                </div>
              }

              @if (backlogState(); as bl) {
                <div class="officer-slots" data-testid="officer-backlog">
                  <span class="k">{{ 'officerCard.backlog.autoPull' | transloco }}</span>
                  <span class="v">
                    @if (bl.auto_pull) {
                      {{ 'officerCard.backlog.on' | transloco }}
                    } @else {
                      <span class="officer-warn">{{ 'officerCard.backlog.off' | transloco }}</span>
                    }
                    @if (bl.worker_spend_ceiling_daily != null) {
                      ·
                      {{
                        'officerCard.backlog.centurySpendCeiling'
                          | transloco: { amount: bl.worker_spend_ceiling_daily }
                      }}
                    }
                    @if (bl.auto_pull_control?.enable_available === false) {
                      ·
                      <span class="officer-warn" data-testid="officer-auto-pull-release-closed">{{
                        'officerCard.backlog.releaseClosed' | transloco
                      }}</span>
                    }
                  </span>
                </div>
                @if (staleClaims().length) {
                  <div class="officer-slots" data-testid="officer-stale-claims">
                    <span class="k">{{ 'officerCard.backlog.stalled' | transloco }}</span>
                    <span class="v officer-warn">{{
                      'officerCard.backlog.stalledDetail'
                        | transloco
                          : {
                              count: staleClaims().length,
                              threshold: staleClaimThresholdHours() ?? '?',
                              source:
                                ('officerCard.backlog.thresholdSource.' +
                                  staleClaimThresholdSource() | transloco),
                            }
                    }}</span>
                  </div>
                }
              }
              @if (provisioningProblems().length) {
                <div class="officer-slots" data-testid="officer-provisioning-state">
                  <span class="k">{{ 'officerCorrectness.provisioningLabel' | transloco }}</span>
                  <span class="v officer-warn">
                    {{
                      'officerCorrectness.provisioningProblem'
                        | transloco: { count: provisioningProblems().length }
                    }}
                    · {{ provisioningStateSummary() }}
                  </span>
                </div>
              }
              @if (knowledgeProblems().length) {
                <div class="officer-slots" data-testid="officer-knowledge-sync">
                  <span class="k">{{ 'officerCorrectness.knowledgeLabel' | transloco }}</span>
                  <span class="v officer-warn">
                    {{
                      'officerCorrectness.knowledgeProblem'
                        | transloco: { count: knowledgeProblems().length }
                    }}
                    · {{ knowledgeStateSummary() }}
                  </span>
                </div>
              }
              @if (latestFloorWake(); as wake) {
                <div class="officer-slots" data-testid="officer-floor-wake">
                  <span class="k">{{ 'officerCorrectness.floorWakeLabel' | transloco }}</span>
                  <span class="v">
                    {{ wake.pool || ('officerCorrectness.poolFallback' | transloco) }} ·
                    @if (wake.delivered_at) {
                      {{ 'officerCorrectness.delivered' | transloco }}
                    } @else if (wake.last_queued_at) {
                      {{ 'officerCorrectness.durablyQueued' | transloco }}
                      @if (wake.failure_class) {
                        ·
                        <span class="officer-warn"
                          >{{ 'officerCorrectness.deliveryFailed' | transloco }} ·
                          {{ wake.failure_class }}</span
                        >
                      }
                    } @else {
                      <span class="officer-warn"
                        >{{ 'officerCorrectness.notQueued' | transloco }} ·
                        {{ wake.failure_class || stateLabel(wake.state) }}</span
                      >
                    }
                  </span>
                </div>
              }
            }
          }

          <!-- Management and operational visibility are separate authorities. -->
          @if (canManage()) {
            <div class="officer-editor" data-testid="officer-editor">
              <div class="officer-editor-head">
                <span class="officer-section-title">{{
                  'officerCard.sections.brain' | transloco
                }}</span>
                @if (showImmediacy()) {
                  <span class="officer-immediacy" data-testid="immediacy-brain">{{
                    immediacy('brain')
                  }}</span>
                }
              </div>
              <div class="officer-slot-row">
                <app-form-field
                  [label]="'officerCard.brain.modelLabel' | transloco"
                  [hint]="'officerCard.brain.modelHint' | transloco"
                >
                  <app-select
                    [ariaLabel]="'officerCard.a11y.brainModel' | transloco"
                    [value]="fBrainModel()"
                    (changed)="fBrainModel.set($event ?? '')"
                  >
                    <option value="">{{ 'officerCard.defaults.session' | transloco }}</option>
                    @for (m of modelOptions(); track m) {
                      <option [value]="m">{{ modelLabels()[m] }}</option>
                    }
                  </app-select>
                </app-form-field>
                <app-form-field
                  [label]="'officerCard.brain.reasoningLabel' | transloco"
                  [hint]="'officerCard.brain.reasoningHint' | transloco"
                >
                  <app-select
                    [ariaLabel]="'officerCard.a11y.reasoning' | transloco"
                    [value]="fReasoning()"
                    (changed)="fReasoning.set($event ?? '')"
                  >
                    <option value="">{{ 'officerCard.reasoning.defaultHigh' | transloco }}</option>
                    <option value="low">{{ 'officerCard.reasoning.low' | transloco }}</option>
                    <option value="medium">{{ 'officerCard.reasoning.medium' | transloco }}</option>
                    <option value="high">{{ 'officerCard.reasoning.high' | transloco }}</option>
                    <option value="xhigh">{{ 'officerCard.reasoning.xhigh' | transloco }}</option>
                    <option value="max">{{ 'officerCard.reasoning.max' | transloco }}</option>
                  </app-select>
                </app-form-field>
                <app-form-field
                  [label]="'officerCard.brain.actionsLabel' | transloco"
                  [hint]="'officerCard.brain.actionsHint' | transloco"
                >
                  <app-input
                    type="number"
                    [ariaLabel]="'officerCard.a11y.actionsPerWake' | transloco"
                    [value]="fMaxActions()"
                    (changed)="fMaxActions.set($event)"
                    [placeholder]="'officerCard.defaults.default' | transloco"
                  />
                </app-form-field>
              </div>

              <div class="officer-editor-head">
                <span class="officer-section-title">{{
                  'officerCard.sections.kit' | transloco
                }}</span>
                @if (showImmediacy()) {
                  <span class="officer-immediacy" data-testid="immediacy-slots">{{
                    immediacy('slots')
                  }}</span>
                }
              </div>
              <p class="officer-hint dim">{{ 'officerCard.roster.hint' | transloco }}</p>
              @if (rosterError(); as error) {
                <div class="officer-validation" role="alert" data-testid="officer-roster-error">
                  {{ error }}
                </div>
              }
              @for (row of slotDrafts(); track $index; let i = $index) {
                <div class="officer-slot-row">
                  <app-form-field [label]="'officerCard.roster.slot' | transloco">
                    <app-input
                      [ariaLabel]="'officerCard.a11y.slotName' | transloco: { index: i + 1 }"
                      [value]="row.name"
                      (changed)="patchSlot(i, { name: $event })"
                      [placeholder]="'officerCard.roster.slotPlaceholder' | transloco"
                    />
                  </app-form-field>
                  <app-form-field
                    [label]="'officerCard.roster.count' | transloco"
                    [hint]="'officerCard.roster.countHint' | transloco"
                  >
                    <app-input
                      type="number"
                      [ariaLabel]="'officerCard.a11y.slotCount' | transloco: { index: i + 1 }"
                      [value]="'' + row.count"
                      (changed)="patchSlot(i, { count: toCount($event) })"
                    />
                  </app-form-field>
                  <app-form-field [label]="'officerCard.roster.model' | transloco">
                    <app-select
                      [ariaLabel]="'officerCard.a11y.slotModel' | transloco: { index: i + 1 }"
                      [value]="row.model"
                      (changed)="patchSlot(i, { model: $event ?? '' })"
                    >
                      <option value="">{{ 'officerCard.defaults.worker' | transloco }}</option>
                      @for (m of modelOptions(); track m) {
                        <option [value]="m">{{ modelLabels()[m] }}</option>
                      }
                    </app-select>
                  </app-form-field>
                  <app-form-field [label]="'officerCard.roster.workspace' | transloco">
                    <app-select
                      [ariaLabel]="'officerCard.a11y.slotWorkspace' | transloco: { index: i + 1 }"
                      [value]="row.backend"
                      (changed)="patchSlot(i, { backend: $event ?? '' })"
                    >
                      <option value="">{{ 'officerCard.defaults.default' | transloco }}</option>
                      <option value="sandbox">
                        {{ 'officerCard.workspace.sandbox' | transloco }}
                      </option>
                      <option value="virtual">
                        {{ 'officerCard.workspace.virtual' | transloco }}
                      </option>
                      <option value="none">{{ 'officerCard.workspace.none' | transloco }}</option>
                      <option value="vm">{{ 'officerCard.workspace.vmRoot' | transloco }}</option>
                    </app-select>
                  </app-form-field>
                  <app-form-field [label]="'officerCard.roster.pool' | transloco">
                    <app-select
                      [ariaLabel]="'officerCard.a11y.slotPool' | transloco: { index: i + 1 }"
                      [value]="row.category"
                      (changed)="patchSlot(i, { category: $event ?? '' })"
                    >
                      @for (c of categoryOptions; track c.value) {
                        <option [value]="c.value">{{ c.labelKey | transloco }}</option>
                      }
                    </app-select>
                  </app-form-field>
                  <app-form-field
                    [label]="'officerCard.roster.spendCeiling' | transloco"
                    [hint]="'officerCard.roster.spendCeilingHint' | transloco"
                  >
                    <app-input
                      type="number"
                      inputmode="decimal"
                      [ariaLabel]="
                        'officerCard.a11y.slotSpendCeiling' | transloco: { index: i + 1 }
                      "
                      [value]="row.spendCeilingDaily == null ? '' : '' + row.spendCeilingDaily"
                      (changed)="patchSlot(i, { spendCeilingDaily: toOptionalCeiling($event) })"
                      [placeholder]="'officerCard.defaults.unlimited' | transloco"
                    />
                  </app-form-field>
                  <app-button
                    variant="secondary"
                    size="sm"
                    [ariaLabel]="
                      'officerCard.a11y.removeSlot' | transloco: { name: row.name || i + 1 }
                    "
                    (clicked)="removeSlot(i)"
                    >✕</app-button
                  >
                </div>
                @if (drainHints()[i]; as hint) {
                  <div class="officer-drain" data-testid="drain-hint">{{ hint }}</div>
                }
              }
              <div class="officer-slot-row">
                <app-form-field
                  [label]="'officerCard.roster.maxWorkers' | transloco"
                  [hint]="'officerCard.roster.maxWorkersHint' | transloco"
                >
                  <app-input
                    type="number"
                    [ariaLabel]="'officerCard.a11y.maxWorkers' | transloco"
                    [value]="fMaxWorkers()"
                    (changed)="fMaxWorkers.set($event)"
                    placeholder="3"
                  />
                </app-form-field>
                <app-button
                  variant="secondary"
                  size="sm"
                  [disabled]="busy() || slotDrafts().length >= 8"
                  (clicked)="addSlot()"
                >
                  {{ 'officerCard.actions.addSlot' | transloco }}
                </app-button>
              </div>

              <div class="officer-editor-head">
                <span class="officer-section-title">{{
                  'officerCard.sections.autoPull' | transloco
                }}</span>
                @if (showImmediacy()) {
                  <span class="officer-immediacy" data-testid="immediacy-auto-pull">{{
                    immediacy('auto_pull')
                  }}</span>
                }
              </div>
              <div class="officer-slot-row" data-testid="officer-auto-pull-control">
                <app-checkbox
                  [checked]="fAutoPull()"
                  [disabled]="busy() || (!autoPullEnableAvailable() && !fAutoPull())"
                  [ariaLabel]="'officerCard.a11y.autoPull' | transloco"
                  (changed)="requestAutoPullChange($event)"
                >
                  {{ 'officerCard.autoPull.label' | transloco }}
                </app-checkbox>
                <app-form-field
                  [label]="'officerCard.budgets.workerSpendCeiling' | transloco"
                  [hint]="'officerCard.budgets.workerSpendCeilingHint' | transloco"
                >
                  <app-input
                    type="number"
                    inputmode="decimal"
                    [ariaLabel]="'officerCard.a11y.workerSpendCeiling' | transloco"
                    [value]="fWorkerSpendCeiling()"
                    (changed)="fWorkerSpendCeiling.set($event)"
                    [placeholder]="'officerCard.defaults.unlimited' | transloco"
                  />
                </app-form-field>
              </div>
              @if (!autoPullEnableAvailable() && !fAutoPull()) {
                <p class="officer-hint officer-warn" data-testid="officer-auto-pull-gate-hint">
                  {{ 'officerCard.autoPull.releaseClosed' | transloco }}
                </p>
              }
              @if (autoPullArmed()) {
                <div class="officer-confirm" data-testid="officer-auto-pull-confirm" role="alert">
                  <p class="officer-hint">{{ 'officerCard.autoPull.confirmation' | transloco }}</p>
                  <div class="officer-actions">
                    <app-button
                      variant="danger"
                      size="sm"
                      [disabled]="busy()"
                      (clicked)="confirmAutoPull()"
                      data-testid="officer-auto-pull-confirm-enable"
                    >
                      {{ 'officerCard.autoPull.confirmEnable' | transloco }}
                    </app-button>
                    <app-button variant="secondary" size="sm" (clicked)="cancelAutoPull()">{{
                      'officerCard.actions.cancel' | transloco
                    }}</app-button>
                  </div>
                </div>
              }

              <div class="officer-editor-head">
                <span class="officer-section-title">{{
                  'officerCard.sections.budgets' | transloco
                }}</span>
                @if (showImmediacy()) {
                  <span class="officer-immediacy" data-testid="immediacy-budgets">{{
                    immediacy('daily_token_ceiling')
                  }}</span>
                }
              </div>
              <div class="officer-slot-row">
                <app-form-field
                  [label]="'officerCard.budgets.tokenCeiling' | transloco"
                  [hint]="'officerCard.budgets.tokenCeilingHint' | transloco"
                >
                  <app-input
                    type="number"
                    [ariaLabel]="'officerCard.a11y.tokenCeiling' | transloco"
                    [value]="fTokenCeiling()"
                    (changed)="fTokenCeiling.set($event)"
                    [placeholder]="'officerCard.defaults.unlimited' | transloco"
                  />
                </app-form-field>
              </div>

              <div class="officer-editor-head">
                <span class="officer-section-title">{{
                  'officerCard.sections.sleep' | transloco
                }}</span>
                @if (showImmediacy()) {
                  <span class="officer-immediacy" data-testid="immediacy-sleep">{{
                    immediacy('sleep')
                  }}</span>
                }
              </div>
              <div class="officer-slot-row">
                <app-form-field
                  [label]="'officerCard.sleep.min' | transloco"
                  [hint]="'officerCard.sleep.hint' | transloco"
                >
                  <app-input
                    type="number"
                    [ariaLabel]="'officerCard.a11y.sleepMin' | transloco"
                    [value]="fSleepMin()"
                    (changed)="fSleepMin.set($event)"
                    placeholder="5"
                  />
                </app-form-field>
                <app-form-field [label]="'officerCard.sleep.max' | transloco">
                  <app-input
                    type="number"
                    [ariaLabel]="'officerCard.a11y.sleepMax' | transloco"
                    [value]="fSleepMax()"
                    (changed)="fSleepMax.set($event)"
                    placeholder="60"
                  />
                </app-form-field>
              </div>
            </div>
          } @else {
            <p class="officer-hint officer-read-only" data-testid="officer-read-only">
              {{ 'officerCard.readOnly' | transloco }}
            </p>
          }

          @if (postState() === 'vacant') {
            @if (canManage()) {
              <div class="officer-actions">
                <app-button
                  variant="primary"
                  size="sm"
                  [disabled]="busy()"
                  (clicked)="commission()"
                  data-testid="officer-commission"
                >
                  {{ 'officerCard.actions.commission' | transloco }}
                </app-button>
              </div>
              <p class="officer-hint dim">
                {{ 'officerCard.vacant.flatCapHint' | transloco }}
              </p>
            }
          } @else {
            <div class="officer-actions">
              @if (canManage()) {
                <app-button
                  variant="secondary"
                  size="sm"
                  [disabled]="busy() || recycleActive()"
                  (clicked)="recycle()"
                  data-testid="officer-recycle"
                  [ariaLabel]="'officerCard.a11y.recycle' | transloco"
                >
                  {{ 'officerCard.actions.recycle' | transloco }}
                </app-button>
                <app-button
                  variant="primary"
                  size="sm"
                  [disabled]="busy() || !dirty()"
                  (clicked)="saveEdits()"
                  data-testid="officer-save"
                >
                  {{ 'officerCard.actions.save' | transloco }}
                </app-button>
              }
              <app-button variant="secondary" size="sm" (clicked)="openLog()">{{
                'officerCard.actions.openLog' | transloco
              }}</app-button>
              @if (canManage()) {
                <app-button
                  variant="secondary"
                  size="sm"
                  [disabled]="busy()"
                  (clicked)="openConference()"
                >
                  {{
                    conference()
                      ? ('officerCard.actions.rejoinConference' | transloco)
                      : ('officerCard.actions.conference' | transloco)
                  }}
                </app-button>
                @if (postState() === 'held' && !recycleActive()) {
                  <app-button
                    variant="secondary"
                    size="sm"
                    [disabled]="busy()"
                    (clicked)="release()"
                    data-testid="officer-release"
                  >
                    {{ 'officerCard.actions.release' | transloco }}
                  </app-button>
                } @else if (!holdArmed()) {
                  <app-button
                    variant="secondary"
                    size="sm"
                    [disabled]="busy()"
                    (clicked)="holdArmed.set(true)"
                  >
                    {{ 'officerCard.actions.hold' | transloco }}
                  </app-button>
                }
                @if (!decommissionArmed()) {
                  <app-button
                    variant="danger"
                    size="sm"
                    [disabled]="busy()"
                    (clicked)="decommissionArmed.set(true)"
                  >
                    {{ 'officerCard.actions.decommission' | transloco }}
                  </app-button>
                }
              }
            </div>

            @if (canManage() && holdArmed()) {
              <div class="officer-confirm" data-testid="officer-hold-confirm">
                <app-form-field
                  [label]="'officerCard.hold.noteLabel' | transloco"
                  [hint]="'officerCard.hold.noteHint' | transloco"
                >
                  <app-input
                    [ariaLabel]="'officerCard.a11y.holdNote' | transloco"
                    [value]="holdNote()"
                    (changed)="holdNote.set($event)"
                    [placeholder]="'officerCard.hold.placeholder' | transloco"
                  />
                </app-form-field>
                <div class="officer-actions">
                  <app-button variant="primary" size="sm" [disabled]="busy()" (clicked)="hold()">
                    {{ 'officerCard.actions.confirmHold' | transloco }}
                  </app-button>
                  <app-button variant="secondary" size="sm" (clicked)="holdArmed.set(false)">{{
                    'officerCard.actions.cancel' | transloco
                  }}</app-button>
                </div>
              </div>
            }

            @if (canManage() && decommissionArmed()) {
              <div class="officer-confirm" data-testid="officer-decommission-confirm">
                @if (decommissionWarning(); as warn) {
                  <p class="officer-hint" data-testid="officer-decommission-warning">
                    {{
                      'officerCard.decommission.inFlightWarning'
                        | transloco: { count: warn.in_flight_jobs?.length ?? 0 }
                    }}
                  </p>
                  @for (j of warn.in_flight_jobs ?? []; track j.job_id) {
                    <div class="officer-ledger-item">
                      <span>{{ j.title || shortId(j.job_id) }}</span>
                      @if (j.slot) {
                        <span class="officer-slot-chip">{{ j.slot }}</span>
                      }
                      @if (j.status) {
                        <span class="dim">{{ statusLabel(j.status) }}</span>
                      }
                    </div>
                  }
                  <div class="officer-actions">
                    <app-button
                      variant="danger"
                      size="sm"
                      [disabled]="busy()"
                      (clicked)="decommission(true)"
                      data-testid="officer-decommission-force"
                    >
                      {{ 'officerCard.actions.decommissionKeepRunning' | transloco }}
                    </app-button>
                    <app-button variant="secondary" size="sm" (clicked)="cancelDecommission()">{{
                      'officerCard.actions.keepOfficer' | transloco
                    }}</app-button>
                  </div>
                } @else {
                  <p class="officer-hint">
                    {{ 'officerCard.decommission.hint' | transloco }}
                  </p>
                  <div class="officer-actions">
                    <app-button
                      variant="danger"
                      size="sm"
                      [disabled]="busy()"
                      (clicked)="decommission(false)"
                      data-testid="officer-decommission"
                    >
                      {{ 'officerCard.actions.confirmDecommission' | transloco }}
                    </app-button>
                    <app-button variant="secondary" size="sm" (clicked)="cancelDecommission()">{{
                      'officerCard.actions.keepOfficer' | transloco
                    }}</app-button>
                  </div>
                }
              </div>
            }
          }

          <!-- Worker questions — the row-only routing policy; exists in every state -->
          <div class="officer-policy" data-testid="officer-policy">
            <div class="officer-editor-head">
              <span class="officer-section-title">{{
                'officerCard.policy.title' | transloco
              }}</span>
            </div>
            @if (canManage()) {
              <app-form-field
                [label]="'officerCard.policy.label' | transloco"
                [hint]="policyHint()"
              >
                <app-select
                  [ariaLabel]="'officerCard.a11y.routingPolicy' | transloco"
                  [value]="policyValue()"
                  (changed)="setPolicy($event)"
                >
                  <option value="user_direct">
                    {{ 'officerCard.policy.userDirect' | transloco }}
                  </option>
                  <option value="officer_and_user">
                    {{ 'officerCard.policy.officerAndUser' | transloco }}
                  </option>
                  <option value="officer_first">{{ officerFirstLabel() }}</option>
                </app-select>
              </app-form-field>
            } @else {
              <span class="v" data-testid="officer-policy-read-only">{{ policyLabel() }}</span>
            }
          </div>

          @if (postState() === 'vacant' && incarnations().length) {
            <div class="officer-incarnations" data-testid="officer-incarnations">
              <div class="officer-section-title">
                {{ 'officerCard.vacant.pastIncarnations' | transloco }}
              </div>
              @for (inc of incarnations(); track inc.thread_id) {
                <div class="officer-ledger-item">
                  <a class="officer-incarnation-link" [routerLink]="['/sessions', inc.thread_id]">
                    {{ shortId(inc.thread_id) }}
                  </a>
                  <span class="dim">
                    {{ inc.commissioned_at | date: 'mediumDate' }} →
                    {{ inc.decommissioned_at | date: 'mediumDate' }}
                  </span>
                  @if (inc.reason) {
                    <span>{{ inc.reason }}</span>
                  }
                </div>
              }
            </div>
          }

          @if (postState() !== 'vacant' && recentNotifications().length) {
            <!-- The feed rows recorded about this officer's session (pages,
                 runtime alerts) — the unified feed replaced his digest ring. -->
            <div class="officer-recent" data-testid="officer-recent-notifications">
              <div class="officer-recent-title">
                {{ 'officerCard.recentNotifications.title' | transloco }}
              </div>
              @for (n of recentNotifications(); track n.id) {
                <a
                  class="officer-recent-item"
                  [routerLink]="['/inbox']"
                  [queryParams]="{ n: n.id }"
                  data-testid="officer-recent-item"
                >
                  <span class="dim">{{ n.created_at | date: 'short' }}</span>
                  <span class="officer-recent-severity" [attr.data-severity]="n.severity">
                    {{ 'notifications.severity.' + n.severity | transloco }}
                  </span>
                  <span class="officer-recent-subject" [class.dim]="!!n.resolved_at">{{ n.subject }}</span>
                </a>
              }
            </div>
          }
        </div>
      }

      @if (message()) {
        <div class="officer-message" role="status" aria-live="polite" data-testid="officer-message">
          {{ message() }}
        </div>
      }
    </div>
  `,
  styles: [
    `
      .officer-tab {
        display: flex;
        flex-direction: column;
        gap: 16px;
      }
      .officer-intro h3 {
        margin: 0 0 6px;
        font-size: 15px;
      }
      .officer-intro p {
        margin: 0;
        color: var(--text-secondary);
        font-size: 13px;
        max-width: 70ch;
      }
      .officer-loading {
        display: flex;
        justify-content: center;
        padding: 32px;
      }
      .officer-card {
        border: 1px solid var(--border-color);
        border-radius: var(--radius-surface);
        padding: 16px;
        display: flex;
        flex-direction: column;
        gap: 12px;
        background: var(--surface-0);
      }
      .officer-status-row {
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
      }
      .officer-badge {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        padding: 2px 8px;
        border-radius: var(--radius-pill);
        background: var(--surface-1);
        color: var(--text-secondary);
      }
      .officer-badge[data-status='active'] {
        background: color-mix(in srgb, var(--success, #22c55e) 18%, transparent);
        color: var(--success, #22c55e);
      }
      .officer-badge[data-status='suspended'] {
        background: color-mix(in srgb, var(--warning, #eab308) 18%, transparent);
        color: var(--warning, #eab308);
      }
      .officer-hold {
        font-size: 12px;
        color: var(--warning, #eab308);
      }
      .officer-hold-note {
        font-size: 12px;
        color: var(--text-secondary);
        font-style: italic;
      }
      .officer-title {
        font-weight: 600;
        font-size: 13px;
      }
      .officer-meta {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 8px 16px;
      }
      .officer-meta .k,
      .officer-slots .k {
        display: block;
        font-size: 11px;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      .officer-meta .v {
        font-size: 13px;
      }
      .officer-warn {
        color: var(--warning, #eab308);
        font-size: 12px;
      }
      .officer-slots {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
      }
      .officer-slot-chip {
        font-size: 12px;
        padding: 2px 10px;
        border-radius: var(--radius-pill);
        border: 1px solid var(--border-color);
        background: var(--surface-1);
      }
      /* A starved or broken pool. Border + text rather than a fill: it must
         read as attention-needed at a glance without competing with a real
         error state elsewhere on the card. */
      .officer-slot-chip-alert {
        border-color: var(--color-warning, #b45309);
        color: var(--color-warning, #b45309);
      }
      .officer-editor {
        display: flex;
        flex-direction: column;
        gap: 10px;
      }
      .officer-editor-head {
        display: flex;
        align-items: baseline;
        gap: 10px;
        margin-top: 4px;
      }
      .officer-section-title {
        font-size: 12px;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      .officer-immediacy {
        font-size: 11px;
        color: var(--text-muted);
        font-style: italic;
        padding: 1px 8px;
        border-radius: var(--radius-pill);
        border: 1px dashed var(--border-color);
      }
      .officer-drain {
        font-size: 12px;
        color: var(--warning, #eab308);
        padding-left: 2px;
      }
      .officer-validation {
        font-size: 12px;
        color: var(--danger, #ef4444);
      }
      .officer-read-only {
        border-left: 3px solid var(--border-color);
        padding-left: 10px;
      }
      .officer-actions {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
      }
      .officer-confirm {
        border: 1px solid var(--border-color);
        border-radius: var(--radius-control);
        padding: 10px 12px;
        display: flex;
        flex-direction: column;
        gap: 8px;
        background: var(--surface-1);
      }
      .officer-policy {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }
      .officer-ledger,
      .officer-incarnations {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }
      .officer-ledger-item {
        display: flex;
        gap: 8px;
        font-size: 13px;
        flex-wrap: wrap;
        align-items: baseline;
      }
      .officer-ledger-status {
        font-size: 12px;
        color: var(--text-secondary);
      }
      .officer-incarnation-link {
        font-family: var(--font-mono, monospace);
        font-size: 12px;
      }
      .officer-recent {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }
      .officer-recent-title {
        font-size: 12px;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      .officer-recent-item {
        display: flex;
        gap: 8px;
        align-items: baseline;
        font-size: 13px;
        flex-wrap: wrap;
        color: var(--text-primary);
        text-decoration: none;
      }
      .officer-recent-item:hover .officer-recent-subject {
        text-decoration: underline;
      }
      .officer-recent-severity {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-secondary);
      }
      .officer-recent-severity[data-severity='high'] {
        color: var(--warning);
      }
      .officer-recent-severity[data-severity='critical'] {
        color: var(--danger);
      }
      .officer-slot-row {
        display: flex;
        gap: 8px;
        align-items: end;
        flex-wrap: wrap;
      }
      .officer-hint {
        margin: 0;
        font-size: 13px;
        color: var(--text-secondary);
      }
      .officer-hint.dim {
        color: var(--text-muted);
        font-size: 12px;
      }
      .officer-message {
        font-size: 13px;
        color: var(--text-secondary);
      }
      .dim {
        color: var(--text-muted);
      }
    `,
  ],
})
export class ProjectOfficerComponent implements OnInit, OnDestroy {
  readonly projectId = input.required<string>();
  readonly projectName = input<string>('');

  private readonly http = inject(HttpClient);
  private readonly api = inject(ApiService);
  private readonly notificationFeed = inject(NotificationService);
  private readonly router = inject(Router);
  private readonly modelService = inject(ModelService);
  private readonly transloco = inject(TranslocoService);

  readonly language = signal(this.transloco.getActiveLang());
  private readonly languageSubscription = this.transloco.langChanges$.subscribe((lang) =>
    this.language.set(lang),
  );
  private readonly tr: OfficerTranslate = (key, params) => {
    this.language();
    return String(this.transloco.translate(key, params));
  };

  readonly post = signal<OfficerPost | null>(null);
  readonly loading = signal(true);
  readonly busy = signal(false);
  readonly message = signal('');
  readonly decommissionArmed = signal(false);
  readonly decommissionWarning = signal<OfficerDecommissionResult | null>(null);
  readonly holdArmed = signal(false);
  readonly holdNote = signal('');
  readonly autoPullArmed = signal(false);

  // The kit editor — one set of drafts, every state. Seeded from the post on
  // state transitions only (editorSeedKey), so the 15s poll never clobbers
  // in-progress edits.
  readonly slotDrafts = signal<SlotDraft[]>([{ ...STARTER_SLOT_DRAFT }]);
  readonly categoryOptions = WORK_CATEGORY_OPTIONS;
  // The officer's OWN brain (distinct from the slot models, which are what
  // his workers run on — the classic mistake is arming the troops and leaving
  // the commander on the account default).
  readonly fBrainModel = signal('');
  readonly fAutoPull = signal(false);
  readonly fWorkerSpendCeiling = signal('');
  readonly fReasoning = signal('');
  readonly fSleepMin = signal('');
  readonly fSleepMax = signal('');
  readonly fTokenCeiling = signal('');
  readonly fMaxActions = signal('');
  readonly fMaxWorkers = signal('');

  private editorSeedKey: string | null = null;
  private pollHandle: ReturnType<typeof setInterval> | null = null;

  /** §7's per-field honesty, exposed to the template. */
  readonly immediacy = (field: OfficerEditField): string => immediacyLabel(field, this.tr);

  readonly modelOptions = computed(() => this.modelService.models().flatMap((g) => g.models));
  /** Option text per model id: the catalog label; the value stays the id. */
  readonly modelLabels = computed(() => chatModelLabels(this.modelService.models()));
  readonly postState = computed<OfficerPostState>(() => postStateOf(this.post()));
  readonly canManage = computed(() => this.post()?.can_manage === true);
  readonly autoPullEnableAvailable = computed(
    () => this.post()?.backlog?.auto_pull_control?.enable_available === true,
  );
  readonly recycleActive = computed(() => {
    const phase = this.post()?.runtime_lifecycle?.recycle_phase;
    return !!phase && !['idle', 'complete', 'cancelled'].includes(phase);
  });
  readonly showImmediacy = computed(() => this.postState() !== 'vacant');
  readonly holdLabel = computed(() => holdBadgeLabel(this.post()?.held, this.tr));
  readonly wakeLabel = computed(() =>
    nextWakeLabel(this.post()?.officer?.next_wake_at ?? null, this.tr),
  );
  /** Feed rows about the officer's session, newest first (`source_kind=thread`). */
  readonly recentNotifications = signal<Notification[]>([]);
  readonly kitRows = computed(() =>
    kitChips(this.post()?.kit, this.tr, this.post()?.backlog?.breakers, new Date()),
  );
  readonly conference = computed(
    () => this.post()?.conference ?? this.post()?.officer?.conference ?? null,
  );
  /** Pool policy the officer is operating under, or null when he has no pools. */
  readonly backlogState = computed(() => {
    const post = this.post();
    const hasPools = Object.values(post?.kit ?? {}).some((s) => !!s.category);
    return hasPools ? (post?.backlog ?? null) : null;
  });
  readonly staleClaims = computed(() => this.backlogState()?.stale_claims ?? []);
  readonly staleClaimThresholdHours = computed(() => {
    const minutes = this.backlogState()?.stale_claim_policy?.threshold_minutes;
    return minutes === undefined ? null : minutes / 60;
  });
  readonly staleClaimThresholdSource = computed(
    () => this.backlogState()?.stale_claim_policy?.threshold_source ?? 'unavailable',
  );
  readonly provisioningProblems = computed(
    () => this.post()?.backlog?.provisioning_preflights ?? [],
  );
  readonly provisioningStateSummary = computed(() =>
    [
      ...new Set(
        this.provisioningProblems().map((row) =>
          this.stateLabel(row.context?.provisioning_preflight?.state ?? 'unknown'),
        ),
      ),
    ].join(', '),
  );
  readonly knowledgeProblems = computed(() =>
    (this.post()?.backlog?.knowledge_materialization ?? []).filter(
      (row) => row.canonical_state !== 'canonical' || row.projection_state !== 'synced',
    ),
  );
  readonly knowledgeStateSummary = computed(() =>
    [
      ...new Set(
        this.knowledgeProblems().map(
          (row) =>
            `${this.stateLabel(row.canonical_state)}/${this.stateLabel(row.projection_state)}`,
        ),
      ),
    ].join(', '),
  );
  readonly latestFloorWake = computed(() => this.post()?.backlog?.floor_wakes?.[0] ?? null);
  readonly spendCeiling = computed(
    () => this.post()?.spend_today?.ceiling ?? this.post()?.officer?.token_ceiling?.daily ?? null,
  );
  readonly incarnations = computed(() => [...(this.post()?.incarnations ?? [])].reverse());
  readonly vacantLedger = computed(() => vacantLedgerOf(this.post()));
  readonly rosterIssue = computed(() => rosterValidationIssue(this.slotDrafts()));
  readonly rosterError = computed(() => {
    const issue = this.rosterIssue();
    return issue ? this.tr(issue.key, issue.params) : null;
  });

  private readonly currentDraft = computed<OfficerEditorDraft>(() => ({
    slots: this.slotDrafts(),
    autoPull: this.fAutoPull(),
    workerSpendCeilingDaily: this.fWorkerSpendCeiling(),
    brainModel: this.fBrainModel(),
    reasoning: this.fReasoning(),
    sleepMin: this.fSleepMin(),
    sleepMax: this.fSleepMax(),
    tokenCeiling: this.fTokenCeiling(),
    maxActions: this.fMaxActions(),
    maxWorkers: this.fMaxWorkers(),
  }));
  /** What Save would send — diffed against the post's last known state. */
  readonly pendingPatch = computed(() =>
    this.rosterIssue() ? {} : buildOfficerPatch(draftFromPost(this.post()), this.currentDraft()),
  );
  readonly dirty = computed(() => Object.keys(this.pendingPatch()).length > 0);
  /** Per-draft-row drain hint when shrinking a slot below its in-flight count. */
  readonly drainHints = computed<(string | null)[]>(() => {
    const kit = this.post()?.kit ?? null;
    const vacant = this.postState() === 'vacant';
    return this.slotDrafts().map((row) => {
      if (vacant || !kit) return null;
      const live = kit[row.name.trim().toLowerCase()];
      return drainHint(live?.in_flight, Math.min(20, Math.max(0, Math.floor(row.count))), this.tr);
    });
  });

  readonly policyValue = computed<WorkerMessagesPolicy>(
    () => this.post()?.communication_policy?.worker_messages ?? 'user_direct',
  );
  readonly officerFirstLabel = computed(() =>
    this.tr(
      this.postState() === 'vacant'
        ? 'officerCard.policy.officerFirst'
        : 'officerCard.policy.officerFirstRecommended',
    ),
  );
  readonly policyLabel = computed(() => this.tr(`officerCard.policy.value.${this.policyValue()}`));
  readonly policyHint = computed(() => {
    const mins = this.post()?.communication_policy?.officer_response_minutes ?? 15;
    return this.tr(
      this.postState() === 'vacant' ? 'officerCard.policy.hintVacant' : 'officerCard.policy.hint',
      { minutes: mins },
    );
  });

  ngOnInit(): void {
    const pid = this.projectId();
    if (pid) this.modelService.load(pid);
    this.refresh();
    this.pollHandle = setInterval(() => this.refresh(true), 15000);
  }

  ngOnDestroy(): void {
    if (this.pollHandle) clearInterval(this.pollHandle);
    this.languageSubscription.unsubscribe();
  }

  refresh(silent = false): void {
    const pid = this.projectId();
    if (!pid) {
      this.loading.set(false);
      return;
    }
    if (!silent) this.loading.set(true);
    this.api.getOfficerPost(pid).subscribe((p) => this.applyPost(p));
  }

  /**
   * A transport failure (null) never flips a commissioned card back to the
   * vacant editor — stale beats wrong-state; the next poll heals it.
   */
  private applyPost(p: OfficerPost | null): void {
    if (p) {
      const authorityChanged = this.post() !== null && this.post()?.can_manage !== p.can_manage;
      if (authorityChanged) {
        this.editorSeedKey = null;
        this.autoPullArmed.set(false);
      }
      this.post.set(p);
      this.loadRecentNotifications(p.commissioned ? (p.officer?.thread_id ?? null) : null);
      const key = p.commissioned ? `c:${p.officer?.thread_id ?? ''}` : 'vacant';
      if (this.editorSeedKey !== key) {
        this.seedEditor(draftFromPost(p));
        this.editorSeedKey = key;
      }
    } else if (!this.post() && this.editorSeedKey === null) {
      this.seedEditor(draftFromPost(null));
      this.editorSeedKey = 'none';
    }
    this.loading.set(false);
  }

  /** The last ten feed rows recorded about this officer's session. A
   *  transport failure keeps the previous list (same rule as the post). */
  private loadRecentNotifications(threadId: string | null): void {
    if (!threadId) {
      this.recentNotifications.set([]);
      return;
    }
    this.notificationFeed.listBySource('thread', threadId, 10).subscribe((page) => {
      if (page) this.recentNotifications.set(page.items ?? []);
    });
  }

  private seedEditor(draft: OfficerEditorDraft): void {
    this.slotDrafts.set(draft.slots);
    this.fAutoPull.set(draft.autoPull);
    this.fWorkerSpendCeiling.set(draft.workerSpendCeilingDaily);
    this.autoPullArmed.set(false);
    this.fBrainModel.set(draft.brainModel);
    this.fReasoning.set(draft.reasoning);
    this.fSleepMin.set(draft.sleepMin);
    this.fSleepMax.set(draft.sleepMax);
    this.fTokenCeiling.set(draft.tokenCeiling);
    this.fMaxActions.set(draft.maxActions);
    this.fMaxWorkers.set(draft.maxWorkers);
  }

  patchSlot(index: number, patch: Partial<SlotDraft>): void {
    if (!this.canManage()) return;
    this.slotDrafts.update((rows) => rows.map((r, i) => (i === index ? { ...r, ...patch } : r)));
  }

  toCount(value: string | null | undefined): number {
    const n = parseInt(value ?? '1', 10);
    return Number.isNaN(n) ? 1 : Math.min(20, Math.max(0, n));
  }

  toOptionalCeiling(value: string | null | undefined): number | null {
    const trimmed = (value ?? '').trim();
    if (!trimmed) return null;
    const amount = Number(trimmed);
    return Number.isFinite(amount) ? amount : null;
  }

  /** Enabling is a two-step money-spend acknowledgement; disabling is direct. */
  requestAutoPullChange(enabled: boolean): void {
    if (!this.canManage()) return;
    if (!enabled) {
      this.autoPullArmed.set(false);
      this.fAutoPull.set(false);
      return;
    }
    if (this.fAutoPull() || !this.autoPullEnableAvailable()) return;
    this.autoPullArmed.set(true);
  }

  confirmAutoPull(): void {
    if (!this.canManage() || !this.autoPullEnableAvailable()) return;
    this.fAutoPull.set(true);
    this.autoPullArmed.set(false);
  }

  cancelAutoPull(): void {
    this.autoPullArmed.set(false);
  }

  addSlot(): void {
    if (!this.canManage() || this.slotDrafts().length >= 8) return;
    this.slotDrafts.update((rows) => [
      ...rows,
      // Uncategorized: a new row is plain capacity until someone chooses a
      // pool for it, so adding a slot can never arm auto-pull by accident.
      {
        name: '',
        count: 1,
        model: '',
        backend: '',
        category: '',
        spendCeilingDaily: null,
      },
    ]);
  }

  removeSlot(index: number): void {
    if (!this.canManage()) return;
    this.slotDrafts.update((rows) => rows.filter((_, i) => i !== index));
  }

  statusLabel(status: string | null | undefined): string {
    if (!status) return this.tr('officerCard.status.unknown');
    const known = new Set([
      'active',
      'suspended',
      'ended',
      'created',
      'processing',
      'waiting_for_reply',
      'paused',
      'pending_review',
      'completed',
      'failed',
      'cancelled',
    ]);
    return known.has(status) ? this.tr(`officerCard.status.${status}`) : status;
  }

  stateLabel(state: string | null | undefined): string {
    if (!state) return this.tr('officerCard.status.unknown');
    const normalized = state.replaceAll('_', '-');
    const known = new Set([
      'not-attempted',
      'in-progress',
      'retryable-failed',
      'permanent-failed',
      'activated',
      'pending-sync',
      'canonical',
      'failed',
      'superseded',
      'pending',
      'synced',
      'projection-only',
      'retryable',
      'queued',
      'delivered',
      'permanent-failed',
      'unknown',
    ]);
    return known.has(normalized) ? this.tr(`officerCard.machineState.${normalized}`) : state;
  }

  shortId(id: string): string {
    return id.length > 8 ? id.slice(0, 8) : id;
  }

  /** Raise an officer onto the post with the editor's config (§5). */
  async commission(): Promise<void> {
    const pid = this.projectId();
    if (!pid || !this.canManage() || this.busy()) return;
    const rosterError = this.rosterError();
    if (rosterError) {
      this.message.set(rosterError);
      return;
    }
    this.busy.set(true);
    this.message.set('');
    try {
      const resp = await firstValueFrom(
        this.api.commissionOfficer(pid, buildOfficerConfig(this.currentDraft())),
      );
      this.message.set(
        resp?.thread_id
          ? this.tr('officerCard.messages.commissionedWithBrief')
          : this.tr('officerCard.messages.commissioned'),
      );
      this.editorSeedKey = null;
      this.refresh(true);
    } catch (err) {
      this.message.set(this.errText(err, 'officerCard.errors.commission'));
    } finally {
      this.busy.set(false);
    }
  }

  /** PATCH only what changed; the sitrep's capacity line carries the truth. */
  async saveEdits(): Promise<void> {
    const pid = this.projectId();
    const patch = this.pendingPatch();
    if (!pid || !this.canManage() || this.busy()) return;
    const rosterError = this.rosterError();
    if (rosterError) {
      this.message.set(rosterError);
      return;
    }
    if (!Object.keys(patch).length) return;
    this.busy.set(true);
    this.message.set('');
    try {
      await firstValueFrom(this.api.updateOfficerPost(pid, patch));
      this.message.set(this.tr('officerCard.messages.updated'));
      this.editorSeedKey = null;
      this.refresh(true);
    } catch (err) {
      this.message.set(this.errText(err, 'officerCard.errors.update'));
    } finally {
      this.busy.set(false);
    }
  }

  /**
   * Non-forced first: with jobs in flight the server answers with the warning
   * + list instead of decommissioning; the forced retry proceeds, leaving
   * them running (their completions land on the post's ledger).
   */
  async decommission(force: boolean): Promise<void> {
    const pid = this.projectId();
    if (!pid || !this.canManage() || this.busy()) return;
    this.busy.set(true);
    this.message.set('');
    try {
      const resp = await firstValueFrom(this.api.decommissionOfficer(pid, force));
      if (!force && ((resp?.in_flight_jobs?.length ?? 0) > 0 || resp?.warning)) {
        this.decommissionWarning.set(resp);
        return;
      }
      this.decommissionArmed.set(false);
      this.decommissionWarning.set(null);
      this.message.set(this.tr('officerCard.messages.decommissioned'));
      this.editorSeedKey = null;
      this.refresh(true);
    } catch (err) {
      this.message.set(this.errText(err, 'officerCard.errors.decommission'));
    } finally {
      this.busy.set(false);
    }
  }

  cancelDecommission(): void {
    if (!this.canManage()) return;
    this.decommissionArmed.set(false);
    this.decommissionWarning.set(null);
  }

  /** Maintenance hold — commissioned, standing down; never self-healed away. */
  async hold(): Promise<void> {
    const pid = this.projectId();
    if (!pid || !this.canManage() || this.busy()) return;
    this.busy.set(true);
    this.message.set('');
    try {
      await firstValueFrom(this.api.holdOfficer(pid, this.holdNote()));
      this.holdArmed.set(false);
      this.holdNote.set('');
      this.message.set(this.tr('officerCard.messages.held'));
      this.refresh(true);
    } catch (err) {
      this.message.set(this.errText(err, 'officerCard.errors.hold'));
    } finally {
      this.busy.set(false);
    }
  }

  async release(): Promise<void> {
    const pid = this.projectId();
    if (!pid || !this.canManage() || this.busy()) return;
    this.busy.set(true);
    this.message.set('');
    try {
      await firstValueFrom(this.api.releaseOfficer(pid));
      this.message.set(this.tr('officerCard.messages.released'));
      this.refresh(true);
    } catch (err) {
      this.message.set(this.errText(err, 'officerCard.errors.release'));
    } finally {
      this.busy.set(false);
    }
  }

  async recycle(): Promise<void> {
    const pid = this.projectId();
    if (!pid || !this.canManage() || this.busy() || this.recycleActive()) return;
    this.busy.set(true);
    this.message.set('');
    try {
      await firstValueFrom(this.api.recycleOfficer(pid));
      this.message.set(this.tr('officerCard.messages.recycleStarted'));
      this.refresh(true);
    } catch (err) {
      this.message.set(this.errText(err, 'officerCard.errors.recycle'));
    } finally {
      this.busy.set(false);
    }
  }

  /**
   * The one row-only field: PATCHes `communication_policy` alone, optimistic
   * so the select doesn't snap back while the write is in flight.
   */
  async setPolicy(value: string | null): Promise<void> {
    const pid = this.projectId();
    const v = value as WorkerMessagesPolicy | null;
    if (!pid || !this.canManage() || !v || v === this.policyValue() || this.busy()) return;
    const prev = this.post();
    this.post.update((p) =>
      p
        ? {
            ...p,
            communication_policy: {
              ...(p.communication_policy ?? {}),
              worker_messages: v,
            },
          }
        : p,
    );
    try {
      await firstValueFrom(
        this.api.updateOfficerPost(pid, {
          communication_policy: { worker_messages: v },
        }),
      );
      this.refresh(true);
    } catch (err) {
      this.post.set(prev);
      this.message.set(this.errText(err, 'officerCard.errors.policy'));
    }
  }

  openLog(): void {
    const tid = this.post()?.officer?.thread_id;
    if (tid) void this.router.navigate(['/sessions', tid]);
  }

  async openConference(): Promise<void> {
    const pid = this.projectId();
    if (!pid || !this.canManage() || this.busy()) return;
    const existing = this.conference();
    if (existing) {
      await this.router.navigate(['/sessions', existing.thread_id]);
      return;
    }
    this.busy.set(true);
    this.message.set('');
    try {
      const resp = await firstValueFrom(
        this.http.post<{ thread_id: string }>(
          `${environment.apiUrl}/persistent/threads`,
          buildConferenceThreadCreateBody(
            pid,
            this.projectName() || this.tr('officerCard.defaults.project'),
            this.tr('officerCard.actions.conference'),
          ),
        ),
      );
      await this.router.navigate(['/sessions', resp.thread_id]);
    } catch (err) {
      // conference_open 409 → someone beat us; refresh finds it.
      this.message.set(this.errText(err, 'officerCard.errors.conference'));
      this.busy.set(false);
      this.refresh(true);
    }
  }

  private errText(err: unknown, fallbackKey: string): string {
    const detail = (err as { error?: { detail?: unknown } })?.error?.detail;
    const fallback = this.tr(fallbackKey);
    return typeof detail === 'string' && detail
      ? this.tr('officerCard.errors.withDetail', { fallback, detail })
      : fallback;
  }
}
