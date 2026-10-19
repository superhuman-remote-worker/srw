import {ChangeDetectionStrategy, Component, computed, inject, OnInit, signal} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {SidebarToggleComponent} from '../../../shell/sidebar-toggle/sidebar-toggle.component';
import {AdminGrantsService, ScopeKind} from '../../../core/services/admin-grants.service';
import {AdminUsersService} from '../../../core/services/admin-users.service';
import {ApiService} from '../../../core/services/api.service';
import {Expert, GrantCatalogEntry} from '../../../core/models/api.model';
import {normalizePermissionMode} from '../../../core/models/permission-mode';
import {TranslocoPipe} from '@jsverse/transloco';
import {RouterLink} from '@angular/router';

const INHERIT = '__inherit__';

/**
 * Admin → Grants panel: set/revoke capability grants per user/project/global.
 * Deny-by-default; restrict-only. Reads the catalog dynamically from the API
 * (never hardcodes the keys). Server-side admin-gated.
 */
@Component({
  selector: 'app-admin-grants',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule, SidebarToggleComponent, TranslocoPipe, RouterLink],
  template: `
    <div class="admin-page">
      <div class="admin-container">
        <div class="page-header">
          <app-sidebar-toggle />
          <h1 class="page-title">Capability Grants</h1>
        </div>

        <section class="admin-section expert-defaults-section">
          <h2>{{ 'adminExpertDefaults.title' | transloco }}</h2>
          <p class="desc">{{ 'adminExpertDefaults.desc' | transloco }}</p>
          <div class="scope-bar">
            @for (type of expertTypes; track type) {
              <label>{{ ('adminExpertDefaults.' + type) | transloco }}
                <select
                  [ngModel]="applicationDefaults()[type] ?? ''"
                  [disabled]="applicationDefaultSaving() === type"
                  (ngModelChange)="setApplicationDefault(type, $event)"
                >
                  @for (expert of globalExperts(type); track expert.id) {
                    <option [value]="expert.id">{{ expert.display_name }}</option>
                  }
                </select>
                @if (applicationDefaults()[type]; as expertId) {
                  <a class="default-edit-link" [routerLink]="['/experts', expertId, 'edit']">
                    {{ 'adminExpertDefaults.edit' | transloco }}
                  </a>
                }
              </label>
            }
          </div>
        </section>

        <section class="admin-section">
          <p class="desc">
            Set or revoke per-user, per-project, or global capability grants.
            Deny-by-default: a capability that isn't granted is blocked at expert
            save-time and at job/session dispatch.
          </p>

          <div class="scope-bar">
            <label>Scope
              <select [ngModel]="scopeKind()" (ngModelChange)="onScopeKindChange($event)">
                <option value="user">User</option>
                <option value="project">Project</option>
                <option value="global">Global</option>
              </select>
            </label>

            @if (scopeKind() === 'user') {
              <label>User
                <select [ngModel]="scopeId()" (ngModelChange)="onScopeId($event)">
                  <option [ngValue]="null">— select —</option>
                  @for (u of users.users(); track u.id) {
                    <option [ngValue]="u.id">{{ u.display_name }} ({{ u.email }}){{ u.is_admin ? ' — admin' : '' }}</option>
                  }
                </select>
              </label>
            }
            @if (scopeKind() === 'project') {
              <label>Project
                <select [ngModel]="scopeId()" (ngModelChange)="onScopeId($event)">
                  <option [ngValue]="null">— select —</option>
                  @for (p of projects(); track p.id) {
                    <option [ngValue]="p.id">{{ p.name }}</option>
                  }
                </select>
              </label>
            }
          </div>

          @if (selectedUserIsAdmin()) {
            <div class="banner info">
              Admin — unrestricted. This user bypasses all capability checks;
              the grants below are informational (they only take effect if the
              user is ever demoted). An empty list would NOT mean "no
              permission".
            </div>
          }

          @if (scopeReady()) {
            <table class="grid app-table">
              <thead><tr><th>Capability</th><th>Default</th><th>Grant</th></tr></thead>
              <tbody>
                @for (k of catalogKeys(); track k) {
                  <tr>
                    <td><code>{{ k }}</code></td>
                    <td class="muted">{{ fmt(svc.catalog()[k].default) }}</td>
                    <td>
                      @if (svc.catalog()[k].type === 'bool') {
                        <select [ngModel]="currentValue(k)" (ngModelChange)="apply(k, $event)">
                          <option value="__inherit__">Inherit (default: {{ fmt(svc.catalog()[k].default) }})</option>
                          <option value="true">Allow</option>
                          <option value="false">Deny</option>
                        </select>
                      } @else if (svc.catalog()[k].type === 'enum') {
                        <select [ngModel]="currentValue(k)" (ngModelChange)="apply(k, $event)">
                          <option value="__inherit__">Inherit (default: {{ fmt(svc.catalog()[k].default) }})</option>
                          @for (o of svc.catalog()[k].order; track o) {
                            <option [value]="o">{{ o }}</option>
                          }
                        </select>
                      } @else {
                        <input type="text" [ngModel]="listText(k)"
                               (ngModelChange)="listDraft.set($event)"
                               (blur)="applyList(k, listDraft())"
                               placeholder="model ids, comma-separated — empty = all" />
                      }
                    </td>
                  </tr>
                }
              </tbody>
            </table>
          } @else {
            <p class="muted">Pick a scope to view and edit its grants.</p>
          }

          @if (error()) { <div class="banner err">{{ error() }}</div> }
        </section>
      </div>
    </div>
  `,
  styles: [`
    /* The scroll container is the HOST, not an inner div: the shell's
       .content-area is overflow:hidden, and a routed host with no height is
       auto-height, so a child's height:100% resolved to auto and the tail of
       the grants table (the newest capability rows) was clipped away with no
       scrollbar. Same shape every other admin screen uses. */
    :host { display: block; height: 100%; overflow: auto; }
    .admin-container { padding: 32px; max-width: var(--content-max-width); margin: 0 auto; }
    .page-header { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem; }
    .page-title { margin: 0; color: var(--text-primary); }
    .admin-section {
      background: var(--panel-bg); border: 1px solid var(--border-color);
      border-radius: var(--radius-lg); padding: 1.25rem;
    }
    .expert-defaults-section { margin-bottom: 1rem; }
    .expert-defaults-section h2 { margin-top: 0; color: var(--text-primary); }
    .default-edit-link { color: var(--accent-color); font-size: 0.8rem; }
    .desc { color: var(--text-muted); margin: 0 0 1rem; }
    .scope-bar { display: flex; flex-wrap: wrap; gap: 1rem; margin-bottom: 1rem; }
    .scope-bar label { display: flex; flex-direction: column; gap: 0.25rem; color: var(--text-muted); font-size: 0.85rem; }
    .scope-bar select, .scope-bar input, .grid select, .grid input {
      padding: 0.35rem 0.5rem; background: var(--surface-0); color: var(--text-primary);
      border: 1px solid var(--border-color); border-radius: var(--radius-surface);
    }
    .grid .muted, .muted { color: var(--text-muted); }
    .banner { margin-top: 1rem; padding: 0.5rem 0.75rem; border-radius: var(--radius-control); }
    .banner.err { background: var(--danger-tint); color: var(--danger); }
    .banner.info {
      margin: 0 0 1rem; background: var(--surface-0);
      border: 1px solid var(--border-color); color: var(--text-muted);
    }

    @media (max-width: 560px) {
      /* Scope bar: stack each control full-width (bigger tap targets; also caps
         the User/Project <select> intrinsic width so a long name can't overflow). */
      .scope-bar { flex-direction: column; gap: 0.75rem; }
      .scope-bar select, .scope-bar input { width: 100%; box-sizing: border-box; }

      /* Grants grid -> one card per capability (a fixed 3-col table can't fit a
         phone; this restores full-width controls and removes the h-scroll). */
      .grid, .grid tbody, .grid tr, .grid td { display: block; }
      .grid thead { display: none; }
      .grid tr {
        border: 1px solid var(--border-color); border-radius: var(--radius-surface);
        padding: 0.75rem; margin-bottom: 0.75rem;
      }
      .grid td { border-bottom: none; padding: 0.2rem 0; }
      .grid td:first-child { padding-bottom: 0.35rem; }
      .grid td:nth-child(2)::before { content: 'Default: '; color: var(--text-muted); }
      .grid td:nth-child(3) select,
      .grid td:nth-child(3) input { width: 100%; box-sizing: border-box; }
    }
  `],
})
export class AdminGrantsComponent implements OnInit {
  readonly svc = inject(AdminGrantsService);
  readonly users = inject(AdminUsersService);
  private readonly api = inject(ApiService);

  scopeKind = signal<ScopeKind>('user');
  scopeId = signal<string | null>(null);
  listDraft = signal('');
  projects = signal<{id: string; name: string}[]>([]);
  error = signal('');
  readonly expertTypes = ['worker', 'session'] as const;
  readonly experts = signal<Expert[]>([]);
  readonly applicationDefaults = signal<Record<'worker' | 'session', string | null>>({
    worker: null,
    session: null,
  });
  readonly applicationDefaultSaving = signal<'worker' | 'session' | null>(null);

  catalogKeys = computed(() => Object.keys(this.svc.catalog()));
  scopeReady = computed(() => this.scopeKind() === 'global' || !!this.scopeId());
  /** Admins bypass the PDP everywhere — surface that instead of letting an
   *  empty grant list read as "no permission" (the vm_upgrade incident). */
  selectedUserIsAdmin = computed(() =>
    this.scopeKind() === 'user' &&
    !!this.users.users().find((u) => u.id === this.scopeId())?.is_admin,
  );

  ngOnInit(): void {
    this.users.loadUsers();
    this.api.getProjects().subscribe({
      next: (ps) => this.projects.set((ps ?? []).map((p) => ({id: p.id, name: p.name}))),
      // A scope picker with no projects is honest about what it can offer; the
      // grants table itself does not depend on this load.
      error: () => this.projects.set([]),
    });
    this.reload();
    this.loadApplicationDefaults();
  }

  globalExperts(type: 'worker' | 'session'): Expert[] {
    return this.experts().filter(
      (expert) => expert.expert_type === type &&
        (expert.source === 'global' || expert.source === 'managed'),
    );
  }

  private loadApplicationDefaults(): void {
    this.api.getExperts().subscribe((experts) => this.experts.set(experts));
    this.api.getApplicationExpertDefaults().subscribe({
      next: (response) => {
        this.applicationDefaults.set({
          worker: response.defaults.worker?.id ?? null,
          session: response.defaults.session?.id ?? null,
        });
        this.applicationDefaultSaving.set(null);
      },
      error: () => this.applicationDefaultSaving.set(null),
    });
  }

  setApplicationDefault(type: 'worker' | 'session', expertId: string): void {
    if (!expertId || this.applicationDefaultSaving()) return;
    this.error.set('');
    this.applicationDefaultSaving.set(type);
    this.api.setApplicationExpertDefault(type, expertId).subscribe({
      next: () => this.loadApplicationDefaults(),
      error: (e: unknown) => {
        this.error.set(this.detail(e));
        this.applicationDefaultSaving.set(null);
      },
    });
  }

  onScopeKindChange(kind: ScopeKind): void {
    this.scopeKind.set(kind);
    this.scopeId.set(null);
    this.reload();
  }

  onScopeId(id: string | null): void {
    this.scopeId.set(id);
    this.reload();
  }

  reload(): void {
    if (!this.scopeReady()) return;
    this.svc.load(this.scopeKind(), this.scopeId());
  }

  fmt(v: unknown): string {
    return v === null ? 'all' : String(v);
  }

  spec(key: string): GrantCatalogEntry {
    return this.svc.catalog()[key];
  }

  /** The explicitly-set value as a select-bindable string, or the INHERIT sentinel. */
  currentValue(key: string): string {
    const row = this.svc.grants().find((g) => g.key === key);
    if (!row) return INHERIT;
    // A row written before the rename still says `supervised`; show the
    // option it means rather than a blank select.
    if (key === 'permission_mode') return normalizePermissionMode(row.value_json) ?? String(row.value_json);
    return String(row.value_json);
  }

  listText(key: string): string {
    const row = this.svc.grants().find((g) => g.key === key);
    return Array.isArray(row?.value_json) ? (row!.value_json as string[]).join(', ') : '';
  }

  apply(key: string, raw: string): void {
    this.error.set('');
    const done = {
      next: () => this.svc.load(this.scopeKind(), this.scopeId()),
      error: (e: unknown) => this.error.set(this.detail(e)),
    };
    if (raw === INHERIT) {
      this.svc.deleteGrant(this.scopeKind(), this.scopeId(), key).subscribe(done);
    } else {
      const v: unknown = this.spec(key).type === 'bool' ? raw === 'true' : raw;
      this.svc.setGrant(this.scopeKind(), this.scopeId(), key, v).subscribe(done);
    }
  }

  applyList(key: string, raw: string): void {
    this.error.set('');
    const ids = raw.split(',').map((s) => s.trim()).filter(Boolean);
    const done = {
      next: () => this.svc.load(this.scopeKind(), this.scopeId()),
      error: (e: unknown) => this.error.set(this.detail(e)),
    };
    if (ids.length === 0) {
      this.svc.deleteGrant(this.scopeKind(), this.scopeId(), key).subscribe(done);
    } else {
      this.svc.setGrant(this.scopeKind(), this.scopeId(), key, ids).subscribe(done);
    }
  }

  private detail(err: unknown): string {
    const d = (err as {error?: {detail?: unknown}})?.error?.detail;
    return typeof d === 'string' ? d : 'Request failed';
  }
}
