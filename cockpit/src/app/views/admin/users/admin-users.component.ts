import {ChangeDetectionStrategy, Component, computed, effect, inject, OnInit, signal} from '@angular/core';
import {SidebarToggleComponent} from '../../../shell/sidebar-toggle/sidebar-toggle.component';
import {AdminUsersService} from '../../../core/services/admin-users.service';
import {NotificationService} from '../../../core/services/notification.service';
import {McpServerQuestionsChoice, User} from '../../../core/models/api.model';
import {AppCheckboxComponent} from '../../../ui/checkbox';
import {AppBadgeComponent} from '../../../ui/badge';
import {TranslocoPipe} from '@jsverse/transloco';

@Component({
  selector: 'app-admin-users',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [SidebarToggleComponent, AppCheckboxComponent, AppBadgeComponent, TranslocoPipe],
  template: `
    <div class="admin-page">
      <div class="admin-container">
        <div class="page-header">
          <app-sidebar-toggle />
          <h1 class="page-title">Users &amp; VM Workspaces</h1>
        </div>
        <p class="page-desc">
          Manage user roles and grant access to VM workspaces.
        </p>

        <!-- Kill-switch -->
        <section class="admin-section">
          <h2 class="section-title">Global VM Workspaces</h2>
          <p class="section-desc">
            When disabled, every VM workspace request is rejected — including
            those from admin users. Already-running VMs are not torn down.
          </p>
          <app-checkbox
            [checked]="admin.vmWorkspaces().enabled"
            (changed)="setKillSwitch($event)"
          >
            <span class="toggle-label">
              VM workspaces enabled
              @if (!admin.vmWorkspaces().enabled) {
                <app-badge tone="danger" size="xs" appearance="solid">disabled</app-badge>
              }
            </span>
          </app-checkbox>
          @if (admin.vmWorkspaces().updated_at) {
            <p class="muted small">
              Last changed {{ formatDate(admin.vmWorkspaces().updated_at) }}
              by {{ admin.vmWorkspaces().updated_by || 'unknown' }}
            </p>
          }
        </section>

        <!-- What an MCP connector without its own choice does when its
             server asks a yes/no question mid-call. -->
        <section class="admin-section" data-testid="mcp-questions-default">
          <h2 class="section-title">{{ 'admin.mcpQuestions.title' | transloco }}</h2>
          <p class="section-desc">{{ 'admin.mcpQuestions.hint' | transloco }}</p>
          <div
            class="choice-row"
            role="radiogroup"
            [attr.aria-label]="'admin.mcpQuestions.title' | transloco"
          >
            <label class="choice" data-choice="decline">
              <input
                type="radio"
                name="mcp-questions-default"
                [checked]="mcpQuestionsDefault() === 'decline'"
                [disabled]="mcpQuestionsDefault() === null"
                (change)="setMcpQuestionsDefault('decline')"
              >
              {{ 'admin.mcpQuestions.decline' | transloco }}
            </label>
            <label class="choice" data-choice="accept">
              <input
                type="radio"
                name="mcp-questions-default"
                [checked]="mcpQuestionsDefault() === 'accept'"
                [disabled]="mcpQuestionsDefault() === null"
                (change)="setMcpQuestionsDefault('accept')"
              >
              {{ 'admin.mcpQuestions.accept' | transloco }}
            </label>
          </div>
          @if (mcpQuestionsFailed()) {
            <p class="small error" role="alert">{{ 'admin.mcpQuestions.saveFailed' | transloco }}</p>
          } @else if (admin.mcpServerQuestions()?.updated_at) {
            <p class="muted small">
              {{ 'admin.mcpQuestions.lastChanged' | transloco: {
                when: formatDate(admin.mcpServerQuestions()?.updated_at ?? null),
                who: admin.mcpServerQuestions()?.updated_by || ('admin.mcpQuestions.unknownAdmin' | transloco)
              } }}
            </p>
          }
        </section>

        <!-- Users -->
        <section class="admin-section">
          <h2 class="section-title">Users</h2>
          <p class="section-desc">
            Admins always have VM access. Non-admins need an explicit grant.
            New registrants stay pending until an admin approves them.
          </p>

          <div class="user-toolbar">
            <div class="filters">
              <button
                type="button"
                class="filter-chip"
                [class.active]="filter() === 'all'"
                (click)="setFilter('all')"
              >
                All ({{ admin.users().length }})
              </button>
              <button
                type="button"
                class="filter-chip"
                [class.active]="filter() === 'pending'"
                (click)="setFilter('pending')"
              >
                Pending ({{ pendingCount() }})
              </button>
            </div>
            @if (selectedCount() > 0) {
              <button type="button" class="approve-btn" (click)="approveSelected()">
                Approve selected ({{ selectedCount() }})
              </button>
            }
          </div>

          @if (filteredUsers().length > 0) {
            <div class="user-table">
              <div class="user-header">
                <span class="col-select"></span>
                <span class="col-name">Name</span>
                <span class="col-email">Email</span>
                <span class="col-status">Status</span>
                <span class="col-flag">Admin</span>
                <span class="col-flag">VM</span>
              </div>
              @for (u of filteredUsers(); track u.id) {
                <div class="user-row">
                  <span class="col-select">
                    @if (!u.is_approved) {
                      <app-checkbox
                        size="sm"
                        [checked]="isSelected(u)"
                        [ariaLabel]="'Select ' + u.display_name + ' for approval'"
                        (changed)="toggleSelect(u, $event)"
                      />
                    }
                  </span>
                  <span class="col-name">
                    <span class="dot" [style.background]="u.avatar_color"></span>
                    {{ u.display_name }}
                  </span>
                  <span class="col-email mono">{{ u.email || '-' }}</span>
                  <span class="col-status">
                    @if (u.is_approved) {
                      <app-badge
                        tone="success"
                        size="xs"
                        appearance="subtle"
                        [title]="u.approved_at ? 'Approved ' + formatDate(u.approved_at) : 'Approved'"
                        >Approved</app-badge
                      >
                    } @else {
                      <app-badge tone="warning" size="xs" appearance="solid"
                        >Pending</app-badge
                      >
                    }
                  </span>
                  <span class="col-flag">
                    <app-checkbox
                      size="sm"
                      [checked]="!!u.is_admin"
                      [disabled]="true"
                      [ariaLabel]="'Admin: ' + u.display_name"
                      [title]="'Admin role is managed in Keycloak'"
                    />
                  </span>
                  <span class="col-flag">
                    <app-checkbox
                      size="sm"
                      [checked]="!!(u.is_admin || u.can_use_vm)"
                      [disabled]="!!u.is_admin"
                      [ariaLabel]="'VM access: ' + u.display_name"
                      [title]="u.is_admin ? 'Admins always have VM access' : ''"
                      (changed)="toggleVm(u, $event)"
                    />
                  </span>
                </div>
              }
            </div>
          } @else {
            <p class="empty-state">
              @if (filter() === 'pending') {
                No pending users.
              } @else {
                No users loaded.
              }
            </p>
          }
        </section>
      </div>
    </div>
  `,
  styles: [`
    :host {
      display: block;
      height: 100%;
      overflow: auto;
    }
    .admin-page {
      padding: 32px;
      max-width: var(--content-max-width);
      margin: 0 auto;
      color: var(--text-primary);
    }
    .page-header {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 8px;
    }
    .page-title {
      font-size: 24px;
      font-weight: 700;
      margin: 0;
      color: var(--text-primary);
    }
    .page-desc {
      font-size: 13px;
      color: var(--text-muted);
      margin: 0 0 32px 0;
    }
    .admin-section {
      background: var(--panel-bg);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-lg);
      padding: 24px;
      margin-bottom: 24px;
    }
    .section-title {
      font-size: 18px;
      font-weight: 600;
      margin-bottom: 4px;
      color: var(--text-primary);
    }
    .section-desc {
      font-size: 13px;
      color: var(--text-muted);
      margin-bottom: 20px;
    }
    .choice-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px 20px;
      font-size: 13px;
      color: var(--text-primary);
    }
    .choice {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      cursor: pointer;
    }
    .error {
      color: var(--danger);
    }
    .toggle-label {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .small {
      font-size: 12px;
      margin-top: 8px;
    }
    .muted {
      color: var(--text-muted);
    }
    .mono {
      font-family: 'JetBrains Mono', 'Fira Code', monospace;
      font-size: 12px;
      color: var(--text-muted);
    }
    .user-table {
      border: 1px solid var(--border-color);
      border-radius: var(--radius-surface);
      overflow: hidden;
    }
    .user-toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }
    .filters {
      display: inline-flex;
      gap: 6px;
    }
    .filter-chip {
      border: 1px solid var(--border-color);
      background: var(--surface-0);
      color: var(--text-muted);
      border-radius: var(--radius-surface);
      padding: 4px 12px;
      font-size: 12px;
      font-weight: 500;
      cursor: pointer;
    }
    .filter-chip.active {
      background: var(--accent-color);
      color: var(--on-accent);
      border-color: var(--accent-color);
    }
    .approve-btn {
      border: 1px solid var(--accent-color);
      background: var(--accent-color);
      color: var(--on-accent);
      border-radius: var(--radius-surface);
      padding: 6px 14px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
    }
    .approve-btn:hover {
      background: var(--accent-hover);
      border-color: var(--accent-hover);
    }
    .user-header,
    .user-row {
      display: grid;
      grid-template-columns: 32px 1.4fr 1.6fr 110px 70px 70px;
      padding: 10px 14px;
      gap: 8px;
      align-items: center;
      font-size: 13px;
    }
    .col-select,
    .col-status {
      display: flex;
      align-items: center;
    }
    .col-select {
      justify-content: center;
    }
    .user-header {
      background: var(--surface-0);
      font-weight: 600;
      font-size: 12px;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .user-row {
      border-top: 1px solid var(--border-color);
    }
    .dot {
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      margin-right: 8px;
    }
    .empty-state {
      text-align: center;
      color: var(--text-muted);
      padding: 20px;
      font-size: 13px;
    }
    @media (max-width: 720px) {
      /* The Users table is a 6-col grid (32px 1.4fr 1.6fr 110px 70px 70px) —
         the ~282px of fixed columns alone overflow a phone, and
         .user-table{overflow:hidden} clips the Admin/VM toggles off-screen
         with no scrollbar. Re-grid each row into a self-contained card.
         Desktop (>720px) keeps the table unchanged. */
      .user-header {
        display: none;
      }
      .user-row {
        grid-template-columns: auto 1fr auto;
        column-gap: 8px;
        row-gap: 8px;
        padding: 12px 14px;
      }
      .col-select {
        grid-row: 1;
        grid-column: 1;
      }
      .col-name {
        grid-row: 1;
        grid-column: 2;
        font-weight: 600;
        min-width: 0;
        overflow-wrap: anywhere;
      }
      .col-status {
        grid-row: 1;
        grid-column: 3;
        justify-self: end;
      }
      .col-email {
        grid-row: 2;
        grid-column: 1 / -1;
      }
      /* The two flag toggles become full-width settings-list rows. The header
         row that labelled them is hidden on mobile, so re-introduce visible
         labels via ::before (5th child = Admin, 6th = VM). */
      .user-row > .col-flag {
        grid-column: 1 / -1;
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding-top: 8px;
        border-top: 1px solid var(--border-color);
        font-size: 13px;
        color: var(--text-primary);
      }
      .user-row > .col-flag:nth-child(5) {
        grid-row: 3;
      }
      .user-row > .col-flag:nth-child(5)::before {
        content: 'Admin access';
      }
      .user-row > .col-flag:nth-child(6) {
        grid-row: 4;
      }
      .user-row > .col-flag:nth-child(6)::before {
        content: 'VM access';
      }
      /* Let the Approve button wrap below the filters instead of squeezing. */
      .user-toolbar {
        flex-wrap: wrap;
      }
    }
  `],
})
export class AdminUsersComponent implements OnInit {
  protected readonly admin = inject(AdminUsersService);
  private readonly notifications = inject(NotificationService);

  /** Refresh the table live when a user_registered SSE frame arrives. */
  private readonly _onUserRegistered = effect(() => {
    if (this.notifications.adminUserRegistered()) {
      this.admin.loadUsers();
    }
  });

  /** Table filter: all users vs. only those awaiting approval. */
  readonly filter = signal<'all' | 'pending'>('all');
  /** Ids checked for the next "Approve selected" batch. */
  readonly selectedIds = signal<Set<string>>(new Set());

  readonly pendingCount = computed(
    () => this.admin.users().filter((u) => !u.is_approved).length,
  );
  readonly filteredUsers = computed(() => {
    const rows = this.admin.users();
    return this.filter() === 'pending'
      ? rows.filter((u) => !u.is_approved)
      : rows;
  });
  readonly selectedCount = computed(() => this.selectedIds().size);
  /** The choice being saved; shown until the server answers, then the
   *  stored one again (a refused save springs back). */
  private readonly mcpQuestionsPending = signal<McpServerQuestionsChoice | null>(null);
  readonly mcpQuestionsFailed = signal(false);
  readonly mcpQuestionsDefault = computed<McpServerQuestionsChoice | null>(
    () => this.mcpQuestionsPending() ?? this.admin.mcpServerQuestions()?.default ?? null,
  );

  ngOnInit(): void {
    this.admin.loadUsers();
    this.admin.loadVmWorkspaces();
    this.admin.loadMcpServerQuestions();
  }

  isSelected(u: User): boolean {
    return this.selectedIds().has(u.id);
  }

  setFilter(f: 'all' | 'pending'): void {
    this.filter.set(f);
  }

  toggleSelect(u: User, checked: boolean): void {
    const next = new Set(this.selectedIds());
    if (checked) {
      next.add(u.id);
    } else {
      next.delete(u.id);
    }
    this.selectedIds.set(next);
  }

  approveSelected(): void {
    const ids = [...this.selectedIds()];
    if (ids.length === 0) {
      return;
    }
    this.admin.approveUsers(ids).subscribe({
      next: () => this.selectedIds.set(new Set()),
      error: () => this.admin.loadUsers(),
    });
  }

  setKillSwitch(enabled: boolean): void {
    this.admin.setVmWorkspacesEnabled(enabled).subscribe({
      error: () => this.admin.loadVmWorkspaces(),
    });
  }

  setMcpQuestionsDefault(value: McpServerQuestionsChoice): void {
    this.mcpQuestionsPending.set(value);
    this.mcpQuestionsFailed.set(false);
    this.admin.setMcpServerQuestionsDefault(value).subscribe({
      next: () => this.mcpQuestionsPending.set(null),
      error: () => {
        this.mcpQuestionsPending.set(null);
        this.mcpQuestionsFailed.set(true);
        this.admin.loadMcpServerQuestions();
      },
    });
  }

  toggleVm(u: User, checked: boolean): void {
    if (u.is_admin) {
      return;
    }
    this.admin.patchUser(u.id, {can_use_vm: checked}).subscribe({
      error: () => this.admin.loadUsers(),
    });
  }

  formatDate(iso: string | null): string {
    if (!iso) return '';
    try {
      return new Date(iso).toLocaleString();
    } catch {
      return iso;
    }
  }
}
