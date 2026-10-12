import {ChangeDetectionStrategy, Component, OnInit, inject, signal} from '@angular/core';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {forkJoin, of, switchMap} from 'rxjs';

import {Contact, ContactProjectRef} from '../../core/models/api.model';
import {ApiService} from '../../core/services/api.service';
import {ContactsService} from '../../core/services/contacts.service';
import {UserService} from '../../core/services/user.service';
import {CustomizeTabsComponent} from '../../shell/customize-tabs/customize-tabs.component';
import {AppButtonComponent} from '../../ui/button';
import {AppConfirmNameDialogComponent} from '../../ui/confirm-name-dialog';
import {AppIconComponent} from '../../ui/icon';
import {AppInputComponent} from '../../ui/input';
import {AppSelectComponent} from '../../ui/select';
import {AppSpinnerComponent} from '../../ui/spinner';
import {ContactFormComponent, ContactFormResult} from './contact-form.component';
import {ContactListComponent} from './contact-list.component';

@Component({
  selector: 'app-contacts-page',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    TranslocoPipe,
    ContactListComponent,
    ContactFormComponent,
    CustomizeTabsComponent,
    AppButtonComponent,
    AppConfirmNameDialogComponent,
    AppIconComponent,
    AppInputComponent,
    AppSelectComponent,
    AppSpinnerComponent,
  ],
  template: `
    <div class="page">
      <app-customize-tabs />

      <main class="page-content">
        <header class="header">
          <div class="header__text">
            <h2 class="header__title">{{ 'contacts.title' | transloco }}</h2>
            <p class="header__subtitle">{{ 'contacts.subtitle' | transloco }}</p>
          </div>
          <app-button variant="primary" [disabled]="showForm()" (clicked)="openNew()">
            {{ 'contacts.new' | transloco }}
          </app-button>
        </header>

        @if (saveError(); as err) {
          <div class="banner" role="alert">
            <app-icon size="sm">error</app-icon>
            <span class="banner__text">{{ err }}</span>
            <app-button variant="ghost" size="sm" (clicked)="saveError.set(null)">
              {{ 'common.dismiss' | transloco }}
            </app-button>
          </div>
        }

        <div class="filters">
          <app-input
            class="filters__search"
            [value]="q()"
            (valueChange)="q.set($event); reload()"
            [placeholder]="'contacts.search' | transloco"
            [ariaLabel]="'contacts.search' | transloco"
          />
          <app-select
            class="filters__select"
            [value]="channel()"
            (valueChange)="channel.set($event ?? ''); reload()"
            [fullWidth]="false"
            [ariaLabel]="'contacts.filter.channelAria' | transloco"
          >
            <option value="">{{ 'contacts.filter.allChannels' | transloco }}</option>
            <option value="email">email</option>
            <option value="whatsapp">whatsapp</option>
          </app-select>
          <app-select
            class="filters__select"
            [value]="projectId()"
            (valueChange)="projectId.set($event ?? ''); reload()"
            [fullWidth]="false"
            [ariaLabel]="'contacts.filter.projectAria' | transloco"
          >
            <option value="">{{ 'contacts.filter.allProjects' | transloco }}</option>
            @for (p of projects(); track p.id) {
              <option [value]="p.id">{{ p.name }}</option>
            }
          </app-select>
        </div>

        @if (showForm()) {
          <app-contact-form
            [contact]="editing()"
            [projects]="projects()"
            (saved)="save($event)"
            (cancelled)="closeForm()"
          />
        }

        @if (loading()) {
          <div class="loading"><app-spinner size="md" /></div>
        } @else {
          <app-contact-list
            [contacts]="contacts()"
            [currentUserId]="userService.currentUserId()"
            (edit)="openEdit($event)"
            (remove)="askDelete($event)"
          />
        }
      </main>
    </div>

    @if (deleting(); as target) {
      <app-confirm-name-dialog
        [open]="!!deleting()"
        [title]="'contacts.delete.title' | transloco"
        [message]="deleteMessage(target)"
        [requiredName]="target.display_name"
        [namePrompt]="'contacts.delete.namePrompt' | transloco"
        [confirmLabel]="'common.delete' | transloco"
        [cancelLabel]="'common.cancel' | transloco"
        (confirmed)="doDelete(target)"
        (dismissed)="deleting.set(null)"
      />
    }
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
      }

      .page {
        display: flex;
        flex-direction: column;
        height: 100%;
      }

      .page-content {
        flex: 1;
        min-height: 0;
        overflow-y: auto;
        padding: 8px 16px 24px;
        max-width: var(--content-max-width);
        width: 100%;
        margin: 0 auto;
      }

      .header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 16px;
        margin-bottom: 16px;
      }

      .header__title {
        margin: 0;
        font-size: 24px;
        font-weight: 600;
        letter-spacing: -0.02em;
      }

      .header__subtitle {
        margin: 2px 0 0;
        color: var(--text-muted);
        font-size: 0.85rem;
        max-width: 60ch;
      }

      .banner {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 8px 8px 8px 12px;
        margin-bottom: 12px;
        border: 1px solid var(--danger);
        border-radius: var(--radius-control);
        background: var(--danger-tint);
        color: var(--danger);
      }

      .banner__text {
        flex: 1;
        font-size: 0.85rem;
      }

      .filters {
        display: flex;
        gap: 8px;
        margin-bottom: 16px;
      }

      .filters__search {
        flex: 1;
        min-width: 0;
      }

      .filters__select {
        flex: 0 0 auto;
      }

      .loading {
        display: grid;
        place-items: center;
        padding: 48px 0;
      }

      @media (max-width: 640px) {
        .header {
          flex-direction: column;
          align-items: stretch;
        }

        .filters {
          flex-wrap: wrap;
        }

        .filters__search {
          flex: 1 0 100%;
        }
      }
    `,
  ],
})
export class ContactsPageComponent implements OnInit {
  private readonly api = inject(ContactsService);
  private readonly projectsApi = inject(ApiService);   // loadProjects: house projects listing
  private readonly transloco = inject(TranslocoService);
  // Not private: read directly from the template ([currentUserId] binding
  // below), mirroring sidebar.component.ts's `readonly userService`.
  readonly userService = inject(UserService);

  readonly contacts = signal<Contact[]>([]);
  readonly projects = signal<ContactProjectRef[]>([]);
  readonly showForm = signal(false);
  readonly editing = signal<Contact | null>(null);
  readonly deleting = signal<Contact | null>(null);
  readonly saveError = signal<string | null>(null);
  readonly loading = signal(true);
  readonly q = signal('');
  readonly channel = signal('');
  readonly projectId = signal('');

  ngOnInit(): void {
    this.reload();
    this.loadProjects();
  }

  reload(): void {
    this.api.list({q: this.q() || undefined, channel: this.channel() || undefined,
                   project_id: this.projectId() || undefined})
      .subscribe({
        next: rows => { this.contacts.set(rows); this.loading.set(false); },
        error: () => {
          // The list is the page: a failed load must not leave a spinner
          // spinning forever with no explanation.
          this.loading.set(false);
          this.saveError.set(this.transloco.translate('contacts.loadError'));
        },
      });
  }

  private loadProjects(): void {
    // Only the project names shown against a contact; a failure here leaves the
    // rest of the page usable, so it degrades to an empty list rather than
    // taking the view down with it. (`getProjects` no longer swallows errors
    // of its own — see ApiService.)
    this.projectsApi.getProjects().subscribe({
      next: rows => this.projects.set(rows),
      error: () => this.projects.set([]),
    });
  }

  openNew(): void { this.saveError.set(null); this.editing.set(null); this.showForm.set(true); }
  openEdit(c: Contact): void { this.saveError.set(null); this.editing.set(c); this.showForm.set(true); }
  closeForm(): void { this.saveError.set(null); this.showForm.set(false); this.editing.set(null); }

  deleteMessage(c: Contact): string {
    const names = c.projects.map(p => p.name).join(', ');
    return this.transloco.translate('contacts.delete.message', {projects: names || '—'});
  }

  askDelete(c: Contact): void { this.deleting.set(c); }

  doDelete(c: Contact): void {
    this.api.remove(c.id).subscribe({
      next: () => { this.deleting.set(null); this.reload(); },
      error: () => {
        // Without this handler a 403 (non-owner) or 404 (already deleted by
        // someone else) left `deleting()` non-null while the dialog had
        // already self-closed ([open]="!!deleting()" never changes value
        // again) — dead for the rest of the page's life. Clear it here so
        // the dialog can reopen, and surface the failure like save errors do.
        this.deleting.set(null);
        this.saveError.set(this.transloco.translate('contacts.deleteError'));
      },
    });
  }

  save(result: ContactFormResult): void {
    this.saveError.set(null);
    const existing = this.editing();
    const base$ = existing
      ? this.api.update(existing.id, {display_name: result.display_name, notes: result.notes})
      : this.api.create({display_name: result.display_name, notes: result.notes,
                         addresses: result.addresses});
    base$.pipe(switchMap(contact => {
      const ops = [];
      if (existing) {
        const beforeAddrs = existing.addresses;
        for (const a of beforeAddrs) {
          if (!result.addresses.some(r => r.id === a.id)) ops.push(this.api.removeAddress(a.id));
        }
        for (const r of result.addresses) {
          if (!r.id) { ops.push(this.api.addAddress(contact.id, r)); continue; }
          const before = beforeAddrs.find(a => a.id === r.id);
          if (before && (before.address !== r.address || before.is_primary !== r.is_primary)) {
            ops.push(this.api.patchAddress(r.id, {address: r.address, is_primary: r.is_primary}));
          }
        }
      }
      const beforeProjects = new Set((existing?.projects ?? []).map(p => p.id));
      for (const pid of result.projectIds) {
        if (!beforeProjects.has(pid)) ops.push(this.api.link(contact.id, pid));
      }
      for (const pid of beforeProjects) {
        if (!result.projectIds.includes(pid)) ops.push(this.api.unlink(contact.id, pid));
      }
      return ops.length ? forkJoin(ops) : of(null);
    })).subscribe({next: () => { this.closeForm(); this.reload(); },
                   error: () => {
                     // forkJoin fails fast: some ops may already have applied server-side.
                     // No rollback — surface the failure and reload to show real state;
                     // the (still-open) form keeps the user's input so they can retry.
                     this.saveError.set(this.transloco.translate('contacts.saveError'));
                     this.reload();
                   }});
  }
}
