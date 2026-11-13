import {Component, computed, inject, OnInit, signal} from '@angular/core';
import {ActivatedRoute, Router} from '@angular/router';
import {TranslocoPipe, TranslocoService} from '@jsverse/transloco';
import {ApiService} from '../../core/services/api.service';
import type {SkillCreateRequest, SkillUpdateRequest} from '../../core/models/api.model';
import {AppButtonComponent} from '../../ui/button';
import {AppInputComponent} from '../../ui/input';
import {AppTextareaComponent} from '../../ui/textarea';
import {AppFormFieldComponent} from '../../ui/form-field';
import {AppIconComponent} from '../../ui/icon';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {
  filesToRecord,
  hasSkillMd,
  NEW_SKILL_TEMPLATE,
  recordToFiles,
  SkillFile,
} from './skill-editor.util';

@Component({
  selector: 'app-skill-editor',
  standalone: true,
  imports: [
    TranslocoPipe,
    AppButtonComponent,
    AppInputComponent,
    AppTextareaComponent,
    AppFormFieldComponent,
    AppIconComponent,
    SidebarToggleComponent,
  ],
  template: `
    <div class="editor">
      <header class="head">
        <!-- Full-page route (no page shell), so carry the mobile sidebar toggle
             here too — otherwise the off-canvas nav is unreachable on mobile.
             Renders nothing on desktop (sidebar always expanded). -->
        <app-sidebar-toggle />
        <div class="head-row">
          <h1>{{ (isEdit() ? 'skills.editTitle' : 'skills.newTitle') | transloco }}</h1>
          <div class="head-actions">
            <app-button variant="secondary" (clicked)="cancel()">
              {{ 'skills.cancel' | transloco }}
            </app-button>
            <app-button variant="primary" [disabled]="saving()" (clicked)="save()">
              {{ 'skills.save' | transloco }}
            </app-button>
          </div>
        </div>
      </header>

      <section class="meta">
        <app-form-field [label]="'skills.displayName' | transloco">
          <app-input [value]="form.display_name" (valueChange)="form.display_name = $event" />
        </app-form-field>
        <app-form-field [label]="'skills.icon' | transloco">
          <app-input [value]="form.icon" (valueChange)="form.icon = $event" placeholder="extension" />
        </app-form-field>
        <app-form-field [label]="'skills.color' | transloco">
          <app-input [value]="form.color" (valueChange)="form.color = $event" placeholder="#6B7280" />
        </app-form-field>
        <app-form-field [label]="'skills.tags' | transloco">
          <app-input [value]="form.tags" (valueChange)="form.tags = $event" />
        </app-form-field>
      </section>

      <section class="files">
        <p class="hint">{{ 'skills.filesHint' | transloco }}</p>
        <div class="file-tabs">
          @for (f of files(); track f.path; let i = $index) {
            <button
              type="button"
              class="file-tab"
              [class.active]="i === selected()"
              (click)="selected.set(i)"
            >
              {{ f.path }}
            </button>
          }
          <app-button variant="secondary" (clicked)="addFile()">
            + {{ 'skills.addFile' | transloco }}
          </app-button>
        </div>
        <app-textarea
          [value]="currentContent()"
          [rows]="22"
          (valueChange)="setContent($event)"
        />
        @if (canRemoveCurrent()) {
          <div class="file-remove">
            <app-button variant="danger" (clicked)="removeCurrent()">
              <app-icon size="sm">delete</app-icon>
              {{ 'skills.removeFile' | transloco }}
            </app-button>
          </div>
        }
      </section>

      @if (errorMessage()) {
        <div class="banner err">{{ errorMessage() }}</div>
      }
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
        overflow-y: auto;
      }
      .editor {
        padding: 1rem 1.5rem;
        max-width: 1100px;
        margin: 0 auto;
      }
      .head {
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        gap: 0.5rem;
        margin-bottom: 1rem;
      }
      .head-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        width: 100%;
      }
      .head h1 {
        margin: 0;
      }
      .head-actions {
        display: flex;
        gap: 0.5rem;
      }
      .meta {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.75rem 1rem;
        margin-bottom: 1.25rem;
      }
      .hint {
        color: var(--text-muted);
        margin: 0 0 0.5rem;
      }
      .file-tabs {
        display: flex;
        flex-wrap: wrap;
        gap: 0.25rem;
        align-items: center;
        margin-bottom: 0.5rem;
      }
      .file-tab {
        padding: 0.25rem 0.6rem;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-control);
        background: var(--surface-2, transparent);
        color: var(--text-primary);
        cursor: pointer;
        font-family: var(--font-mono, monospace);
        font-size: 0.85rem;
      }
      .file-tab.active {
        border-color: var(--accent-color);
        background: color-mix(in srgb, var(--accent-color) 12%, transparent);
      }
      .file-remove {
        margin-top: 0.5rem;
      }
      .banner {
        margin-top: 1rem;
        padding: 0.5rem 0.75rem;
        border-radius: var(--radius-control);
      }
      .banner.err {
        background: var(--danger-tint);
        color: var(--danger);
      }
    `,
  ],
})
export class SkillEditorComponent implements OnInit {
  private api = inject(ApiService);
  private router = inject(Router);
  private route = inject(ActivatedRoute);
  private transloco = inject(TranslocoService);

  editingId = signal<string | null>(null);
  saving = signal(false);
  errorMessage = signal('');
  files = signal<SkillFile[]>([{path: 'SKILL.md', content: NEW_SKILL_TEMPLATE}]);
  selected = signal(0);

  form = {display_name: '', icon: 'extension', color: '#6B7280', tags: ''};

  isEdit = computed(() => this.editingId() !== null);
  currentContent = computed(() => this.files()[this.selected()]?.content ?? '');
  canRemoveCurrent = computed(() => this.files()[this.selected()]?.path !== 'SKILL.md');

  ngOnInit(): void {
    const id = this.route.snapshot.paramMap.get('id');
    if (!id) return;
    this.editingId.set(id);
    this.api.getSkillDetail(id).subscribe((d) => {
      if (!d) return;
      this.form.display_name = d.display_name ?? '';
      this.form.icon = d.icon ?? 'extension';
      this.form.color = d.color ?? '#6B7280';
      this.form.tags = (d.tags ?? []).join(', ');
      const arr = recordToFiles(d.files ?? {});
      this.files.set(arr.length ? arr : [{path: 'SKILL.md', content: NEW_SKILL_TEMPLATE}]);
      this.selected.set(0);
    });
  }

  setContent(v: string): void {
    const i = this.selected();
    this.files.update((fs) => fs.map((f, idx) => (idx === i ? {...f, content: v} : f)));
  }

  addFile(): void {
    const path = prompt(this.transloco.translate('skills.newFilePath'), 'references/guide.md');
    if (!path || this.files().some((f) => f.path === path)) return;
    this.files.update((fs) => [...fs, {path, content: ''}]);
    this.selected.set(this.files().length - 1);
  }

  removeCurrent(): void {
    const i = this.selected();
    if (this.files()[i]?.path === 'SKILL.md') return;
    this.files.update((fs) => fs.filter((_, idx) => idx !== i));
    this.selected.set(0);
  }

  save(): void {
    this.errorMessage.set('');
    if (!hasSkillMd(this.files())) {
      this.errorMessage.set(this.transloco.translate('skills.needSkillMd'));
      return;
    }
    const tags = this.form.tags
      .split(',')
      .map((t) => t.trim())
      .filter(Boolean);
    const files = filesToRecord(this.files());
    this.saving.set(true);
    const id = this.editingId();
    const obs = id
      ? this.api.updateSkill(id, {
          files,
          display_name: this.form.display_name,
          icon: this.form.icon,
          color: this.form.color,
          tags,
        } as SkillUpdateRequest)
      : this.api.createSkill({
          files,
          display_name: this.form.display_name || null,
          icon: this.form.icon,
          color: this.form.color,
          tags,
        } as SkillCreateRequest);
    obs.subscribe({
      next: () => this.router.navigate(['/skills']),
      error: (err) => {
        this.saving.set(false);
        const d = (err as {error?: {detail?: unknown}})?.error?.detail;
        this.errorMessage.set(
          typeof d === 'string' ? d : this.transloco.translate('skills.saveFailed'),
        );
      },
    });
  }

  cancel(): void {
    this.router.navigate(['/skills']);
  }
}
