import { Component, inject, signal, effect, computed } from '@angular/core';
import { ApiService } from '../../core/services/api.service';
import { DataService } from '../../core/services/data.service';
import { AppIconButtonComponent } from '../../ui/icon-button';

interface RepoEntry {
  name: string;
  path: string;
  type: string; // "file" | "dir"
  size: number;
}

/**
 * Workspace Browser component for navigating job workspace files.
 *
 * Displays a file tree from the job's Gitea repository and renders
 * file contents with basic formatting. Registered as a cockpit panel.
 */
@Component({
  selector: 'app-workspace-browser',
  standalone: true,
  imports: [AppIconButtonComponent],
  template: `
    <div class="browser-container">
      <!-- Header -->
      <div class="header">
        <span class="title">Workspace</span>
        <app-icon-button
          variant="ghost"
          size="sm"
          class="refresh-btn"
          ariaLabel="Refresh"
          tooltip="Refresh"
          [disabled]="isLoadingTree()"
          (clicked)="loadDirectory(currentPath())"
        >
          ↻
        </app-icon-button>
      </div>

      @if (!currentJobId()) {
        <div class="empty-state">
          <span class="empty-hint">Select a job to browse workspace</span>
        </div>
      } @else {
        <div class="content-area">
          <!-- File Tree Panel -->
          <div class="tree-panel">
            <!-- Breadcrumb -->
            <div class="breadcrumb">
              <button class="crumb" (click)="loadDirectory('')" [class.active]="currentPath() === ''">
                /
              </button>
              @for (crumb of breadcrumbs(); track crumb.path) {
                <span class="crumb-sep">/</span>
                <button class="crumb" (click)="loadDirectory(crumb.path)" [class.active]="currentPath() === crumb.path">
                  {{ crumb.name }}
                </button>
              }
            </div>

            <!-- Loading -->
            @if (isLoadingTree()) {
              <div class="loading-inline">
                <div class="spinner-sm"></div>
                <span>Loading...</span>
              </div>
            }

            <!-- Directory Listing -->
            @if (!isLoadingTree() && entries().length > 0) {
              <div class="file-list">
                @if (currentPath()) {
                  <div class="file-entry dir-entry" (click)="navigateUp()">
                    <span class="entry-icon">..</span>
                    <span class="entry-name">(parent)</span>
                  </div>
                }
                @for (entry of sortedEntries(); track entry.path) {
                  <div
                    class="file-entry"
                    [class.dir-entry]="entry.type === 'dir'"
                    [class.selected]="selectedFile() === entry.path"
                    (click)="onEntryClick(entry)"
                  >
                    <span class="entry-icon">{{ entry.type === 'dir' ? '&#x1F4C1;' : getFileIcon(entry.name) }}</span>
                    <span class="entry-name">{{ entry.name }}</span>
                    @if (entry.type === 'file') {
                      <span class="entry-size">{{ formatSize(entry.size) }}</span>
                    }
                  </div>
                }
              </div>
            }

            @if (!isLoadingTree() && entries().length === 0 && currentJobId()) {
              <div class="empty-dir">
                <span>Empty directory or repository not available</span>
              </div>
            }
          </div>

          <!-- File Content Panel -->
          <div class="content-panel">
            @if (!selectedFile()) {
              <div class="no-file">
                <span>Select a file to view its contents</span>
              </div>
            } @else if (isLoadingFile()) {
              <div class="loading-inline">
                <div class="spinner-sm"></div>
                <span>Loading file...</span>
              </div>
            } @else if (fileContent() !== null) {
              <div class="file-header">
                <span class="file-path">{{ selectedFile() }}</span>
                <span class="file-size">{{ formatSize(fileSize()) }}</span>
              </div>
              @if (fileIsBinary()) {
                <div class="no-file">
                  <span>Binary file, no text preview</span>
                </div>
              } @else {
                <div class="file-content">
                  <pre class="code-block"><code>{{ fileContent() }}</code></pre>
                </div>
              }
            } @else {
              <div class="no-file">
                <span>Failed to load file</span>
              </div>
            }
          </div>
        </div>
      }
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
        overflow: hidden;
      }

      .browser-container {
        display: flex;
        flex-direction: column;
        height: 100%;
        background: var(--panel-bg, var(--panel-bg));
      }

      .header {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 10px 12px;
        background: var(--panel-header-bg);
        border-bottom: 1px solid var(--border-color, var(--surface-0));
        flex-shrink: 0;
      }

      .title {
        font-weight: 600;
        color: var(--text-primary, var(--text-primary));
      }

      .refresh-btn {
        margin-left: auto;
      }

      .empty-state {
        display: flex;
        align-items: center;
        justify-content: center;
        flex: 1;
        color: var(--text-muted);
        font-size: 12px;
      }

      .content-area {
        display: flex;
        flex: 1;
        overflow: hidden;
      }

      /* Tree Panel */
      .tree-panel {
        width: 240px;
        min-width: 180px;
        border-right: 1px solid var(--border-color, var(--surface-0));
        display: flex;
        flex-direction: column;
        overflow: hidden;
      }

      .breadcrumb {
        display: flex;
        align-items: center;
        gap: 2px;
        padding: 6px 8px;
        background: var(--surface-0, var(--surface-0));
        font-size: 11px;
        font-family: 'JetBrains Mono', monospace;
        overflow-x: auto;
        flex-shrink: 0;
        white-space: nowrap;
      }

      .crumb {
        padding: 2px 4px;
        border: none;
        border-radius: var(--radius-tag);
        background: transparent;
        color: var(--text-muted);
        font-size: 11px;
        font-family: inherit;
        cursor: pointer;
      }

      .crumb:hover {
        color: var(--text-primary, var(--text-primary));
        background: rgba(255, 255, 255, 0.05);
      }

      .crumb.active {
        color: var(--accent-color, var(--accent-color));
      }

      .crumb-sep {
        color: var(--text-muted);
        opacity: 0.5;
      }

      .file-list {
        flex: 1;
        overflow: auto;
      }

      .file-entry {
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 4px 8px;
        cursor: pointer;
        font-size: 12px;
        color: var(--text-primary, var(--text-primary));
        transition: background 0.1s ease;
      }

      .file-entry:hover {
        background: var(--surface-0, var(--surface-0));
      }

      .file-entry.selected {
        background: color-mix(in srgb, var(--accent-color) 20%, transparent);
      }

      .file-entry.dir-entry {
        color: var(--info);
      }

      .entry-icon {
        flex-shrink: 0;
        width: 16px;
        text-align: center;
        font-size: 12px;
      }

      .entry-name {
        flex: 1;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
      }

      .entry-size {
        flex-shrink: 0;
        font-size: 10px;
        font-family: 'JetBrains Mono', monospace;
        color: var(--text-muted);
      }

      .empty-dir {
        padding: 20px 12px;
        text-align: center;
        color: var(--text-muted);
        font-size: 11px;
      }

      /* Content Panel */
      .content-panel {
        flex: 1;
        display: flex;
        flex-direction: column;
        overflow: hidden;
      }

      .no-file {
        display: flex;
        align-items: center;
        justify-content: center;
        flex: 1;
        color: var(--text-muted);
        font-size: 12px;
      }

      .file-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 6px 12px;
        background: var(--surface-0, var(--surface-0));
        border-bottom: 1px solid var(--border-color, var(--surface-0));
        flex-shrink: 0;
      }

      .file-path {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        color: var(--text-primary, var(--text-primary));
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }

      .file-size {
        font-family: 'JetBrains Mono', monospace;
        font-size: 10px;
        color: var(--text-muted);
        flex-shrink: 0;
        margin-left: 12px;
      }

      .file-content {
        flex: 1;
        overflow: auto;
        padding: 8px;
      }

      .code-block {
        margin: 0;
        padding: 0;
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        line-height: 1.5;
        color: var(--text-primary, var(--text-primary));
        white-space: pre-wrap;
        word-wrap: break-word;
        tab-size: 2;
      }

      .code-block code {
        font-family: inherit;
      }

      /* Loading */
      .loading-inline {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 12px;
        color: var(--text-muted);
        font-size: 11px;
      }

      .spinner-sm {
        width: 14px;
        height: 14px;
        border: 2px solid var(--surface-0, var(--surface-0));
        border-top-color: var(--accent-color, var(--accent-color));
        border-radius: 50%;
        animation: spin 0.8s linear infinite;
      }

      @keyframes spin {
        to { transform: rotate(360deg); }
      }
    `,
  ],
})
export class WorkspaceBrowserComponent {
  private readonly api = inject(ApiService);
  private readonly data = inject(DataService);

  readonly currentJobId = this.data.currentJobId;
  readonly currentPath = signal('');
  readonly entries = signal<RepoEntry[]>([]);
  readonly selectedFile = signal<string | null>(null);
  readonly fileContent = signal<string | null>(null);
  readonly fileSize = signal(0);
  /** The selected file came back base64 (PDF, image, database). */
  readonly fileIsBinary = signal(false);
  readonly isLoadingTree = signal(false);
  readonly isLoadingFile = signal(false);

  readonly sortedEntries = computed(() => {
    const items = this.entries();
    // Directories first, then files, alphabetically within each group
    return [...items].sort((a, b) => {
      if (a.type !== b.type) return a.type === 'dir' ? -1 : 1;
      return a.name.localeCompare(b.name);
    });
  });

  readonly breadcrumbs = computed(() => {
    const path = this.currentPath();
    if (!path) return [];

    const parts = path.split('/');
    return parts.map((name, i) => ({
      name,
      path: parts.slice(0, i + 1).join('/'),
    }));
  });

  constructor() {
    // React to job selection changes
    effect(() => {
      const jobId = this.currentJobId();
      if (jobId) {
        this.currentPath.set('');
        this.selectedFile.set(null);
        this.fileContent.set(null);
        this.loadDirectory('');
      } else {
        this.entries.set([]);
        this.selectedFile.set(null);
        this.fileContent.set(null);
      }
    });
  }

  loadDirectory(path: string): void {
    const jobId = this.currentJobId();
    if (!jobId) return;

    this.currentPath.set(path);
    this.isLoadingTree.set(true);

    this.api.listRepoContents(jobId, path).subscribe((entries) => {
      this.entries.set(entries);
      this.isLoadingTree.set(false);
    });
  }

  onEntryClick(entry: RepoEntry): void {
    if (entry.type === 'dir') {
      this.loadDirectory(entry.path);
    } else {
      this.loadFile(entry.path);
    }
  }

  navigateUp(): void {
    const path = this.currentPath();
    if (!path) return;

    const parts = path.split('/');
    parts.pop();
    this.loadDirectory(parts.join('/'));
  }

  loadFile(path: string): void {
    const jobId = this.currentJobId();
    if (!jobId) return;

    this.selectedFile.set(path);
    this.isLoadingFile.set(true);
    this.fileContent.set(null);
    this.fileIsBinary.set(false);

    this.api.getRepoFile(jobId, path).subscribe((result) => {
      if (result) {
        const binary = result.encoding === 'base64';
        this.fileIsBinary.set(binary);
        this.fileContent.set(binary ? '' : result.content);
        this.fileSize.set(result.size);
      } else {
        this.fileContent.set(null);
        this.fileSize.set(0);
      }
      this.isLoadingFile.set(false);
    });
  }

  getFileIcon(name: string): string {
    const ext = name.split('.').pop()?.toLowerCase();
    switch (ext) {
      case 'md':
        return '\u{1F4DD}';
      case 'yaml':
      case 'yml':
        return '\u{2699}';
      case 'json':
        return '\u{1F4CB}';
      case 'py':
        return '\u{1F40D}';
      case 'txt':
        return '\u{1F4C4}';
      case 'log':
        return '\u{1F4DC}';
      default:
        return '\u{1F4C4}';
    }
  }

  formatSize(bytes: number): string {
    if (bytes === 0) return '0 B';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
}
