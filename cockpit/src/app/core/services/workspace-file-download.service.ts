import {HttpErrorResponse} from '@angular/common/http';
import {Injectable, inject} from '@angular/core';
import {TranslocoService} from '@jsverse/transloco';
import {firstValueFrom} from 'rxjs';

import {snapshotViewType} from '../markdown/citation-snapshot';
import {AppToastService} from '../../ui/toast';
import {ApiService} from './api.service';

/**
 * The type a workspace file is shown as in a tab, from its extension, or null
 * when it must be downloaded instead. The server sends every workspace file
 * as an inert octet-stream attachment, so the type is decided here, through
 * the same allowlist as a cited snapshot (`snapshotViewType`): a PDF or image
 * opens as itself, text opens as plain text, and anything else (HTML, SVG,
 * Office files) is saved rather than rendered on the Cockpit origin.
 */
const EXTENSION_TYPES: Readonly<Record<string, string>> = {
  pdf: 'application/pdf',
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  gif: 'image/gif',
  webp: 'image/webp',
  txt: 'text/plain',
  md: 'text/markdown',
  csv: 'text/csv',
  json: 'text/plain',
  yaml: 'text/plain',
  yml: 'text/plain',
  log: 'text/plain',
};

export function workspaceFileViewType(path: string): string | null {
  const name = path.split('/').pop() ?? '';
  const dot = name.lastIndexOf('.');
  const type = dot > 0 ? EXTENSION_TYPES[name.slice(dot + 1).toLowerCase()] : undefined;
  return type ? snapshotViewType(type) : null;
}

/**
 * Saves one file of a Session's workspace to the user's device, or opens it
 * in a tab.
 *
 * The one way to get a file out of a Session without a cloud folder: a file
 * link the agent writes in chat, the Canvas toolbar, or a cited document.
 * Works on a phone; a container or VM workspace has to be running.
 */
@Injectable({providedIn: 'root'})
export class WorkspaceFileDownloadService {
  private readonly api = inject(ApiService);
  private readonly toast = inject(AppToastService);
  private readonly transloco = inject(TranslocoService);

  async download(threadId: string, path: string): Promise<boolean> {
    try {
      const blob = await firstValueFrom(this.api.downloadThreadFile(threadId, path));
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = path.split('/').pop() || path;
      link.click();
      // Let the browser start reading it, then release the object URL.
      setTimeout(() => URL.revokeObjectURL(url), 60_000);
      return true;
    } catch (error) {
      this.toast.danger(this.transloco.translate(downloadErrorKey(error), {path}));
      return false;
    }
  }

  /**
   * Open the file in a new tab when it can be shown safely (a PDF at `page`
   * when one is given), else save it like `download`.
   *
   * Call it straight from a click: the tab is opened before the file is
   * fetched, while the click still counts as a user gesture, so a slow
   * workspace read cannot get it popup-blocked.
   */
  async open(threadId: string, path: string, options: {page?: number} = {}): Promise<boolean> {
    const viewType = workspaceFileViewType(path);
    if (!viewType) return this.download(threadId, path);
    const tab = window.open('', '_blank');
    if (tab) tab.opener = null;
    try {
      const blob = await firstValueFrom(this.api.downloadThreadFile(threadId, path));
      const url = URL.createObjectURL(new Blob([blob], {type: viewType}));
      const target =
        viewType === 'application/pdf' && options.page ? `${url}#page=${options.page}` : url;
      if (tab) {
        tab.location.href = target;
      } else {
        window.open(target, '_blank');
      }
      // Let the tab read it, then release the object URL.
      setTimeout(() => URL.revokeObjectURL(url), 60_000);
      return true;
    } catch (error) {
      tab?.close();
      this.toast.danger(this.transloco.translate(downloadErrorKey(error), {path}));
      return false;
    }
  }
}

function downloadErrorKey(error: unknown): string {
  const status = error instanceof HttpErrorResponse ? error.status : 0;
  if (status === 404) return 'canvas.workspaceFile.notFound';
  if (status === 413) return 'canvas.workspaceFile.tooLarge';
  // The container or VM is asleep, or the workspace is being replaced.
  if (status === 409) return 'canvas.workspaceFile.unavailable';
  return 'canvas.workspaceFile.downloadFailed';
}
