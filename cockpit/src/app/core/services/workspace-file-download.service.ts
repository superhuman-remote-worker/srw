import {HttpErrorResponse} from '@angular/common/http';
import {Injectable, inject} from '@angular/core';
import {TranslocoService} from '@jsverse/transloco';
import {firstValueFrom} from 'rxjs';

import {AppToastService} from '../../ui/toast';
import {ApiService} from './api.service';

/**
 * Saves one file of a Session's workspace to the user's device.
 *
 * The one way to get a file out of a Session without a cloud folder: a file
 * link the agent writes in chat, or the Canvas toolbar. Works on a phone; a
 * container or VM workspace has to be running.
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
}

function downloadErrorKey(error: unknown): string {
  const status = error instanceof HttpErrorResponse ? error.status : 0;
  if (status === 404) return 'canvas.workspaceFile.notFound';
  if (status === 413) return 'canvas.workspaceFile.tooLarge';
  // The container or VM is asleep, or the workspace is being replaced.
  if (status === 409) return 'canvas.workspaceFile.unavailable';
  return 'canvas.workspaceFile.downloadFailed';
}
