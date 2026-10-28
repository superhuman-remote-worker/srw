import {HttpErrorResponse} from '@angular/common/http';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {of, throwError} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';

import {AppToastService} from '../../ui/toast';
import {ApiService} from './api.service';
import {WorkspaceFileDownloadService} from './workspace-file-download.service';

describe('WorkspaceFileDownloadService', () => {
  let api: {downloadThreadFile: ReturnType<typeof vi.fn>};
  let toast: {danger: ReturnType<typeof vi.fn>};
  let clicked: {href: string; download: string}[];

  beforeEach(() => {
    api = {downloadThreadFile: vi.fn()};
    toast = {danger: vi.fn()};
    clicked = [];
    vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:file');
    vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => undefined);
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (
      this: HTMLAnchorElement,
    ) {
      clicked.push({href: this.href, download: this.download});
    });
    TestBed.configureTestingModule({
      providers: [
        {provide: ApiService, useValue: api},
        {provide: AppToastService, useValue: toast},
        {
          provide: TranslocoService,
          useValue: {translate: (key: string, p?: {path?: string}) => `${key}:${p?.path}`},
        },
      ],
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    TestBed.resetTestingModule();
  });

  it('saves the file under its own name', async () => {
    api.downloadThreadFile.mockReturnValue(of(new Blob(['<h1>hi</h1>'])));

    const saved = await TestBed.inject(WorkspaceFileDownloadService).download(
      'thread-1',
      'output/site/index.html',
    );

    expect(saved).toBe(true);
    expect(api.downloadThreadFile).toHaveBeenCalledWith('thread-1', 'output/site/index.html');
    expect(clicked).toEqual([{href: 'blob:file', download: 'index.html'}]);
    expect(toast.danger).not.toHaveBeenCalled();
  });

  it.each([
    [404, 'canvas.workspaceFile.notFound'],
    [413, 'canvas.workspaceFile.tooLarge'],
    [409, 'canvas.workspaceFile.unavailable'],
    [500, 'canvas.workspaceFile.downloadFailed'],
  ])('says why a %s could not be saved', async (status, key) => {
    api.downloadThreadFile.mockReturnValue(
      throwError(() => new HttpErrorResponse({status})),
    );

    const saved = await TestBed.inject(WorkspaceFileDownloadService).download(
      'thread-1',
      'output/report.md',
    );

    expect(saved).toBe(false);
    expect(clicked).toEqual([]);
    expect(toast.danger).toHaveBeenCalledWith(`${key}:output/report.md`);
  });
});
