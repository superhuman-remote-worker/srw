import {HttpErrorResponse} from '@angular/common/http';
import {TestBed} from '@angular/core/testing';
import {TranslocoService} from '@jsverse/transloco';
import {of, throwError} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';

import {AppToastService} from '../../ui/toast';
import {ApiService} from './api.service';
import {WorkspaceFileDownloadService, workspaceFileViewType} from './workspace-file-download.service';

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

  describe('open', () => {
    let tab: {location: {href: string}; opener: unknown; close: ReturnType<typeof vi.fn>};
    let openSpy: ReturnType<typeof vi.spyOn>;
    let blobTypes: string[];

    beforeEach(() => {
      tab = {location: {href: ''}, opener: window, close: vi.fn()};
      openSpy = vi.spyOn(window, 'open').mockReturnValue(tab as unknown as Window);
      blobTypes = [];
      (URL.createObjectURL as unknown as ReturnType<typeof vi.fn>).mockImplementation(
        (b: Blob) => {
          blobTypes.push(b.type);
          return 'blob:file';
        },
      );
    });

    it('opens a PDF in a tab at the cited page, the tab taken before the fetch', async () => {
      api.downloadThreadFile.mockImplementation(() => {
        // The tab must already exist: a click's popup allowance does not
        // survive a slow workspace read.
        expect(openSpy).toHaveBeenCalledWith('', '_blank');
        return of(new Blob(['%PDF-1.7'], {type: 'application/octet-stream'}));
      });

      const opened = await TestBed.inject(WorkspaceFileDownloadService).open(
        'thread-1',
        'uploads/credit.pdf',
        {page: 2},
      );

      expect(opened).toBe(true);
      expect(blobTypes).toEqual(['application/pdf']);
      expect(tab.location.href).toBe('blob:file#page=2');
      expect(tab.opener).toBeNull();
      expect(clicked).toEqual([]);
    });

    it('shows text as plain text', async () => {
      api.downloadThreadFile.mockReturnValue(of(new Blob(['# notes'])));

      await TestBed.inject(WorkspaceFileDownloadService).open('thread-1', 'notes/a.md', {page: 4});

      expect(blobTypes).toEqual(['text/plain;charset=utf-8']);
      expect(tab.location.href).toBe('blob:file');
    });

    it('downloads a type it may not render, without opening a tab', async () => {
      api.downloadThreadFile.mockReturnValue(of(new Blob(['<svg/>'])));

      await TestBed.inject(WorkspaceFileDownloadService).open('thread-1', 'out/chart.svg');

      expect(openSpy).not.toHaveBeenCalled();
      expect(clicked).toEqual([{href: 'blob:file', download: 'chart.svg'}]);
    });

    it('closes the tab and says why when the file cannot be read', async () => {
      api.downloadThreadFile.mockReturnValue(throwError(() => new HttpErrorResponse({status: 404})));

      const opened = await TestBed.inject(WorkspaceFileDownloadService).open(
        'thread-1',
        'uploads/gone.pdf',
      );

      expect(opened).toBe(false);
      expect(tab.close).toHaveBeenCalled();
      expect(toast.danger).toHaveBeenCalledWith('canvas.workspaceFile.notFound:uploads/gone.pdf');
    });
  });

  it('workspaceFileViewType allowlists by extension', () => {
    expect(workspaceFileViewType('a/B.PDF')).toBe('application/pdf');
    expect(workspaceFileViewType('scan.jpeg')).toBe('image/jpeg');
    expect(workspaceFileViewType('data.csv')).toBe('text/plain;charset=utf-8');
    expect(workspaceFileViewType('site/index.html')).toBeNull();
    expect(workspaceFileViewType('report.docx')).toBeNull();
    expect(workspaceFileViewType('.pdf')).toBeNull();
    expect(workspaceFileViewType('Makefile')).toBeNull();
  });
});
