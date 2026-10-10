import { describe, expect, it } from 'vitest';
import de from '../../../assets/i18n/de-DE.json';
import en from '../../../assets/i18n/en.json';
import {
  CHECKOUT_CAPTURE_STATES,
  checkoutCaptureState,
  JOB_OUTBOX_FAILURE_REASONS,
  JOB_OUTBOX_UNSHARED_REASONS,
  jobCloudAction,
  jobOutboxRecord,
  noProjectFolderChanges,
} from './job-cloud';

describe('a job outbox record', () => {
  it('reads a delivered copy, shared or not and why', () => {
    expect(jobOutboxRecord({ job_outbox: { state: 'delivered', files: 4, shared: true } })).toEqual({
      state: 'delivered',
      files: 4,
      shared: true,
      unshared: null,
      reason: null,
    });
    // JSONB may arrive as text.
    expect(
      jobOutboxRecord(JSON.stringify({ job_outbox: { state: 'delivered', files: 1, shared: false, unshared: 'no_account' } })),
    ).toMatchObject({ shared: false, unshared: 'no_account' });
    // An unknown reason reads as a failed share, never raw.
    expect(
      jobOutboxRecord({ job_outbox: { state: 'delivered', files: 1, shared: false, unshared: 'later' } })?.unshared,
    ).toBe('share_failed');
  });

  it('reads a failed copy by its closed reason', () => {
    expect(jobOutboxRecord({ job_outbox: { state: 'failed', reason: 'file_missing', detail: 'x' } })).toMatchObject({
      state: 'failed',
      reason: 'file_missing',
    });
    expect(jobOutboxRecord({ job_outbox: { state: 'failed', reason: 'nope' } })?.reason).toBe('copy_failed');
  });

  it('is null without a record', () => {
    expect(jobOutboxRecord(null)).toBeNull();
    expect(jobOutboxRecord({})).toBeNull();
    expect(jobOutboxRecord({ job_outbox: { state: 'queued' } })).toBeNull();
  });

  it('has words for every reason in both languages', () => {
    for (const lang of [en, de]) {
      const outbox = (lang as any).jobReview.outbox;
      for (const reason of JOB_OUTBOX_UNSHARED_REASONS) expect(outbox.unshared[reason]).toBeTruthy();
      for (const reason of JOB_OUTBOX_FAILURE_REASONS) expect(outbox.reason[reason]).toBeTruthy();
      expect((lang as any).toasts.jobs.exportedRateLimited).toBeTruthy();
      expect((lang as any).toasts.jobs.exportedShareFailed).toBeTruthy();
      expect((lang as any).jobs.action.resyncCloudFolder).toBeTruthy();
    }
  });
});

describe("a job's project folder copy", () => {
  it('reads what the completion capture found', () => {
    for (const state of CHECKOUT_CAPTURE_STATES) {
      expect(checkoutCaptureState({ cloud_checkout: { access: 'reviewed_write_back', capture: { state } } })).toBe(state);
    }
    expect(checkoutCaptureState({ cloud_checkout: { access: 'read_only' } })).toBeNull();
    expect(checkoutCaptureState({ cloud_checkout: { capture: { state: 'odd' } } })).toBeNull();
    expect(checkoutCaptureState(undefined)).toBeNull();
  });

  it('has nothing to review when it goes through review without a diff', () => {
    expect(noProjectFolderChanges({ cloud_review_mode: 'diff', diff_status: null })).toBe(true);
    expect(noProjectFolderChanges({ cloud_review_mode: 'diff', diff_status: 'pending' })).toBe(false);
    expect(noProjectFolderChanges({ cloud_review_mode: 'open_folder', diff_status: null })).toBe(false);
  });
});

describe('jobCloudAction', () => {
  it('re-syncs once exported, and degrades without a URL', () => {
    const job = { status: 'completed', exported_at: '2026-10-10T00:00:00Z' };
    expect(jobCloudAction({ ...job, exported_folder_url: 'https://cloud/f' })).toBe('open');
    expect(jobCloudAction(job)).toBe('exported');
    expect(jobCloudAction({ status: 'pending_review', diff_status: 'pending' })).toBe('none');
  });
});
