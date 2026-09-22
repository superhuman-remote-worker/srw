import { describe, expect, it } from 'vitest';
import { queueParkReasonKey } from './queue-park-reason';

describe('queueParkReasonKey', () => {
  it('maps the five retryable park reasons to their copy', () => {
    expect(queueParkReasonKey('attach_failed')).toBe('chat.parked.reason.attachFailed');
    expect(queueParkReasonKey('shutdown_cancelled')).toBe('chat.parked.reason.shutdownCancelled');
    expect(queueParkReasonKey('completion_cas_failed')).toBe('chat.parked.reason.stopped');
    expect(queueParkReasonKey('reaper_max_attempts')).toBe('chat.parked.reason.stopped');
    // A deterministic failure that exhausted its retry budget: owner-retryable,
    // so it must never fall through to the "an administrator has to release
    // it" line that renders beside no Retry button.
    expect(queueParkReasonKey('retry_exhausted')).toBe('chat.parked.reason.retryExhausted');
  });

  it('falls back to the generic line for unknown or missing reasons', () => {
    expect(queueParkReasonKey('something_new')).toBe('chat.parked.reason.generic');
    expect(queueParkReasonKey(null)).toBe('chat.parked.reason.generic');
    expect(queueParkReasonKey(undefined)).toBe('chat.parked.reason.generic');
  });
});
