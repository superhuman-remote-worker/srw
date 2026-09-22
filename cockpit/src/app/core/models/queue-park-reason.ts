/**
 * i18n key for a run_queue park reason. Shared by the chat's parked bubble and
 * the admin capacity page; lives beside the models so the chat chunk never
 * imports an admin component.
 */
export function queueParkReasonKey(reason: string | null | undefined): string {
  switch (reason) {
    case 'attach_failed':
      return 'chat.parked.reason.attachFailed';
    case 'shutdown_cancelled':
      return 'chat.parked.reason.shutdownCancelled';
    case 'completion_cas_failed':
    case 'reaper_max_attempts':
      return 'chat.parked.reason.stopped';
    case 'retry_exhausted':
      return 'chat.parked.reason.retryExhausted';
    default:
      return 'chat.parked.reason.generic';
  }
}
