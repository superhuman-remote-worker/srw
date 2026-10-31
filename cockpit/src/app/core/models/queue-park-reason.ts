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
    case 'model_unavailable':
      return 'chat.parked.reason.modelUnavailable';
    // Shown only when the Continue / Stop card cannot list the cut-off calls
    // (an ambiguous history or a hold): then it is an operator's repair.
    case 'loop_died_after_tool_effect':
      return 'chat.parked.reason.toolResultUnknown';
    default:
      return 'chat.parked.reason.generic';
  }
}
