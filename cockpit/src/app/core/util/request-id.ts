/**
 * Request ids that work on a plain-HTTP origin.
 *
 * `crypto.randomUUID()` exists only in secure contexts (HTTPS / localhost).
 * On a plain-HTTP origin a bare call throws, which breaks Stop, rewind and
 * config updates. The backend types `client_request_id` as a UUID, so the
 * fallback builds an RFC 4122 version-4 UUID from `crypto.getRandomValues`
 * (available in every context).
 */
export function newRequestId(): string {
  if (typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
  bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 10xx
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
