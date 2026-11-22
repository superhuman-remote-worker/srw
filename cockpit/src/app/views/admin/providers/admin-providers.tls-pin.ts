import {
  LlmEndpointTlsPin,
  LlmEndpointTlsReason,
  LlmEndpointTlsReport,
} from '../../../core/models/api.model';

const REASON_KEYS: Record<string, string> = {
  self_signed: 'selfSigned',
  unknown_issuer: 'unknownIssuer',
  address_mismatch: 'addressMismatch',
  expired: 'expired',
  not_yet_valid: 'notYetValid',
  certificate_changed: 'certificateChanged',
};

/** i18n key of the reason sentence; null for `other` (show the server's message) and null. */
export function tlsReasonKey(reason: LlmEndpointTlsReason | null | undefined): string | null {
  const k = reason ? REASON_KEYS[reason] : undefined;
  return k ? `admin.providers.endpoints.tls.reason.${k}` : null;
}

export interface PinBadge {
  show: boolean;
  /** False on an http:// URL: an edit keeps the pin, but it applies to https only. */
  active: boolean;
  title: string;
  expired: boolean;
  expiredDate: string;
}

/** Card-head badges for an endpoint's stored pin. */
export function pinBadge(
  pin: LlmEndpointTlsPin | null | undefined,
  baseUrl?: string | null,
): PinBadge {
  if (!pin) return {show: false, active: false, title: '', expired: false, expiredDate: ''};
  const active = !baseUrl || /^https:\/\//i.test(baseUrl.trim());
  return {
    show: true,
    active,
    title: pin.sha256,
    expired: active && pin.expired,
    expiredDate: dateOnly(pin.not_after),
  };
}

/** `YYYY-MM-DD` of an ISO timestamp; '' when absent. */
export function dateOnly(iso: string | null | undefined): string {
  return (iso ?? '').slice(0, 10);
}

/**
 * Text of an HTTP error's `detail`: a string as is; a FastAPI 422 list of
 * `{msg}` joined; anything else null (the caller shows a generic message).
 */
export function errorDetailText(err: unknown): string | null {
  const detail = (err as {error?: {detail?: unknown}} | null)?.error?.detail;
  if (typeof detail === 'string' && detail) return detail;
  if (Array.isArray(detail)) {
    const msgs = detail
      .map((d) => (typeof d === 'string' ? d : (d as {msg?: unknown} | null)?.msg))
      .filter((m): m is string => typeof m === 'string' && m.length > 0);
    if (msgs.length) return msgs.join('; ');
  }
  return null;
}

export interface TlsView {
  show: boolean;
  /** The server's own `message`: only for reason `other`, and only when there is one. */
  showMessage: boolean;
  /** The server presents the pinned certificate ("Pinned, matches"). */
  pinMatches: boolean;
  /** Why Trust is not offered although the certificate would be trustable. */
  refusal: string | null;
  /** Matching pin whose certificate has expired: date to print (still trusted). */
  expiredPinDate: string | null;
  reasonKey: string | null;
  showCertificate: boolean;
  /** Which Trust button to offer; the Catalog row never offers one. */
  trust: 'trust' | 'trustNew' | null;
  /** Certificate changed: old (pin) and new fingerprints side by side. */
  sideBySide: boolean;
  /** The server now verifies normally although a pin is stored. */
  verifiesNormally: boolean;
  showRemove: boolean;
}

const HIDDEN: TlsView = {
  show: false,
  showMessage: false,
  pinMatches: false,
  refusal: null,
  expiredPinDate: null,
  reasonKey: null,
  showCertificate: false,
  trust: null,
  sideBySide: false,
  verifiesNormally: false,
  showRemove: false,
};

/**
 * What the Test certificate block shows. Driven by `certificate` / `reason` /
 * `can_trust`, never `verified`: an unreachable host has `verified: false`
 * with no certificate and no reason, and must not read as a certificate fault.
 */
export function tlsView(tls: LlmEndpointTlsReport | null | undefined): TlsView {
  if (!tls) return HIDDEN;
  const expiredPinDate =
    tls.pin_matches && tls.pin?.expired ? dateOnly(tls.pin.not_after) : null;
  if (tls.certificate === null && tls.reason === null && expiredPinDate === null) return HIDDEN;
  const trust = tls.can_trust && tls.certificate
    ? tls.reason === 'certificate_changed'
      ? 'trustNew'
      : 'trust'
    : null;
  return {
    show: true,
    showMessage: tls.reason === 'other' && !!tls.message,
    pinMatches: tls.pin_matches === true,
    refusal: tls.refusal || null,
    expiredPinDate,
    reasonKey: tlsReasonKey(tls.reason),
    showCertificate: tls.certificate !== null,
    trust,
    sideBySide: tls.reason === 'certificate_changed' && tls.pin !== null && tls.certificate !== null,
    verifiesNormally: tls.publicly_valid && tls.pin !== null,
    showRemove: tls.pin !== null && !tls.can_trust,
  };
}
