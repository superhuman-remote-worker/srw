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
  title: string;
  expired: boolean;
  expiredDate: string;
}

/** Card-head badges for an endpoint's stored pin. */
export function pinBadge(pin: LlmEndpointTlsPin | null | undefined): PinBadge {
  if (!pin) return {show: false, title: '', expired: false, expiredDate: ''};
  return {
    show: true,
    title: pin.sha256,
    expired: pin.expired,
    expiredDate: (pin.not_after ?? '').slice(0, 10),
  };
}

export interface TlsView {
  show: boolean;
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
  if (!tls || (tls.certificate === null && tls.reason === null)) return HIDDEN;
  const trust = tls.can_trust && tls.certificate
    ? tls.reason === 'certificate_changed'
      ? 'trustNew'
      : 'trust'
    : null;
  return {
    show: true,
    reasonKey: tlsReasonKey(tls.reason),
    showCertificate: tls.certificate !== null,
    trust,
    sideBySide: tls.reason === 'certificate_changed' && tls.pin !== null && tls.certificate !== null,
    verifiesNormally: tls.publicly_valid && tls.pin !== null,
    showRemove: tls.pin !== null && !tls.can_trust,
  };
}
