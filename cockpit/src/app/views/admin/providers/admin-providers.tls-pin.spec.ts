import {Injector, runInInjectionContext, signal} from '@angular/core';
import {TranslocoService} from '@jsverse/transloco';
import {beforeEach, describe, expect, it, vi} from 'vitest';
import {of, throwError} from 'rxjs';
import {AdminModelsService} from '../../../core/services/admin-models.service';
import {AdminProvidersService} from '../../../core/services/admin-providers.service';
import {AdminModelsCoordinatorService} from '../models/admin-models-coordinator.service';
import {AdminProvidersComponent} from './admin-providers.component';
import {
  LlmEndpoint,
  LlmEndpointCertificate,
  LlmEndpointTlsPin,
  LlmEndpointTlsReport,
} from '../../../core/models/api.model';
import {pinBadge, tlsReasonKey, tlsView} from './admin-providers.tls-pin';

const FP_OLD = 'AA:BB:CC';
const FP_NEW = '11:22:33';

function cert(over: Partial<LlmEndpointCertificate> = {}): LlmEndpointCertificate {
  return {
    sha256: FP_NEW,
    subject: 'CN=gpu.lan',
    issuer: 'CN=gpu.lan',
    not_before: '2026-01-01T00:00:00Z',
    not_after: '2027-01-01T00:00:00Z',
    expired: false,
    san: ['gpu.lan'],
    ...over,
  };
}

function pin(over: Partial<LlmEndpointTlsPin> = {}): LlmEndpointTlsPin {
  return {
    sha256: FP_OLD,
    subject: 'CN=gpu.lan',
    issuer: 'CN=gpu.lan',
    not_after: '2027-01-01T00:00:00Z',
    expired: false,
    trusted_at: '2026-11-01T00:00:00Z',
    ...over,
  };
}

function tls(over: Partial<LlmEndpointTlsReport> = {}): LlmEndpointTlsReport {
  return {
    verified: false,
    reason: 'self_signed',
    message: 'self signed certificate',
    certificate: cert(),
    pin: null,
    pin_matches: false,
    publicly_valid: false,
    can_trust: true,
    ...over,
  };
}

describe('pinBadge', () => {
  it('is hidden without a pin', () => {
    expect(pinBadge(null).show).toBe(false);
    expect(pinBadge(undefined).show).toBe(false);
  });

  it('shows the pinned badge with the fingerprint for a live pin', () => {
    const b = pinBadge(pin());
    expect(b.show).toBe(true);
    expect(b.title).toContain(FP_OLD);
    expect(b.expired).toBe(false);
  });

  it('flags an expired pin and carries its date', () => {
    const b = pinBadge(pin({expired: true, not_after: '2026-10-01T00:00:00Z'}));
    expect(b.expired).toBe(true);
    expect(b.expiredDate).toBe('2026-10-01');
  });
});

describe('tlsReasonKey', () => {
  it.each([
    ['self_signed', 'selfSigned'],
    ['unknown_issuer', 'unknownIssuer'],
    ['address_mismatch', 'addressMismatch'],
    ['expired', 'expired'],
    ['not_yet_valid', 'notYetValid'],
    ['certificate_changed', 'certificateChanged'],
  ] as const)('%s -> %s', (reason, key) => {
    expect(tlsReasonKey(reason)).toBe(`admin.providers.endpoints.tls.reason.${key}`);
  });

  it('other and null have no key (the server message is shown)', () => {
    expect(tlsReasonKey('other')).toBeNull();
    expect(tlsReasonKey(null)).toBeNull();
  });
});

describe('tlsView', () => {
  it('renders nothing for null', () => {
    expect(tlsView(null).show).toBe(false);
  });

  it('renders nothing for an unreachable host (no certificate, no reason)', () => {
    const v = tlsView(tls({verified: false, reason: null, certificate: null, can_trust: false}));
    expect(v.show).toBe(false);
  });

  it('untrusted certificate: reason, certificate and Trust', () => {
    const v = tlsView(tls());
    expect(v.show).toBe(true);
    expect(v.reasonKey).toBe('admin.providers.endpoints.tls.reason.selfSigned');
    expect(v.showCertificate).toBe(true);
    expect(v.trust).toBe('trust');
    expect(v.sideBySide).toBe(false);
    expect(v.showRemove).toBe(false);
  });

  it('certificate changed: side by side and "trust the new one"', () => {
    const v = tlsView(
      tls({reason: 'certificate_changed', pin: pin(), pin_matches: false}),
    );
    expect(v.trust).toBe('trustNew');
    expect(v.sideBySide).toBe(true);
  });

  it('publicly valid with a pin: verifies-normally line and only Remove', () => {
    const v = tlsView(
      tls({
        reason: 'certificate_changed',
        pin: pin(),
        publicly_valid: true,
        can_trust: false,
      }),
    );
    expect(v.verifiesNormally).toBe(true);
    expect(v.trust).toBeNull();
    expect(v.showRemove).toBe(true);
  });

  it('matching pin: no reason, no Trust, Remove only', () => {
    const v = tlsView(tls({verified: true, reason: null, pin: pin(), pin_matches: true, can_trust: false}));
    expect(v.trust).toBeNull();
    expect(v.showRemove).toBe(true);
  });

  it('unreadable pin (other + message, no certificate) still renders the message', () => {
    const v = tlsView(
      tls({reason: 'other', message: 'stored pin unreadable', certificate: null, can_trust: false, pin: pin()}),
    );
    expect(v.show).toBe(true);
    expect(v.reasonKey).toBeNull();
    expect(v.showCertificate).toBe(false);
    expect(v.showRemove).toBe(true);
  });
});

// ---- component wiring ------------------------------------------------
const trust = vi.fn();
const removePin = vi.fn();
const confirmFn = vi.fn();
const endpoints = signal<LlmEndpoint[]>([]);

function setup(): AdminProvidersComponent {
  const injector = Injector.create({
    providers: [
      {
        provide: TranslocoService,
        useValue: {translate: vi.fn((key: string) => key)},
      },
      {provide: AdminModelsService, useValue: {models: signal([]), loadModels: vi.fn()}},
      {
        provide: AdminProvidersService,
        useValue: {
          systemApiKeys: signal([]),
          systemEndpoints: endpoints,
          loadSystemApiKeys: vi.fn(),
          loadSystemEndpoints: vi.fn(),
          trustSystemEndpointCertificate: trust,
          removeSystemEndpointPin: removePin,
        },
      },
      {provide: AdminModelsCoordinatorService, useValue: {}},
    ],
  });
  return runInInjectionContext(injector, () => new AdminProvidersComponent());
}

describe('AdminProvidersComponent — trusting a certificate', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal('confirm', confirmFn);
    endpoints.set([]);
  });

  it('Trust confirms, then sends the SHOWN fingerprint', () => {
    confirmFn.mockReturnValue(true);
    trust.mockReturnValue(of({}));
    const c = setup();
    c.testResults.set({e1: {ok: false, status: null, error: 'x', probe_url: 'u', tls: tls()}});
    c.trustCertificate('e1', 'gpu');
    expect(confirmFn).toHaveBeenCalledOnce();
    expect(trust).toHaveBeenCalledWith('e1', FP_NEW);
  });

  it('declining the confirm sends nothing', () => {
    confirmFn.mockReturnValue(false);
    const c = setup();
    c.testResults.set({e1: {ok: false, status: null, error: 'x', probe_url: 'u', tls: tls()}});
    c.trustCertificate('e1', 'gpu');
    expect(trust).not.toHaveBeenCalled();
  });

  it('shows the 409 detail in the card', () => {
    confirmFn.mockReturnValue(true);
    trust.mockReturnValue(
      throwError(() => ({status: 409, error: {detail: 'It already verifies normally.'}})),
    );
    const c = setup();
    c.testResults.set({e1: {ok: false, status: null, error: 'x', probe_url: 'u', tls: tls()}});
    c.trustCertificate('e1', 'gpu');
    expect(c.tlsErrors()['e1']).toBe('It already verifies normally.');
  });

  it('a successful trust drops the stale test result', () => {
    confirmFn.mockReturnValue(true);
    trust.mockReturnValue(of({}));
    const c = setup();
    c.testResults.set({e1: {ok: false, status: null, error: 'x', probe_url: 'u', tls: tls()}});
    c.trustCertificate('e1', 'gpu');
    expect(c.testResults()['e1']).toBeUndefined();
  });

  it('Remove pin asks first, then calls removeSystemEndpointPin', () => {
    confirmFn.mockReturnValue(true);
    removePin.mockReturnValue(of({}));
    const c = setup();
    c.removePin('e1');
    expect(confirmFn).toHaveBeenCalledOnce();
    expect(removePin).toHaveBeenCalledWith('e1');
  });

  it('Remove pin declined does nothing', () => {
    confirmFn.mockReturnValue(false);
    const c = setup();
    c.removePin('e1');
    expect(removePin).not.toHaveBeenCalled();
  });
});
