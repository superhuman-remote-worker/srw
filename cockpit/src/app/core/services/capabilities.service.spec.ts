import {describe, it, expect, vi} from 'vitest';
import {environment} from '../environment';
import {TestBed} from '@angular/core/testing';
import {NEVER, of} from 'rxjs';
import {CapabilitiesService} from './capabilities.service';
import {ApiService} from './api.service';
import type {GrantCatalog, SshGatewayHostKeysResponse, UserCapabilities} from '../models/api.model';

const CATALOG: GrantCatalog = {
  permission_mode: {
    type: 'enum',
    default: 'supervised',
    restrict_only: true,
    order: ['supervised', 'auto_accept', 'autonomous'],
  },
};

// The constructor unconditionally fetches both endpoints, so every test
// double needs both — a mock exposing only `getMyCapabilities` throws
// `getSshHostKeys is not a function` the moment the real service is
// constructed. Deployments with no gateway configured answer this shape
// (never an error), so it doubles as the default "no gateway" stub.
const NO_GATEWAY: SshGatewayHostKeysResponse = {host_keys: [], hostname: ''};

function make(
  caps: UserCapabilities | null,
  sshHostKeys: SshGatewayHostKeysResponse | null = NO_GATEWAY,
): CapabilitiesService {
  TestBed.configureTestingModule({
    providers: [
      CapabilitiesService,
      {
        provide: ApiService,
        useValue: {
          getMyCapabilities: () => of(caps),
          getSshHostKeys: () => of(sshHostKeys),
        },
      },
    ],
  });
  return TestBed.inject(CapabilitiesService);
}

describe('CapabilitiesService', () => {
  it('non-admin, supervised ceiling ⇒ only supervised is selectable', () => {
    const svc = make({is_admin: false, grants: {permission_mode: 'supervised'}, catalog: CATALOG});
    expect(svc.permissionModes()).toEqual(['supervised']);
    expect(svc.allowsPermissionMode('auto_accept')).toBe(false);
    expect(svc.allowsPermissionMode('autonomous')).toBe(false);
    expect(svc.permissionRestricted()).toBe(true);
  });

  it('non-admin, auto_accept ceiling ⇒ supervised + auto_accept (autonomous gated)', () => {
    const svc = make({is_admin: false, grants: {permission_mode: 'auto_accept'}, catalog: CATALOG});
    expect(svc.permissionModes()).toEqual(['supervised', 'auto_accept']);
    expect(svc.allowsPermissionMode('autonomous')).toBe(false);
    expect(svc.permissionRestricted()).toBe(true);
  });

  it('admin (grants null) ⇒ all modes, not restricted', () => {
    const svc = make({is_admin: true, grants: null, catalog: CATALOG});
    expect(svc.permissionModes()).toEqual(['supervised', 'auto_accept', 'autonomous']);
    expect(svc.permissionRestricted()).toBe(false);
  });

  it('API unavailable (null) ⇒ fails open to all modes (Phase 1 backstops)', () => {
    const svc = make(null);
    expect(svc.permissionModes()).toEqual(['supervised', 'auto_accept', 'autonomous']);
    expect(svc.permissionRestricted()).toBe(false);
  });
});

describe('CapabilitiesService.canPublishDatasources', () => {
  it('is true for admins (grants === null)', () => {
    const svc = make({is_admin: true, grants: null, catalog: CATALOG});
    expect(svc.canPublishDatasources()).toBe(true);
  });

  it('is true when the grant resolves true', () => {
    const svc = make({
      is_admin: false,
      grants: {public_datasources: true},
      catalog: CATALOG,
    });
    expect(svc.canPublishDatasources()).toBe(true);
  });

  it('is false when the grant is absent (deny-by-default)', () => {
    const svc = make({is_admin: false, grants: {}, catalog: CATALOG});
    expect(svc.canPublishDatasources()).toBe(false);
  });

  it('fails CLOSED on fetch error, unlike the fail-open mode helpers', () => {
    // Publishing exposes credentials org-wide; on error the section hides
    // (the server gate is the backstop either way).
    const svc = make(null);
    expect(svc.canPublishDatasources()).toBe(false);
  });

  it('fails closed while loading', () => {
    TestBed.configureTestingModule({
      providers: [
        CapabilitiesService,
        {
          provide: ApiService,
          useValue: {getMyCapabilities: () => NEVER, getSshHostKeys: () => of(NO_GATEWAY)},
        },
      ],
    });
    const svc = TestBed.inject(CapabilitiesService);
    expect(svc.canPublishDatasources()).toBe(false);
  });
});

describe('CapabilitiesService.protectedCloudAvailable', () => {
  it('is true when the deployment feature flag is on', () => {
    const svc = make({
      is_admin: false,
      grants: {},
      catalog: CATALOG,
      features: {protected_cloud: true},
    });
    expect(svc.protectedCloudAvailable()).toBe(true);
  });

  it('is false when the flag is off', () => {
    const svc = make({
      is_admin: false,
      grants: {},
      catalog: CATALOG,
      features: {protected_cloud: false},
    });
    expect(svc.protectedCloudAvailable()).toBe(false);
  });

  it('is false when features is absent from the payload', () => {
    const svc = make({is_admin: true, grants: null, catalog: CATALOG});
    expect(svc.protectedCloudAvailable()).toBe(false);
  });

  it('is false while loading / on fetch error', () => {
    const svc = make(null);
    expect(svc.protectedCloudAvailable()).toBe(false);
  });
});

describe('CapabilitiesService.datasourceScopeAutoAttachAvailable', () => {
  it('is true only when the deployment explicitly advertises v1', () => {
    const svc = make({
      is_admin: false,
      grants: {},
      catalog: CATALOG,
      features: {datasource_scope_auto_attach_v1: true},
    });
    expect(svc.datasourceScopeAutoAttachAvailable()).toBe(true);
  });

  it('is false when the flag is false', () => {
    const svc = make({
      is_admin: false,
      grants: {},
      catalog: CATALOG,
      features: {datasource_scope_auto_attach_v1: false},
    });
    expect(svc.datasourceScopeAutoAttachAvailable()).toBe(false);
  });

  it('is false when the flag is omitted', () => {
    const svc = make({
      is_admin: false,
      grants: {},
      catalog: CATALOG,
    });
    expect(svc.datasourceScopeAutoAttachAvailable()).toBe(false);
  });

  it('fails closed when the capabilities request fails', () => {
    expect(make(null).datasourceScopeAutoAttachAvailable()).toBe(false);
  });

  it('fails closed while loading', () => {
    TestBed.configureTestingModule({
      providers: [
        CapabilitiesService,
        {
          provide: ApiService,
          useValue: {getMyCapabilities: () => NEVER, getSshHostKeys: () => of(NO_GATEWAY)},
        },
      ],
    });
    expect(TestBed.inject(CapabilitiesService).datasourceScopeAutoAttachAvailable()).toBe(false);
  });
});

describe('CapabilitiesService.sshGateway', () => {
  it('does not advertise or fetch external SSH setup when the deployment disables external clients', () => {
    const previous = environment.externalClientsEnabled;
    environment.externalClientsEnabled = false;
    const getSshHostKeys = vi.fn();
    try {
      TestBed.configureTestingModule({providers: [
        CapabilitiesService,
        {provide: ApiService, useValue: {
          getMyCapabilities: () => of({is_admin: false, grants: {}, catalog: CATALOG}),
          getSshHostKeys,
        }},
      ]});
      expect(TestBed.inject(CapabilitiesService).sshGateway()).toBeNull();
      expect(getSshHostKeys).not.toHaveBeenCalled();
    } finally {
      environment.externalClientsEnabled = previous;
    }
  });

  it('is null when the deployment has no gateway configured (empty host_keys)', () => {
    // GET /api/ssh/host-keys answers {host_keys: [], hostname: ...} rather
    // than erroring in this case — the empty list must fold to null so the
    // UI can hide the connect panel with one check.
    const svc = make({is_admin: false, grants: {}, catalog: CATALOG}, NO_GATEWAY);
    expect(svc.sshGateway()).toBeNull();
  });

  it('is null even when hostname is set but host_keys is empty', () => {
    const svc = make(
      {is_admin: false, grants: {}, catalog: CATALOG},
      {host_keys: [], hostname: 'ssh.example.test'},
    );
    expect(svc.sshGateway()).toBeNull();
  });

  it('carries hostname and host_keys when the gateway is configured', () => {
    const response: SshGatewayHostKeysResponse = {
      hostname: 'ssh.example.test',
      host_keys: [
        {type: 'ssh-ed25519', public_key: 'ssh-ed25519 AAAA...', fingerprint: 'SHA256:abc'},
      ],
    };
    const svc = make({is_admin: false, grants: {}, catalog: CATALOG}, response);
    expect(svc.sshGateway()).toEqual(response);
  });

  it('is null on a transport failure', () => {
    // ApiService.getSshHostKeys() catches transport errors to null itself
    // (mirroring getMyCapabilities/getVoiceCapabilities); the mock here
    // stands in for that already-caught outcome.
    const svc = make({is_admin: false, grants: {}, catalog: CATALOG}, null);
    expect(svc.sshGateway()).toBeNull();
  });
});
