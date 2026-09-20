import {computed, inject, Injectable, signal} from '@angular/core';
import {ReplaySubject} from 'rxjs';
import {ApiService} from './api.service';
import type {
  GrantCatalog,
  SshGatewayHostKeysResponse,
  SshHostKeyEntry,
  UserCapabilityFeatures,
} from '../models/api.model';
import {allowedEnumOptions} from '../../views/agent-settings/capability-gates';
import {environment} from '../environment';

/** `CapabilitiesService.sshGateway()`'s shape when the deployment has a
 * gateway configured — `hostname` plus its public host keys for client-side
 * pinning. */
export interface SshGatewayInfo {
  hostname: string;
  host_keys: SshHostKeyEntry[];
}

/** Permission modes, lowest→highest autonomy (mirrors the backend
 * `_PERMISSION_ORDER` in src/core/capability_grants.py). */
export const PERMISSION_MODES = ['supervised', 'auto_accept', 'autonomous'];

/**
 * The caller's resolved capability grants, fetched once and shared. Drives
 * permission-mode dropdown greying so a user is never offered a mode the
 * backend will deny at provisioning — the UX half of
 * knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md.
 *
 * Fails open (grants ⇒ all modes) while loading, for admins, and on error:
 * Phase 1's provisioning pre-flight is the authoritative backstop, so this
 * layer only needs to hide options the user demonstrably cannot use.
 */
@Injectable({providedIn: 'root'})
export class CapabilitiesService {
  private readonly api = inject(ApiService);

  // undefined = loading; null = admin / unrestricted; else the resolved grants dict.
  readonly grants = signal<Record<string, unknown> | null | undefined>(undefined);
  readonly catalog = signal<GrantCatalog>({});
  readonly features = signal<UserCapabilityFeatures>({});

  /** The deployment's SSH gateway hostname + public host keys, or `null`
   * while loading, on fetch error, or when the deployment has no gateway
   * configured (`GET /api/ssh/host-keys` answers `{host_keys: [], ...}`
   * rather than erroring in that case — an empty key list is folded to
   * `null` here so the UI can hide the connect panel with one check). */
  readonly sshGateway = signal<SshGatewayInfo | null>(null);

  // True when the capabilities fetch errored (ApiService catches to a null
  // emission, which would otherwise be indistinguishable from an admin's
  // null grants). Fail-closed consumers (publish gating) branch on this;
  // the permission-mode helpers keep their deliberate fail-open behavior.
  private readonly loadFailed = signal(false);
  private readonly datasourceScopeAutoAttachAvailabilitySubject =
    new ReplaySubject<boolean>(1);

  /** Emits once the capabilities request resolves, and again after an
   * explicit reload. Consumers can start in legacy mode, then refresh when
   * the deployment contract is known without treating loading as enabled. */
  readonly datasourceScopeAutoAttachAvailability$ =
    this.datasourceScopeAutoAttachAvailabilitySubject.asObservable();

  constructor() {
    this.load();
    this.loadSshGateway();
  }

  load(): void {
    this.api.getMyCapabilities().subscribe((c) => {
      this.loadFailed.set(c === null);
      this.grants.set(c ? c.grants : null);
      if (c?.catalog) this.catalog.set(c.catalog);
      this.features.set(c?.features ?? {});
      this.datasourceScopeAutoAttachAvailabilitySubject.next(
        c?.features?.datasource_scope_auto_attach_v1 === true,
      );
    });
  }

  private loadSshGateway(): void {
    if (!environment.externalClientsEnabled) return;
    this.api.getSshHostKeys().subscribe((r: SshGatewayHostKeysResponse | null) => {
      if (r && r.host_keys.length > 0) {
        this.sshGateway.set({hostname: r.hostname, host_keys: r.host_keys});
      } else {
        this.sshGateway.set(null);
      }
    });
  }

  /** Whether the datasource Visibility (publish) section may render. Unlike
   * the permission-mode helpers this FAILS CLOSED while loading and on fetch
   * error — the section stays hidden until a successful capabilities fetch
   * proves entitlement (the server gate is the real backstop either way).
   * Spec: knowledge-base/knowledge/features/public_datasources.md. */
  readonly canPublishDatasources = computed(() => {
    if (this.loadFailed()) return false;
    const g = this.grants();
    if (g === null) return true; // admin/unrestricted
    if (g === undefined) return false; // loading
    return g['public_datasources'] === true;
  });

  /** Whether the project Loop and Centurion tabs may render. FAILS CLOSED
   * while loading and on fetch error, same posture as `canPublishDatasources`
   * — an unattended loop or officer is unbounded token spend, so the surface
   * stays hidden until a successful fetch proves entitlement. Hiding is UX
   * only; the orchestrator refuses start/resume/convert/commission with 403
   * and the config PDP refuses `officer.enabled` regardless of what renders.
   * Spec: knowledge-history/done/unattended_operations_grant.md. */
  readonly canRunUnattendedOperations = computed(() => {
    if (this.loadFailed()) return false;
    const g = this.grants();
    if (g === null) return true; // admin/unrestricted
    if (g === undefined) return false; // loading
    return g['unattended_operations'] === true;
  });

  /** permission_mode options at/below the user's ceiling (admin/loading ⇒ all). */
  readonly permissionModes = computed(() =>
    allowedEnumOptions(this.grants() ?? null, 'permission_mode', PERMISSION_MODES, this.catalog()),
  );

  /** True if `mode` is selectable for this user. */
  allowsPermissionMode(mode: string): boolean {
    return this.permissionModes().includes(mode);
  }

  /** True when at least one permission mode is gated (drives the lock hint). */
  readonly permissionRestricted = computed(
    () => this.permissionModes().length < PERMISSION_MODES.length,
  );

  /** Whether protected cloud mode is enabled at the deployment level (Slice
   * C's session-create toggle gate). Fails closed while loading / on error,
   * same posture as `canPublishDatasources` — the flag only flips true after
   * a successful fetch confirms it. */
  readonly protectedCloudAvailable = computed(() => !!this.features().protected_cloud);

  /** Whether the datasource project-scope / auto-attach policy contract is
   * available end-to-end. This deliberately fails closed while capabilities
   * are loading, when the request fails, and when an older orchestrator omits
   * the flag. Legacy connector CRUD remains available without policy fields. */
  readonly datasourceScopeAutoAttachAvailable = computed(
    () => this.features().datasource_scope_auto_attach_v1 === true,
  );
}
