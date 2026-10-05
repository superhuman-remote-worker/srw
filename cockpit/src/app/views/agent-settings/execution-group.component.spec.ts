import {describe, expect, it, vi} from 'vitest';
import {signal} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {of} from 'rxjs';
import {TranslocoService} from '@jsverse/transloco';
import {ExecutionGroupComponent} from './execution-group.component';
import {UserService} from '../../core/services/user.service';
import {User} from '../../core/models/api.model';

/**
 * Build under TestBed (which supplies the ChangeDetectionScheduler the
 * constructor's effect() needs) with a mocked UserService signal, so
 * canUseVm() resolves against controllable state.
 */
function createWith(
  user: Partial<User> | null = {is_admin: true},
  translations: Record<string, string> = {},
) {
  const currentUser = signal<User | null>(
    user
      ? ({
          id: 'u1',
          display_name: 'Test',
          avatar_color: '#fff',
          created_at: '2025-01-01T00:00:00Z',
          ...user,
        } as User)
      : null,
  );

  const mockUserService = {
    currentUser,
    users: signal([]),
    isAuthenticated: signal(true),
    sessionReady: signal(true),
    isApproved: signal(true),
    currentUserId: signal(user ? 'u1' : null),
    loadCurrentUser: vi.fn(),
    loadUsers: vi.fn(),
    logout: vi.fn(),
  };

  // Echoes the key back by default (most tests read raw keys), but a caller
  // may supply real strings for the handful of keys it asserts rendered text
  // against — see the workspace-default-hint suite below.
  const mockTransloco = {
    translate: (key: string) => translations[key] ?? key,
    langChanges$: of('en'),
    getActiveLang: () => 'en',
  };

  TestBed.configureTestingModule({
    providers: [
      ExecutionGroupComponent,
      {provide: UserService, useValue: mockUserService},
      {provide: TranslocoService, useValue: mockTransloco},
    ],
  });

  return {component: TestBed.inject(ExecutionGroupComponent), currentUser};
}

function createComponent(): ExecutionGroupComponent {
  return createWith().component;
}

describe('ExecutionGroupComponent image quality', () => {
  it('defaults to standard with no override emitted', () => {
    const c = createComponent();
    expect(c.imageQuality()).toBeNull();
    expect(c.resolvedImageQuality()).toBe('standard');
    expect(c.getOverrides()['image_quality']).toBeUndefined();
  });

  it('captures a non-default tier into the override fragment', () => {
    const c = createComponent();
    c.onImageQualityChange('high');
    expect(c.imageQuality()).toBe('high');
    // image_quality is added regardless of mode (top-level knob for job+session)
    expect(c.getOverrides()['image_quality']).toBe('high');
  });

  // Choosing a value is intent, even when that value happens to be the one
  // already resolved. Collapsing it back to "inherit" made the displayed
  // default the one option the form could not express, and left the user
  // unable to pin against a later change to the expert/account layer. Only the
  // reset control clears an override now.
  it('pins the resolved default when it is explicitly selected', () => {
    const c = createComponent();
    c.onImageQualityChange('economy');
    expect(c.imageQuality()).toBe('economy');
    c.onImageQualityChange('standard'); // equals the resolved default
    expect(c.imageQuality()).toBe('standard');
    expect(c.getOverrides()['image_quality']).toBe('standard');
  });

  it('pins the resolved default when the user interacts without changing it', () => {
    const c = createComponent();
    expect(c.imageQuality()).toBeNull();
    // What PinOnInteractDirective's (pin) output triggers: a native <select>
    // fires no change event when the shown option is re-picked.
    c.pinValue(c.imageQuality, c.resolvedImageQuality());
    expect(c.imageQuality()).toBe('standard');
    expect(c.getOverrides()['image_quality']).toBe('standard');
  });

  it('pinning never overwrites a value the user already chose', () => {
    const c = createComponent();
    c.onImageQualityChange('high');
    c.pinValue(c.imageQuality, c.resolvedImageQuality());
    expect(c.imageQuality()).toBe('high');
  });

  it('counts toward modifiedCount and clears on resetAll', () => {
    const c = createComponent();
    c.onImageQualityChange('high');
    expect(c.modifiedCount()).toBe(1);
    c.resetAll();
    expect(c.imageQuality()).toBeNull();
    expect(c.modifiedCount()).toBe(0);
  });
});

describe('ExecutionGroupComponent — VM permission', () => {
  it('is false for a non-admin without can_use_vm', () => {
    const {component} = createWith({is_admin: false, can_use_vm: false});
    expect(component.canUseVm()).toBe(false);
  });

  it('is true for an admin without explicit grant', () => {
    const {component} = createWith({is_admin: true, can_use_vm: false});
    expect(component.canUseVm()).toBe(true);
  });

  it('is true for a non-admin with explicit grant', () => {
    const {component} = createWith({is_admin: false, can_use_vm: true});
    expect(component.canUseVm()).toBe(true);
  });

  it('is false when the user signal is null', () => {
    const {component} = createWith(null);
    expect(component.canUseVm()).toBe(false);
  });

  it('tracks user signal changes', () => {
    const {component, currentUser} = createWith({is_admin: false, can_use_vm: false});
    expect(component.canUseVm()).toBe(false);

    currentUser.set({
      id: 'u1',
      display_name: 'Test',
      avatar_color: '#fff',
      created_at: '2025-01-01T00:00:00Z',
      is_admin: false,
      can_use_vm: true,
    } as User);
    expect(component.canUseVm()).toBe(true);
  });

  it('snaps an ineligible user off a vm default so the form cannot submit it', () => {
    const {component} = createWith({is_admin: false, can_use_vm: false});
    Object.defineProperty(component, 'config', {value: () => ({workspace: {backend: 'vm'}})});
    expect(component.resolvedWorkspaceBackend()).toBe('vm');

    TestBed.tick(); // fire the guard effect
    expect(component.workspaceBackend()).toBe('sandbox');
    expect(component.getOverrides()['workspace']).toEqual({backend: 'sandbox'});
  });

  it('leaves an eligible user on a vm default', () => {
    const {component} = createWith({is_admin: true});
    Object.defineProperty(component, 'config', {value: () => ({workspace: {backend: 'vm'}})});

    TestBed.tick();
    expect(component.workspaceBackend()).toBeNull();
  });
});

describe('ExecutionGroupComponent workspace backend', () => {
  it('defaults to sandbox with no override emitted', () => {
    const c = createComponent();
    expect(c.workspaceBackend()).toBeNull();
    expect(c.resolvedWorkspaceBackend()).toBe('sandbox');
    expect(c.getOverrides()['workspace']).toBeUndefined();
  });

  it('reads the resolved default out of the merged config', () => {
    const c = createComponent();
    Object.defineProperty(c, 'config', {value: () => ({workspace: {backend: 'virtual'}})});
    expect(c.resolvedWorkspaceBackend()).toBe('virtual');
    expect(c.isLiteBackend()).toBe(true);
  });

  it('captures a chosen backend into the override fragment', () => {
    const c = createComponent();
    c.onWorkspaceBackendChange('virtual');
    expect(c.getOverrides()['workspace']).toEqual({backend: 'virtual'});
  });

  it('pins the resolved default when the user interacts without changing it', () => {
    const c = createComponent();
    c.pinValue(c.workspaceBackend, c.resolvedWorkspaceBackend());
    expect(c.workspaceBackend()).toBe('sandbox');
    expect(c.getOverrides()['workspace']).toEqual({backend: 'sandbox'});
  });

  it('derives the lite tiers the Advanced accordion greys on', () => {
    const c = createComponent();
    expect(c.isLiteBackend()).toBe(false);
    expect(c.isNoneBackend()).toBe(false);

    c.onWorkspaceBackendChange('vm');
    expect(c.isLiteBackend()).toBe(false);

    c.onWorkspaceBackendChange('virtual');
    expect(c.isLiteBackend()).toBe(true);
    expect(c.isNoneBackend()).toBe(false);

    c.onWorkspaceBackendChange('none');
    expect(c.isLiteBackend()).toBe(true);
    expect(c.isNoneBackend()).toBe(true);
  });

  it('counts toward modifiedCount, and reset clears it and notifies the host', () => {
    const c = createComponent();
    let changes = 0;
    c.change.subscribe(() => changes++);

    c.onWorkspaceBackendChange('none');
    expect(c.modifiedCount()).toBe(1);
    expect(changes).toBe(1);

    c.resetWorkspaceBackend();
    expect(c.workspaceBackend()).toBeNull();
    expect(c.modifiedCount()).toBe(0);
    // The accordion's greying and the datasource picker's repo filter both
    // read this, so the reset has to reach the host too.
    expect(changes).toBe(2);
  });

  it('clears on resetAll', () => {
    const c = createComponent();
    c.onWorkspaceBackendChange('virtual');
    c.resetAll();
    expect(c.workspaceBackend()).toBeNull();
  });
});

describe('ExecutionGroupComponent without its workspace row (Slice A3)', () => {
  it('emits no backend when the picker owns the workspace', () => {
    const {component} = createWith({is_admin: true});
    Object.defineProperty(component, 'showWorkspaceBackend', {value: () => false});
    component.workspaceBackend.set('vm');
    expect(component.getOverrides()['workspace']).toBeUndefined();
  });

  it('does not snap an ineligible user off a vm default it is not showing', () => {
    const {component} = createWith({is_admin: false, can_use_vm: false});
    Object.defineProperty(component, 'showWorkspaceBackend', {value: () => false});
    Object.defineProperty(component, 'config', {value: () => ({workspace: {backend: 'vm'}})});
    TestBed.tick();
    expect(component.workspaceBackend()).toBeNull();
  });
});
