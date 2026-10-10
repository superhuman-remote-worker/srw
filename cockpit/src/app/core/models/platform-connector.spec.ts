import {describe, expect, it} from 'vitest';
import {
  accessLevelLabelKey,
  isCloudConnectorType,
  isPlatformOwned,
  platformConnectorOwner,
  platformConnectorOwnerId,
  platformKindLabelKey,
  platformManagedLabelKey,
  platformOwnedKey,
  platformOwnedKind,
} from './platform-connector';
import type {DatasourceType} from './api.model';
// The real catalogue, so these specs also prove the keys they name exist.
import en from '../../../assets/i18n/en.json';
import de from '../../../assets/i18n/de-DE.json';

const P = '11111111-1111-1111-1111-111111111111';
const U = '22222222-2222-2222-2222-222222222222';

function row(type: string, extra: {managed_key?: string | null; config?: object} = {}) {
  return {type: type as DatasourceType, ...extra};
}

function lookup(tree: unknown, key: string): unknown {
  return key.split('.').reduce<unknown>(
    (node, part) => (node as Record<string, unknown> | undefined)?.[part],
    tree,
  );
}

describe('platform-owned connectors', () => {
  it('reads the managed key first, and kind and owner from it', () => {
    const folder = row('cloud_folder', {managed_key: `project-cloud:${P}`});
    expect(platformOwnedKey(folder)).toBe(`project-cloud:${P}`);
    expect(isPlatformOwned(folder)).toBe(true);
    expect(platformOwnedKind(folder)).toBe('project-cloud');
    expect(platformConnectorOwner(folder)).toBe('project');
    expect(platformConnectorOwnerId(folder)).toBe(P);

    const outbox = row('cloud_outbox', {managed_key: `cloud-outbox:${U}`});
    expect(platformConnectorOwner(outbox)).toBe('user');
    expect(platformConnectorOwnerId(outbox)).toBe(U);
    expect(platformConnectorOwner(row('cloud_folder', {managed_key: `user-cloud-root:${U}`}))).toBe(
      'user',
    );
    expect(
      platformConnectorOwner(row('cloud_folder_checkout', {managed_key: `project-cloud-checkout:${P}`})),
    ).toBe('project');
  });

  it("falls back to the older marker, on a knowledge base only, as the server does", () => {
    const legacy = row('kb', {config: {native_project_id: P}});
    expect(platformOwnedKey(legacy)).toBe(`project-kb:${P}`);
    expect(platformOwnedKind(legacy)).toBe('project-kb');
    expect(platformConnectorOwner(legacy)).toBe('project');
    // The key wins over the marker.
    expect(platformOwnedKind(row('kb', {managed_key: `project-kb:${P}`, config: {native_project_id: U}}))).toBe(
      'project-kb',
    );
    // A marker on another type, an empty one, or none: a user's own row.
    expect(isPlatformOwned(row('postgresql', {config: {native_project_id: P}}))).toBe(false);
    expect(isPlatformOwned(row('kb', {config: {native_project_id: ''}}))).toBe(false);
    expect(isPlatformOwned(row('kb', {config: {root_path: 'vault'}}))).toBe(false);
    expect(isPlatformOwned(row('postgresql', {managed_key: null}))).toBe(false);
    expect(isPlatformOwned(null)).toBe(false);
    expect(platformConnectorOwner(row('postgresql'))).toBeNull();
  });

  it('passes a kind it does not know through, managed by SRW', () => {
    const later = row('cloud_folder', {managed_key: `some-later-kind:${P}`});
    expect(isPlatformOwned(later)).toBe(true);
    expect(platformOwnedKind(later)).toBe('some-later-kind');
    expect(platformConnectorOwner(later)).toBe('srw');
    expect(platformKindLabelKey(later)).toBeNull();
    expect(platformManagedLabelKey(later)).toBe('datasources.platform.managedBySrw');
    // Never a prototype member mistaken for a known kind.
    expect(platformKindLabelKey(row('kb', {managed_key: `constructor:${P}`}))).toBeNull();
  });

  it('labels each kind, and who manages it, with keys both catalogues carry', () => {
    const labels = {
      [`project-kb:${P}`]: 'Knowledge base',
      [`project-cloud:${P}`]: 'Project folder',
      [`project-cloud-checkout:${P}`]: 'Project folder copy (jobs)',
      [`user-cloud-root:${U}`]: 'My cloud',
      [`cloud-outbox:${U}`]: 'Session outbox',
    };
    for (const [key, label] of Object.entries(labels)) {
      const labelKey = platformKindLabelKey(row('cloud_folder', {managed_key: key}));
      expect(labelKey).not.toBeNull();
      expect(lookup(en, labelKey!)).toBe(label);
      expect(typeof lookup(de, labelKey!)).toBe('string');
    }
    expect(platformKindLabelKey(row('postgresql'))).toBeNull();

    const folder = row('cloud_folder', {managed_key: `project-cloud:${P}`});
    const outbox = row('cloud_outbox', {managed_key: `cloud-outbox:${U}`});
    expect(platformManagedLabelKey(folder, U)).toBe('datasources.platform.managedByProject');
    expect(platformManagedLabelKey(outbox, U)).toBe('datasources.platform.managedForYou');
    // An administrator looking at someone else's outbox.
    expect(platformManagedLabelKey(outbox, 'someone-else')).toBe(
      'datasources.platform.managedForItsUser',
    );
    expect(platformManagedLabelKey(row('postgresql'), U)).toBeNull();
    for (const key of [
      'datasources.platform.managedByProject',
      'datasources.platform.managedForYou',
      'datasources.platform.managedForItsUser',
      'datasources.platform.managedBySrw',
    ]) {
      expect(typeof lookup(en, key)).toBe('string');
      expect(typeof lookup(de, key)).toBe('string');
    }
  });

  it('knows the cloud types and names their levels', () => {
    for (const type of ['cloud_folder', 'cloud_folder_checkout', 'cloud_outbox']) {
      expect(isCloudConnectorType(type)).toBe(true);
      expect(typeof lookup(en, `datasources.filter.${type}`)).toBe('string');
      expect(typeof lookup(de, `datasources.filter.${type}`)).toBe('string');
    }
    expect(isCloudConnectorType('webdav')).toBe(false);
    expect(isCloudConnectorType(null)).toBe(false);
    for (const level of ['read_only', 'protected', 'read_write', 'reviewed_write_back']) {
      const key = accessLevelLabelKey(level);
      expect(typeof lookup(en, key!)).toBe('string');
      expect(typeof lookup(de, key!)).toBe('string');
    }
    expect(accessLevelLabelKey('ReadOnly')).toBeNull();
    expect(accessLevelLabelKey(null)).toBeNull();
  });
});
