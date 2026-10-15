import {describe, expect, it} from 'vitest';
import {pickOwnDefaultProject} from './own-default-project';

describe('pickOwnDefaultProject', () => {
  it("picks the caller's own default, not a default they are only a member of", () => {
    const projects = [
      {id: 'theirs', is_default: true, user_role: 'editor' as const},
      {id: 'mine', is_default: true, user_role: 'owner' as const},
    ];
    expect(pickOwnDefaultProject(projects)?.id).toBe('mine');
  });

  it('returns undefined when no default is owned', () => {
    expect(
      pickOwnDefaultProject([
        {id: 'theirs', is_default: true, user_role: 'viewer' as const},
        {id: 'owned-not-default', is_default: false, user_role: 'owner' as const},
        {id: 'no-role', is_default: true},
      ]),
    ).toBeUndefined();
  });

  it("prefers the caller's own default_project_id over a co-owned colleague default", () => {
    const projects = [
      {id: 'colleague', is_default: true, user_role: 'owner' as const},
      {id: 'mine', is_default: true, user_role: 'owner' as const},
    ];
    expect(pickOwnDefaultProject(projects, 'mine')?.id).toBe('mine');
  });

  it('falls back to the owner rule when the id is absent or not in the list', () => {
    const projects = [
      {id: 'colleague', is_default: true, user_role: 'owner' as const},
      {id: 'mine', is_default: true, user_role: 'owner' as const},
    ];
    expect(pickOwnDefaultProject(projects, null)?.id).toBe('colleague');
    expect(pickOwnDefaultProject(projects, undefined)?.id).toBe('colleague');
    expect(pickOwnDefaultProject(projects, 'gone')?.id).toBe('colleague');
  });
});
