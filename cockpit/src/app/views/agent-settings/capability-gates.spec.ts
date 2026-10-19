import {describe, it, expect} from 'vitest';
import {hasGrant, allowedEnumOptions, isModelAllowed} from './capability-gates';
import type {GrantCatalog} from '../../core/models/api.model';

const CAT: GrantCatalog = {
  shell_tools: {type: 'bool', default: false, restrict_only: true},
  autonomy_ceiling: {
    type: 'enum',
    default: 'review',
    restrict_only: true,
    order: ['dependent', 'guided', 'partial', 'review', 'full'],
  },
  model_selection: {type: 'list', default: null, restrict_only: true},
};

describe('capability-gates', () => {
  it('hasGrant: admin (null grants) is always granted', () => {
    expect(hasGrant(null, 'shell_tools')).toBe(true);
  });

  it('hasGrant: bool key reads the resolved value; absent ⇒ denied', () => {
    expect(hasGrant({shell_tools: true}, 'shell_tools')).toBe(true);
    expect(hasGrant({shell_tools: false}, 'shell_tools')).toBe(false);
    expect(hasGrant({}, 'shell_tools')).toBe(false);
  });

  it('allowedEnumOptions: admin keeps all; non-admin filters above the ceiling', () => {
    const all = CAT['autonomy_ceiling'].order as string[];
    expect(allowedEnumOptions(null, 'autonomy_ceiling', all, CAT)).toEqual(all);
    expect(
      allowedEnumOptions({autonomy_ceiling: 'review'}, 'autonomy_ceiling', all, CAT),
    ).toEqual(['dependent', 'guided', 'partial', 'review']); // 'full' dropped
  });

  it('allowedEnumOptions: a legacy `supervised` permission ceiling or order means read_only', () => {
    const all = ['read_only', 'auto_accept', 'autonomous'];
    const legacy: GrantCatalog = {
      permission_mode: {
        type: 'enum',
        default: 'supervised',
        restrict_only: true,
        order: ['supervised', 'auto_accept', 'autonomous'],
      },
    };
    expect(allowedEnumOptions({permission_mode: 'supervised'}, 'permission_mode', all, legacy))
      .toEqual(['read_only']);
    expect(allowedEnumOptions({permission_mode: 'auto_accept'}, 'permission_mode', all, legacy))
      .toEqual(['read_only', 'auto_accept']);
  });

  it('isModelAllowed: null/admin ⇒ all; list restricts', () => {
    expect(isModelAllowed(null, 'gpt-x')).toBe(true);
    expect(isModelAllowed({model_selection: null}, 'gpt-x')).toBe(true);
    expect(isModelAllowed({model_selection: ['a', 'b']}, 'a')).toBe(true);
    expect(isModelAllowed({model_selection: ['a', 'b']}, 'c')).toBe(false);
  });
});
