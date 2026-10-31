import {describe, expect, it} from 'vitest';
import {acknowledgeableIds, groupDriftForDisplay} from './config-drift-dialog.component';

describe('groupDriftForDisplay', () => {
    it('collapses identical labels into one row with a count', () => {
        const rows = groupDriftForDisplay([
            {id: 'connector:a', kind: 'connector', reason: 'revoked',
             label: 'a connector you no longer have access to'},
            {id: 'connector:b', kind: 'connector', reason: 'revoked',
             label: 'a connector you no longer have access to'},
        ]);

        expect(rows).toEqual([{
            kind: 'connector', reason: 'revoked',
            label: 'a connector you no longer have access to', count: 2,
        }]);
    });

    it('keeps distinct labels separate and preserves order', () => {
        const rows = groupDriftForDisplay([
            {id: 'connector:a', kind: 'connector', reason: 'deleted',
             label: 'WidgetShop'},
            {id: 'grant:shell_tools', kind: 'grant', reason: 'revoked',
             label: 'shell tools'},
        ]);

        expect(rows.map(r => r.label)).toEqual(['WidgetShop', 'shell tools']);
        expect(rows.every(r => r.count === 1)).toBe(true);
    });

    it('returns nothing for an empty list', () => {
        expect(groupDriftForDisplay([])).toEqual([]);
    });
});

describe('acknowledgeableIds', () => {
    it('returns every item id, not one per collapsed display row', () => {
        const items = [
            {id: 'connector:a', kind: 'connector' as const, reason: 'revoked' as const,
             label: 'a connector you no longer have access to'},
            {id: 'connector:b', kind: 'connector' as const, reason: 'revoked' as const,
             label: 'a connector you no longer have access to'},
            {id: 'grant:shell_tools', kind: 'grant' as const, reason: 'revoked' as const,
             label: 'shell tools'},
        ];

        // Pin the premise this test exists to guard: 3 items collapse to 2
        // display rows (the first two share a label), so routing `ids`
        // through `rows` instead of `items` would silently drop one id.
        expect(groupDriftForDisplay(items)).toHaveLength(2);

        expect(acknowledgeableIds(items)).toEqual([
            'connector:a', 'connector:b', 'grant:shell_tools',
        ]);
    });
});
