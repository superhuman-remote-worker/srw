import {describe, expect, it} from 'vitest';
import {classifyResumeError} from './resume-error';

describe('classifyResumeError', () => {
    it('treats 428 as drift and extracts the items', () => {
        const result = classifyResumeError({
            status: 428,
            error: {
                detail: {
                    code: 'config_drift',
                    drift: [{id: 'connector:abc', kind: 'connector',
                             reason: 'deleted', label: 'WidgetShop'}],
                },
            },
        });

        expect(result).toEqual({
            kind: 'drift',
            items: [{id: 'connector:abc', kind: 'connector',
                     reason: 'deleted', label: 'WidgetShop'}],
        });
    });

    it('classifies only typed session_not_ended 409 as an unproven successor', () => {
        expect(classifyResumeError({
            status: 409,
            error: {detail: {code: 'session_not_ended'}},
        })).toEqual({kind: 'not_ended'});
        expect(classifyResumeError({status: 409}))
            .toEqual({kind: 'error', status: 409});
        expect(classifyResumeError({
            status: 409,
            error: {detail: {code: 'protected_cloud_unsupported_session_class'}},
        })).toEqual({kind: 'error', status: 409});
    });

    it('surfaces 403 as a real error instead of swallowing it', () => {
        expect(classifyResumeError({status: 403})).toEqual({kind: 'error', status: 403});
    });

    it('surfaces an unknown failure as an error', () => {
        expect(classifyResumeError(new Error('offline')))
            .toEqual({kind: 'error', status: 0});
    });

    it('falls back to an error when 428 carries no usable drift list', () => {
        expect(classifyResumeError({status: 428, error: {}}))
            .toEqual({kind: 'error', status: 428});
    });
});
