import {describe, expect, it} from 'vitest';
import {
    buildToolCardView,
    parseJobEntity,
    resolveToolDescriptor,
} from './tool-descriptors';
import {NormalizedToolCall} from '../models/tool-card.model';
import {
    asRecord,
    canResumeJob,
    effectiveJobStatus,
    isRunningJobStatus,
    isTerminalJobStatus,
    jobStatusTone,
} from '../util/job-status';

/**
 * Job tool card (unified_tool_cards slice 4).
 *
 * Pure functions only — `TestBed.createComponent` does not work under vitest in
 * this repo, so the testable surface is the id parser, the status vocabulary,
 * and the view builder.
 */

const JOB_ID = '38e17406-3dbf-410e-9630-df15e49a0543';

function call(over: Partial<NormalizedToolCall> = {}): NormalizedToolCall {
    return {
        tool: 'create_job',
        args: {description: 'Explore a warm-neutral theme'},
        status: 'ok',
        result: `Job created successfully.\nJob ID: ${JOB_ID}\nConfig: worker_base`,
        ...over,
    };
}

describe('parseJobEntity', () => {
    it('recovers the job id from the tool receipt', () => {
        expect(parseJobEntity(call())).toEqual({kind: 'job', id: JOB_ID});
    });

    it('recovers it from a JSON result too', () => {
        const entity = parseJobEntity(call({result: JSON.stringify({job_id: JOB_ID})}));
        expect(entity).toEqual({kind: 'job', id: JOB_ID});
    });

    it('picks the LABELLED uuid, not the first one in the text', () => {
        // The receipt also prints an owner id and an agent id. Grabbing the
        // first uuid would silently point the card at the wrong entity — and it
        // would still look plausible, because every field is a uuid.
        const owner = '52b14734-ed8c-464b-a045-4de8c36a18f1';
        const agent = '4336d637-d5c7-4748-8f6d-6c638e34d530';
        const entity = parseJobEntity(
            call({
                result:
                    `Owner user ID: ${owner}\nJob ID: ${JOB_ID}\nAgent: ${agent}`,
            }),
        );
        expect(entity).toEqual({kind: 'job', id: JOB_ID});
    });

    it('parses the verbatim result a dev job actually produced', () => {
        // Captured from thread_messages on the dev cluster, 2026-07-29. Pinning
        // the real string guards the parser against a wording change in the
        // tool's receipt — note it repeats the id inside a get_job()
        // hint, so a greedier regex would still work but a label-anchored one
        // must match the first, labelled occurrence.
        const realResult = [
            'Job created successfully.',
            `Job ID: ${JOB_ID}`,
            'Config: worker_base',
            'Overrides: {"scholar": {"enabled": false}}',
            'Priority: 5',
            'Description: Card live gate: write a short markdown note.',
            '',
            `A worker agent will pick this up from the dispatch queue. Use get_job('${JOB_ID}') to check progress.`,
        ].join('\n');
        expect(parseJobEntity(call({result: realResult}))).toEqual({kind: 'job', id: JOB_ID});
    });

    it('is absent for a failed call', () => {
        // A card for a job that was never created must not poll a nonexistent id.
        expect(parseJobEntity(call({status: 'error'}))).toBeUndefined();
    });

    it('is absent when the result carries no id', () => {
        expect(parseJobEntity(call({result: 'Job created successfully.'}))).toBeUndefined();
        expect(parseJobEntity(call({result: null}))).toBeUndefined();
    });

    it('never attaches to another tool', () => {
        expect(parseJobEntity(call({tool: 'send_message'}))).toBeUndefined();
    });
});

describe('buildToolCardView — job card', () => {
    it('carries the entity so the card can watch the row', () => {
        const view = buildToolCardView(call());
        expect(view.entity).toEqual({kind: 'job', id: JOB_ID});
    });

    it('uses the descriptor rather than the generic fallback', () => {
        const view = buildToolCardView(call());
        expect(view.title).toBe('Schedule job');
        expect(view.subtitle).toBe('Explore a warm-neutral theme');
    });

    it('does not echo the receipt as result content', () => {
        // "Job created successfully. Job ID: …" is a receipt, not something the
        // user needs re-read to them; the live panel replaces it. A descriptor
        // result kind of 'none' yields no result block at all (same as
        // set_canvas, whose result is likewise state metadata).
        expect(buildToolCardView(call()).result).toBeUndefined();
    });

    it('leaves every other card inert', () => {
        const view = buildToolCardView({
            tool: 'read_file',
            args: {path: '/tmp/x'},
            status: 'ok',
            result: `Job ID: ${JOB_ID}`,
        });
        expect(view.entity).toBeUndefined();
    });
});

describe('canonical job-tool recognition', () => {
    it('keeps create_job on the durable live job card', () => {
        expect(resolveToolDescriptor('create_job').result?.kind).toBe('none');
        expect(parseJobEntity(call())).toEqual({kind: 'job', id: JOB_ID});
    });

    it('recognises the other generated canonical job tools', () => {
        expect(resolveToolDescriptor('cancel_job').icon).toBe('hub');
        expect(resolveToolDescriptor('get_job_file').icon).toBe('manage_search');
        expect(resolveToolDescriptor('steer_job').dynamicParams).toBe(true);
    });

    it('does not retain the removed runtime spelling', () => {
        expect(resolveToolDescriptor('create_worker_job').icon).toBe('build');
        expect(parseJobEntity(call({tool: 'create_worker_job'}))).toBeUndefined();
    });
});

describe('asRecord — the JSONB-is-a-string trap', () => {
    // Found by the dev live gate 2026-07-29: GET /api/jobs/{id} returns
    // `context` and `freeze_data` as raw JSON TEXT, while the cockpit Job model
    // typed them as objects. Indexing straight in compiles and silently yields
    // undefined forever — the card's summary simply never appeared, with no
    // error anywhere.
    it('parses the JSON string the API actually sends', () => {
        expect(asRecord('{"summary":"did the thing"}')).toEqual({summary: 'did the thing'});
    });

    it('passes a real object straight through', () => {
        expect(asRecord({summary: 'x'})).toEqual({summary: 'x'});
    });

    it('refuses everything that is not a usable object', () => {
        for (const v of [null, undefined, '', '   ', 'not json', '[1,2]', '"str"', '42', 42, []]) {
            expect(asRecord(v)).toBeNull();
        }
    });
});

describe('job status vocabulary', () => {
    it('treats only completed/failed/cancelled as terminal', () => {
        for (const s of ['completed', 'failed', 'cancelled']) {
            expect(isTerminalJobStatus(s)).toBe(true);
        }
    });

    it('does NOT treat pending_review as terminal', () => {
        // The poller stops at terminal. If pending_review counted, the card
        // would freeze on "awaiting review" and never notice the approval that
        // flips it to completed — which is the one transition the review loop
        // exists to show.
        expect(isTerminalJobStatus('pending_review')).toBe(false);
    });

    it('does NOT treat paused as terminal', () => {
        // The dispatcher re-picks a paused job; it is a pause, not an ending.
        expect(isTerminalJobStatus('paused')).toBe(false);
    });

    it('handles absent status without claiming terminal', () => {
        expect(isTerminalJobStatus(null)).toBe(false);
        expect(isTerminalJobStatus(undefined)).toBe(false);
        expect(isTerminalJobStatus('')).toBe(false);
    });

    it('separates "working" from "waiting on a human"', () => {
        expect(isRunningJobStatus('processing')).toBe(true);
        expect(isRunningJobStatus('pending_review')).toBe(false);
        expect(isRunningJobStatus('completed')).toBe(false);
    });

    it('keeps the tones the two existing job surfaces already used', () => {
        expect(jobStatusTone('completed')).toBe('success');
        expect(jobStatusTone('failed')).toBe('danger');
        expect(jobStatusTone('pending_review')).toBe('warning');
        expect(jobStatusTone('blocked_undelivered')).toBe('warning');
        expect(jobStatusTone('reviewing')).toBe('accent');
        expect(jobStatusTone('anything-else')).toBe('neutral');
    });

    it('presents a terminal blocker distinctly and never offers resume', () => {
        const blocked = {
            status: 'cancelled',
            completion_outcome_kind: 'blocked_undelivered',
        };
        expect(effectiveJobStatus(blocked)).toBe('blocked_undelivered');
        expect(canResumeJob(blocked)).toBe(false);
        expect(effectiveJobStatus({status: 'failed'})).toBe('failed');
        expect(canResumeJob({status: 'failed'})).toBe(true);
    });

    it('uses creation resume advice even for a paused or created job', () => {
        expect(canResumeJob({status: 'failed', vm_creation: {resumable: false}})).toBe(false);
        expect(canResumeJob({status: 'paused', vm_creation: {resumable: true}})).toBe(true);
        expect(canResumeJob({status: 'created', vm_creation: {resumable: true}})).toBe(true);
        expect(canResumeJob({status: 'cancelled', vm_creation: {resumable: true}})).toBe(false);
    });
});
