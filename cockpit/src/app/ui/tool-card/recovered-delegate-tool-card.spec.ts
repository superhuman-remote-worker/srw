import {signal, ɵresolveComponentResources} from '@angular/core';
import {provideHttpClient} from '@angular/common/http';
import {provideHttpClientTesting} from '@angular/common/http/testing';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import {provideMarkdown} from 'ngx-markdown';
import {afterEach, beforeAll, beforeEach, describe, expect, it, vi} from 'vitest';
import {SubagentRecoveryResult} from '../../core/models/subagent-recovery.model';
import {ToolCardView} from '../../core/models/tool-card.model';
import {ToolCallEvent} from '../../core/models/turn.model';
import {CanvasService} from '../../core/services/canvas.service';
import {toolCardViewFromEvent} from '../../core/tools/tool-card-adapters';
import {buildToolCardView} from '../../core/tools/tool-descriptors';
import {AppToolCardComponent} from './tool-card.component';

// The result header's copy button has a required signal input this pipeline
// cannot bind; see markdown-tool-card.spec.ts.
vi.mock('../icon-button', () => import('./icon-button.stub'));

/**
 * The ordinary card of a lone `delegate_agent` call whose result a
 * delegation-batch recovery wrote. One settle covers every message of the
 * turn (plan §5.1), so a call that is not part of a fan-out can be settled as
 * interrupted or not started too; its card must say so, with the label and
 * tone the same call has as a batch-card row, instead of a plain "completed"
 * over text that says NOT STARTED.
 *
 * Design: knowledge-base/knowledge/features/parallel_subagents.md §6.5 step 3.
 */

function delegateCall(recovery?: SubagentRecoveryResult): ToolCallEvent {
    return {
        kind: 'tool_call',
        id: 'd1',
        tool: 'delegate_agent',
        args: {subagent_type: 'explorer', description: 'Map the auth flow'},
        status: 'completed',
        result: 'text for the model',
        startedAt: 0,
        ...(recovery ? {recovery} : {}),
    };
}

describe('the ordinary tool card of a recovered delegate_agent call', () => {
    beforeAll(async () => {
        await ɵresolveComponentResources(() => Promise.resolve(''));
    });

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [
                AppToolCardComponent,
                TranslocoTestingModule.forRoot({
                    langs: {
                        en: {
                            toolCard: {
                                status: {ok: 'completed', denied: 'denied'},
                                titles: {delegate_agent: 'Delegate to subagent'},
                                sections: {result: 'Result'},
                                delegateBatch: {status: {notStarted: 'Not started'}},
                            },
                            jobs: {detail: {subagentsStatuses: {interrupted: 'Interrupted', cancelled: 'Cancelled'}}},
                        },
                    },
                    translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
                    // The pill label goes through translate(), which needs the
                    // language loaded before the first render.
                    preloadLangs: true,
                }),
            ],
            providers: [
                provideHttpClient(),
                provideHttpClientTesting(),
                provideMarkdown(),
                {provide: CanvasService, useValue: {state: vi.fn(() => null)}},
            ],
        });
    });

    afterEach(() => TestBed.resetTestingModule());

    /** Renders the real card; signal inputs don't compile here — see notify-user-tool-card.spec.ts. */
    async function render(view: ToolCardView): Promise<ComponentFixture<AppToolCardComponent>> {
        await TestBed.compileComponents();
        const fixture = TestBed.createComponent(AppToolCardComponent);
        (fixture.componentInstance as unknown as {view: () => ToolCardView}).view = signal(view);
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();
        return fixture;
    }

    function pill(fixture: ComponentFixture<AppToolCardComponent>) {
        const el = (fixture.nativeElement as HTMLElement).querySelector('.tc__status') as HTMLElement;
        const icon = el.querySelector('.tc__status-icon')?.textContent?.trim();
        const label = el.textContent!.replace(icon ?? '', '').trim();
        return {classes: [...el.classList].filter((c) => c !== 'tc__status'), icon, label};
    }

    it.each([
        ['not_started', null, 'Not started', 'tc__status--tone-neutral', 'do_not_disturb_on'],
        ['interrupted', 'interrupted', 'Interrupted', 'tc__status--tone-warning', 'warning'],
        ['retired', 'cancelled', 'Cancelled', 'tc__status--tone-danger', 'cancel'],
    ] as const)('shows %s as the batch row does', async (cls, subagentStatus, label, tone, icon) => {
        const fixture = await render(toolCardViewFromEvent(delegateCall({class: cls, subagentStatus})));
        expect(pill(fixture)).toEqual({classes: [tone], icon, label});
        // Still a finished call otherwise: not auto-opened like an error.
        expect((fixture.nativeElement as HTMLElement).querySelector('details.tc')?.hasAttribute('open')).toBe(false);
    });

    it('renders a call without a marker exactly as before', async () => {
        const view = toolCardViewFromEvent(delegateCall());
        expect(view).toStrictEqual(
            buildToolCardView({
                tool: 'delegate_agent',
                args: {subagent_type: 'explorer', description: 'Map the auth flow'},
                status: 'ok',
                result: 'text for the model',
                durationMs: undefined,
                exitCode: undefined,
            }),
        );
        const fixture = await render(view);
        expect(pill(fixture)).toEqual({classes: ['tc__status--ok'], icon: 'check_circle', label: 'completed'});
    });

    it('leaves a recovered completed child on its own "completed"', async () => {
        const fixture = await render(toolCardViewFromEvent(delegateCall({class: 'completed', subagentStatus: 'completed'})));
        expect(pill(fixture)).toEqual({classes: ['tc__status--ok'], icon: 'check_circle', label: 'completed'});
    });

    it('never marks another tool, whatever its row carries', () => {
        const other = {...delegateCall({class: 'not_started', subagentStatus: null}), tool: 'run_command'};
        expect(toolCardViewFromEvent(other).outcome).toBeUndefined();
    });
});
