import {Component, EventEmitter, Input, Output} from '@angular/core';
import {ToolCardView} from '../../core/models/tool-card.model';

/**
 * TEST-ONLY stand-in for `<app-tool-card>`, exported under the real name so a
 * spec can `vi.mock('./tool-card.component', () => import('./tool-card.stub'))`.
 *
 * The real card declares `view = input.required<ToolCardView>()`. This
 * project's vitest pipeline drops signal-input metadata when a parent template
 * binds it (see `job-tool-card-panel.stub.ts`), so the real card throws NG0950
 * inside any parent under test. Decorator inputs still bind. The card's own
 * behaviour is covered by `tool-card.component.spec.ts`.
 *
 * Not referenced by application code and therefore never bundled.
 */
@Component({
    selector: 'app-tool-card',
    standalone: true,
    template: '<span class="tc-stub">{{ view?.tool }}:{{ view?.subtitle }}</span>',
})
export class AppToolCardComponent {
    @Input() view?: ToolCardView;
    @Input() defaultOpen = false;
    @Output() actionRequested = new EventEmitter<unknown>();
    @Output() jobDiffRequested = new EventEmitter<string>();
}
