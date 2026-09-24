import {ChangeDetectionStrategy, Component, input} from '@angular/core';
import {DatePipe} from '@angular/common';
import {TranslocoPipe} from '@jsverse/transloco';
import {VMCreationView} from '../models/api.model';

/** One owner's wait. It intentionally has no fleet count, rank or ETA. */
@Component({
  selector: 'app-vm-creation-wait',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [DatePipe, TranslocoPipe],
  template: `
    @if (creation().wait; as wait) {
      <div class="wait" data-testid="vm-owner-wait" [attr.data-kind]="wait.kind" role="status">
        <strong>{{ ('vmWait.kind.' + wait.kind) | transloco }}</strong>
        @if (wait.size_nonfit) {
          <span>{{ 'vmWait.sizeNonfit' | transloco }}</span>
        }
        @if (wait.guest_vcpus && wait.guest_memory_bytes) {
          <span>{{ 'vmWait.requested' | transloco }}: {{ wait.guest_vcpus }} vCPU · {{ sizeGiB(wait.guest_memory_bytes) }} GiB</span>
        }
        @if (wait.since) {
          <span>{{ 'vmWait.since' | transloco }} {{ wait.since | date:'short' }}</span>
        }
      </div>
    }
  `,
  styles: [`
    :host { display: block; min-width: 0; }
    .wait { display: flex; flex-wrap: wrap; gap: 3px 10px; align-items: baseline;
      margin: 6px 0; padding: 8px 10px; border-left: 3px solid var(--warning);
      border-radius: var(--radius-control); background: var(--panel-bg);
      color: var(--text-secondary); font-size: 12px; overflow-wrap: anywhere; }
    strong { color: var(--text-primary); font-size: 12px; }
  `],
})
export class VMCreationWaitComponent {
  readonly creation = input.required<VMCreationView>();

  sizeGiB(bytes: number): string {
    return (bytes / (1024 ** 3)).toFixed(1);
  }
}
