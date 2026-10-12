import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  afterRenderEffect,
  computed,
  effect,
  inject,
  input,
  signal,
  untracked,
  viewChild,
} from '@angular/core';
import { TranslocoModule } from '@jsverse/transloco';
import { Call, isLiveCall } from '../../core/models/call.model';
import { CallService, CallWatch } from '../../core/services/call.service';
import { ViewportService } from '../../core/services/viewport.service';
import { AppBadgeComponent, BadgeTone } from '../../ui/badge';
import { AppButtonComponent } from '../../ui/button';
import { callDuration, projectCallTimeline } from './call-timeline';

@Component({
  selector: 'app-job-call-card',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoModule, AppBadgeComponent, AppButtonComponent],
  templateUrl: './job-call-card.component.html',
  styleUrl: './job-call-card.component.scss',
})
export class JobCallCardComponent {
  readonly jobId = input.required<string>();
  readonly initialCall = input.required<Call>();
  readonly viewport = inject(ViewportService);
  private readonly service = inject(CallService);
  private readonly identity = computed(() => `${this.jobId()}/${this.initialCall().id}`);
  readonly watch = signal<CallWatch | null>(null);
  readonly call = computed(() => this.watch()?.call() ?? this.initialCall());
  readonly journal = computed(() => projectCallTimeline(this.watch()?.events() ?? []));
  readonly terminal = computed(() => !isLiveCall(this.call()));
  readonly now = signal(Date.now());
  readonly duration = computed(() => callDuration(this.call(), this.now()));
  readonly following = signal(true);
  readonly transcript = viewChild<ElementRef<HTMLElement>>('transcript');
  readonly tone = computed<BadgeTone>(() => {
    switch (this.call().status) {
      case 'active':
        return 'success';
      case 'ringing':
        return 'warning';
      case 'failed':
        return 'danger';
      default:
        return 'neutral';
    }
  });

  constructor() {
    effect((onCleanup) => {
      this.identity();
      const watch = untracked(() => this.service.watchCall(this.jobId(), this.initialCall()));
      this.watch.set(watch);
      this.following.set(true);
      onCleanup(() => watch.destroy());
    });
    effect(() => {
      const initial = this.initialCall();
      const watch = this.watch();
      if (watch) untracked(() => watch.updateCall(initial));
    });
    effect((onCleanup) => {
      if (this.call().status !== 'active') return;
      this.now.set(Date.now());
      const timer = setInterval(() => this.now.set(Date.now()), 1000);
      onCleanup(() => clearInterval(timer));
    });
    afterRenderEffect(() => {
      this.journal();
      const target = this.transcript()?.nativeElement;
      if (target && this.following()) target.scrollTop = target.scrollHeight;
    });
  }

  onScroll(): void {
    const target = this.transcript()?.nativeElement;
    if (target)
      this.following.set(target.scrollHeight - target.scrollTop - target.clientHeight < 48);
  }

  jumpToLatest(): void {
    this.following.set(true);
    const target = this.transcript()?.nativeElement;
    if (target) target.scrollTop = target.scrollHeight;
  }
}

@Component({
  selector: 'app-job-call-panel',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [JobCallCardComponent, TranslocoModule],
  template: `
    @if (calls().length) {
      <section class="job-call-panel" [attr.aria-label]="'jobs.calls.title' | transloco">
        @for (call of calls(); track call.id) {
          <app-job-call-card [jobId]="jobId()" [initialCall]="call" />
        }
      </section>
    }
  `,
  styles: [
    `
      :host {
        display: block;
      }
      .job-call-panel {
        display: grid;
        gap: 16px;
        margin: 20px 0;
      }
    `,
  ],
})
export class JobCallPanelComponent {
  readonly jobId = input.required<string>();
  readonly calls = input<Call[]>([]);
}
