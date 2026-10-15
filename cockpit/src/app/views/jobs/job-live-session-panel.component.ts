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
import { TranslocoModule, TranslocoService } from '@jsverse/transloco';
import { LiveSession, isActiveLiveSession } from '../../core/models/live-session.model';
import { LiveSessionService, LiveSessionWatch } from '../../core/services/live-session.service';
import { ApiService } from '../../core/services/api.service';
import { ViewportService } from '../../core/services/viewport.service';
import { AppBadgeComponent, BadgeTone } from '../../ui/badge';
import { AppButtonComponent } from '../../ui/button';
import { sessionDuration, projectCallTimeline } from './live-session-timeline';

@Component({
  selector: 'app-job-live-session-card',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TranslocoModule, AppBadgeComponent, AppButtonComponent],
  templateUrl: './job-live-session-card.component.html',
  styleUrl: './job-live-session-card.component.scss',
})
export class JobLiveSessionCardComponent {
  private readonly transloco = inject(TranslocoService);
  readonly closeReason = computed(() => {
    const reason = this.session().close_reason;
    const key = `jobs.liveSessions.reason.${reason}`;
    const translated = this.transloco.translate(key);
    return translated === key ? reason : translated;
  });
  readonly jobId = input.required<string>();
  readonly initialSession = input.required<LiveSession>();
  readonly viewport = inject(ViewportService);
  private readonly service = inject(LiveSessionService);
  private readonly api = inject(ApiService);
  private readonly connectorId = computed(() => this.session().connector_id);
  readonly connectorName = signal('');
  private readonly identity = computed(() => `${this.jobId()}/${this.initialSession().id}`);
  readonly watch = signal<LiveSessionWatch | null>(null);
  readonly session = computed(() => this.watch()?.session() ?? this.initialSession());
  readonly journal = computed(() => projectCallTimeline(this.watch()?.events() ?? []));
  readonly terminal = computed(() => !isActiveLiveSession(this.session()));
  readonly now = signal(Date.now());
  readonly duration = computed(() => sessionDuration(this.session(), this.now()));
  readonly following = signal(true);
  readonly transcript = viewChild<ElementRef<HTMLElement>>('transcript');
  readonly tone = computed<BadgeTone>(() => {
    switch (this.session().status) {
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
      const id = this.connectorId();
      this.connectorName.set('');
      const read = this.api.getDatasource(id).subscribe({
        next: (connector) => this.connectorName.set(connector?.name ?? ''),
        error: () => this.connectorName.set(''),
      });
      onCleanup(() => read.unsubscribe());
    });
    effect((onCleanup) => {
      this.identity();
      const watch = untracked(() => this.service.watchSession(this.jobId(), this.initialSession()));
      this.watch.set(watch);
      this.following.set(true);
      onCleanup(() => watch.destroy());
    });
    effect(() => {
      const initial = this.initialSession();
      const watch = this.watch();
      if (watch) untracked(() => watch.updateSession(initial));
    });
    effect((onCleanup) => {
      if (this.session().status !== 'active') return;
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
  selector: 'app-job-live-session-panel',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [JobLiveSessionCardComponent, TranslocoModule],
  template: `
    @if (sessions().length) {
      <section class="job-live-session-panel" [attr.aria-label]="'jobs.liveSessions.title' | transloco">
        @for (session of sessions(); track session.id) {
          <app-job-live-session-card [jobId]="jobId()" [initialSession]="session" />
        }
      </section>
    }
  `,
  styles: [
    `
      :host {
        display: block;
      }
      .job-live-session-panel {
        display: grid;
        gap: 16px;
        margin: 20px 0;
      }
    `,
  ],
})
export class JobLiveSessionPanelComponent {
  readonly jobId = input.required<string>();
  readonly sessions = input<LiveSession[]>([]);
}
