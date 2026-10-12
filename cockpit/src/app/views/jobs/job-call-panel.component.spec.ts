import { readFileSync } from 'node:fs';
import { CUSTOM_ELEMENTS_SCHEMA, signal, ɵresolveComponentResources } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { TranslocoModule, TranslocoService, TranslocoTestingModule } from '@jsverse/transloco';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { Call, CallEvent } from '../../core/models/call.model';
import {
  callEventFixture as event,
  callFixture as call,
} from '../../core/models/fixtures/call.fixture';
import { CallService } from '../../core/services/call.service';
import { ViewportService } from '../../core/services/viewport.service';
import { JobCallCardComponent, JobCallPanelComponent } from './job-call-panel.component';
import { callDuration, projectCallTimeline } from './call-timeline';
import en from '../../../assets/i18n/en.json';
import de from '../../../assets/i18n/de-DE.json';

describe('call timeline projection', () => {
  it('orders/deduplicates deltas, merges partial words and separates speakers', () => {
    const rows = [
      event(3, { speaker: 'assistant', text: 'Ja.' }),
      event(2, { text: 'bad?' }),
      event(1, { text: 'Haben Sie ein Schwimm' }),
      event(2, { text: 'bad?' }),
    ];
    expect(projectCallTimeline(rows).timeline).toEqual([
      { type: 'transcript', id: 1, speaker: 'caller', text: 'Haben Sie ein Schwimmbad?' },
      { type: 'transcript', id: 3, speaker: 'assistant', text: 'Ja.' },
    ]);
  });

  it('links a question and answer to the delegation in its original transcript position', () => {
    const rows = [
      event(1, { text: 'Schwimmbad?' }),
      event(2, { kind: 'delegation', delegation_id: 'd-1', text: 'Frage prüfen' }),
      event(3, {
        kind: 'question',
        delegation_id: 'd-1',
        text: 'Envelope',
        payload: { request: 'Haben Sie ein Schwimmbad?' },
      }),
      event(4, { speaker: 'assistant', text: 'Ich frage nach.' }),
      event(5, { kind: 'answer', delegation_id: 'd-1', text: 'Ja, ein Therapiebecken.' }),
    ];
    const result = projectCallTimeline(rows);
    expect(result.timeline.map((item) => item.id)).toEqual([1, 2, 4]);
    expect(result.timeline[1]).toMatchObject({
      type: 'backoffice',
      state: 'answered',
      request: 'Haben Sie ein Schwimmbad?',
      response: 'Ja, ein Therapiebecken.',
    });
    expect(result.followUps).toEqual([]);
  });

  it('keeps independent delegations separate even when answers arrive in reverse order', () => {
    const result = projectCallTimeline([
      event(1, { kind: 'question', delegation_id: 'a', text: 'Becken?' }),
      event(2, { kind: 'question', delegation_id: 'b', text: 'Termin?' }),
      event(3, { kind: 'answer', delegation_id: 'b', text: 'Dienstag.' }),
      event(4, { kind: 'answer', delegation_id: 'a', text: 'Ja.' }),
    ]);
    expect(result.timeline[0]).toMatchObject({ request: 'Becken?', response: 'Ja.' });
    expect(result.timeline[1]).toMatchObject({ request: 'Termin?', response: 'Dienstag.' });
  });

  it('includes every explicit follow-up and unanswered requests without duplicating linked questions', () => {
    const result = projectCallTimeline([
      event(1, { kind: 'question', delegation_id: 'a', text: 'Termin?' }),
      event(2, { kind: 'fallback', delegation_id: 'a', text: 'Wir rufen zurück.' }),
      event(3, {
        kind: 'follow_up',
        delegation_id: 'a',
        text: '',
        payload: { request: 'Termin vereinbaren' },
      }),
      event(4, { kind: 'follow_up', delegation_id: 'a', text: 'Unterlagen zusenden' }),
      event(5, { kind: 'question', delegation_id: 'b', text: 'Kosten?' }),
      event(6, { kind: 'question', delegation_id: null, text: 'Anreise?' }),
    ]);
    expect(result.followUps).toEqual([
      { id: 3, text: 'Termin vereinbaren' },
      { id: 4, text: 'Unterlagen zusenden' },
      { id: 5, text: 'Kosten?' },
      { id: 6, text: 'Anreise?' },
    ]);
    expect(result.timeline[0]).toMatchObject({ state: 'followUp' });
  });

  it('shows unanswered fallback requests in the post-call summary', () => {
    const result = projectCallTimeline([
      event(1, { kind: 'question', delegation_id: 'a', text: 'Termin?' }),
      event(2, { kind: 'fallback', delegation_id: 'a', text: 'Wir rufen zurück.' }),
    ]);
    expect(result.timeline[0]).toMatchObject({ state: 'noted', response: 'Wir rufen zurück.' });
    expect(result.followUps).toEqual([{ id: 1, text: 'Termin?' }]);
  });

  it('does not merge caller bubbles across steering or hidden usage rows', () => {
    const result = projectCallTimeline([
      event(1),
      event(2, { kind: 'instruction', speaker: 'job', text: 'Kurz antworten.' }),
      event(3),
      event(4, { kind: 'usage', text: '' }),
      event(5),
    ]);
    expect(result.timeline.filter((item) => item.type === 'transcript')).toHaveLength(3);
    expect(result.timeline[1]).toMatchObject({ type: 'system', kind: 'instruction' });
  });

  it('formats live, final, long and missing durations', () => {
    expect(callDuration(call(), Date.parse('2026-10-12T08:01:07Z'))).toBe('1:02');
    expect(callDuration(call({ status: 'ended', ended_at: '2026-10-12T08:00:47Z' }), 0)).toBe(
      '0:42',
    );
    expect(callDuration(call({ status: 'ended', accepted_at: null }), 0)).toBe('0:42');
    expect(callDuration(call({ status: 'ended', accepted_at: null, usage_seconds: 3661 }), 0)).toBe(
      '1:01:01',
    );
    expect(callDuration(call({ status: 'ringing' }), 0)).toBeNull();
    expect(callDuration(call({ accepted_at: null }), 0)).toBeNull();
    expect(callDuration(call({ accepted_at: 'invalid' }), 0)).toBeNull();
    expect(callDuration(call(), 0)).toBe('0:00');
  });
});

describe('JobCallPanelComponent / JobCallCardComponent', () => {
  beforeAll(async () => {
    const template = readFileSync('src/app/views/jobs/job-call-card.component.html', 'utf8');
    await ɵresolveComponentResources((url) =>
      Promise.resolve(url.endsWith('.html') ? template : ''),
    );
  });
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-10-12T08:00:47Z'));
    TestBed.overrideComponent(JobCallCardComponent, {
      set: {
        template: readFileSync('src/app/views/jobs/job-call-card.component.html', 'utf8'),
        templateUrl: undefined,
        styleUrl: undefined,
        styleUrls: [],
        styles: [],
        imports: [TranslocoModule],
        schemas: [CUSTOM_ELEMENTS_SCHEMA],
      },
    });
  });
  afterEach(() => {
    TestBed.resetTestingModule();
    vi.useRealTimers();
  });

  function mount(rows: CallEvent[] = [], initial: Call = call(), locale = 'en') {
    const watch = {
      call: signal(initial),
      events: signal(rows),
      connected: signal(true),
      loading: signal(false),
      error: signal(false),
      updateCall: vi.fn(),
      refresh: vi.fn().mockResolvedValue(true),
      destroy: vi.fn(),
    };
    const watchCall = vi.fn().mockReturnValue(watch);
    const mobile = signal(false);
    TestBed.configureTestingModule({
      imports: [
        JobCallCardComponent,
        TranslocoTestingModule.forRoot({
          langs: { en, 'de-DE': de },
          translocoConfig: { availableLangs: ['en', 'de-DE'], defaultLang: locale },
        }),
      ],
      providers: [
        { provide: CallService, useValue: { watchCall } },
        { provide: ViewportService, useValue: { isMobile: mobile } },
      ],
    });
    // Vitest JIT cannot bind initializer-based inputs of design-system
    // children. Render the real feature template with custom element shells;
    // the production AOT build validates the primitive bindings.
    const transloco = TestBed.inject(TranslocoService);
    transloco.setTranslation(en, 'en');
    transloco.setTranslation(de, 'de-DE');
    transloco.setActiveLang(locale);
    const fixture = TestBed.createComponent(JobCallCardComponent);
    const initialInput = signal(initial);
    Object.defineProperty(fixture.componentInstance, 'initialCall', { value: initialInput });
    Object.defineProperty(fixture.componentInstance, 'jobId', { value: signal('job-1') });
    fixture.detectChanges();
    // Initializer-based viewChild queries need the same signal mock as inputs
    // in this JIT harness. DOM geometry is set explicitly in the scroll specs.
    Object.defineProperty(fixture.componentInstance, 'transcript', {
      value: signal({
        nativeElement: (fixture.nativeElement as HTMLElement).querySelector('.call-transcript'),
      }),
    });
    return {
      fixture,
      component: fixture.componentInstance,
      root: fixture.nativeElement as HTMLElement,
      watch,
      watchCall,
      mobile,
      initialInput,
    };
  }

  it('renders consecutive fragments as one bubble and labels both speakers', () => {
    const { root } = mount([
      event(1, { text: 'Haben Sie ein Schwimm' }),
      event(2, { text: 'bad?' }),
      event(3, { speaker: 'assistant', text: 'Ich frage nach.' }),
    ]);
    expect(root.querySelectorAll('.transcript-bubble')).toHaveLength(2);
    expect(root.querySelector('.transcript-bubble')?.textContent).toContain(
      'Haben Sie ein Schwimmbad?',
    );
    expect(root.querySelector('.transcript-bubble--assistant')?.textContent).toContain(
      'Voice assistant',
    );
  });

  it('updates the inline back-office card when a linked answer arrives', () => {
    const { fixture, root, watch } = mount([
      event(1, { kind: 'delegation', delegation_id: 'd-1' }),
      event(2, { kind: 'question', delegation_id: 'd-1', payload: { request: 'Therapiebecken?' } }),
    ]);
    expect(root.querySelectorAll('.backoffice-card')).toHaveLength(1);
    expect(root.querySelector('.backoffice-card')?.getAttribute('data-state')).toBe('pending');
    watch.events.update((rows) => [
      ...rows,
      event(3, { kind: 'answer', delegation_id: 'd-1', text: 'Ja, täglich geöffnet.' }),
    ]);
    fixture.detectChanges();
    expect(root.querySelectorAll('.backoffice-card')).toHaveLength(1);
    expect(root.querySelector('.backoffice-card')?.getAttribute('data-state')).toBe('answered');
    expect(root.querySelector('.backoffice-response')?.textContent).toContain(
      'Ja, täglich geöffnet.',
    );
  });

  it('renders fallback, follow-up and steering text in German with a terminal summary', () => {
    const { root } = mount(
      [
        event(1, { kind: 'question', delegation_id: 'a', text: 'Termin?' }),
        event(2, { kind: 'fallback', delegation_id: 'a', text: 'Wir rufen zurück.' }),
        event(3, { kind: 'question', delegation_id: 'b', payload: { request: 'Kosten?' } }),
        event(4, { kind: 'follow_up', delegation_id: 'b', text: 'Kosten ermitteln' }),
        event(5, { kind: 'instruction', speaker: 'job', text: 'Höflich bleiben.' }),
      ],
      call({ status: 'ended', ended_at: '2026-10-12T08:00:47Z' }),
      'de-DE',
    );
    expect(root.textContent).toContain('Rückfrage an das Backoffice');
    expect(root.textContent).toContain('Notiert');
    expect(root.textContent).toContain('Follow-up');
    expect(root.querySelector('.system-line')?.textContent).toContain('Steuerung:');
    expect(root.querySelector('.call-follow-ups')?.textContent).toContain('Termin?');
    expect(root.querySelectorAll('.call-follow-ups li')).toHaveLength(2);
  });

  it('keeps post-call follow-ups out of the active-call footer', () => {
    const { root } = mount([event(1, { kind: 'follow_up', text: 'Termin vereinbaren' })]);
    expect(root.querySelector('.backoffice-card')?.textContent).toContain('Follow-up');
    expect(root.querySelector('.call-follow-ups')).toBeNull();
  });

  it('renders a live status, direction and advancing duration, then freezes after hang-up', async () => {
    const { fixture, component, root, watch } = mount();
    expect(root.querySelector('.call-state')?.textContent).toContain('Live');
    expect(root.querySelector('.live-dot')).not.toBeNull();
    expect(root.querySelector('.direction-icon')?.textContent).toContain('call_received');
    expect(root.querySelector('.call-duration')?.textContent).toBe('0:42');
    expect(component.tone()).toBe('success');
    await vi.advanceTimersByTimeAsync(1000);
    fixture.detectChanges();
    expect(root.querySelector('.call-duration')?.textContent).toBe('0:43');
    watch.call.set(
      call({ status: 'ended', ended_at: '2026-10-12T08:00:48Z', close_reason: 'remote_hangup' }),
    );
    fixture.detectChanges();
    await vi.advanceTimersByTimeAsync(5000);
    fixture.detectChanges();
    expect(root.querySelector('.live-dot')).toBeNull();
    expect(root.querySelector('.call-state')?.textContent).toContain('Ended');
    expect(root.querySelector('.close-reason')?.textContent).toContain('Caller hung up');
    expect(root.querySelector('.call-duration')?.textContent).toBe('0:43');
  });

  it.each(['ringing', 'failed', 'rejected'] as const)(
    'renders the %s pill without the live pulse',
    (status) => {
      const { root } = mount([], call({ status, direction: 'outbound' }));
      expect(root.querySelector('.call-state')?.textContent).toContain(
        en.jobs.calls.status[status],
      );
      expect(root.querySelector('.live-dot')).toBeNull();
      expect(root.querySelector('.direction-icon')?.textContent).toContain('call_made');
      if (status === 'ringing') expect(root.querySelector('.call-duration')).toBeNull();
    },
  );

  it('pauses scroll following when the reader scrolls up, then jumps to latest', () => {
    const { fixture, component, root, watch } = mount([event(1)]);
    const transcript = root.querySelector('.call-transcript') as HTMLElement;
    Object.defineProperty(transcript, 'scrollHeight', { value: 1000, configurable: true });
    Object.defineProperty(transcript, 'clientHeight', { value: 200 });
    transcript.scrollTop = 300;
    transcript.dispatchEvent(new Event('scroll'));
    expect(component.following()).toBe(false);
    watch.events.update((rows) => [...rows, event(2)]);
    fixture.detectChanges();
    expect(transcript.scrollTop).toBe(300);
    expect(root.querySelector('.jump-latest')).not.toBeNull();
    root.querySelector('.jump-latest app-button')?.dispatchEvent(new CustomEvent('clicked'));
    fixture.detectChanges();
    expect(component.following()).toBe(true);
    expect(transcript.scrollTop).toBe(1000);
    expect(root.querySelector('.jump-latest')).toBeNull();
  });

  it('automatically follows new transcript text while the reader is at the bottom', () => {
    const { fixture, root, watch } = mount([event(1)]);
    const transcript = root.querySelector('.call-transcript') as HTMLElement;
    Object.defineProperty(transcript, 'scrollHeight', { value: 1000 });
    watch.events.update((rows) => [...rows, event(2)]);
    fixture.detectChanges();
    expect(transcript.scrollTop).toBe(1000);
  });

  it('keeps the same stream owner on a job-detail refresh and disposes it on close', () => {
    const { fixture, watchCall, initialInput, watch } = mount();
    initialInput.set(call({ usage_seconds: 45 }));
    fixture.detectChanges();
    expect(watchCall).toHaveBeenCalledOnce();
    expect(watch.updateCall).toHaveBeenLastCalledWith(call({ usage_seconds: 45 }));
    fixture.destroy();
    expect(watch.destroy).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('uses ViewportService for the same responsive card', () => {
    const { fixture, root, mobile } = mount();
    mobile.set(true);
    fixture.detectChanges();
    expect(root.querySelector('.call-card--mobile')).not.toBeNull();
    expect(root.querySelectorAll('.call-transcript')).toHaveLength(1);
  });

  it('renders transcript text as text, including untrusted markup', () => {
    const { root } = mount([event(1, { text: '<img src=x onerror=alert(1)>' })]);
    expect(root.querySelector('.transcript-bubble')?.textContent).toContain(
      '<img src=x onerror=alert(1)>',
    );
    expect(root.querySelector('img')).toBeNull();
  });

  it('hides the panel when the job has no calls', () => {
    TestBed.configureTestingModule({ imports: [JobCallPanelComponent] });
    const fixture = TestBed.createComponent(JobCallPanelComponent);
    Object.defineProperty(fixture.componentInstance, 'calls', { value: signal([]) });
    fixture.detectChanges();
    expect((fixture.nativeElement as HTMLElement).querySelector('.job-call-panel')).toBeNull();
    expect((fixture.nativeElement as HTMLElement).querySelector('app-job-call-card')).toBeNull();
  });
});
