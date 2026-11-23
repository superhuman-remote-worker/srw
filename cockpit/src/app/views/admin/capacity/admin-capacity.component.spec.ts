import { CUSTOM_ELEMENTS_SCHEMA, Pipe, PipeTransform, ɵresolveComponentResources } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { of } from 'rxjs';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { TranslocoService } from '@jsverse/transloco';
import { ApiService } from '../../../core/services/api.service';
import { AppToastService } from '../../../ui/toast';
import { AdminCapacityComponent } from './admin-capacity.component';

const capacity = {
  observed_at: '2026-09-08T15:00:00Z',
  executors: { total: 3, ready: 3, busy: 2 },
  queued: { session_turn: 1, worker_batch: 0, total: 1 },
  oldest_queued_age_s: 42,
  desired: 4,
  params: { min_replicas: 2, reserve: 1 },
  parked: [
    {
      unit_id: 'ad7eb761-4f9a-4ba4-8020-f0e365c75d0b',
      unit_kind: 'session_turn',
      thread_id: 'ad7eb761-4f9a-4ba4-8020-f0e365c75d0b',
      title: 'Comparing Take-Home Pay',
      owner: 'knaeckebrothero',
      park_reason: 'attach_failed',
      parked_at: '2026-09-08T14:50:00Z',
      attempts: 12,
      pending_input: true,
    },
  ],
};

const api = {
  getAdminCapacity: vi.fn(() => of(capacity)),
  unparkRunQueueUnit: vi.fn(() => of({ unit_id: capacity.parked[0].unit_id, state: 'queued' })),
};
const toast = { success: vi.fn(), danger: vi.fn() };
const transloco = { translate: vi.fn((key: string) => key) };

/** Keys straight through — the template is asserted on structure, not copy. */
@Pipe({ name: 'transloco', standalone: true })
class TranslocoStubPipe implements PipeTransform {
  transform(key: string): string {
    return key;
  }
}

describe('AdminCapacityComponent', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    api.getAdminCapacity.mockClear();
    api.unparkRunQueueUnit.mockClear();
    toast.success.mockClear();
    toast.danger.mockClear();
    TestBed.configureTestingModule({
      imports: [AdminCapacityComponent],
      providers: [
        { provide: ApiService, useValue: api },
        { provide: AppToastService, useValue: toast },
        { provide: TranslocoService, useValue: transloco },
      ],
    });
    TestBed.overrideComponent(AdminCapacityComponent, {
      set: { imports: [TranslocoStubPipe], schemas: [CUSTOM_ELEMENTS_SCHEMA] },
    });
  });

  it('renders executors, queue depth, desired, and the parked list', async () => {
    const fixture = TestBed.createComponent(AdminCapacityComponent);
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();

    expect(api.getAdminCapacity).toHaveBeenCalledTimes(1);
    const host = fixture.nativeElement as HTMLElement;
    const kpis = host.querySelector('[data-testid="capacity-kpis"]');
    expect(kpis).not.toBeNull();
    expect(kpis!.textContent).toContain('3');
    expect(kpis!.textContent).toContain('4');
    const rows = host.querySelectorAll('[data-testid="parked-table"] tbody tr');
    expect(rows).toHaveLength(1);
    expect(rows[0].textContent).toContain('Comparing Take-Home Pay');
    expect(rows[0].textContent).toContain('attach_failed');
    expect(rows[0].textContent).toContain('12');
  });

  it('Unpark calls the admin verb for that unit and reloads', async () => {
    const fixture = TestBed.createComponent(AdminCapacityComponent);
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();

    const button = (fixture.nativeElement as HTMLElement).querySelector<HTMLButtonElement>(
      '[data-testid="unpark"]',
    );
    expect(button).not.toBeNull();
    button!.click();
    await fixture.whenStable();
    fixture.detectChanges();

    expect(api.unparkRunQueueUnit).toHaveBeenCalledWith(capacity.parked[0].unit_id);
    expect(toast.success).toHaveBeenCalled();
    expect(api.getAdminCapacity).toHaveBeenCalledTimes(2);
  });

  it('shows the load-failed line when the read degrades to null', async () => {
    api.getAdminCapacity.mockReturnValueOnce(of(null as any));
    const fixture = TestBed.createComponent(AdminCapacityComponent);
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    const host = fixture.nativeElement as HTMLElement;
    expect(host.querySelector('.load-failed')).not.toBeNull();
    expect(host.querySelector('[data-testid="capacity-kpis"]')).toBeNull();
  });

  it('shows the stalled banner only when chat_pool_stalled', async () => {
    const stalled = {
      ...capacity,
      chat_pool_stalled: true,
      stall_threshold_s: 300,
      queued: { session_turn: 3, worker_batch: 0, total: 3 },
      oldest_queued_age_s: 600,
      oldest_session_turn_age_s: 600,
    };
    api.getAdminCapacity.mockReturnValueOnce(of(stalled as any));
    let fixture = TestBed.createComponent(AdminCapacityComponent);
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    const banner = (fixture.nativeElement as HTMLElement).querySelector('[data-testid="capacity-stalled"]');
    expect(banner).not.toBeNull();
    expect(banner!.getAttribute('role')).toBe('alert');
    expect(banner!.textContent).toContain('admin.capacity.stalled');

    api.getAdminCapacity.mockReturnValueOnce(
      of({ ...stalled, chat_pool_stalled: false } as any),
    );
    fixture = TestBed.createComponent(AdminCapacityComponent);
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    expect(
      (fixture.nativeElement as HTMLElement).querySelector('[data-testid="capacity-stalled"]'),
    ).toBeNull();
  });
});
