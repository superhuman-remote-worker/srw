import {signal, ɵresolveComponentResources} from '@angular/core';
import {TestBed} from '@angular/core/testing';
import {TranslocoTestingModule} from '@jsverse/transloco';
import {beforeAll, beforeEach, describe, expect, it} from 'vitest';
import en from '../../../../assets/i18n/en.json';
import {AdminVMCapacity, VMResourceCategory, VMResourceVector} from '../../../core/models/api.model';
import {AdminVMCapacityComponent} from './admin-vm-capacity.component';

const zero: VMResourceVector = {
  cpu_millicores: 0, memory_bytes: 0, ephemeral_storage_bytes: 0,
  kvm_devices: 0, tun_devices: 0, vhost_net_devices: 0,
};
const rows: VMResourceCategory[] = [
  'allocatable', 'headroom', 'external', 'unbound', 'bound_reserved',
  'active', 'warm', 'teardown', 'available', 'shortfall',
];
const totals = Object.fromEntries(rows.map(row => [row, zero])) as Record<VMResourceCategory, VMResourceVector>;
const held = {
  unbound: zero, bound_reserved: zero, active: zero, warm: zero, teardown: zero,
  total: {...zero, cpu_millicores: 2000, ephemeral_storage_bytes: null},
};

function capacity(available = false): AdminVMCapacity {
  return {
    available, reason: available ? null : 'inventory_stale',
    observed_at: '2026-09-24T04:00:00Z',
    clusters: [{
      cluster_id: 'cluster-a', namespace: 'workers', mode: available ? 'shadow' : 'drain',
      policy_digest: 'sha256:' + 'a'.repeat(64),
      available, reason: available ? null : 'inventory_stale',
      inventory: available ? {
        observed_at: '2026-09-24T04:00:00Z', received_at: '2026-09-24T04:00:01Z',
        age_seconds: 1, complete: true, fresh: true, stale_after_seconds: 60,
      } : null,
      held, waiting: {count: 2, nonfit: 1, oldest_age_seconds: 80, bypasses: 3, protected: 1},
      teardown: {count: 1, unknown_age: 1, overdue: 0, oldest_progress_age_seconds: null, overdue_after_seconds: 300},
      totals: available ? totals : null,
      nodes: available ? [{name: 'worker-a', general_exclusion: null, request_fit_required: true, resources: totals}] : null,
      orphaned_held: available ? {count: 0, resources: zero} : null,
      pending_external: available ? zero : null,
      admission_budget: null,
      count_backstop: {observed: available ? 0 : null, maximum: null, reason: 'maximum_not_observed'},
    }],
  };
}

describe('AdminVMCapacityComponent', () => {
  beforeAll(async () => {
    await ɵresolveComponentResources(() => Promise.resolve(''));
  });

  beforeEach(() => {
    TestBed.configureTestingModule({imports: [AdminVMCapacityComponent,
      TranslocoTestingModule.forRoot({
        langs: {en}, translocoConfig: {availableLangs: ['en'], defaultLang: 'en'},
      }),
    ]});
  });

  it('keeps durable six-dimensional holds visible when inventory and count maximum are unknown', () => {
    const fixture = TestBed.createComponent(AdminVMCapacityComponent);
    Object.defineProperty(fixture.componentInstance, 'vm', {value: signal(capacity())});
    fixture.detectChanges();
    const host = fixture.nativeElement as HTMLElement;
    expect(host.querySelector('[data-testid="vm-held-table"]')?.textContent).toContain('2,000');
    expect(host.querySelector('[data-testid="vm-held-table"]')?.textContent).toContain('–');
    expect(host.querySelector('[data-testid="vm-totals-table"]')).toBeNull();
    expect(host.textContent).toContain('Node and cluster accounting is unavailable');
    expect(host.textContent).toContain('– / –');
    expect(host.textContent).toContain('Pending external demand');
  });

  it('shows observed zero separately from unknown maximum and opens exact node accounting', () => {
    const fixture = TestBed.createComponent(AdminVMCapacityComponent);
    Object.defineProperty(fixture.componentInstance, 'vm', {value: signal(capacity(true))});
    fixture.detectChanges();
    const host = fixture.nativeElement as HTMLElement;
    expect(host.textContent).toContain('0 / –');
    expect(host.querySelectorAll('[data-testid="vm-totals-table"] tbody tr')).toHaveLength(10);
    expect(host.querySelector('details summary')?.textContent).toContain('(1)');
    expect(host.textContent).toContain('worker-a');
    expect(host.textContent).not.toContain('ETA');
  });

  it('shows configured installation and per-owner budgets while node accounting is stale', () => {
    const view = capacity();
    view.clusters[0].admission_budget = {
      installation: {...zero, cpu_millicores: 17250, memory_bytes: 30064771072,
        ephemeral_storage_bytes: 10737418240, kvm_devices: 8, tun_devices: 8, vhost_net_devices: 8},
      per_owner: {...zero, cpu_millicores: 8615, memory_bytes: 17179869184,
        ephemeral_storage_bytes: 5368709120, kvm_devices: 4, tun_devices: 4, vhost_net_devices: 4},
      cpu_cost: {millicores_per_vcpu_numerator: 1375,
        millicores_per_vcpu_denominator: 4, launcher_overhead_millicores: 25},
    };
    const fixture = TestBed.createComponent(AdminVMCapacityComponent);
    Object.defineProperty(fixture.componentInstance, 'vm', {value: signal(view)});
    fixture.detectChanges();
    const host = fixture.nativeElement as HTMLElement;
    const table = host.querySelector('[data-testid="vm-budget-table"]');
    expect(table?.querySelectorAll('tbody tr')).toHaveLength(2);
    expect(table?.textContent).toContain('Installation');
    expect(table?.textContent).toContain('Per owner');
    expect(table?.textContent).toContain('17,250');
    expect(table?.textContent).toContain('30,064,771,072');
    expect(table?.textContent).toContain('10,737,418,240');
    expect(table?.textContent).toContain('8,615');
    expect(host.textContent).toContain('1,375');
    expect(host.textContent).toContain('25m');
    expect(host.querySelector('[data-testid="vm-totals-table"]')).toBeNull();
    expect(host.textContent).toContain('– / –');
  });
});
