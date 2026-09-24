import {ChangeDetectionStrategy, Component, input} from '@angular/core';
import {TranslocoPipe} from '@jsverse/transloco';
import {
  AdminVMCapacity,
  VMResourceCategory,
} from '../../../core/models/api.model';

const RESOURCE_ROWS: readonly VMResourceCategory[] = [
  'allocatable', 'headroom', 'external', 'unbound', 'bound_reserved',
  'active', 'warm', 'teardown', 'available', 'shortfall',
];
const HELD_ROWS = ['unbound', 'bound_reserved', 'active', 'warm', 'teardown', 'total'] as const;

/** Operator accounting, never a placement promise or a queue position. */
@Component({
  selector: 'app-admin-vm-capacity',
  standalone: true,
  imports: [TranslocoPipe],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <section class="vm-capacity" data-testid="vm-capacity" aria-labelledby="vm-capacity-title">
      <h2 id="vm-capacity-title">{{ 'admin.capacity.vm.title' | transloco }}</h2>
      <p class="intro">{{ 'admin.capacity.vm.description' | transloco }}</p>
      @if (vm().clusters.length === 0) {
        <p role="status">{{ 'admin.capacity.vm.noPolicy' | transloco }}</p>
      }
      @for (cluster of vm().clusters; track cluster.cluster_id) {
        <article class="cluster" [attr.data-cluster]="cluster.cluster_id">
          <header>
            <div>
              <h3>{{ cluster.cluster_id }}</h3>
              <span class="subtle">{{ cluster.namespace }} · {{ ('admin.capacity.vm.mode.' + cluster.mode) | transloco }}</span>
            </div>
            <span class="state" [class.unknown]="!cluster.available">
              {{ (cluster.available ? 'admin.capacity.vm.accounted' : 'admin.capacity.vm.unavailable') | transloco }}
            </span>
          </header>
          @if (cluster.reason) {
            <p class="reason" role="status">{{ 'admin.capacity.vm.reason' | transloco }}: <code>{{ cluster.reason }}</code></p>
          }
          <div class="facts">
            <div>
              <span class="fact-label">{{ 'admin.capacity.vm.inventory' | transloco }}</span>
              <strong>{{ (cluster.inventory?.fresh ? 'admin.capacity.vm.fresh' : 'admin.capacity.vm.unknown') | transloco }}</strong>
              @if (cluster.inventory; as inventory) {
                <small>{{ 'admin.capacity.vm.observed' | transloco }}: {{ inventory.observed_at }}</small>
                <small>{{ 'admin.capacity.vm.age' | transloco }}: {{ age(inventory.age_seconds) }}</small>
                @if (!inventory.complete) { <small>{{ 'admin.capacity.vm.incomplete' | transloco }}</small> }
              }
            </div>
            <div>
              <span class="fact-label">{{ 'admin.capacity.vm.countBackstop' | transloco }}</span>
              <strong>{{ known(cluster.count_backstop.observed) }} / {{ known(cluster.count_backstop.maximum) }}</strong>
              <small>{{ 'admin.capacity.vm.countDescription' | transloco }}</small>
            </div>
            <div>
              <span class="fact-label">{{ 'admin.capacity.vm.waiting' | transloco }}</span>
              <strong>{{ cluster.waiting.count }} · {{ cluster.waiting.nonfit }} {{ 'admin.capacity.vm.nonfit' | transloco }}</strong>
              <small>{{ 'admin.capacity.vm.oldest' | transloco }}: {{ age(cluster.waiting.oldest_age_seconds) }}</small>
              <small>{{ 'admin.capacity.vm.bypasses' | transloco }}: {{ cluster.waiting.bypasses }} · {{ 'admin.capacity.vm.protected' | transloco }}: {{ cluster.waiting.protected }}</small>
            </div>
            <div>
              <span class="fact-label">{{ 'admin.capacity.vm.teardown' | transloco }}</span>
              <strong>{{ cluster.teardown.count }} · {{ cluster.teardown.overdue }} {{ 'admin.capacity.vm.overdue' | transloco }}</strong>
              <small>{{ 'admin.capacity.vm.unknownAge' | transloco }}: {{ cluster.teardown.unknown_age }}</small>
              <small>{{ 'admin.capacity.vm.oldestProgress' | transloco }}: {{ age(cluster.teardown.oldest_progress_age_seconds) }}</small>
            </div>
          </div>

          <h4>{{ 'admin.capacity.vm.held' | transloco }}</h4>
          <p class="subtle">{{ 'admin.capacity.vm.heldDescription' | transloco }}</p>
          <div class="table-wrap">
            <table class="app-table" data-testid="vm-held-table">
              <thead><tr><th scope="col">{{ 'admin.capacity.vm.categoryHeading' | transloco }}</th><th scope="col">CPU <span class="unit">(m)</span></th><th scope="col">{{ 'admin.capacity.vm.memory' | transloco }} <span class="unit">(B)</span></th><th scope="col">{{ 'admin.capacity.vm.storage' | transloco }} <span class="unit">(B)</span></th><th scope="col">KVM</th><th scope="col">TUN</th><th scope="col">vhost-net</th></tr></thead>
              <tbody>
                @for (row of heldRows; track row) {
                  <tr><th scope="row">{{ ('admin.capacity.vm.category.' + row) | transloco }}</th>
                    <td>{{ known(cluster.held[row].cpu_millicores) }}</td><td>{{ known(cluster.held[row].memory_bytes) }}</td><td>{{ known(cluster.held[row].ephemeral_storage_bytes) }}</td><td>{{ known(cluster.held[row].kvm_devices) }}</td><td>{{ known(cluster.held[row].tun_devices) }}</td><td>{{ known(cluster.held[row].vhost_net_devices) }}</td>
                  </tr>
                }
              </tbody>
            </table>
          </div>

          @if (cluster.totals; as totals) {
            <h4>{{ 'admin.capacity.vm.accounting' | transloco }}</h4>
            <div class="table-wrap">
              <table class="app-table" data-testid="vm-totals-table">
                <thead><tr><th scope="col">{{ 'admin.capacity.vm.categoryHeading' | transloco }}</th><th scope="col">CPU <span class="unit">(m)</span></th><th scope="col">{{ 'admin.capacity.vm.memory' | transloco }} <span class="unit">(B)</span></th><th scope="col">{{ 'admin.capacity.vm.storage' | transloco }} <span class="unit">(B)</span></th><th scope="col">KVM</th><th scope="col">TUN</th><th scope="col">vhost-net</th></tr></thead>
                <tbody>
                  @for (row of resourceRows; track row) {
                    <tr><th scope="row">{{ ('admin.capacity.vm.category.' + row) | transloco }}</th>
                      <td>{{ known(totals[row].cpu_millicores) }}</td><td>{{ known(totals[row].memory_bytes) }}</td><td>{{ known(totals[row].ephemeral_storage_bytes) }}</td><td>{{ known(totals[row].kvm_devices) }}</td><td>{{ known(totals[row].tun_devices) }}</td><td>{{ known(totals[row].vhost_net_devices) }}</td>
                    </tr>
                  }
                </tbody>
              </table>
            </div>
          } @else {
            <p class="subtle">{{ 'admin.capacity.vm.accountingUnavailable' | transloco }}</p>
          }
          <h4>{{ 'admin.capacity.vm.separate' | transloco }}</h4>
          <div class="table-wrap">
            <table class="app-table" data-testid="vm-separate-table">
              <thead><tr><th scope="col">{{ 'admin.capacity.vm.categoryHeading' | transloco }}</th><th scope="col">CPU <span class="unit">(m)</span></th><th scope="col">{{ 'admin.capacity.vm.memory' | transloco }} <span class="unit">(B)</span></th><th scope="col">{{ 'admin.capacity.vm.storage' | transloco }} <span class="unit">(B)</span></th><th scope="col">KVM</th><th scope="col">TUN</th><th scope="col">vhost-net</th></tr></thead>
              <tbody>
                <tr><th scope="row">{{ 'admin.capacity.vm.orphaned' | transloco }} ({{ known(cluster.orphaned_held?.count) }})</th>
                  <td>{{ known(cluster.orphaned_held?.resources?.cpu_millicores) }}</td><td>{{ known(cluster.orphaned_held?.resources?.memory_bytes) }}</td><td>{{ known(cluster.orphaned_held?.resources?.ephemeral_storage_bytes) }}</td><td>{{ known(cluster.orphaned_held?.resources?.kvm_devices) }}</td><td>{{ known(cluster.orphaned_held?.resources?.tun_devices) }}</td><td>{{ known(cluster.orphaned_held?.resources?.vhost_net_devices) }}</td>
                </tr>
                <tr><th scope="row">{{ 'admin.capacity.vm.pendingExternal' | transloco }}</th>
                  <td>{{ known(cluster.pending_external?.cpu_millicores) }}</td><td>{{ known(cluster.pending_external?.memory_bytes) }}</td><td>{{ known(cluster.pending_external?.ephemeral_storage_bytes) }}</td><td>{{ known(cluster.pending_external?.kvm_devices) }}</td><td>{{ known(cluster.pending_external?.tun_devices) }}</td><td>{{ known(cluster.pending_external?.vhost_net_devices) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          @if (cluster.nodes; as nodes) {
            <details>
              <summary>{{ 'admin.capacity.vm.nodes' | transloco }} ({{ nodes.length }})</summary>
              <p class="subtle">{{ 'admin.capacity.vm.nodeNote' | transloco }}</p>
              @for (node of nodes; track node.name) {
                <div class="node">
                  <h5>{{ node.name }}</h5>
                  @if (node.general_exclusion) { <code>{{ node.general_exclusion }}</code> }
                  <div class="table-wrap">
                    <table class="app-table">
                      <thead><tr><th scope="col">{{ 'admin.capacity.vm.categoryHeading' | transloco }}</th><th scope="col">CPU <span class="unit">(m)</span></th><th scope="col">{{ 'admin.capacity.vm.memory' | transloco }} <span class="unit">(B)</span></th><th scope="col">{{ 'admin.capacity.vm.storage' | transloco }} <span class="unit">(B)</span></th><th scope="col">KVM</th><th scope="col">TUN</th><th scope="col">vhost-net</th></tr></thead>
                      <tbody>
                        @for (row of resourceRows; track row) {
                          <tr><th scope="row">{{ ('admin.capacity.vm.category.' + row) | transloco }}</th>
                            <td>{{ known(node.resources[row].cpu_millicores) }}</td><td>{{ known(node.resources[row].memory_bytes) }}</td><td>{{ known(node.resources[row].ephemeral_storage_bytes) }}</td><td>{{ known(node.resources[row].kvm_devices) }}</td><td>{{ known(node.resources[row].tun_devices) }}</td><td>{{ known(node.resources[row].vhost_net_devices) }}</td>
                          </tr>
                        }
                      </tbody>
                    </table>
                  </div>
                </div>
              }
            </details>
          }
        </article>
      }
    </section>
  `,
  styles: [`
    :host { display: block; min-width: 0; }
    .vm-capacity { margin: 32px 0; }
    h2 { font-size: 20px; margin: 0 0 8px; }
    h3 { font-size: 17px; margin: 0; overflow-wrap: anywhere; }
    h4 { font-size: 14px; margin: 20px 0 4px; }
    h5 { font-size: 13px; margin: 14px 0 4px; }
    .intro, .subtle, small { color: var(--text-secondary); }
    .intro { margin: 0 0 16px; }
    .cluster { border: 1px solid var(--border-hairline); border-radius: var(--radius-surface); background: var(--panel-bg); padding: 18px; margin: 12px 0; min-width: 0; }
    header { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
    .state { color: var(--success); font-size: 12px; font-weight: 600; }
    .state.unknown, .reason { color: var(--danger); }
    .reason { overflow-wrap: anywhere; }
    .facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 16px 0; }
    .facts > div { border: 1px solid var(--border-hairline); padding: 10px; border-radius: var(--radius-control); min-width: 0; }
    .fact-label, small { display: block; font-size: 12px; }
    .fact-label { color: var(--text-muted); margin-bottom: 4px; }
    small { overflow-wrap: anywhere; }
    .table-wrap { max-width: 100%; overflow-x: auto; }
    /* Header, row and divider look come from the global .app-table
       (src/styles/_app-table.scss); only the numeric grid stays local. */
    .app-table { min-width: 790px; font-variant-numeric: tabular-nums; }
    .app-table th, .app-table td { white-space: nowrap; }
    .app-table thead th:not(:first-child), .app-table td { text-align: right; }
    /* Row labels are <th scope="row">, which the global recipe leaves bare:
       give them the body-cell metrics so the label column lines up. */
    .app-table tbody th { text-align: left; font-weight: 500; padding: 10px 12px; border-bottom: 1px solid var(--border-hairline); color: var(--text-primary); }
    /* The eyebrow header uppercases; a unit must keep its case (m is milli, M is mega). */
    .unit { text-transform: none; letter-spacing: normal; }
    details { margin-top: 18px; }
    summary { cursor: pointer; font-weight: 600; }
    .node { border-top: 1px solid var(--border-hairline); margin-top: 10px; }
    @media (max-width: 640px) { .cluster { padding: 12px; } }
  `],
})
export class AdminVMCapacityComponent {
  readonly vm = input.required<AdminVMCapacity>();
  readonly resourceRows = RESOURCE_ROWS;
  readonly heldRows = HELD_ROWS;

  known(value: number | null | undefined): string {
    return value === null || value === undefined ? '–' : value.toLocaleString();
  }

  age(seconds: number | null | undefined): string {
    if (seconds === null || seconds === undefined) return '–';
    const whole = Math.max(0, Math.floor(seconds));
    if (whole < 60) return `${whole}s`;
    const minutes = Math.floor(whole / 60);
    return minutes < 60 ? `${minutes}m` : `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
  }

}
