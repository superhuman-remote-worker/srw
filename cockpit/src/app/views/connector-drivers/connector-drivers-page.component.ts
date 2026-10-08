import {Component, computed, inject, OnInit, signal} from '@angular/core';
import {RouterLink} from '@angular/router';
import {TranslocoPipe} from '@jsverse/transloco';
import {ConnectorDriversService} from '../../core/services/connector-drivers.service';
import {ConnectorDriver, ConnectorEgressStatus} from '../../core/models/connector-driver.model';
import {SidebarToggleComponent} from '../../shell/sidebar-toggle/sidebar-toggle.component';
import {AppBadgeComponent, type BadgeTone} from '../../ui/badge';
import {AppButtonComponent} from '../../ui/button';
import {AppInputComponent} from '../../ui/input';
import {AppSpinnerComponent} from '../../ui/spinner';

/** Egress reasons the matrix reports; anything newer shows its raw status. */
const EGRESS_REASONS = new Set(['runs_in_srw_process', 'driver_hosting_not_available']);
const PLANES = new Set(['harness', 'bind_time', 'service', 'in_pod']);

/** A stable element id for a driver, so a link can open the page at it. */
export function driverAnchor(name: string): string {
  return 'driver-' + name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
}

/**
 * Settings → Connector drivers: the generated capability matrix
 * (connector_drivers.md, "A generated capability matrix", slice D2).
 *
 * Every installed driver with what its spec declares: access levels and
 * what enforces each, plane, delivery, workspace backends, whether it holds
 * upstream credentials, its credential slots, trust and egress. Nothing is
 * written here by hand — the page renders `GET /api/datasources/drivers`.
 * For a driver outside the trusted list each claim says it is the author's.
 *
 * Any signed-in user may read it: it describes installed software and no
 * connector. Reached from the Settings rail and from the Connectors page.
 */
@Component({
  selector: 'app-connector-drivers-page',
  standalone: true,
  imports: [
    RouterLink,
    TranslocoPipe,
    SidebarToggleComponent,
    AppBadgeComponent,
    AppButtonComponent,
    AppInputComponent,
    AppSpinnerComponent,
  ],
  template: `
    <div class="drivers-page">
      <div class="drivers-container">
        <div class="page-header">
          <app-sidebar-toggle />
          <h1 class="page-title">{{ 'connectorDrivers.title' | transloco }}</h1>
        </div>
        <p class="page-desc">
          {{ 'connectorDrivers.desc' | transloco }}
          <a routerLink="/datasources" class="page-link">{{ 'connectorDrivers.toConnectors' | transloco }}</a>
        </p>

        <div class="toolbar">
          <app-input
            size="sm"
            type="search"
            [value]="query()"
            (valueChange)="query.set($event)"
            [placeholder]="'connectorDrivers.search' | transloco"
            [ariaLabel]="'connectorDrivers.search' | transloco"
          />
          @if (drivers(); as list) {
            <span class="count">{{ 'connectorDrivers.count' | transloco: {count: list.length} }}</span>
          }
        </div>

        @if (service.loadFailed()) {
          <div class="error-banner" role="alert">
            <span>{{ 'connectorDrivers.loadFailed' | transloco }}</span>
            <app-button variant="secondary" size="sm" (clicked)="service.load(true)">
              {{ 'connectorDrivers.retry' | transloco }}
            </app-button>
          </div>
        } @else if (drivers() === null) {
          <div class="loading"><app-spinner size="sm" /> {{ 'connectorDrivers.loading' | transloco }}</div>
        }

        @for (driver of visible(); track driver.name) {
          <section class="driver-card" [id]="anchor(driver.name)" [attr.data-driver]="driver.name">
            <header class="driver-head">
              <div class="driver-title">
                <h2>{{ driver.title }}</h2>
                <code class="driver-name">{{ driver.name }}</code>
              </div>
              <div class="driver-badges">
                <app-badge [tone]="trustTone(driver)" size="sm">
                  {{ 'connectorDrivers.trust.' + driver.trust.tier | transloco }}
                </app-badge>
                @if (driver.holds_upstream_credentials) {
                  <app-badge tone="warning" size="sm">{{ 'connectorDrivers.holdsCredentials' | transloco }}</app-badge>
                }
                @if (driver.forced_read_only) {
                  <app-badge tone="info" size="sm">{{ 'connectorDrivers.alwaysReadOnly' | transloco }}</app-badge>
                }
              </div>
            </header>

            @if (driver.trust.claims_declared_by_author) {
              <p class="author-note" data-claims="author">{{ 'connectorDrivers.authorNote' | transloco }}</p>
            }

            <div class="driver-body">
              <div class="access" data-section="access">
                <h3>
                  {{ 'connectorDrivers.accessLevels' | transloco }}
                  @if (driver.trust.claims_declared_by_author) {
                    <span class="claim-source">{{ 'connectorDrivers.declaredByAuthor' | transloco }}</span>
                  }
                </h3>
                @if (driver.access_levels.length === 0) {
                  <p class="muted">{{ 'connectorDrivers.noAccessLevels' | transloco }}</p>
                } @else {
                  <ol class="levels">
                    @for (level of driver.access_levels; track level.id) {
                      <li class="level" [attr.data-level]="level.id">
                        <div class="level-head">
                          <span class="level-id">{{ level.id }}</span>
                          @if (level.advisory) {
                            <app-badge tone="alert" size="xs">{{ 'connectorDrivers.advisory' | transloco }}</app-badge>
                          } @else {
                            <app-badge tone="success" size="xs">{{ 'connectorDrivers.enforced' | transloco }}</app-badge>
                          }
                          @if (driver.default_access === level.id) {
                            <app-badge tone="neutral" size="xs">{{ 'connectorDrivers.defaultLevel' | transloco }}</app-badge>
                          }
                        </div>
                        <p class="enforced-by">
                          <span class="enforced-label">{{ 'connectorDrivers.enforcedBy' | transloco }}</span>
                          {{ level.enforced_by }}
                        </p>
                        <p class="tools">
                          @if (level.tools === '*') {
                            {{ 'connectorDrivers.toolsDiscovered' | transloco }}
                          } @else if (level.tools.length === 0) {
                            {{ 'connectorDrivers.toolsNone' | transloco }}
                          } @else {
                            @for (tool of level.tools; track tool) {
                              <code>{{ tool }}</code>
                            }
                          }
                        </p>
                      </li>
                    }
                  </ol>
                }
              </div>

              <dl class="facts">
                <dt>{{ 'connectorDrivers.plane' | transloco }}</dt>
                <dd>{{ planeLabel(driver) | transloco }}</dd>

                <dt>{{ 'connectorDrivers.delivery' | transloco }}</dt>
                <dd>{{ driver.delivery_forms.join(', ') }}</dd>

                <dt>
                  {{ 'connectorDrivers.backends' | transloco }}
                  @if (driver.trust.claims_declared_by_author) {
                    <span class="claim-source">{{ 'connectorDrivers.declaredByAuthor' | transloco }}</span>
                  }
                </dt>
                <dd>
                  {{ driver.supported_backends.join(', ') }}
                  <span class="sub">{{ driver.workspace_requirements }}</span>
                </dd>

                <dt>{{ 'connectorDrivers.trustLabel' | transloco }}</dt>
                <dd>
                  {{ 'connectorDrivers.trust.' + driver.trust.tier | transloco }}
                  <span class="sub">
                    @if (driver.trust.image) {
                      <code>{{ driver.trust.image }}</code>
                    } @else {
                      {{ 'connectorDrivers.shipsWithSrw' | transloco }}
                    }
                  </span>
                </dd>

                <dt>
                  {{ 'connectorDrivers.credentials' | transloco }}
                  @if (driver.trust.claims_declared_by_author) {
                    <span class="claim-source">{{ 'connectorDrivers.declaredByAuthor' | transloco }}</span>
                  }
                </dt>
                <dd>
                  @if (driver.credential_slots.length === 0) {
                    {{ 'connectorDrivers.noSlots' | transloco }}
                  }
                  <ul class="slots">
                    @for (slot of driver.credential_slots; track slot.name) {
                      <li [attr.data-slot]="slot.name">
                        <code>{{ slot.name }}</code>
                        {{ 'datasources.generic.slotKind.' + slot.kind | transloco }}
                        · {{ (slot.required ? 'connectorDrivers.slotRequired' : 'connectorDrivers.slotOptional') | transloco }}
                        · {{ 'connectorDrivers.slotDelivery.' + (slot.delivery ?? 'held') | transloco }}
                        @if (slot.access_levels.length > 0) {
                          · {{ 'datasources.generic.slotLevels' | transloco: {levels: slot.access_levels.join(', ')} }}
                        }
                      </li>
                    }
                  </ul>
                </dd>

                <dt>
                  {{ 'connectorDrivers.egress' | transloco }}
                  @if (driver.trust.claims_declared_by_author) {
                    <span class="claim-source">{{ 'connectorDrivers.declaredByAuthor' | transloco }}</span>
                  }
                </dt>
                <dd class="egress">
                  <span class="egress-row" data-egress="declared">
                    <span class="egress-col">{{ 'connectorDrivers.egressDeclared' | transloco }}</span>
                    @if (driver.egress.declared.rules.length === 0) {
                      {{ 'connectorDrivers.egressNone' | transloco }}
                    }
                    @for (rule of driver.egress.declared.rules; track $index) {
                      <code>{{ rule.host }}:{{ rule.ports.join(',') }}/{{ rule.protocol }}</code>
                    }
                    @if (driver.egress.declared.needs_dns) {
                      <span class="sub">{{ 'connectorDrivers.needsDns' | transloco: {reason: driver.egress.declared.needs_dns} }}</span>
                    }
                  </span>
                  <span class="egress-row" data-egress="enforced">
                    <span class="egress-col">{{ 'connectorDrivers.egressEnforced' | transloco }}</span>
                    {{ egressText(driver.egress.enforced) | transloco }}
                  </span>
                  <span class="egress-row" data-egress="installation">
                    <span class="egress-col">{{ 'connectorDrivers.egressInstallation' | transloco }}</span>
                    {{ egressText(driver.egress.installation) | transloco }}
                  </span>
                </dd>
              </dl>
            </div>
          </section>
        } @empty {
          @if (drivers() !== null) {
            <p class="muted">{{ 'connectorDrivers.noMatch' | transloco }}</p>
          }
        }
      </div>
    </div>
  `,
  styles: `
    /* overflow: auto as the other settings pages: a short viewport must
       scroll, not clip. */
    :host {
      display: block;
      height: 100%;
      overflow: auto;
    }

    .drivers-page {
      padding: 24px;
      max-width: var(--content-max-width);
      margin: 0 auto;
    }

    .drivers-container {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .page-header {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .page-title {
      margin: 0;
      font-size: 1.5rem;
    }

    .page-desc {
      margin: 0;
      color: var(--text-secondary);
    }

    .page-link {
      margin-left: 4px;
      color: var(--accent-color);
    }

    .toolbar {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .toolbar app-input {
      flex: 1;
      max-width: 360px;
    }

    .count,
    .muted {
      color: var(--text-muted);
      font-size: 0.85rem;
    }

    .error-banner {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 10px 12px;
      border-radius: var(--radius-surface);
      background: color-mix(in srgb, var(--danger) 10%, transparent);
      color: var(--danger);
    }

    .loading {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--text-muted);
    }

    .driver-card {
      display: flex;
      flex-direction: column;
      gap: 12px;
      padding: 16px 20px;
      background: var(--panel-bg);
      border: 1px solid var(--border-hairline);
      border-radius: var(--radius-surface);
      scroll-margin-top: 16px;
    }

    .driver-head {
      display: flex;
      flex-wrap: wrap;
      align-items: flex-start;
      justify-content: space-between;
      gap: 8px;
    }

    .driver-title h2 {
      margin: 0;
      font-size: 1.1rem;
    }

    .driver-name {
      font-family: var(--font-mono);
      font-size: 0.8rem;
      color: var(--text-muted);
    }

    .driver-badges {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }

    .author-note {
      margin: 0;
      font-size: 0.85rem;
      color: var(--warning);
    }

    .claim-source {
      margin-left: 6px;
      font-size: 0.7rem;
      font-weight: 400;
      color: var(--warning);
    }

    .driver-body {
      display: grid;
      grid-template-columns: minmax(0, 1.3fr) minmax(0, 1fr);
      gap: 20px;
    }

    h3 {
      margin: 0 0 8px;
      font-size: 0.9rem;
      color: var(--text-secondary);
    }

    .levels {
      display: flex;
      flex-direction: column;
      gap: 10px;
      margin: 0;
      padding: 0;
      list-style: none;
    }

    .level {
      padding: 8px 10px;
      border-left: 2px solid var(--border-color);
    }

    .level-head {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
    }

    .level-id {
      font-weight: 600;
    }

    .enforced-by {
      margin: 4px 0 0;
      font-size: 0.85rem;
    }

    .enforced-label {
      margin-right: 4px;
      font-weight: 600;
      color: var(--text-secondary);
    }

    .tools {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
      margin: 4px 0 0;
      font-size: 0.8rem;
      color: var(--text-muted);
    }

    code {
      font-family: var(--font-mono);
      font-size: 0.78rem;
      overflow-wrap: anywhere;
    }

    .facts {
      display: grid;
      grid-template-columns: max-content minmax(0, 1fr);
      /* Rows as tall as their text, not stretched to the access column. */
      align-content: start;
      gap: 6px 12px;
      margin: 0;
      font-size: 0.85rem;
    }

    .facts dt {
      font-weight: 600;
      color: var(--text-secondary);
    }

    .facts dd {
      margin: 0;
      min-width: 0;
      overflow-wrap: anywhere;
    }

    .sub {
      display: block;
      color: var(--text-muted);
      font-size: 0.8rem;
    }

    .slots {
      margin: 0;
      padding-left: 16px;
    }

    .egress {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }

    .egress-col {
      margin-right: 4px;
      font-weight: 500;
      color: var(--text-secondary);
    }

    @media (max-width: 860px) {
      .driver-body {
        grid-template-columns: minmax(0, 1fr);
      }
    }

    @media (max-width: 640px) {
      .drivers-page {
        padding: 16px;
      }

      .driver-card {
        padding: 12px 14px;
      }

      .facts {
        grid-template-columns: minmax(0, 1fr);
      }

      .facts dd {
        margin-bottom: 6px;
      }
    }
  `,
})
export class ConnectorDriversPageComponent implements OnInit {
  protected readonly service = inject(ConnectorDriversService);

  readonly query = signal('');
  readonly drivers = this.service.drivers;

  readonly visible = computed(() => {
    const drivers = this.drivers() ?? [];
    const q = this.query().trim().toLowerCase();
    if (!q) return drivers;
    return drivers.filter((driver) =>
      [driver.title, driver.name, driver.legacy_type ?? '']
        .some((text) => text.toLowerCase().includes(q)),
    );
  });

  protected readonly anchor = driverAnchor;

  ngOnInit(): void {
    this.service.load(true);
  }

  protected trustTone(driver: ConnectorDriver): BadgeTone {
    if (driver.trust.tier === 'builtin') return 'success';
    return driver.trust.trusted ? 'info' : 'warning';
  }

  protected planeLabel(driver: ConnectorDriver): string {
    return PLANES.has(driver.plane) ? `connectorDrivers.planes.${driver.plane}` : driver.plane;
  }

  protected egressText(column: ConnectorEgressStatus): string {
    return EGRESS_REASONS.has(column.reason)
      ? `connectorDrivers.egressReason.${column.reason}`
      : column.status;
  }
}
