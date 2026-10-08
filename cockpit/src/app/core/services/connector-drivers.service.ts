import {inject, Injectable, signal} from '@angular/core';
import {ApiService} from './api.service';
import {ConnectorDriver, driverForType} from '../models/connector-driver.model';

/**
 * The installed connector drivers, read once per app session from the
 * capability matrix (`GET /api/datasources/drivers`). The set changes only
 * when the orchestrator is upgraded or, from D6, when a driver is
 * registered — `load(true)` refetches.
 *
 * `drivers()` stays null until the first answer and after a failed one;
 * callers then keep their pre-matrix behaviour instead of hiding choices.
 */
@Injectable({providedIn: 'root'})
export class ConnectorDriversService {
  private readonly api = inject(ApiService);

  readonly drivers = signal<ConnectorDriver[] | null>(null);
  readonly loading = signal(false);
  readonly loadFailed = signal(false);

  load(force = false): void {
    if (this.loading() || (this.drivers() !== null && !force)) return;
    this.loading.set(true);
    this.loadFailed.set(false);
    this.api.getConnectorDrivers().subscribe({
      next: (matrix) => {
        this.drivers.set(matrix.drivers);
        this.loading.set(false);
      },
      error: () => {
        this.loadFailed.set(true);
        this.loading.set(false);
      },
    });
  }

  /** The driver serving a stored connector type, or null when unknown. */
  forType(type: string | null | undefined): ConnectorDriver | null {
    return driverForType(this.drivers(), type);
  }
}
