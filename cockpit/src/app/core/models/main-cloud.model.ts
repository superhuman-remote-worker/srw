/**
 * The admin Main cloud page, as `GET /api/admin/main-cloud` returns it
 * (main_cloud_as_connectors.md, slice 2). The main cloud is configured by
 * Helm only, so the page reports and changes nothing.
 *
 * `tests/test_b04_lane_m_main_cloud_settings_routes.py` pins the shape against
 * `fixtures/main-cloud.json`.
 */

/** offered: delivered and enforced; planned: a later slice builds it. */
export type MainCloudCellStatus = 'offered' | 'planned' | 'unsupported';

export interface MainCloudCell {
  status: MainCloudCellStatus;
  /** What enforces an offered or planned level, or why it is unsupported. */
  note: string;
  /** Workspace tiers it is offered on (`sandbox`, `vm`, …). */
  workspace_backends: string[];
  slice: number | null;
}

export interface MainCloudMatrixRow {
  connector_type: string;
  folder_kind: string | null;
  access: string;
  /** One cell per provider, keyed by backend id. */
  cells: Record<string, MainCloudCell>;
}

export interface MainCloudMatrixProvider {
  backend_id: string;
  title: string;
  active: boolean;
}

export interface MainCloudMatrix {
  providers: MainCloudMatrixProvider[];
  rows: MainCloudMatrixRow[];
}

export type MainCloudHelmState = 'matches' | 'differs' | 'invalid';

/** How the provider schedules its background jobs (Nextcloud's `backgroundjobs_mode`). */
export type MainCloudBackgroundJobsMode = 'cron' | 'ajax' | 'webcron' | 'none';

/** The provider's background-job (cron) health (bundled_nextcloud_performance.md, D13). */
export interface MainCloudBackgroundJobs {
  /** null: it could not be read. */
  mode: MainCloudBackgroundJobsMode | null;
  /** The last finished run (ISO 8601); null before the first, or unread. */
  last_run_at: string | null;
  /** The mode is cron and no run finished in the last 15 minutes, or none ever did. */
  stale: boolean;
  /** Whether the chart runs its cron container; null: an external Nextcloud. */
  cron_container: boolean | null;
}

/** Trash and version retention: pinned by the chart, or the server's own administrator's. */
export type MainCloudRetention =
  | {source: 'chart'; trashbin: string | null; versions: string | null}
  | {source: 'external'};

export interface MainCloudPage {
  provider: {
    backend_id: string;
    title: string;
    public_url: string | null;
    backend_instance_id: string | null;
    activation_revision: number;
    activated_at: string | null;
    initialized: boolean;
  };
  health: {ok: boolean; latency_ms: number | null; detail: string};
  /**
   * null for a provider without background jobs (OpenCloud); absent from an
   * orchestrator older than D13. Either way the page shows nothing.
   */
  background_jobs?: MainCloudBackgroundJobs | null;
  /** Reported with `background_jobs`: trash and version expiry are background jobs. */
  retention?: MainCloudRetention | null;
  configuration: {
    source: 'helm';
    helm: {state: MainCloudHelmState; backend_id: string | null; detail: string};
    replace_installation: string | null;
  };
  matrix: MainCloudMatrix;
}
