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
  configuration: {
    source: 'helm';
    helm: {state: MainCloudHelmState; backend_id: string | null; detail: string};
    replace_installation: string | null;
  };
  matrix: MainCloudMatrix;
}
