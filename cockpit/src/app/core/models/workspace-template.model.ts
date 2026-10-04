/** Slice A3: WorkspaceTemplate resources as the cockpit lists, authors and selects them.
 *  Shapes follow `src/shared/manifests/schema.json` and `manifest_store.resource_view`. */

export type WorkspaceBackend = 'sandbox' | 'vm' | 'virtual';
export type ScopeKind = 'Account' | 'Project' | 'Catalog';

export interface ResourceScope {
  kind: ScopeKind;
  name: string;
}

export const CATALOG_SHARED: ResourceScope = {kind: 'Catalog', name: 'shared'};
export const ACCOUNT_ME: ResourceScope = {kind: 'Account', name: 'me'};

export interface WorkspaceStep {
  command: string[];
}

export interface WorkspaceTemplateSpec {
  backend: WorkspaceBackend;
  resources?: {
    cpu?: number;
    memory?: string;
    storage?: string;
    requests?: {cpu?: number; memory?: string};
  };
  environment?: {
    image: string;
    pullPolicy?: 'IfNotPresent' | 'Always' | 'Never';
    prepare?: WorkspaceStep[];
    cache?: 'Reuse' | 'Rebuild';
  };
  initialize?: WorkspaceStep[];
  retention?: 'Delete' | 'Retain';
  network?: {profileRef: Record<string, unknown>};
}

export interface WorkspaceTemplateDocument {
  apiVersion: 'srw/v1alpha1';
  kind: 'WorkspaceTemplate';
  metadata: {
    name: string;
    scope: ResourceScope;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
    tags?: string[];
  };
  spec: WorkspaceTemplateSpec;
}

/** One row of `GET /api/resources` and `GET /api/resources/{uid}`. */
export interface WorkspaceTemplateItem {
  resource: WorkspaceTemplateDocument;
  uid: string;
  resourceVersion: number;
  revision?: string;
  activeRevision?: string | null;
  installationManaged: boolean;
}

export interface WorkspaceTemplateItems {
  resources: WorkspaceTemplateItem[];
}

export interface TemplateRef {
  name: string;
  scope: ResourceScope;
}

/** The REST `workspace` field. `null` means no workspace; omit the field for the defaults chain. */
export type WorkspaceBinding =
  | {template: {ref: TemplateRef}}
  | {template: {inline: WorkspaceTemplateSpec}}
  | null;

/** What a create form's picker holds. `default` omits the field. */
export type WorkspaceChoice =
  | {kind: 'default'}
  | {kind: 'none'}
  | {kind: 'ref'; ref: TemplateRef; backend: WorkspaceBackend; label: string}
  | {kind: 'inline'; spec: WorkspaceTemplateSpec};

export interface ManifestApplyResult {
  apiVersion: string;
  operation: string;
  operationId: string;
  resources: {uid: string; resourceVersion: number; changed: boolean; [key: string]: unknown}[];
}

/** The `detail` of a 422 from `/api/manifests/*` (`ManifestIssue`); `path` is a JSON pointer. */
export interface ManifestErrorDetail {
  code: string;
  message: string;
  document?: number;
  path?: string;
}
