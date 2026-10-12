import {WorkspacePreview} from '../../core/models/workspace.model';
import {WorkspaceBinding, WorkspaceChoice} from '../../core/models/workspace-template.model';
import {choiceRequestFields} from '../workspaces/workspace-template-utils';

/** Create-form request fields (Slice A3). The picker owns the workspace, so
 *  `config_override.workspace` keeps only behaviour (read/write limits, git
 *  versioning). The legacy `backend` and `vm` sizing are never sent again. */
export function workspaceCreationFields(
  overrides: Record<string, unknown>,
  choice: WorkspaceChoice,
): {config_override: Record<string, unknown>; workspace?: WorkspaceBinding} {
  const config = {...overrides};
  const privateWorkspace = {...(config['workspace'] as Record<string, unknown> | undefined)};
  delete privateWorkspace['backend'];
  delete privateWorkspace['vm'];
  if (Object.keys(privateWorkspace).length) config['workspace'] = privateWorkspace;
  else delete config['workspace'];
  return {config_override: config, ...choiceRequestFields(choice)};
}

export function workspacePreviewConfig(config: Record<string, unknown>, preview?: WorkspacePreview | null): Record<string, unknown> {
  if (!preview) return config;
  return {...config, workspace: {...(config['workspace'] as Record<string, unknown> | undefined), backend: preview.backend}};
}
