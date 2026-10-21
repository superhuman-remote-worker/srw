export interface WorkspacePreview {
  backend: string;
  source: 'request' | 'project' | 'default' | 'recommendation';
  binding: Record<string, unknown> | null;
  sources?: {tier: string | null; template: string | null};
  template_name?: string | null;
  /** A default Virtual tier a cloud folder takes to a Container (New
   *  Session): the form keeps its cloud folders selectable. */
  cloud_folders_take_container?: boolean;
}
