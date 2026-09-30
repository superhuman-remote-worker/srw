export interface WorkspacePreview {
  backend: string;
  source: 'request' | 'project' | 'default' | 'recommendation';
  binding: Record<string, unknown> | null;
  sources?: {tier: string | null; template: string | null};
  template_name?: string | null;
}
