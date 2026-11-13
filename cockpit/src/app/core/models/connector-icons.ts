/**
 * The icon that stands for a connector type, wherever a connector is listed:
 * the Customize catalog, a project's Connectors tab and the pickers of the
 * creation forms. One table, so the same connector looks the same everywhere.
 */
const TYPE_ICONS: Readonly<Record<string, string>> = {
  credentials: 'key',
  generic: 'settings_input_component',
  repository: 'code',
  kb: 'menu_book',
  postgresql: 'database',
  neo4j: 'hub',
  mongodb: 'eco',
  webdav: 'cloud',
  email: 'mail',
  mcp: 'extension',
  kubeconfig: 'rocket_launch',
  ssh_key: 'key',
  generic_file: 'description',
  cloud_folder: 'folder',
  cloud_folder_checkout: 'folder_copy',
  cloud_outbox: 'outbox',
  gpt_live_sip: 'phone',
};

/** A Material Symbols name; a type this table does not know gets the generic
 *  storage icon. */
export function connectorTypeIcon(type: string | null | undefined): string {
  return TYPE_ICONS[(type ?? '').toString()] ?? 'storage';
}
