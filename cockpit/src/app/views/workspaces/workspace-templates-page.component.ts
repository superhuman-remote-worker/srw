import {Component} from '@angular/core';
import {CustomizeTabsComponent} from '../../shell/customize-tabs/customize-tabs.component';
import {WorkspaceTemplatesListComponent} from './workspace-templates-list.component';

/** `/workspaces`: the fifth Customize tab (spec §1). */
@Component({
  selector: 'app-workspace-templates-page',
  standalone: true,
  imports: [CustomizeTabsComponent, WorkspaceTemplatesListComponent],
  template: `
    <div class="page">
      <app-customize-tabs />
      <main class="page-content">
        <app-workspace-templates-list />
      </main>
    </div>
  `,
  styles: [`
    :host { display: block; height: 100%; }
    .page { display: flex; flex-direction: column; height: 100%; }
    .page-content { flex: 1; overflow: hidden; }
  `],
})
export class WorkspaceTemplatesPageComponent {}
