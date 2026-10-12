import {Component} from '@angular/core';
import {DatasourceListComponent} from './datasource-list.component';
import {CustomizeTabsComponent} from '../../shell/customize-tabs/customize-tabs.component';

@Component({
  selector: 'app-datasources-page',
  standalone: true,
  imports: [DatasourceListComponent, CustomizeTabsComponent],
  template: `
    <div class="page">
      <app-customize-tabs />
      <main class="page-content">
        <app-datasource-list />
      </main>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
      }

      .page {
        display: flex;
        flex-direction: column;
        height: 100%;
      }


      .page-content {
        flex: 1;
        overflow: hidden;
      }
    `,
  ],
})
export class DatasourcesPageComponent {}
