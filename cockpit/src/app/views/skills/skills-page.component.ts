import {Component} from '@angular/core';
import {SkillsListComponent} from './skills-list.component';
import {CustomizeTabsComponent} from '../../shell/customize-tabs/customize-tabs.component';

@Component({
  selector: 'app-skills-page',
  standalone: true,
  imports: [SkillsListComponent, CustomizeTabsComponent],
  template: `
    <div class="page">
      <app-customize-tabs />
      <main class="page-content">
        <app-skills-list />
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
export class SkillsPageComponent {}
