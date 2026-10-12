import { Component, inject, signal } from '@angular/core';
import { TimelineComponent } from '../components/timeline/timeline.component';
import { SplitPanelComponent } from '../layout/split-panel/split-panel.component';
import { LayoutPickerComponent } from '../components/layout-picker/layout-picker.component';
import { SidebarToggleComponent } from '../../shell/sidebar-toggle/sidebar-toggle.component';
import { AppIconButtonComponent } from '../../ui/icon-button';
import { AppIconComponent } from '../../ui/icon';
import { LayoutService } from '../services/layout.service';

/** The layout picker's own width (layout-picker.component.ts). */
const PICKER_WIDTH = 320;

@Component({
  selector: 'app-workbench-page',
  standalone: true,
  imports: [
    TimelineComponent,
    SplitPanelComponent,
    LayoutPickerComponent,
    SidebarToggleComponent,
    AppIconButtonComponent,
    AppIconComponent,
  ],
  template: `
    <div class="workbench-frame">
      <header class="workbench-header">
        <app-sidebar-toggle />
        <app-timeline />
        <!-- The layout controls live with the panels they arrange, not in
             the rail (navigation_fixed_rail.md F6). -->
        <div class="workbench-actions">
          <app-icon-button
            ariaLabel="Choose layout"
            tooltip="Choose layout"
            (clicked)="toggleLayoutPicker($event)"
          >
            <app-icon size="md">dashboard_customize</app-icon>
          </app-icon-button>
          <app-icon-button
            ariaLabel="Reset layout"
            tooltip="Reset layout"
            (clicked)="layoutService.resetLayout()"
          >
            <app-icon size="md">restart_alt</app-icon>
          </app-icon-button>
        </div>
      </header>
      @if (pickerOpen()) {
        <app-layout-picker [top]="pickerTop()" [left]="pickerLeft()" (closed)="pickerOpen.set(false)" />
      }
      <main class="workbench-main">
        <app-split-panel [config]="layoutService.layout()" />
      </main>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        height: 100%;
      }

      .workbench-frame {
        display: flex;
        flex-direction: column;
        height: 100%;
        overflow: hidden;
      }

      /* The bar's surface belongs to the header, not the timeline inside it,
         so the sidebar toggle and the layout controls sit on it too. */
      .workbench-header {
        display: flex;
        align-items: center;
        flex-shrink: 0;
        background: var(--timeline-bg);
        border-bottom: 1px solid var(--border-color);
      }

      .workbench-header app-sidebar-toggle {
        padding-left: 12px;
      }

      .workbench-header app-timeline {
        flex: 1;
        min-width: 0;
      }

      .workbench-actions {
        display: flex;
        align-items: center;
        gap: 4px;
        padding-right: 16px;
        flex-shrink: 0;
      }

      .workbench-main {
        flex: 1;
        overflow: hidden;
      }
    `,
  ],
})
export class WorkbenchPageComponent {
  readonly layoutService = inject(LayoutService);

  readonly pickerOpen = signal(false);
  readonly pickerTop = signal(0);
  readonly pickerLeft = signal(0);

  /** Opens the picker right-aligned under its button. The click must not
   *  reach the document, where the picker's own outside-click handler would
   *  close it again straight away. */
  toggleLayoutPicker(event: MouseEvent): void {
    event.stopPropagation();
    if (!this.pickerOpen()) {
      const rect = ((event.currentTarget ?? event.target) as HTMLElement).getBoundingClientRect();
      this.pickerTop.set(rect.bottom + 6);
      this.pickerLeft.set(Math.max(8, rect.right - PICKER_WIDTH));
    }
    this.pickerOpen.update((open) => !open);
  }
}
