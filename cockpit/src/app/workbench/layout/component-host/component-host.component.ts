import {
  Component,
  Type,
  input,
  inject,
  ViewContainerRef,
  effect,
  viewChild,
  computed,
  DestroyRef,
} from '@angular/core';
import { ComponentMetadata, ComponentType } from '../../layout.model';
import { ComponentRegistryService } from '../../../core/services/component-registry.service';
import { LayoutService } from '../../services/layout.service';
import { PanelHeaderComponent } from '../panel-header/panel-header.component';

/**
 * Dynamically loads and displays a registered component.
 * Wraps the component with a panel header showing the display name
 * and controls for switching components, splitting, and closing.
 */
@Component({
  selector: 'app-component-host',
  imports: [PanelHeaderComponent],
  template: `
    <div class="component-host">
      <app-panel-header
        [title]="displayName()"
        [componentType]="componentType()"
        [availableComponents]="availableComponents()"
        [canClose]="canClose()"
        (componentChange)="onComponentChange($event)"
        (splitHorizontal)="onSplitHorizontal()"
        (splitVertical)="onSplitVertical()"
        (close)="onClose()"
      />
      <div class="component-content">
        <ng-container #outlet />
      </div>
    </div>
  `,
  styles: [
    `
      .component-host {
        display: flex;
        flex-direction: column;
        height: 100%;
        background: var(--panel-bg);
      }

      .component-content {
        flex: 1;
        overflow: auto;
        position: relative;
      }
    `,
  ],
})
export class ComponentHostComponent {
  private readonly registry = inject(ComponentRegistryService);
  private readonly layoutService = inject(LayoutService);
  private readonly destroyRef = inject(DestroyRef);
  private destroyed = false;
  private loadSeq = 0;

  readonly componentType = input.required<ComponentType>();
  readonly path = input<number[]>([]);
  private readonly outlet = viewChild('outlet', { read: ViewContainerRef });

  /** Available components for the dropdown */
  readonly availableComponents = computed<ComponentMetadata[]>(() => {
    return this.registry
      .getRegisteredTypes()
      .map((type) => this.registry.get(type))
      .filter((meta): meta is ComponentMetadata => meta !== undefined);
  });

  /** Whether this panel can be closed (more than one panel exists) */
  readonly canClose = computed(() => this.layoutService.getPanelCount() > 1);

  constructor() {
    this.destroyRef.onDestroy(() => (this.destroyed = true));

    effect(() => {
      const type = this.componentType();
      const container = this.outlet();
      if (!container) return;

      container.clear();
      void this.mountPanel(type, container, ++this.loadSeq);
    });
  }

  /** Loads the panel class, then creates it unless a newer request or destroy superseded this one. */
  private async mountPanel(type: ComponentType, container: ViewContainerRef, seq: number): Promise<void> {
    let componentClass: Type<unknown> | undefined;
    try {
      componentClass = await this.registry.loadComponent(type);
    } catch (err) {
      console.error(`Failed to load workbench panel "${type}":`, err);
      return;
    }
    if (this.destroyed || seq !== this.loadSeq || !componentClass) return;
    container.createComponent(componentClass);
  }

  displayName(): string {
    return this.registry.getDisplayName(this.componentType());
  }

  onComponentChange(newType: ComponentType): void {
    this.layoutService.updateComponent(this.path(), newType);
  }

  onSplitHorizontal(): void {
    this.layoutService.splitPanel(this.path(), 'horizontal');
  }

  onSplitVertical(): void {
    this.layoutService.splitPanel(this.path(), 'vertical');
  }

  onClose(): void {
    this.layoutService.closePanel(this.path());
  }
}
