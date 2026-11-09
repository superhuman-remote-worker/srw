import { Component, input, output, ElementRef, HostListener, inject } from '@angular/core';
import { ComponentMetadata, ComponentType } from '../../layout.model';

/**
 * Header bar for layout panels with component switching dropdown and panel actions.
 * Displays the panel title and allows users to switch components, split, or close panels.
 */
@Component({
  selector: 'app-panel-header',
  template: `
    <div class="panel-header">
      @if (icon()) {
        <span class="panel-icon">{{ icon() }}</span>
      }
      <button class="title-button" (click)="toggleDropdown($event)">
        <span class="panel-title">{{ title() }}</span>
        @if (availableComponents().length > 0) {
          <span class="dropdown-arrow" [class.open]="isDropdownOpen">&#9662;</span>
        }
      </button>

      @if (isDropdownOpen && availableComponents().length > 0) {
        <div class="dropdown-menu">
          @for (comp of availableComponents(); track comp.type) {
            <button
              class="dropdown-item"
              [class.active]="comp.type === componentType()"
              (click)="selectComponent(comp.type, $event)"
            >
              {{ comp.displayName }}
            </button>
          }
        </div>
      }

      <div class="panel-actions">
        <button
          class="action-btn"
          title="Split horizontally (top/bottom)"
          (click)="onSplitH($event)"
        >
          <!-- Material Design: horizontal_split -->
          <svg viewBox="0 0 24 24" fill="currentColor">
            <path d="M3 19h18v-6H3v6zm0-8h18V9H3v2zm0-6v2h18V5H3z"/>
          </svg>
        </button>
        <button
          class="action-btn"
          title="Split vertically (left/right)"
          (click)="onSplitV($event)"
        >
          <!-- Material Design: vertical_split -->
          <svg viewBox="0 0 24 24" fill="currentColor">
            <path d="M3 15h8V5H3v10zm0 4h8v-2H3v2zm10 0h8V5h-8v14z"/>
          </svg>
        </button>
        @if (canClose()) {
          <button
            class="action-btn close-btn"
            title="Close panel"
            (click)="onClose($event)"
          >
            <!-- Material Design: close -->
            <svg viewBox="0 0 24 24" fill="currentColor">
              <path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/>
            </svg>
          </button>
        }
      </div>
    </div>
  `,
  styles: [
    `
      .panel-header {
        display: flex;
        align-items: center;
        gap: 8px;
        height: 32px;
        padding: 0 12px;
        background: var(--panel-header-bg);
        border-bottom: 1px solid var(--border-color);
        font-size: 12px;
        font-weight: 500;
        color: var(--text-secondary);
        user-select: none;
        position: relative;
      }

      .panel-icon {
        font-size: 14px;
      }

      .title-button {
        display: flex;
        align-items: center;
        gap: 6px;
        background: none;
        border: none;
        padding: 4px 8px;
        margin: -4px -8px;
        border-radius: var(--radius-control);
        color: inherit;
        font: inherit;
        cursor: pointer;
        transition: background-color 0.15s ease;
      }

      .title-button:hover {
        background: rgba(255, 255, 255, 0.08);
      }

      .panel-title {
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      .dropdown-arrow {
        font-size: 10px;
        transition: transform 0.15s ease;
        opacity: 0.6;
      }

      .dropdown-arrow.open {
        transform: rotate(180deg);
      }

      .dropdown-menu {
        position: absolute;
        top: 100%;
        left: 8px;
        min-width: 180px;
        max-height: 300px;
        overflow-y: auto;
        background: var(--surface-0);
        border: 1px solid var(--border-color);
        border-radius: var(--radius-surface);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
        z-index: 100;
        padding: 4px 0;
        margin-top: 2px;
      }

      .dropdown-item {
        display: block;
        width: 100%;
        padding: 8px 12px;
        background: none;
        border: none;
        text-align: left;
        color: var(--text-primary);
        font-size: 12px;
        cursor: pointer;
        transition: background-color 0.1s ease;
      }

      .dropdown-item:hover {
        background: rgba(255, 255, 255, 0.08);
      }

      .dropdown-item.active {
        background: color-mix(in srgb, var(--accent-color) 20%, transparent);
        color: var(--accent-color);
      }

      .panel-actions {
        display: flex;
        align-items: center;
        gap: 2px;
        margin-left: auto;
      }

      .action-btn {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 24px;
        height: 24px;
        background: none;
        border: none;
        border-radius: var(--radius-control);
        color: var(--text-muted);
        cursor: pointer;
        transition: all 0.15s ease;
      }

      .action-btn svg {
        width: 14px;
        height: 14px;
      }

      .action-btn:hover {
        background: rgba(255, 255, 255, 0.1);
        color: var(--text-primary);
      }

      .action-btn.close-btn:hover {
        background: color-mix(in srgb, var(--danger) 20%, transparent);
        color: var(--danger);
      }
    `,
  ],
})
export class PanelHeaderComponent {
  private readonly elementRef = inject(ElementRef);

  readonly title = input.required<string>();
  readonly icon = input<string>();
  readonly componentType = input<ComponentType>();
  readonly availableComponents = input<ComponentMetadata[]>([]);
  readonly canClose = input<boolean>(true);

  readonly componentChange = output<ComponentType>();
  readonly splitHorizontal = output<void>();
  readonly splitVertical = output<void>();
  readonly closed = output<void>();

  isDropdownOpen = false;

  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    if (!this.elementRef.nativeElement.contains(event.target)) {
      this.isDropdownOpen = false;
    }
  }

  toggleDropdown(event: MouseEvent): void {
    event.stopPropagation();
    this.isDropdownOpen = !this.isDropdownOpen;
  }

  selectComponent(type: ComponentType, event: MouseEvent): void {
    event.stopPropagation();
    this.componentChange.emit(type);
    this.isDropdownOpen = false;
  }

  onSplitH(event: MouseEvent): void {
    event.stopPropagation();
    this.splitHorizontal.emit();
  }

  onSplitV(event: MouseEvent): void {
    event.stopPropagation();
    this.splitVertical.emit();
  }

  onClose(event: MouseEvent): void {
    event.stopPropagation();
    this.closed.emit();
  }
}
