import {DOCUMENT} from '@angular/common';
import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  HostListener,
  ViewContainerRef,
  contentChildren,
  inject,
  signal,
  viewChild,
} from '@angular/core';
import {FocusKeyManager} from '@angular/cdk/a11y';
import {AppMenuItemComponent} from './menu-item.component';

export type MenuPlacement = 'bottom-start' | 'bottom-end' | 'top-start' | 'top-end';

/** A viewport position, as a MouseEvent's clientX/clientY report it. */
export interface MenuPoint {
  x: number;
  y: number;
}

@Component({
  selector: 'app-menu',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div
      #panel
      class="app-menu__panel"
      role="menu"
      [attr.data-open]="isOpen() || null"
      (click)="onItemClick($event)"
    >
      <ng-content select="app-menu-item"></ng-content>
    </div>
  `,
  styleUrl: './menu.component.scss',
})
export class AppMenuComponent {
  protected items = contentChildren(AppMenuItemComponent);
  protected panelRef = viewChild.required<ElementRef<HTMLElement>>('panel');

  readonly isOpen = signal(false);

  private keyManager?: FocusKeyManager<AppMenuItemComponent>;
  private anchor: HTMLElement | null = null;
  private host = inject(ElementRef<HTMLElement>);
  private vcr = inject(ViewContainerRef);
  private doc = inject(DOCUMENT);

  private readonly onDocClick = (e: MouseEvent) => {
    const panel = this.panelRef().nativeElement;
    const target = e.target as Node | null;
    if (target && !panel.contains(target) && !this.anchor?.contains(target)) {
      this.close();
    }
  };
  private readonly onScroll = () => this.reposition();
  private readonly onResize = () => this.reposition();

  /**
   * `at` opens the menu at a point instead of beside the anchor — a context
   * menu opens where the pointer was. The anchor still owns focus restore
   * and the outside-click test.
   */
  open(anchor: HTMLElement, placement: MenuPlacement = 'bottom-start', at?: MenuPoint): void {
    if (this.isOpen()) return;
    this.anchor = anchor;
    this.placement = placement;
    this.point = at ?? null;

    const panel = this.panelRef().nativeElement;
    if (panel.parentElement !== this.doc.body) {
      this.doc.body.appendChild(panel);
    }
    this.isOpen.set(true);

    const items = this.items();
    if (items.length > 0) {
      this.keyManager = new FocusKeyManager<AppMenuItemComponent>(
        items as readonly AppMenuItemComponent[],
      )
        .withWrap()
        .withVerticalOrientation()
        .withTypeAhead();
      // Defer focus to after panel is positioned
      queueMicrotask(() => this.keyManager?.setFirstItemActive());
    }

    this.reposition();
    this.doc.addEventListener('mousedown', this.onDocClick, true);
    this.doc.addEventListener('scroll', this.onScroll, true);
    this.doc.defaultView?.addEventListener('resize', this.onResize);
  }

  close(): void {
    if (!this.isOpen()) return;
    this.isOpen.set(false);
    const restoreTo = this.anchor;
    this.anchor = null;
    this.doc.removeEventListener('mousedown', this.onDocClick, true);
    this.doc.removeEventListener('scroll', this.onScroll, true);
    this.doc.defaultView?.removeEventListener('resize', this.onResize);
    this.keyManager = undefined;
    restoreTo?.focus();
  }

  toggle(anchor: HTMLElement, placement?: MenuPlacement): void {
    if (this.isOpen()) {
      this.close();
    } else {
      this.open(anchor, placement);
    }
  }

  ngOnDestroy(): void {
    this.close();
    const panel = this.panelRef().nativeElement;
    if (panel.parentElement === this.doc.body) {
      this.doc.body.removeChild(panel);
    }
  }

  protected onItemClick(_event: MouseEvent): void {
    // Close on item activation. Items emit (activated) before this bubbles up.
    this.close();
  }

  @HostListener('document:keydown.escape', ['$event'])
  protected onEscape(event: Event): void {
    if (!this.isOpen()) return;
    event.preventDefault();
    this.close();
  }

  @HostListener('document:keydown', ['$event'])
  protected onKeydown(event: KeyboardEvent): void {
    if (!this.isOpen() || !this.keyManager) return;
    const panel = this.panelRef().nativeElement;
    if (panel.contains(event.target as Node)) {
      this.keyManager.onKeydown(event);
    }
  }

  private placement: MenuPlacement = 'bottom-start';
  private point: MenuPoint | null = null;

  private reposition(): void {
    if (!this.anchor || !this.isOpen()) return;
    const panel = this.panelRef().nativeElement;
    // open() calls this synchronously, before change detection applies the
    // [data-open] binding — so the panel can still be `display: none` (zero
    // size), which breaks width-based placement (e.g. bottom-end ends up off
    // screen). Force the open style on for measurement; the binding re-affirms
    // it on the next tick, and close() clears it via the same binding.
    if (!panel.hasAttribute('data-open')) panel.setAttribute('data-open', '');
    const point = this.point;
    const a = point
      ? {left: point.x, right: point.x, top: point.y, bottom: point.y}
      : this.anchor.getBoundingClientRect();
    const margin = point ? 0 : 4;

    panel.style.visibility = 'hidden';
    panel.style.left = '0px';
    panel.style.top = '0px';
    const p = panel.getBoundingClientRect();

    let left = a.left;
    let top = a.bottom + margin;
    if (this.placement === 'bottom-end') left = a.right - p.width;
    if (this.placement === 'top-start') top = a.top - p.height - margin;
    if (this.placement === 'top-end') {
      left = a.right - p.width;
      top = a.top - p.height - margin;
    }

    const viewW = this.doc.defaultView?.innerWidth ?? 1024;
    const viewH = this.doc.defaultView?.innerHeight ?? 768;
    // At a point, flip to the pointer's other side when the menu doesn't fit,
    // as native context menus do, rather than sliding it over the pointer.
    if (point) {
      if (left + p.width > viewW - 8) left = point.x - p.width;
      if (top + p.height > viewH - 8) top = point.y - p.height;
    }
    left = Math.max(8, Math.min(left, viewW - p.width - 8));
    top = Math.max(8, Math.min(top, viewH - p.height - 8));

    panel.style.left = `${Math.round(left)}px`;
    panel.style.top = `${Math.round(top)}px`;
    panel.style.visibility = 'visible';
  }
}
