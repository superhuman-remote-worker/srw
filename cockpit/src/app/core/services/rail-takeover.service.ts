import {Injectable, Signal, TemplateRef, computed, signal} from '@angular/core';

interface RailClaim {
  template: TemplateRef<unknown>;
  active: Signal<boolean>;
}

const ALWAYS = signal(true).asReadonly();

/**
 * Lets a page lend the rail its own navigation (navigation_fixed_rail.md F9).
 *
 * The page keeps owning the content: it hands over a TemplateRef declared in
 * its own template, so the rail renders the page's list with the page's
 * state, bindings and styles — nothing is duplicated or moved into a shared
 * store. The rail adds only the way back to the app around it.
 */
@Injectable({providedIn: 'root'})
export class RailTakeoverService {
  private readonly claimed = signal<RailClaim | null>(null);

  /** What the rail should render in place of its own rows, if anything. */
  readonly template = computed(() => {
    const claim = this.claimed();
    return claim && claim.active() ? claim.template : null;
  });

  /**
   * `active` says when the rail should show it — a page whose list only
   * belongs in the rail on some layouts passes the same condition it uses to
   * stop rendering the list itself, so the list is always in exactly one place.
   */
  claim(template: TemplateRef<unknown>, active: Signal<boolean> = ALWAYS): void {
    this.claimed.set({template, active});
  }

  /** Release only what this caller claimed: a page being destroyed after the
   *  next one has already claimed the rail must not take its content away. */
  release(template: TemplateRef<unknown>): void {
    if (this.claimed()?.template === template) this.claimed.set(null);
  }
}
