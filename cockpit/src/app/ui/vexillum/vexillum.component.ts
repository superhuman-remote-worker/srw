import {ChangeDetectionStrategy, Component} from '@angular/core';

/**
 * SRW vexillum — the standard-with-banner brand mark, drawn inline so it
 * follows the accent axis: the banner is --accent-color, the lettering and
 * the two inlays are --on-accent, the staff is currentColor (host colour
 * --text-secondary). Same geometry as assets/icons/icon-mark.svg, which is
 * frozen in the default Tyrian purple for the favicon and PWA icons (static
 * files cannot read a token), so they stay purple under every accent.
 *
 * Decorative: its one call site (the chat hero) names the product in the
 * title right underneath, so the SVG is hidden from assistive tech.
 *
 * The lettering is the one Cinzel use outside the rail brand block and the
 * chat hero title; styles/visual-language.spec.ts lists this file.
 */
@Component({
  selector: 'srw-vexillum',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <svg viewBox="0 0 32 32" focusable="false" role="presentation" aria-hidden="true">
      <line x1="16" y1="2.5" x2="16" y2="29.5" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" />
      <path d="M 16 2.2 L 15.4 4 L 16.6 4 Z" fill="currentColor" />
      <circle cx="16" cy="4.5" r="0.6" fill="currentColor" />
      <line x1="5.4" y1="6" x2="26.6" y2="6" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" />
      <circle cx="16" cy="28.5" r="0.7" fill="currentColor" />
      <rect class="banner" x="7" y="7" width="18" height="17" />
      <rect class="inlay" x="7.5" y="8" width="17" height="0.6" />
      <rect class="inlay" x="7.5" y="22.4" width="17" height="0.6" />
      <text class="lettering" x="16" y="16.9" text-anchor="middle" font-weight="700" font-size="5.6">SRW</text>
    </svg>
  `,
  styles: [`
    :host {
      display: inline-block;
      color: var(--text-secondary);
      line-height: 0;
    }
    svg {
      display: block;
      width: 100%;
      height: 100%;
    }
    .banner { fill: var(--accent-color); }
    .inlay { fill: var(--on-accent); opacity: 0.55; }
    .lettering {
      fill: var(--on-accent);
      font-family: var(--font-display, 'Cinzel', Georgia, serif);
      letter-spacing: 0.4px; /* brand */
    }
  `],
})
export class VexillumComponent {}
