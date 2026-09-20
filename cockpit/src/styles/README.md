# Cockpit Styles

This is the engineering side of the design system. For brand intent, palette rationale, and "when to use which theme", see [`knowledge-base/knowledge/design/cockpit/themes/`](../../../knowledge-base/knowledge/design/cockpit/themes/README.md) at the repo root.

## Layout

```
src/styles/
├── _variables.scss            Sass scalars (spacing, breakpoints, z-index, type scale). Radii live in _root-tokens.scss as CSS variables — there are no Sass `$radius-*` scalars anymore.
├── _mixins.scss               Utility mixins (focus-ring, breakpoints, truncate, visually-hidden).
├── _root-tokens.scss          Primitive CSS variables at :root (--radius-sm/md/lg/xl/full, --font-family-base/display/mono).
├── _semantic-tokens.scss      Role tokens at :root (--radius-control/surface/pill/tag, --font-primary/control/mono).
├── _shape-recipes.scss        Shape archetype mixins (control, surface, pill, tag, circle).
├── _typography-recipes.scss   Typography recipe mixins (display, eyebrow, heading, mono).
├── _app-table.scss            Global .app-table look for every data table (class hook on <table>).
├── visual-language.spec.ts    Text-level guards for the 2026-09-10 refresh: root scale, Cinzel readers, no stamp, table hook, no raw buttons, token radii, eyebrow tracking.
└── themes/
    ├── _theme-config.scss     Token maps — one per theme. Source of truth for palette + on-tokens.
    ├── _themes.scss           apply-app-theme($name) mixin. Emits the map as CSS custom properties.
    ├── _roman-accents.scss  Roman accents: the Cinzel brand face, the approval-card rule, header inlays. Scoped under .theme-* selectors. No radii, no stamps (retired 2026-09-10).
    └── _typography.scss       Type-scale Sass maps (legacy; being folded into recipes).
```

The single entry point is `src/styles.scss` — it `@use`s the modules above, defines the `.theme-*` body classes, and exposes Material Symbols + a few global resets.

## Theme architecture

Themes work via **CSS custom properties + body class swap**. There is no per-component theme logic.

1. Each theme is an SCSS map in `_theme-config.scss` (e.g. `$senate-theme`, `$travertine-theme`).
2. Each map is registered in `$themes` keyed by name.
3. `styles.scss` emits a body class per theme:
   ```scss
   .theme-senate     { @include theme.apply-app-theme('senate'); }
   ```
4. `apply-app-theme($name)` walks the map and emits `--<token>: <value>;` for each entry, then emits the **default accent** and one nested `&.accent-<name>` block per accent (see below).
5. Components consume `var(--token-name)` and stay theme-agnostic.

**Accent axis (since 2026-09-10).** The brand / interactive colour is a second body class beside `theme-<mode>`: `<body class="theme-senate accent-graphite">`. `$accents` in `_theme-config.scss` holds five tokens per accent per mode — `--accent-color`, `--accent-hover`, `--on-accent`, `--user-bubble`, `--user-bubble-text` — and the base theme maps carry none of them (`theme-config.spec.ts` enforces that). Everything else that looks accent-coloured (focus `--ring`, selection tint, approval rule, header inlay, `--shadow-glow`, Senate's `--shadow-md` inlay) derives from `--accent-color` with `color-mix`, so it follows any accent for free. `$default-accent` (`tyrian`) is emitted on the bare `.theme-*` class, which is what the no-JS first paint and a fresh device get; `theme.service.ts` mirrors it as `DEFAULT_ACCENT` and stamps `accent-<key>` from `localStorage['cockpit:accent']`.

Token names are bare (`--accent-color`, `--panel-bg`, `--text-primary`) — no prefix. They're stable across themes, so adding a new theme rarely requires touching components.

## Token tiers

Three layers. Only the middle one is what most primitives actually read.

1. **Primitive scale** — declared in `_root-tokens.scss` at `:root`. Raw values: `--radius-sm/md/lg/xl/full`, `--font-family-base/display/mono`. Themes can override these to retheme the whole system (setting `--radius-md: 0` would cascade through every role that aliases it — the Roman themes did exactly that until 2026-09-10; both active themes now use the scale as-is).

2. **Semantic roles** — declared in `_semantic-tokens.scss` at `:root`. Each aliases a primitive: `--radius-control: var(--radius-md)`, `--font-primary: var(--font-family-base)`, etc. Primitives consume these via recipe mixins (`@include shape.control`), never the raw primitive scale directly.

3. **Component-local** (optional, per primitive) — a primitive may declare `--btn-radius: var(--radius-control)` and read its own local var. This is the override surface for one-off variants without writing competing selectors. Bootstrap 5.3 pattern.

Themes override at the tier that gives the right scope:

- **Global retheme** → override the primitive (`--radius-md: 0` under a `.theme-*` class would flatten everything that uses md — and the role tokens must be re-declared under the same class, see the note in `_roman-accents.scss`).
- **Role retheme** → override the role (`--radius-control: var(--radius-full)` makes every control pill-shaped without touching the surface or tag scales).
- **One-off** → override the component-local (`--btn-radius: 12px` on a specific button).

## Active themes

| Key | Mode | Notes |
|---|---|---|
| `travertine` | Light | **Light default + initial paint fallback.** Default when `system` resolves to light. |
| `senate` | Dark | Default when `system` resolves to dark. Lifted slate base (was charcoal in earlier revisions — the lift fixed contrast and obviated Praetorian). |

First-run preference is `'system'` — the app respects the OS preference. A pre-paint script in `index.html` resolves the right body class (and the stored accent class) before Angular hydrates so dark-OS users don't flash through the Travertine fallback.

### Accents

| Key | Travertine fill | Senate fill | Notes |
|---|---|---|---|
| `tyrian` | `#5f499c` (white label, 7.2:1) | `#7f65ca` (white label, 4.6:1) | **Default.** Senatorial purple, ~90° from the danger red in OKLCH hue, so primary and destructive never share a hex. |
| `porphyry` | `#9c2832` | `#cc4647` | The original blood red, kept as an option. It *is* the danger hex — the user opts into that collision knowingly. |
| `graphite` | `#3d2f22` (white label) | `#e8e4dc` (dark label) | No hue. Status colours become the only colour on screen; the label colour flips per mode. |

Picker: `app-accent-toggle` (`src/app/ui/accent-toggle/`) on the Settings page, labels from `settings.appearance.accents.<key>`. Danger, Success, Warning and Info do not change with the accent.

The browser/PWA `theme-color` and Windows tile color follow the resolved `--accent-color` token once theme CSS is available and whenever the theme or accent changes. The HTML and web manifest retain Tyrian purple as the static fallback. Installed icons and launch screens use static assets/manifest metadata; their refresh timing is controlled by the browser, not the appearance picker. Regenerate the purple PWA icons and favicons from their SVG sources with `node scripts/generate-pwa-icons.mjs` from `cockpit/` (requires Inkscape and ImageMagick).

`theme.service.ts` migrates legacy localStorage values transparently:
- `dark` → `senate` (Catppuccin era)
- `light` → `travertine` (Catppuccin era)
- `praetorian` → `senate` (retired high-contrast theme)

## How to add a theme

1. **Define the token map** in `_theme-config.scss`. Easiest path: copy `$senate-theme`, rename, change colors. Keep the same keys — components depend on them.
2. **Register it** in the `$themes` map at the bottom of the same file:
   ```scss
   $themes: (
     'travertine': $travertine-theme,
     'senate':     $senate-theme,
     'mytheme':    $mytheme-theme,   // <-- here
   );
   ```
3. **Add the body class** in `src/styles.scss`:
   ```scss
   .theme-mytheme { @include theme.apply-app-theme('mytheme'); }
   ```
4. **Extend the type union** in `src/app/core/services/theme.service.ts`:
   ```ts
   export type ConcreteTheme = 'travertine' | 'senate' | 'mytheme';
   ```
   And add `'mytheme'` to `VALID_PREFERENCES`.
5. **Add it to the picker** — `OPTIONS` in `src/app/ui/theme-toggle/theme-toggle.component.ts`. Pick a `group` (`'light'` or `'dark'`) so it lands in the right `<optgroup>`.
6. **Document the design intent** in `knowledge-base/knowledge/design/cockpit/themes/README.md` — palette story, when to use it, what it's *for*.
7. **Test the picker test** — `theme.service.spec.ts` should already cover the new theme via the generic body-class swap test, but add a smoke test if your theme has special semantics.

If your theme departs from the shared shape language (different radii, different display font, etc.), you'll also need to either:
- Override the relevant tokens (`--font-display`, `--radius-md`) inside the map, or
- Add a `.theme-mytheme { ... }` block to `_roman-accents.scss` that resets/overrides the shared Roman overrides.

A new theme must also appear under every accent in `$accents` (each accent lists its modes by theme key), or `apply-app-theme` errors at build time.

## How to add an accent

1. **Add the sub-maps** to `$accents` in `_theme-config.scss`: one `'<key>': ('travertine': (...), 'senate': (...))` entry with all five tokens per mode. Check the label contrast on the fill (≥ 4.5:1) and pick `on-accent` per mode accordingly — Graphite is the precedent for a dark label in Senate.
2. **Extend the union** in `src/app/core/services/theme.service.ts`: `AccentPreference` and `ACCENT_OPTIONS` (the picker renders that array).
3. **Name it** in `src/assets/i18n/en.json` and `de-DE.json` under `settings.appearance.accents.<key>`.
4. **Pre-paint**: add the key to the `validAccent` table in `src/index.html`.
5. **Capture it**: `VISUAL_WALK_ACCENT=<key> VISUAL_WALK_LABEL=accent-<key> npm run test:e2e:visual-walk`.
6. **Document the intent** in `knowledge-base/knowledge/design/cockpit/themes/README.md`.

`theme-config.spec.ts` checks the structure (every accent for both modes, all five tokens, base maps free of accent tokens, default ≠ danger); `theme.service.spec.ts` covers the class swap and persistence.

## Token catalog

The current token set:

**Surfaces**: `--app-bg`, `--panel-bg`, `--panel-header-bg`, `--timeline-bg`, `--surface-0`, `--surface-1`, `--surface-2`

**Borders**: `--border-color` (controls), `--border-hairline` (derived: 55% border over panel — cards, dividers, panel edges)

**Focus**: `--ring` (derived: 28% accent — the focus halo on every primitive)

**Text**: `--text-primary`, `--text-secondary`, `--text-muted`

**Accent**: `--accent-color`, `--accent-hover`, `--on-accent`, `--user-bubble`, `--user-bubble-text` — per accent, from `$accents` (see the accent axis above); `--ring` and the accent-tinted shadows derive from `--accent-color`.

**Tracks/gutters** (split panes, sliders): `--track-bg`, `--gutter-color`, `--gutter-hover`

**Interactive overlays**: `--hover`, `--active`

**Semantic colors** (each with a matching `-tint` variant): `--success`, `--warning`, `--alert`, `--info`, `--danger`

**On-tokens** (foreground on solid-fill variants, WCAG-AA-tuned per theme): `--on-accent`, `--on-warning`, `--on-success`, `--on-danger`, `--on-info`

**Shadows**: `--shadow-sm`, `--shadow-md`, `--shadow-glow`

**Shape primitives** (`_root-tokens.scss`, `:root`): `--radius-sm/md/lg/xl/full`

**Shape roles** (`_semantic-tokens.scss`, `:root`): `--radius-control/surface/pill/tag`

**Typography primitives**: `--font-family-base/display/mono`

**Typography roles**: `--font-primary`, `--font-control`, `--font-mono`

**Brand-only**: `--font-display` (legacy alias for `--font-family-display`) — read by four selectors (rail brand block, chat hero title, vexillum lettering); guarded by `visual-language.spec.ts`. `--letter-spacing-display` / `--text-transform-display` are optional theme hooks no active theme sets. `--user-bubble`, `--user-bubble-text` are chat-bubble colour tokens.

Don't introduce hex literals in component SCSS. If a needed color token is missing, add it to **every** theme map at once — leaving a token undefined for one theme means components break under that theme.

## Authoring a primitive

Three rules:

1. Never hardcode `border-radius` — consume a recipe mixin (`shape.control`, `shape.surface`, etc.).
2. Never hardcode `font-family` for primary or display text — use `type.display`, `type.eyebrow`, or `type.mono`.
3. Component-local override surfaces are opt-in. Primitives with realistic one-off needs (button, input, card, dialog) declare `--<name>-radius`. Primitives with a single site (spinner, switch, radio) consume the role directly.

### Picking the right recipe

| Component archetype | Shape recipe |
|---|---|
| Button, input, select, chip, badge, tab, icon-button | `@include shape.control` |
| Card, dialog, menu, toast, panel | `@include shape.surface` |
| Pill chip, status pill | `@include shape.pill` |
| Checkbox tile, small inline tag | `@include shape.tag` |
| Radio dot, switch knob, spinner, avatar, round icon-button | `@include shape.circle` |

| Typographic role | Recipe |
|---|---|
| Primary button label, brand text, panel title | `@include type.display` |
| Section label, kicker, eyebrow | `@include type.eyebrow` |
| Code, tool args, debug surfaces | `@include type.mono` |

### Worked example — button

```scss
@use '../../../styles/shape-recipes' as shape;
@use '../../../styles/typography-recipes' as type;

.app-button__btn {
  --btn-radius: var(--radius-control);   // opt-in local override surface
  border-radius: var(--btn-radius);

  &[data-variant='primary'] {
    @include type.display;
    background: var(--accent-color);
    color: var(--on-accent);
  }

  &[data-variant='warning'] {
    background: var(--warning);
    color: var(--on-warning);
  }
}
```

### Worked example — card (surface, no override needed)

```scss
@use '../../../styles/shape-recipes' as shape;

.app-card {
  @include shape.surface;
  background: var(--surface-0);
  box-shadow: var(--shadow-md);
}
```

### Worked example — switch knob (functional circle)

```scss
@use '../../../styles/shape-recipes' as shape;

.app-switch__knob {
  @include shape.circle;
  background: var(--surface-0);
}
```

### Component-local override consumers

A primitive that exposes `--btn-radius` (as in the button example above) can be re-shaped by any consumer without rewriting selectors:

```scss
// In a feature component that wraps the primitive:
.my-special-page .app-button__btn {
  --btn-radius: 12px;  // one-off; doesn't affect any other button
}
```

## Roman accents

`_roman-accents.scss` is scoped under `.theme-travertine, .theme-senate` and declares what is left of the Roman look: Cinzel as the brand face (read by four selectors: the rail brand block, the hero title and the SRW lettering of the inline vexillum) and the approval-card left rule. Per-theme tweaks (Travertine's gold inlay under panel headers, Senate's accent-mix equivalent) follow in their own scoped blocks. Both the rule and the Senate inlay read `--accent-color`, so they follow the accent axis.

Radii are deliberately **not** overridden there any more. The original sharp-corner pass (`--radius-sm/md/xl: 0`, `--radius-lg: 2px`) was retired on 2026-09-10; both themes use the rounded primitive scale via the role tokens — controls `md` (0.5rem, 8px), surfaces `lg` (0.75rem, 12px), small tags `sm` (0.25rem, 4px), pills and functional circles unchanged. `roman-accents.spec.ts` guards against the flatten creeping back.

Buttons are flat since 2026-09-10: the Inset Stamp recipe and its `--stamp-*` tokens were removed (`visual-language.spec.ts` guards against their return). Tinted button variants render subtle by default; `appearance="solid"` is the opt-in for the one destructive confirm button in a dialog.

Legacy component-class selectors (`.btn`, `.session-message .message-bubble`, `.approval-card`) in `_roman-accents.scss` predate the recipe model and target classes that have largely been renamed during the BEM migration. They're being removed as primitives migrate to recipes (`knowledge-base/knowledge/features/design_system_completion.md` Phase 3). New shape rules belong in a recipe mixin, not as a body-class-scoped selector.

## Verification

When changing themes or tokens, run:

```bash
npm test -- --run              # vitest, incl. theme.service.spec.ts and styles/visual-language.spec.ts
npm run build                  # full Angular production build (the only template type-check; vitest does not type-check)
npm run lint:styles            # stylelint on src/**/*.scss — the gate is the delta, baseline 73 (2026-09-10)
VISUAL_WALK_LABEL=x npm run test:e2e:visual-walk   # capture walk against https://localhost, reviewed by eye
VISUAL_WALK_ACCENT=graphite VISUAL_WALK_LABEL=x-graphite npm run test:e2e:visual-walk   # same, under another accent
```

The Python side mirrors the default accent: `tests/test_brand_palette.py` (brand.py ↔ SCSS), `tests/test_keycloak_theme_infra.py` (login CSS + email wrapper ↔ brand.py). Run them from the repo root with `PYTHONPATH=src .venv/bin/python -m pytest tests/test_brand_palette.py tests/test_keycloak_theme_infra.py`.

The theme service spec covers preference resolution, legacy migration, system-mode listening, and body-class swapping. SCSS errors surface during the production build (the dev server's HMR can hide them).

## Stylelint

`.stylelintrc.json` extends `stylelint-config-standard-scss` with cockpit-specific overrides. The rule that exists *because of this design system* is the `border-radius` regression guard:

```json
"declaration-property-value-disallowed-list": {
  "border-radius": ["/\\d+px/"],
  "border-top-left-radius": ["/\\d+px/"],
  "border-top-right-radius": ["/\\d+px/"],
  "border-bottom-left-radius": ["/\\d+px/"],
  "border-bottom-right-radius": ["/\\d+px/"]
}
```

`var(--*)`, `0`, `50%`, and shorthand asymmetric values like `0 var(--radius-control) var(--radius-control) 0` are all allowed. Raw `Npx` values are blocked. If you genuinely need a one-off px value — don't; consume `--radius-control` or declare a component-local override (`--btn-radius: var(--radius-control)` then override that). The rule is the safety net that protects the token consistency built over Phases 1-4.

**Scope:** the lint script only runs on `*.scss` files, not on Angular inline `styles:` arrays in `.ts` files. Inline styles ship through Angular's SCSS preprocessor but stylelint has no Angular-aware processor to extract them. Inline-TS radii are not lint-enforced; a periodic grep (`grep -rnE "border-radius: *[0-9]+px" src/`) is the manual backstop.

A few standard-scss rules are disabled — see `.stylelintrc.json` comments-in-spirit:
- `value-keyword-case` (would lowercase `BlinkMacSystemFont` and break font convention)
- `scss/comment-no-empty` (flags `// --- Section ---` divider comments)
- `color-function-alias-notation` (keeps `rgba()` legal alongside `rgb()` with alpha)
- A handful of cosmetic rules (`declaration-block-single-line-max-declarations`, etc.) that don't add signal at this scale.

## Cross-references

- Brand intent + palette rationale: [`knowledge-base/knowledge/design/cockpit/themes/README.md`](../../../knowledge-base/knowledge/design/cockpit/themes/README.md)
- Theme service: `src/app/core/services/theme.service.ts`
- Theme picker: `src/app/ui/theme-toggle/theme-toggle.component.ts`
- Brand mark: `src/app/ui/legion-mark/legion-mark.component.ts`
