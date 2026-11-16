# UI/UX Design Process

These are default instructions for design tasks. Follow them unless the user provides specific instructions that override this workflow.

## 1. Understand the Feature

- Read the task description and all provided documents carefully
- Identify what the feature does from the user's perspective
- Identify the primary user flow (the happy path)
- Identify edge cases: empty states, error states, overflow, permissions
- Record assumptions and open questions via the kb_write tool (type=question)

## 2. Audit Existing Patterns

- Explore the cockpit codebase to understand current UI conventions
- Read the global stylesheet for design tokens (colors, spacing, typography)
- Read 3-5 existing components that are closest to what you'll be designing
- Document the component patterns you find: card layouts, list styles, form inputs, modals, toasts, navigation
- Identify reusable components — your design should leverage these, not reinvent them
- If the app is running, use `browser_navigate` then `browser_screenshot` to see the actual rendered UI

Record findings in `reference/pattern_audit.md`:
- Design tokens in use (CSS custom properties)
- Common layout patterns (panel-based, split-view, tab-based)
- Component conventions (how buttons look, how lists are structured, how errors are shown)
- Spacing and sizing conventions (padding, margins, border-radius)
- Typography scale (font sizes, weights, line heights in use)

## 3. Design Planning

- List every screen, view, or component the feature requires
- For each, identify:
  - Which existing components to reuse
  - What's genuinely new and needs to be designed
  - All states: default, empty, loading, error, populated, disabled
- Plan the responsive behavior: what changes between mobile (< 768px) and desktop
- Plan the interaction model: clicks, hovers, keyboard shortcuts, transitions

Write the plan to `plan.md`:
- Feature overview (one paragraph)
- Screen/component inventory (what to mock up)
- Reuse map (existing component → where it's used in the new feature)
- Mockup priority order (which screens to design first)

## 4. Create Mockups

Work through the mockup list from plan.md, one per todo.

### Mockup Requirements

Every mockup file must:
- Be a self-contained HTML file with all CSS inlined
- Use the project's CSS custom properties for all visual values
- Include realistic content (never "Lorem ipsum")
- Show multiple states (at minimum: default + empty/error)
- Be responsive (mobile + desktop layouts)
- Use semantic HTML (headings, lists, buttons, not just divs)
- Include hover/focus/active states for interactive elements

### File Organization

| Path | Purpose |
|------|---------|
| `mockups/` | All HTML mockup files |
| `mockups/[feature]_[screen].html` | One file per screen/view |
| `mockups/[feature]_components.html` | Shared component variants (buttons, cards, etc.) |
| `reference/` | Pattern audit, screenshots, notes |
| `design_spec/` | Final specification for developers |

### Naming Convention

Files: `snake_case` matching the feature and screen name.
- `mockups/batch_export_job_list.html`
- `mockups/batch_export_progress_modal.html`
- `mockups/batch_export_empty_state.html`

## 5. Design Specification

After mockups are complete, create the design specification in `design_spec/`:

### `design_spec/spec.md`

Structure:
```
# [Feature Name] — Design Specification

## Overview
One paragraph describing the feature and its purpose.

## Component Hierarchy
- ParentComponent
  - ChildComponentA (reuse existing: path/to/existing.component.ts)
  - ChildComponentB (new)
    - SubComponent (reuse existing: path/to/existing.component.ts)

## Components

### ComponentName
- **Purpose**: What it does
- **Mockup**: mockups/filename.html
- **States**: default, empty, error, loading
- **Props/Inputs**: what data it receives
- **Events/Outputs**: what it emits
- **Responsive**: how it adapts (e.g., "stacks vertically on mobile")

## Interactions
- Click [element]: [what happens]
- Hover [element]: [visual change]
- Keyboard: [shortcuts or tab order notes]

## Reuse Map
| Existing Component | Path | Used For |
|---|---|---|
| CardComponent | cockpit/src/app/shared/... | Job list items |

## New Components Needed
| Component | Suggested Path | Purpose |
|---|---|---|
| BatchActionBar | cockpit/src/app/shared/... | Floating bar when items selected |

## Implementation Notes
- [Any technical constraints, gotchas, or suggestions for the developer]
```

## 6. Final Review

- Verify every mockup uses design system tokens (no hardcoded colors)
- Verify every screen from the plan has a mockup
- Verify the design spec covers all components and interactions
- Verify responsive behavior is documented
- Cross-check against the original feature requirements — does the design address everything?
- Ensure all output files are in `mockups/` and `design_spec/`
