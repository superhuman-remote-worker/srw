---
name: present-with-canvas
description: Present or collaboratively edit a substantial visual artifact, document, image, static HTML prototype, or other workspace file on the user's shared Canvas when seeing or working with it is more useful than reading it inline in chat.
---

# Present With Canvas

Use Canvas as a stage for an artifact, not as a second artifact store. Keep the
source in `output/` or its existing project location and present that file.

## Decide Whether to Present

Present substantial content that benefits from inspection, visual layout, or
continued collaboration. Examples include reports, Markdown with math, code or
text documents, generated images, and self-contained static HTML prototypes.

Keep short answers, tiny snippets, progress updates, and content that reads
better inline in chat out of Canvas.

## Present a File

1. Call `get_canvas` before replacing an existing presentation.
2. Create or update the real workspace artifact. Reuse an existing project file
   or a stable path under `output/`; do not copy it into a Canvas-only folder.
3. Verify that the file exists and contains the intended final bytes.
4. Call `set_canvas` with `source_type="workspace_file"`, its workspace-relative
   path, a useful title when the filename is unclear, and the narrowest suitable
   renderer. Prefer `auto` when detection is unambiguous. Set `editable=true`
   only when the user should collaborate directly on a supported text,
   Markdown, code, LaTeX, or strict-static-HTML source. Leave it false for
   finished/read-only artifacts and all images.
5. For a standalone raster image source, supply concise, meaningful tool-level
   `alt_text`. Give images embedded in Markdown their own inline alt text.
6. Tell the user what is on the stage and what feedback would be useful.

For strict static HTML, use `renderer="html"` or `auto` and make one script-free
document with simple, bounded inline styles. Style blocks, animations, filters,
scripts, forms, and embedded or external resources are removed. Do not embed
images or fonts, including data URLs, and do not use relative resources because
the strict renderer never resolves sibling workspace files. Present a raster
image as its own Canvas source.

For a self-contained HTML mockup, visualization, widget, or small game that
needs style blocks or inline JavaScript, explicitly choose
`renderer="html-interactive"`. This mode preserves the document's HTML, CSS, and
inline scripts inside an opaque-origin sandbox. It has no network access, forms,
popups, storage, parent-page access, or external/relative resources, so inline
every required CSS, script, image, and font. `auto` never selects this mode.
Keep the file bounded and self-contained, and do not describe it as a live
workspace application.

When the mockup is for a real organization, place, or product, show its real
material instead of drawn stand-ins. Download its logo, a few key photos, and
the web fonts its own site already serves with the shell, downscale the photos
(for example to 1600 px wide JPEG at quality 75), and inline them as data URLs.
The document must stay under 2 MiB including the base64, which grows each file
by a third, so plan on three to five photos. Tell the user which images came
from their site.

Use only source types and fields present in the current `set_canvas` schema;
never guess app, port, browser, or routing fields that are not advertised.

## Refresh Without Losing User Work

Treat the workspace file as shared state. Before overwriting a file already on
Canvas, call `get_canvas` and re-read the current file immediately, even if you
read it earlier in the turn. Incorporate possible user changes instead of
assuming the bytes still match your last write. If the presentation is
editable, treat its current workspace bytes as user-owned input and preserve
them unless the user explicitly asks to discard them. After every material
update, call `set_canvas` again with the same path and editability choice to
publish a new source version.

`clear_canvas` removes only the presentation pointer. It does not delete the
file, so delete or stop underlying resources only when the user separately asks.

## Protect the Stage

- Never present secrets, tokens, credentials, private keys, environment files,
  internal addresses, or hidden operational data.
- Never imitate trusted browser or authentication chrome in untrusted content.
- Never turn a workspace path into an arbitrary URL or `localhost` pointer.
- If validation rejects a file or renderer, fix the artifact or choose a
  supported renderer; do not bypass the boundary.
