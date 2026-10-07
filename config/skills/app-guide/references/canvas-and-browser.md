---
guide_id: canvas.present-and-browse
content_type: how_to
capability_ids:
  - canvas.files
  - canvas.browser
  - workspaces.select
journey_ids:
  - canvas.present-file
  - canvas.share-browser
---

# Show work and share a browser with Canvas

Three related surfaces have different jobs:

- **Canvas** is the shared stage beside a persistent session. It presents a
  workspace file, a deployment-gated live application, or the shared browser.
  The real file or browser remains the source; Canvas is not another place
  where SRW stores a copy.
- **Direct browser tools** let the agent navigate, click, type, select, scroll,
  and take screenshots in a workspace browser.
- **Shared browser** is a deployment-gated Canvas view of that same browser.
  You and the agent can take turns controlling it without losing the current
  page, cookies, or login state.

Web search and page-reading tools are separate. A session may be able to
research the web even when it has no interactive browser or shared-browser
viewer.

## Enable browser tools in this session

For a user asking the agent to open a site, click through a page, or use an
online service, explain the setup path if browser tools are missing:

1. Open the chat header's **Settings** (sliders icon), or on a narrow screen
   choose **Settings** from the header's three-dot menu.
2. In the **Settings** tab, check **Workspace**. A running session can't
   upgrade to a Container: start a new Container session for the direct
   browser. A Virtual session can upgrade to a **VM** when VMs are allowed;
   confirm **Upgrade** and wait for it to finish. This carries existing files
   over and keeps the conversation. There is no live Advanced tab.
3. Under **Tools**, enable **Browser** if it is off. The direct-browser tools
   need a shell-capable workspace and the `browser` capability grant. The
   **Shell** checkbox controls separate command tools; it is not an extra
   requirement for direct browser control. If Browser is unavailable, use its
   shown reason; a grant restriction needs an administrator under
   **Admin → Grants**, while a workspace restriction needs the compatible tier.
4. Settings apply automatically with the **next response**. After the upgrade
   and setting change, send the agent a follow-up to continue. The agent must
   check that the actual browser tools are loaded and report the outcome of
   its browser operation.

If `request_workspace_upgrade` is visible, the agent can present a VM upgrade
offer itself. The user still decides; an offer does not mean a browser is
already available. Meanwhile the agent can use available web research or
prepare files that do not need browser interaction.

For live state, `workspaces.select` covers the workspace observation;
`canvas.browser` covers the **shared-browser viewer**, not direct-browser tool
readiness. The registry has no dedicated direct-browser capability ID. Use
the current Browser row and exact operation tools for that narrower fact;
do not substitute a shared-browser result. Direct browsing does not require
Canvas or the shared-browser deployment feature to be enabled.

## Put a file on Canvas

Ask the agent to present an existing artifact—for example, “Put
`output/report.md` on Canvas.” The agent can do this only when `get_canvas`,
`set_canvas`, and `clear_canvas` are actually in its current tool list. File
Canvas is for authenticated persistent sessions with a compatible, readable
workspace; it is not a worker-job view.

When a new source is presented, Canvas opens automatically on desktop. On
mobile, use the **Canvas** toggle or the **Open Canvas** action on the trusted
tool card. Closing the pane is local to your view: it does not clear the shared
source. Clearing Canvas removes the presentation pointer but does not delete
the file, stop an application, or close a browser.

The shipped file renderers support:

- Markdown, including bounded math rendering;
- plain text, source code, and LaTeX source text;
- strict, self-contained static HTML;
- explicitly requested, self-contained interactive HTML when the current tool
  schema advertises `html-interactive`;
- Word, Excel, and PowerPoint files (`.docx`, `.xlsx`, `.pptx`) and their
  OpenDocument equivalents (`.odt`, `.ods`, `.odp`), only on a deployment that
  enables the Office editor; and
- PNG, JPEG, WebP, and GIF raster images.

SVG, PDF, Mermaid, and compiled full-document LaTeX are not supported file
Canvas renderers today.

Strict `html`—and `auto` when it detects an HTML file—is deliberately inert:
scripts, forms, animations, style blocks, external resources, embedded data
assets, and other active content are removed. For a mockup, visualization,
widget, or small game that needs CSS or inline JavaScript, the agent can
explicitly choose `html-interactive` only when that value appears in its
current `set_canvas` schema. `auto` never selects it. Interactive HTML runs in
an opaque, no-network sandbox with no forms, popups, storage, parent-page
access, or external and sibling resources; every CSS, script, image, and font
must be in the one bounded HTML file. It is an isolated artifact, not a live
workspace application.

An Office document is detected from the file itself, so `auto` is enough and
no other renderer will accept it. On a deployment without the Office editor
the presentation fails outright instead of degrading to text, so ask the agent
for a Markdown or plain-text version of the same content. Where the editor is
enabled, you edit the document in Canvas and the agent edits that same
workspace file between turns; it does not type beside you in the document.

A one-port live application may be available only when the deployment enables
**Live Preview** and the current `set_canvas` tool advertises that source type.

Text, Markdown, code, LaTeX source, strict-static-HTML, supported
interactive-HTML, and Office files may be editable when the workspace is
writable and the agent presented them with editing enabled. Images are not
source-editable.
Before changing an editable file that may have user edits, the agent should
inspect Canvas and re-read the current file. After changing the file, it must
present it again to publish the new source version. If Canvas says the source
changed, ask the agent to present it again; **Refresh** does not silently adopt
different workspace bytes under the old presentation.

## Share the browser with the agent

**Required workspace:** start with or upgrade to a **Container workspace**.
Virtual and None cannot host the shared browser, and VM support must not be
promised.

Shared browser is default-off and deployment-dependent. If it is enabled for a
compatible full workspace, the currently proven path is a **Container
workspace**; Virtual and None do not provide the required browser workspace,
and VM support must not be promised. Use the web icon in the session header or
**Open browser** in the Canvas empty state:

1. Choose **Open browser**. A cold Container session may first show
   **Starting workspace…**, then **Starting browser…**, then **Connecting…**.
2. Choose **Take control** when you need to navigate, dismiss a banner,
   complete a login, or handle a page the agent cannot.
3. Browse in the Canvas toolbar. While you are driving, the agent's mutating
   browser actions return `user_is_driving`; it can still inspect read-only
   snapshots.
4. Choose **Release control**, then tell the agent what to do next. Baton
   changes do not send the agent a separate takeover notification.

This is one Chromium session. After handoff, the agent sees the same DOM,
cookies, and authenticated state. Log out before releasing control if you do
not want the agent to use that account. Closing the Canvas pane merely detaches
your viewer; it does not close the workspace browser.

The stream shows the rendered web page, not browser- or operating-system
dialogs. Native file pickers, print dialogs, certificate or WebAuthn prompts,
browser-level authentication dialogs, and the built-in PDF viewer are known
gaps. Downloading a file is often the practical PDF fallback.

Shared browser is currently a persistent-session surface. It is not available
on the Jobs view, and the proven deployment path is a Container workspace; do
not promise VM support.

## If Open browser is missing or disabled

Use the reported code and shown reason instead of guessing:

- `feature_disabled` — Shared browser is off on this server, so its normal
  action is intentionally hidden.
- `workspace_required` — **This session type does not provide a browser
  workspace**; the session is using Virtual or None. Start with or upgrade to a
  Container workspace.
- `workspace_unattested` — **This workspace is not trusted for shared-browser
  access**; its current workspace binding is not attested. Retrying the same
  action does not grant trust.
- `workspace_unroutable` — **The server cannot securely reach this workspace
  browser**; this is a deployment routing problem.
- `transport_unavailable` — **Secure browser transport is not configured on
  this server**; an administrator must configure the broker transport.

Direct agent browser tools have a separate availability path. They require a
shell-capable workspace, the Browser tool category, and the applicable
`browser` capability grant. Virtual and None sessions filter those tools out
even if an expert configuration requested them. The app guide itself grants
neither direct-browser tools nor the shared-browser feature.
