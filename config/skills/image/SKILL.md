---
name: image
description: Use when the work needs pictures made or changed — an image, a picture, a photo, an illustration, a drawing, a poster, a cover, character art, a character sheet, a storyboard, pictures for a deck or a web page, or a training set for a LoRA. Choose the way in (the pinned image model, an API key in a connector, or a Civitai connector for open models), draft cheaply, look at every result, change one thing at a time, and look at the final file before delivering. Charts and diagrams come from `charts`, not from an image model.
display_name: Image
icon: image
color: "#f5c2e7"
tags:
  - images
  - design
  - illustration
---

# Image

Image models are fast and confident and often wrong: an extra finger, two
people instead of one, a sign that says nonsense. The work is not writing a
prompt; it is the loop around it. Never deliver or describe an image you have
not looked at. References and helpers are in `skills/image/`.

## Choose the way in

1. **Is it a picture at all?** Charts, diagrams, icons, tables, UI mock-ups
   and anything with exact text or numbers are drawn with code (HTML/SVG,
   matplotlib, the `charts` skill): sharper, editable and always correct.
   Image models are for photos, illustrations, characters and scenes.
{% if has_tool("generate_image") %}
2. **The installation's image model:** `generate_image`. It saves into the
   workspace (default `output/images/`), writes the prompt next to each image
   in `.provenance/`, and shows you the result: one image, or a numbered
   contact sheet for several. Paid models have a per-job/Session image cap;
   a subscription model stops when its quota runs low. The result says which.
{% else %}
2. **No image model is pinned here** (`generate_image` is missing). Use one
   of the ways below, or tell the user that an admin can pin one under
   Admin → Models → Defaults → Image generation.
{% endif %}
3. **An API key the user attached** (a connector with `OPENAI_API_KEY`,
   `GEMINI_API_KEY`, …):
{% if has_shell %}
   `skills/image/scripts/imagegen.py` for single images,
   `skills/image/scripts/batch.py` for many. The
   key never leaves the workspace; nothing is capped for you, so `batch.py`
   refuses to run without `--max-spend`.
{% else %}
   these need a shell, which this workspace has not got. Say so; a Container
   workspace runs `skills/image/scripts/`.
{% endif %}
4. **Open models through a Civitai connector** (its tools are named
   `mcp__<the connector>__…`): characters, anime, LoRAs, styles the big
   models refuse to vary. Read `skills/image/references/open-models.md` for
   the prompt language and settings, and `skills/image/references/civitai.md`
   for the tools. For GPT Image and Gemini details see
   `skills/image/references/corporate-models.md`.

When more than one way is open, ask only if it matters (cost, look, who
pays). Business work defaults to the pinned model or the user's API key.

## The loop

1. **Turn the request into a full brief:** subject, setting, composition,
   light, style, the aspect ratio of where it will be used, and what must not
   appear. Never pass a short request through as it is.
2. **Draft cheaply:** low quality, one to four variations.
3. **Look.** One image comes back to you; for a sheet, pick by number, then
   open the candidate with `read_file`. Check the subject, the count of people
   and objects, hands and faces, any text, and what the user asked for in
   particular. A large image is shown to you downscaled: use
   `read_file(path, region=[left, top, right, bottom])` (percent) to check
   faces, hands and small text at full resolution.
4. **Change one thing** and run again. To keep a character, an object or a
   style, pass the approved image as `reference_images` instead of
   describing it again.
5. **Final:** high quality, with the approved draft itself in
   `reference_images` and its prompt, so the composition you approved is
   kept (a new generation from the prompt alone draws a different picture).
   Look at the final file before you deliver it. Describe only what you saw.
6. **Tell the user** what you used (model, size, quality, references) and
   where the file is. Offer the next change.

Show sets side by side with `compose_images` (a labelled grid saved as PNG)
and put it on the Canvas with `set_canvas(renderer="image")`.

## Workflows

**Pictures for a deck** (with `slides`): decide per slide whether it needs a
picture or a chart; fix one style (a reference image or a written style
block) for all of them; keep text out of the pictures, the deck renders it;
generate at the slide's aspect ratio; render the deck and look at the slides.

**A character and its training set:** read
`skills/image/references/training-sets.md`.
In short: character bible → reference sheet the user approves → shot list →
every image one step from the approved sheet (never an edit of an edit) →
look and cull to 15–30 → caption in the base model's style → zip.

**A storyboard:** character sheets first, then each panel against them;
redo the panels that drifted; assemble them into a deck or a sheet.

**Many images:**
{% if has_shell %}
a JSONL manifest → `skills/image/scripts/batch.py --max-spend <USD>` →
contact sheets with `skills/image/scripts/contact_sheet.py` → review →
regenerate the rejects. Split very
large sets across delegated workers.
{% else %}
generate in calls of up to four, review each sheet, and keep a list of what
is done; a Container workspace can run the batch script instead.
{% endif %}
Bulk work never goes through a subscription model: it shares one quota with
everyone's chat.

## Rules

- No real people's likeness, no imitation of a living artist's signature
  style by name, no logos or brands the user does not own, unless the user
  has the rights and says so.
- A refusal by the provider's safety system is final for that prompt: do not
  rephrase around it. Tell the user.
- Keep the originals. Edit copies, and never upscale for a training set.
