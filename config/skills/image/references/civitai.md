# Civitai: open models through the connector

Civitai runs open-weight models (Illustrious, NoobAI, Pony, SDXL, Flux,
Qwen-Image, …) with any LoRA on the site and bills in **Buzz**, the user's
own credit. The user attaches it as an MCP connector; its tools reach you as
`mcp__<connector>__<tool>`, where `<connector>` is the name the user gave it.
Never assume the name: use the tool names you actually have.

Read `open-models.md` first: the prompt language, LoRAs and settings decide
the result far more than the tool calls below.

## Contents

- 1. Find the model and the LoRAs
- 2. Get the input for them
- 3. Check the price, then run
- 4. Long jobs
- 5. Look at every image
- 6. Keep the originals
- 7. When it fails

## 1. Find the model and the LoRAs

Use the site tools when the connector has them (`search_models`,
`get_model`, `get_model_version`). From the model **version** take:

- its **AIR**, e.g. `urn:air:sdxl:checkpoint:civitai:101055@128078`. Copy the
  version's `air` field; never build one yourself, the ecosystem codes are
  inconsistent;
- its **base model** (a LoRA must match the checkpoint's family);
- its **trigger words** (`trainedWords`);
- the **recommended strength and settings** in its description, which beat
  any general rule.

## 2. Get the input for them

Call `get_input_schema` with `resources` set to the checkpoint's and the
LoRAs' AIRs. You get the schema and an example input for the service they
run on. **Use exactly the field names of that example**: they differ
between families (`model` or `checkpointModel`; `sampler`/`scheduler` or
`sampleMethod`/`schedule`; `loras` as an object `{"<AIR>": 0.8}` or a list),
and unknown fields are rejected. Without specific models, `find_services`
with a query like "anime image" or "photoreal image with text" first.

Fill in: prompt, negative prompt (only for families that use one), width and
height at a native size, steps, CFG, sampler, seed and quantity.

## 3. Check the price, then run

1. **Always run `run_step` with `whatif: true` first.** It returns the cost
   in Buzz and runs nothing. Rough guide: SDXL at 1024×1024 and 20 steps is
   about 8 Buzz; cost grows with pixels × steps × quantity; on Flux-type
   models a CFG other than 1 doubles it.
2. Tell the user the cost when it is more than a few images' worth, or when
   they asked to approve spending. The server may also ask them to confirm
   Buzz itself: that question reaches them, and a job declines it unless
   the user set the connector to accept.
3. Run `run_step` with `stepType: "imageGen"` and the `input`. Start with one
   or two images; a quantity of four only for cheap models.

## 4. Long jobs

An SRW MCP call waits **60 seconds** at most. Anything that may take longer
(large images, many steps, Flux.2, video) runs with
`waitForCompletion: false`; then check it with `get_workflow` until it is
done. A timed-out call may still have run on Civitai and cost Buzz: check
with `get_workflow` before running it again.

## 5. Look at every image

Images the result carries come back to you as images (or, for a model that
cannot see, as a description by a vision model). Image *links* come back as
`[resource link] …`: read one with `read_mcp_resource`. Before you show or
describe anything:

1. Open every image of the run.
2. Check it against the request: subject, count of people, pose, style,
   composition, hands and faces, any text.
3. When one clearly misses, find the cause in `open-models.md` (the table
   under "Settings" and the fixes in the skill), change one thing, keep the
   seed, run once more.
4. Show the ones that pass; if none passes after the second run, show the
   best one, say what is wrong and what you would change, and ask first.
5. Say what you used: model, LoRAs with strengths, seed, size, settings.

## 6. Keep the originals

What a tool result shows you may be a preview. To keep a file:

- a resource link: `save_mcp_resource` with the link's URI and a workspace
  path (it refuses a corrupt copy and says why);
- a signed download URL in the result: download it in the shell
  (`curl -fLo output/images/<name>.png "<url>"`) when the workspace has one.
  The URL expires; save it right away.

Write the recipe next to the file (`.provenance/<path>.json`: AIRs,
strengths, prompt, negative prompt, seed, size, sampler, steps, CFG), and
for a character or a style also as a knowledge note, so it can be made again.

## 7. When it fails

- **Validation error:** read the message (it names the valid fields), fix
  the input, run again.
- **`blocked`:** never retry the same input; repeated blocks get the account
  muted. Tell the user.
- **Not enough Buzz:** stop and tell the user; do not lower quality on your
  own to fit.
