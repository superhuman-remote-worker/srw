# Hosted image models: GPT Image and Gemini

What the hosted models take and do well. Model names change every few
months: use the one the installation pinned (`generate_image` says which) or
the one the user names; scripts read it from `--model` or `IMAGEGEN_MODEL`.
Facts below were read from the providers' documentation in November 2026.

## Contents

- GPT Image (OpenAI, also through a ChatGPT subscription)
- Gemini image models ("nano banana")
- Choosing between them
- Prompting hosted models

## GPT Image (OpenAI, also through a ChatGPT subscription)

- **Sizes.** `1024x1024`, `1536x1024`, `1024x1536` on every version. The 2.x
  models (gpt-image-2, 2.5 "sunburst", 2.6 "flare") also take any `WxH` with
  both edges multiples of 16, an aspect ratio within 3:1, no edge above 3840
  and 655,360–8,294,400 pixels in total. `generate_image` fits a size into
  these bounds for you.
- **Quality.** `low` for drafts (fast, a fraction of the price), `medium`,
  `high` for finals. Price follows output image tokens, which grow with size
  and quality.
- **Edits and references.** Up to 16 reference images per call (as files or
  data URLs). On 2.x there is no `input_fidelity` setting; describe what must
  stay the same.
- **Text.** Good at short, quoted text in the image. Still check every
  letter.
- **No seed.** Two calls with the same prompt differ; keep the image you
  like and edit from it.
- **Returned format.** Base64 PNG (`b64_json`); there is no URL to fetch.
- **Through a ChatGPT/Codex subscription** the same models run on the
  user's plan quota, shared with everyone's chat on the installation. Only a
  paid plan can make images. Never bulk through it.

## Gemini image models ("nano banana")

- **Endpoint.** `generateContent` with `responseModalities: ["IMAGE"]` (text
  may come along). One image per call; ask n times for n images.
- **Size.** An aspect ratio (`1:1`, `2:3`, `3:2`, `3:4`, `4:3`, `4:5`, `5:4`,
  `9:16`, `16:9`, `21:9`) and an `imageSize` of `512`, `1K`, `2K` or `4K`.
  There is no quality setting and no seed.
- **References.** Up to 14 images per call; Gemini 3 Pro Image keeps up to 5
  characters consistent and the rest as objects or style.
- **Safety.** A 200 answer can carry no image and a `finishReason` such as
  `IMAGE_SAFETY`: that is a refusal, not a glitch.
- **Thinking.** Some versions think before drawing and bill it as output
  text; parts marked `thought` are not the result.
- **EU users:** the free tier may not be used for EU users' data; use a paid
  key.

## Choosing between them

| Need | Pick |
|---|---|
| Exact short text on a poster or a sign | GPT Image 2.x, then check every letter |
| Photos and product shots | either; GPT Image high quality is the safe default |
| One character across many images | Gemini 3 Pro Image or GPT Image with the reference sheet in every call |
| Very large output (4K) | Gemini at `4K`, or GPT Image 2.x at a custom size up to 3840 |
| Cheapest drafts | GPT Image `low`, Gemini at `512`/`1K` |
| Styles hosted models flatten, anime, LoRAs | open models through Civitai (`civitai.md`) |

## Prompting hosted models

- Write prose, like a brief to a photographer or illustrator: subject,
  action, setting, light, lens or medium, mood. Tags and weights do nothing.
- Say what to keep when editing: "same face, same jacket, change only the
  background to a rainy street".
- Name the use: "a 16:9 hero image for a web page, empty space on the left
  for a headline" gives a better layout than asking for text in the image.
- Hosted models refuse real people's likeness, some brands and violence; a
  refusal is final for that prompt.
