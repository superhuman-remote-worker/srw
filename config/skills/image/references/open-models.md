# Open-weight image models: prompts, resources and settings

The tool-neutral guide to open diffusion models (Illustrious, NoobAI, Pony,
SDXL, SD 1.5, Flux, Qwen-Image, Z-Image, Chroma and others). How to run them
through a Civitai connector is in `civitai.md`; how to look at the results is
in the skill's loop.

## Contents

- Before you start: the four mistakes
- 1. Understand the request
- 2. Choose the model family
- 3. Pick the checkpoint and the LoRAs
- 4. Write the prompt in the family's language
- 5. The negative prompt
- 6. Settings (samplers, steps, CFG, sizes, seeds)
- 7. The same character in several images

## Before you start: the four mistakes

Open-weight diffusion models are not Midjourney: they don't turn a vague idea into a beautiful picture on their own. They do what the prompt says, in the vocabulary they were trained on, with the settings they were made for. You do the work Midjourney hides: turn the user's idea into a full prompt (never pass a short request through as it is), and choose the model, LoRAs and settings that fit it.

Most bad images come from four mistakes. Avoid them first:
1. **A prompt in the wrong style for the family:** tags for a prose model, or score tags on a model that doesn't use them.
2. **Resources that don't match:** a LoRA made for another base model.
3. **Settings from another family:** SDXL's CFG on a Flux model, a Turbo model at 30 steps.
4. **A size outside the model's native sizes:** that gives duplicated people and broken bodies.

## 1. Understand the request

- Find out the subject, the style (photo, anime, illustration, painting, 3D render, pixel art), the mood, the composition, the aspect ratio and how many images.
- Ask at most one short round of questions, and only when the answer changes the image. A request like "a cozy cabin in the snow" is enough: choose, and say what you chose.
- Some requests decide the model:
  - **Text in the image** (a sign, a poster, a title) needs a family that can write: Qwen-Image, Flux or Z-Image.
  - **A known character, an artist's style or a concept** the base model doesn't know needs a LoRA.
- **When the user wants a change to an earlier image,** keep its model, LoRAs, seed and settings, and change one thing.

## 2. Choose the model family

| The user wants | Family |
|---|---|
| Anime, manga, illustration, characters from anime and games | **Illustrious** (by far the most used on Civitai, the most LoRAs) or **NoobAI** (an Illustrious fine-tune with more characters) |
| Western cartoon, furry, stylised art | **Pony V6** |
| Photoreal people, places, products | **SDXL** photoreal checkpoints; **Flux.1 dev** or **Flux.1 Krea** (aesthetic photography); **Z-Image Turbo** (fast and photoreal, but images vary little); **Chroma** |
| Readable text in the image | **Qwen-Image** (best, also Chinese); Flux; Z-Image |
| A complex scene: several people, exact placement, many details | A prose model (Flux, Qwen-Image, HiDream, Z-Image). Their text encoders follow composition better than the CLIP encoders of SDXL and its fine-tunes |
| Quick, cheap drafts | SDXL Lightning or LCM (draft mode), Z-Image Turbo, Flux.1 schnell |
| A checkpoint the user names | That one |

Newer families appear often (Flux.2 and Flux 3 Klein, Krea 2, Anima, HiDream, MOSS Image). For one you don't know, read the model's page for its prompt style and settings before you use it. Civitai can't generate with SD 3 or SD 3.5.

## 3. Pick the checkpoint and the LoRAs

- **Finding them:** use a model search tool if you have one, such as Civitai's site tools (`search_models`, `get_model`, `get_model_version`). Search by base model and style, and prefer models that are popular and recently updated. From the model version you take:
  - its **AIR**, the identifier the generator takes, like `urn:air:sdxl:checkpoint:civitai:101055@128078` (`urn:air:{ecosystem}:{type}:civitai:{modelId}@{versionId}`). Copy the version's `air` field; don't build it yourself, as the ecosystem codes are inconsistent;
  - its **base model**;
  - its **trigger words** (`trainedWords`);
  - the **recommended strength and settings** in its description, which beat any general rule here.
- **Compatibility is mandatory.** A LoRA must be made for the checkpoint's family: SDXL LoRAs for SDXL, Illustrious LoRAs for Illustrious. SDXL, Pony, Illustrious and NoobAI can use each other's LoRAs only partly, so prefer one made for your exact family. A LoRA never runs alone: it needs a checkpoint.
- **Trigger words** go into the prompt, next to what they describe. Some LoRAs need none.
- **Strength:** follow the version's description; otherwise start at 0.7 to 0.8. Character LoRAs often need 0.8 to 1.0, style LoRAs 0.5 to 0.8. Too high gives overcooked colours, artifacts and a prompt that is ignored.
- **Stacking:** a typical combination is one character LoRA, one style LoRA and one detail or concept LoRA. Add them one at a time, and lower the others' strength when you add one. Two style LoRAs fight each other. Civitai allows about 9 resources per image (checkpoint included) on a free account; the median image uses 4.
- **Embeddings** (textual inversions) only work in their own family (EasyNegative is SD 1.5 only). They are named in the prompt or the negative prompt.

## 4. Write the prompt in the family's language

### Tag models: SD 1.5, Pony V6, Illustrious, NoobAI, Anima

Write comma-separated Danbooru-style tags, the most important first. Illustrious from v1.0 on also understands short sentences. The order:

**quality tags → count (`1girl`, `2boys`, `solo`) → character and series → artist or style → look (hair, eyes, clothes) → pose, action, expression → setting and background → framing (`cowboy shot`, `from above`, `close-up`) → light**

Each family has its own quality tags. Using another family's does nothing or makes it worse:

- **Pony V6:**
  - Start with the whole string `score_9, score_8_up, score_7_up, score_6_up, score_5_up, score_4_up` (`score_9` alone is much weaker).
  - Add a source tag (`source_anime`, `source_cartoon`, `source_furry`, `source_pony`) and a rating tag (`rating_safe`, `rating_questionable`, `rating_explicit`).
  - Leave out `masterpiece`.
  - **Clip skip must be 2.** Without it you get blobs.
- **Illustrious:** `masterpiece, best quality, absurdres` (merges often document their own tags on their page; use those). Never Pony's score tags.
- **NoobAI:**
  - `masterpiece, best quality, newest, absurdres, highres`, plus a rating tag (`safe`, `sensitive`, `nsfw`, `explicit`).
  - Year tags (`newest`, `recent`, `mid`, `early`, `old`) set the drawing style's era.
  - `very awa` asks for the most aesthetic images.
- **Anima:**
  - Lowercase tags with spaces instead of underscores: `masterpiece, best quality, score_7, safe`.
  - Artist tags start with `@`.
  - Prose needs at least two sentences.
- **SD 1.5:** `masterpiece, best quality`.

Weights and length:
- **Weights** like `(red scarf:1.2)` work on these models. Use them for two or three terms at most and stay between 0.5 and 1.5: many weights cause artifacts.
- **Length:** CLIP reads the prompt in chunks of 75 tokens, so put what matters first. `BREAK` starts a new chunk, which helps keep two characters' features apart.

### Prose models: Flux, Qwen-Image, Z-Image, Chroma, HiDream, Krea 2, Pony V7

Write sentences, as if describing a photo to someone. Order: the subject first, then what it does, the setting, the light, the camera or medium, and the style and mood. For example:

> A weathered fisherman in a yellow oilskin coat mends a net on a wooden pier at dawn. Thick fog hides the harbour behind him; a single lantern lights his hands from the left with warm, soft light. Shot on 35mm film with an 85mm lens at f/2, shallow depth of field, muted blue and amber tones, quiet and melancholic.

- **Leave out tags and weights:**
  - Quality tags, score tags and `masterpiece` mean nothing to these models.
  - `(word:1.2)` weights are unreliable on their encoders and don't work at all on Krea 2.
- **Describe quality** as what you'd see instead: "sharp focus, fine skin texture, detailed fabric".
- **Text in the image:** give the exact words in quotes, early and short: `a neon sign that reads "OPEN LATE"`. Put only the words the user gave on it: no invented dates, places or programme. Offer to add them instead.
- **Camera language works:**
  - "85mm portrait lens", "wide-angle 24mm";
  - "f/1.8, blurred background" or "f/11, everything sharp";
  - "golden hour backlight", "overcast soft light", "hard flash".
- **Length:** Civitai allows up to 1,000 characters for Flux; aim for under 500. Z-Image likes long, detailed prose. Qwen-Image may end with `, Ultra HD, 4K, cinematic composition.`
- **Pony V7** mixes both: its special tags (`score_X`, `source_X`), then a factual description, then a stylistic one, then extra tags.

### For every model

- **Be concrete:** "a weathered fisherman mending a net" beats "an old man by the sea". Name colours, materials, time of day and weather.
- **No contradictions,** such as "photorealistic" next to "anime style".
- **Several people:** give the count, then describe each person in turn, with their attributes right next to them. Attributes bleed from one subject to the other, especially on CLIP models: prefer a prose model for complex scenes with several people.
- **Never write "no X" in the prompt:** it adds X. Unwanted things go into the negative prompt. On models without one, describe the state you want instead ("clean-shaven", not "no beard").
- **Rating tags** (`rating_safe` … `rating_explicit`, `safe` … `explicit`) steer how explicit an image is. Put the wanted one in the prompt; if the model drifts, put the opposite in the negative prompt.

## 5. The negative prompt

**Models that use it:** SD 1.5, SDXL, Pony, Illustrious, NoobAI, Anima, Qwen-Image, Z-Image Base, HiDream Full and Chroma.

**Models that ignore it:** Flux.1 (dev, schnell, Krea), Flux.2 Klein (distilled), Flux.2 pro, Z-Image Turbo and every model run at CFG 1. Leave it empty for them.

Keep it short and aimed at real problems; long blanket lists make images worse. Starting points:

- **SD 1.5:** `worst quality, low quality, blurry, bad anatomy, bad hands, extra fingers, watermark, text`
- **SDXL (photo):** `cartoon, illustration, painting, deformed, blurry, lowres, watermark, text`
- **Pony V6:** it rarely needs one; add only what goes wrong.
- **Illustrious:** `worst quality, low quality, lowres, bad quality, comic, multiple views, jpeg artifacts, signature, watermark`
- **NoobAI:** `worst quality, old, early, low quality, lowres, signature, username, logo, bad hands, mutated hands`
- **Anima:** `worst quality, low quality, score_1, score_2, score_3, artist name, blurry, jpeg artifacts`

Then add what you actually saw go wrong (`extra arms`, `2girls` when you wanted one).

## 6. Settings

| Family | Sampler | Steps | CFG / guidance | Clip skip |
|---|---|---|---|---|
| SD 1.5 | Euler a or DPM++ 2M Karras | 20–30 | 6–8 | 2 for anime models |
| SDXL | DPM++ 2M Karras or Euler a | 20–30 | 5–7 | 1; 2 for anime checkpoints (when the input schema has `clipSkip`) |
| SDXL Lightning, LCM, Turbo | Euler or LCM | 4–8 | 1–2 | — |
| Pony V6 | Euler a | 25 | 6–7 | **2** |
| Illustrious | Euler a | 20–28 | 5–7 | 1–2 (follow the checkpoint) |
| NoobAI (eps) | Euler a | 25–30 | 5–6 | — |
| NoobAI (v-pred) | **Euler only** | 28–35 | 4–5 | — |
| Anima | er_sde or Euler a | 30–50 (turbo 8–12) | 4–5 (turbo 1) | — |
| Flux.1 dev, Krea | Euler, simple | 20–30 | 3.5 (Krea 4.5) | — |
| Flux.1 schnell | Euler | 1–4 | 0–1 | — |
| Flux.2 Klein (distilled) | Euler | 4–8 | none / 1 | — |
| Chroma | Euler | 25–40 | 3–3.5 | — |
| Qwen-Image | Euler, simple | 20–50 | 2.5–4 | — |
| Z-Image Turbo / Base | Euler, simple | 8–12 / 20–30 | 1 / 3–5 | — |
| HiDream Full / Dev / Fast | UniPC | 50 / 28 / 16 | 5 / 0 / 0 | — |

**Sizes:**
- **SDXL and nearly every newer family (about 1 megapixel):**

  | Shape | Size |
  |---|---|
  | Square | 1024×1024 |
  | Portrait | 832×1216 or 896×1152 |
  | Landscape | 1216×832 or 1152×896 |
  | Tall | 768×1344 |
  | Wide | 1344×768 |
  | Very tall or very wide | 640×1536, 1536×640 |
- **SD 1.5:** 512×768, 768×512, 512×512.
- **Qwen-Image:** 1328×1328, 1664×928 or 928×1664.
- **Bigger than native** gives twins, extra limbs and repeated objects. Generate at a native size and upscale or use hires fix instead (on Civitai a hires-fix pass for the SD family upscales 1.5×; a denoise of 0.3 to 0.5 keeps the image, higher values change it).

**Seeds:** random for a first try. Note the seed of a result the user likes, and reuse it to change one thing at a time.

**How many:** two to four variations of a cheap model help to find a good one; one at a time for expensive ones.

## 7. The same character in several images

- Reuse one fixed description block word for word, the same checkpoint, and the same character LoRA at the same strength. For close variations, keep the seed too.
- Models that edit with reference images (Flux.2 Klein, Qwen-Image Edit) can keep a face or an outfit from an earlier image.
- If you have the knowledge base tools, save a character's or a style's recipe as a note: the model's AIR, the LoRAs and strengths, the prompt block, the negative prompt, the seed and the settings. Then you can reproduce it in later conversations.
