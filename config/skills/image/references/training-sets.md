# A character and its LoRA training set

How to make a consistent character and a set of 15–30 images to train a LoRA
on it. The numbers come from the trainers' own guidance (Civitai Training
Studio, ostris ai-toolkit, kohya) and the model vendors' documentation,
read in November 2026.

## Contents

- 1. The character bible
- 2. The reference sheet
- 3. The shot list
- 4. Generating the set
- 5. Looking and culling
- 6. Sizes and formats
- 7. Captions
- 8. Packing and training
- 9. Picking the checkpoint and keeping the recipe

## 1. The character bible

Talk the character through with the user and write it down (a knowledge
note if you have the tools): name, age, build, face, hair, eyes, skin, marks
and scars, usual clothes, palette, personality as it shows in posture and
expression. Write a **fixed description block**: one paragraph you will
reuse word for word.

## 2. The reference sheet

Make one sheet: front, three-quarter, side and back views, a face close-up
and a row of expressions, on a plain background. Show it to the user (on the
Canvas) and get an OK. Then record the traits that must never change: face,
hair, age, build, marks. This sheet is the source of every later image.

## 3. The shot list

Plan the set as a table, not a loop of prompts. Vary everything that is not
identity:

- framing from close-up to full body (most images medium shots);
- angles: front, three-quarter, side, from above, from below;
- expressions and poses;
- at least two outfits;
- settings and light: indoor, outdoor, day, night, hard and soft light.

## 4. Generating the set

**Generate every image one step from the approved sheet panels: never edit
an edit.** Chained edits pile up artifacts and drift away from the face
(the FLUX.1 Kontext paper; Google's own advice for Gemini).

Reference limits per call:

| Model | References |
|---|---|
| GPT Image | up to 16 images |
| Gemini 3 Pro Image | up to 5 character references (14 images in all) |
| Flux.2 | 4 on klein, 6 on dev, 8 on pro |
| Qwen-Image-Edit 2509/2511 | 1–3; it can rotate a view for side and back |

Avoid generators with little variety between seeds (Z-Image Turbo is rated
low on diversity): the set needs variety around a fixed identity.

## 5. Looking and culling

Open every image. Drop the ones that drift from the sheet, broken anatomy,
text and logos, other faces in the frame, blur, and near-duplicates. Fill
empty cells of the shot list instead of padding with look-alikes.

**Keep 15–30 images, never fewer than 10** (Civitai Training Studio; ostris
recommends 12–18 for a character). Compose contact sheets of the set to
check it as a whole.

## 6. Sizes and formats

Keep the originals at their generated size. Never upscale; crop only to fix
framing. The trainers bucket images by aspect ratio themselves. Save as PNG
or JPG (ai-toolkit has had problems with WebP).

## 7. Captions

One `.txt` per image, same name (`003.png` → `003.txt`), **the trigger word
first**:

- **tag families** (SDXL, Pony, Illustrious, NoobAI): booru tags; NoobAI in
  its documented order;
- **prose families** (Flux, Flux.2, Qwen-Image, Z-Image, Chroma): one or two
  plain sentences.

Describe what varies (clothes, pose, setting, light, expression) and leave
out the fixed identity traits: what you do not caption binds to the trigger
(kohya; Black Forest Labs' guidance).

## 8. Packing and training

Zip the folder flat: images and their `.txt` files, no subfolders. Civitai's
defaults for a character are a sensible start: 2,000 steps and 10 saved
checkpoints, about 35 passes per image. Uploading to Civitai's Training
Studio is done by the user on the site (the MCP server has no upload tool):
give them the zip and the settings.

## 9. Picking the checkpoint and keeping the recipe

Test each checkpoint with three prompts: one close to a training image, one
in a new setting, one in a new outfit. Pick the **earliest** checkpoint where
the face holds and new prompts are followed; later ones overfit.

Save the recipe as a knowledge note: trigger, base model, checkpoint, LoRA
strength, caption style, and the set's manifest. Never train a new LoRA on
the old LoRA's output without the original sheet panels: each round loses
detail.
