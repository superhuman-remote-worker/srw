#!/usr/bin/env python3
"""Generate or edit images with an API key from the workspace environment.

For the user's own key (a connector puts it in the environment). The pinned
installation model is the ``generate_image`` tool instead; this script is
the shell lane, and nothing caps it, so keep runs small and use batch.py
with --max-spend for many images.

    python3 skills/image/scripts/imagegen.py "a red fox in snow" \\
        --out output/images/fox.png [--provider openai|gemini] [--model M] \\
        [--size 1536x1024 | --aspect 16:9] [--quality low] [--n 2] \\
        [--ref sheet.png --ref style.jpg]

Keys and endpoints (environment):
    OPENAI_API_KEY, OPENAI_BASE_URL   provider "openai" (also any OpenAI-
                                      compatible Images API)
    GEMINI_API_KEY                    provider "gemini"
    IMAGEGEN_BASE_URL                 overrides either base URL
    IMAGEGEN_MODEL                    default model

Every image is written with its prompt in ``.provenance/<path>.json`` next to
the workspace root, and its type is taken from its bytes, not the name.
Stdlib only.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

OPENAI_BASE = "https://api.openai.com/v1"
GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta"
DEFAULT_MODELS = {"openai": "gpt-image-2", "gemini": "gemini-3.1-flash-image"}
TIMEOUT = 240
EXTENSIONS = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp", "image/gif": ".gif"}
OPENAI_SIZES = {
    "1:1": "1024x1024",
    "3:2": "1536x1024",
    "16:9": "1536x1024",
    "4:3": "1536x1024",
    "2:3": "1024x1536",
    "9:16": "1024x1536",
    "3:4": "1024x1536",
}


class ImageError(Exception):
    """A provider call failed. ``retryable`` marks 429 and 5xx answers."""

    def __init__(self, message: str, *, status: int | None = None, retryable: bool = False):
        super().__init__(message)
        self.status = status
        self.retryable = retryable


@dataclass
class Result:
    images: list[bytes]
    usage: dict = field(default_factory=dict)


def sniff_mime(data: bytes) -> str | None:
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    return None


def _post(url: str, payload: dict, headers: dict) -> dict:
    body = json.dumps(payload).encode()
    request = urllib.request.Request(
        url, data=body, method="POST", headers={"Content-Type": "application/json", **headers}
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            return json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        detail = exc.read()[:500].decode("utf-8", "replace")
        retryable = exc.code == 429 or exc.code >= 500
        raise ImageError(f"HTTP {exc.code}: {detail}", status=exc.code, retryable=retryable) from None
    except (urllib.error.URLError, TimeoutError) as exc:
        raise ImageError(f"cannot reach the provider: {exc}", retryable=True) from None


def _data_url(path: str) -> tuple[str, str]:
    data = Path(path).read_bytes()
    mime = sniff_mime(data)
    if mime is None:
        raise ImageError(f"{path} is not a PNG, JPEG, WebP or GIF image")
    return mime, base64.b64encode(data).decode()


def call_openai(prompt, *, model, n, size, quality, refs, base_url=None) -> Result:
    key = os.environ.get("OPENAI_API_KEY", "")
    if not key:
        raise ImageError("OPENAI_API_KEY is not set (attach a connector with it)")
    base = (base_url or os.environ.get("IMAGEGEN_BASE_URL") or os.environ.get("OPENAI_BASE_URL") or OPENAI_BASE).rstrip("/")
    payload = {"model": model, "prompt": prompt, "n": n}
    if size:
        payload["size"] = OPENAI_SIZES.get(size, size)
    if quality:
        payload["quality"] = quality
    route = "/images/generations"
    if refs:
        route = "/images/edits"
        payload["images"] = [
            {"image_url": f"data:{mime};base64,{b64}"} for mime, b64 in (_data_url(r) for r in refs)
        ]
    body = _post(base + route, payload, {"Authorization": f"Bearer {key}"})
    images = [base64.b64decode(item["b64_json"]) for item in body.get("data") or [] if item.get("b64_json")]
    if not images:
        raise ImageError("the provider returned no image data")
    return Result(images, body.get("usage") or {})


def call_gemini(prompt, *, model, n, size, quality, refs, base_url=None) -> Result:
    key = os.environ.get("GEMINI_API_KEY", "")
    if not key:
        raise ImageError("GEMINI_API_KEY is not set (attach a connector with it)")
    base = (base_url or os.environ.get("IMAGEGEN_BASE_URL") or GEMINI_BASE).rstrip("/")
    parts: list[dict] = [{"text": prompt}]
    for ref in refs:
        mime, b64 = _data_url(ref)
        parts.append({"inlineData": {"mimeType": mime, "data": b64}})
    config: dict = {"responseModalities": ["IMAGE", "TEXT"]}
    if size and ":" in size:
        config["imageConfig"] = {"aspectRatio": size}
    images: list[bytes] = []
    usage: dict = {}
    for _ in range(n):
        body = _post(
            f"{base}/models/{model}:generateContent",
            {"contents": [{"role": "user", "parts": parts}], "generationConfig": config},
            {"x-goog-api-key": key},
        )
        found = False
        reasons = []
        for candidate in body.get("candidates") or []:
            reasons.append(str(candidate.get("finishReason") or ""))
            for part in (candidate.get("content") or {}).get("parts") or []:
                inline = part.get("inlineData") or part.get("inline_data")
                if inline and inline.get("data") and not part.get("thought"):
                    images.append(base64.b64decode(inline["data"]))
                    found = True
        if not found:
            raise ImageError(f"no image came back ({', '.join(r for r in reasons if r) or 'no reason'})")
        usage = body.get("usageMetadata") or usage
    return Result(images, usage)


PROVIDERS = {"openai": call_openai, "gemini": call_gemini}


def provider_from_env() -> str:
    if os.environ.get("OPENAI_API_KEY"):
        return "openai"
    if os.environ.get("GEMINI_API_KEY"):
        return "gemini"
    raise ImageError("no image key in the environment (OPENAI_API_KEY or GEMINI_API_KEY)")


def target_paths(out: str, count: int, mime: str) -> list[Path]:
    path = Path(out)
    suffix = EXTENSIONS.get(mime, path.suffix or ".png")
    stem = path.with_suffix("")
    if count == 1:
        return [stem.with_suffix(suffix)]
    return [stem.parent / f"{stem.name}-{i + 1}{suffix}" for i in range(count)]


def save(images: list[bytes], out: str, record: dict, *, root: Path = Path(".")) -> list[Path]:
    """Write the images and one provenance sidecar each; return the paths."""
    if not images:
        return []
    mime = sniff_mime(images[0]) or "image/png"
    paths = target_paths(out, len(images), mime)
    written = []
    for data, path in zip(images, paths):
        if sniff_mime(data) is None:
            raise ImageError(f"the provider sent something that is not an image for {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        sidecar = root / ".provenance" / (str(path) + ".json")
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        sidecar.write_text(
            json.dumps(
                {**record, "created_at": datetime.now(timezone.utc).isoformat(), "path": str(path), "bytes": len(data)},
                indent=2,
            )
        )
        written.append(path)
    return written


def generate(prompt, *, provider, model, n=1, size=None, quality=None, refs=(), retries=0, base_url=None) -> Result:
    """One provider call, retried only on 429/5xx with a growing pause."""
    call = PROVIDERS[provider]
    attempt = 0
    while True:
        try:
            return call(prompt, model=model, n=n, size=size, quality=quality, refs=list(refs), base_url=base_url)
        except ImageError as exc:
            if not exc.retryable or attempt >= retries:
                raise
            time.sleep(min(30.0, float(os.environ.get("IMAGEGEN_RETRY_BASE", "2")) * 2**attempt))
            attempt += 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("prompt")
    parser.add_argument("--out", required=True, help="output path, e.g. output/images/fox.png")
    parser.add_argument("--provider", choices=sorted(PROVIDERS))
    parser.add_argument("--model")
    parser.add_argument("--size", help="WxH or an aspect ratio such as 16:9")
    parser.add_argument("--aspect", help="same as --size with a ratio")
    parser.add_argument("--quality", choices=["low", "medium", "high", "auto"])
    parser.add_argument("--n", type=int, default=1)
    parser.add_argument("--ref", action="append", default=[], help="reference image (repeat)")
    args = parser.parse_args(argv)
    if not 1 <= args.n <= 4:
        parser.error("--n must be 1 to 4; use batch.py for more")
    try:
        provider = args.provider or provider_from_env()
        model = args.model or os.environ.get("IMAGEGEN_MODEL") or DEFAULT_MODELS[provider]
        size = args.size or args.aspect
        result = generate(args.prompt, provider=provider, model=model, n=args.n, size=size, quality=args.quality, refs=args.ref)
        paths = save(
            result.images,
            args.out,
            {"tool": "imagegen.py", "prompt": args.prompt, "provider": provider, "model": model, "size": size, "quality": args.quality, "references": args.ref, "billing": "user key"},
        )
    except ImageError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    for path in paths:
        print(path)
    if result.usage:
        print(f"usage: {json.dumps(result.usage)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
