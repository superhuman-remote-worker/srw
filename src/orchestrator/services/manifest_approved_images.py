"""Validate the installation's approved harness image list.

This is deployment policy (``manifestHosting.approvedImages``, delivered as the
MANIFEST_APPROVED_IMAGES JSON env var), never an authored Expert/Connector
setting. The Helm schema copies APPROVED_IMAGES_SCHEMA; a conformance test keeps
that copy aligned. Images must be digest-pinned and are compared by exact string.
"""

from copy import deepcopy
import json
import re

from jsonschema import Draft202012Validator

IMAGE_REFERENCE = re.compile(
    r"^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*(?::[0-9]{1,5})?"
    r"(?:/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*)*"
    r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?@sha256:[a-f0-9]{64}$"
)
_NAME_PATTERN = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"

APPROVED_IMAGES_SCHEMA = {
    "type": "array",
    "maxItems": 64,
    "items": {
        "type": "object",
        "additionalProperties": False,
        "required": ["name", "image"],
        "properties": {
            "name": {"type": "string", "pattern": _NAME_PATTERN},
            "image": {"type": "string", "pattern": IMAGE_REFERENCE.pattern},
            "description": {"type": "string", "maxLength": 512},
        },
    },
}
_VALIDATOR = Draft202012Validator(APPROVED_IMAGES_SCHEMA)
_NAME = re.compile(_NAME_PATTERN)


class ApprovedImagesConfigurationError(ValueError):
    """A sanitized operator configuration error; never include supplied values."""


def _fail(message):
    raise ApprovedImagesConfigurationError(
        "Invalid MANIFEST_APPROVED_IMAGES: " + message
    )


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            _fail("duplicate JSON object fields are not allowed.")
        result[key] = value
    return result


def validate_approved_images(value):
    """Return an owned copy or reject the whole list."""
    if isinstance(value, str):
        if len(value) > 128 * 1024:
            _fail("JSON list exceeds 128 KiB.")
        try:
            value = json.loads(value, object_pairs_hook=_unique_object)
        except (json.JSONDecodeError, RecursionError):
            _fail("expected a JSON array of approved image entries.")
    if next(_VALIDATOR.iter_errors(value), None) is not None:
        _fail("expected at most 64 entries with only name, image, and description.")
    names, images = set(), set()
    for entry in value:
        # fullmatch: "$" would otherwise accept a trailing newline.
        if _NAME.fullmatch(entry["name"]) is None:
            _fail("names must be DNS labels.")
        if IMAGE_REFERENCE.fullmatch(entry["image"]) is None:
            _fail(
                "images must be digest-pinned references (repo[:tag]@sha256:<64 hex>)."
            )
        if entry["name"] in names:
            _fail("names must be unique.")
        if entry["image"] in images:
            _fail("images must be unique.")
        names.add(entry["name"])
        images.add(entry["image"])
    return tuple(deepcopy(value))


def approved_image_entry(entries, image):
    """Return the entry whose image equals ``image`` exactly, else None."""
    for entry in entries:
        if entry["image"] == image:
            return entry
    return None
