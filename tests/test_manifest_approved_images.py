import json

import pytest

from orchestrator.services.manifest_approved_images import (
    APPROVED_IMAGES_SCHEMA,
    ApprovedImagesConfigurationError,
    approved_image_entry,
    validate_approved_images,
)

DIGEST = "sha256:" + "a" * 64
GOOD = [
    {
        "name": "claude-code",
        "image": f"ghcr.io/superhuman-remote-worker/srw-harness-claude-code@{DIGEST}",
        "description": "SRW's Claude Code harness",
    }
]


def test_valid_list_returns_an_owned_copy_from_json_and_from_list():
    from_json = validate_approved_images(json.dumps(GOOD))
    from_list = validate_approved_images(GOOD)
    assert from_json == from_list == tuple(GOOD)
    assert from_list[0] is not GOOD[0]
    assert validate_approved_images("[]") == ()


@pytest.mark.parametrize(
    "image",
    [
        "ghcr.io/org/harness:latest",  # tag only
        "ghcr.io/org/harness@sha256:abc",  # short digest
        "ghcr.io/org/harness@md5:" + "a" * 64,  # wrong algorithm
        "GHCR.io/org/harness@" + DIGEST,  # upper-case repo
        "ghcr.io/org/harness:v1@" + DIGEST + "x",  # trailing junk
    ],
)
def test_images_must_be_digest_pinned_references(image):
    with pytest.raises(ApprovedImagesConfigurationError) as err:
        validate_approved_images([{"name": "x", "image": image}])
    assert str(err.value).startswith("Invalid MANIFEST_APPROVED_IMAGES: ")
    assert image not in str(err.value)


def test_tagged_and_digest_pinned_reference_is_accepted():
    entries = validate_approved_images(
        [{"name": "x", "image": "localhost:5005/harness:gate@" + DIGEST}]
    )
    assert entries[0]["image"].endswith(DIGEST)


@pytest.mark.parametrize(
    "bad",
    [
        "not json",
        '{"a": 1}',
        '[{"name": "x", "image": "ghcr.io/o/h@' + DIGEST + '", "extra": 1}]',
        '[{"name": "Bad_Name", "image": "ghcr.io/o/h@' + DIGEST + '"}]',
        '[{"name": "x", "image": "ghcr.io/o/h@'
        + DIGEST
        + '"}, {"name": "x", "image": "ghcr.io/o/i@'
        + DIGEST
        + '"}]',
        '[{"name": "x", "image": "ghcr.io/o/h@'
        + DIGEST
        + '"}, {"name": "y", "image": "ghcr.io/o/h@'
        + DIGEST
        + '"}]',
        '[{"name": "x", "name": "y", "image": "ghcr.io/o/h@' + DIGEST + '"}]',
        "["
        + ",".join(
            '{"name": "n%d", "image": "ghcr.io/o/h%d@%s"}' % (i, i, DIGEST)
            for i in range(65)
        )
        + "]",
    ],
)
def test_bad_lists_are_rejected_without_echoing_values(bad):
    with pytest.raises(ApprovedImagesConfigurationError) as err:
        validate_approved_images(bad)
    assert DIGEST not in str(err.value)


def test_oversized_json_is_rejected():
    with pytest.raises(ApprovedImagesConfigurationError):
        validate_approved_images("[" + " " * (128 * 1024) + "]")


def test_lookup_is_exact_string_equality():
    entries = validate_approved_images(GOOD)
    assert approved_image_entry(entries, GOOD[0]["image"]) == GOOD[0]
    assert approved_image_entry(entries, GOOD[0]["image"].replace("@", ":v1@")) is None
    assert approved_image_entry((), GOOD[0]["image"]) is None


def test_schema_is_draft_2020_12_and_closed():
    assert APPROVED_IMAGES_SCHEMA["type"] == "array"
    assert APPROVED_IMAGES_SCHEMA["items"]["additionalProperties"] is False
    assert set(APPROVED_IMAGES_SCHEMA["items"]["required"]) == {"name", "image"}
