"""Which harness class an image belongs to (swappable harness image).

Three classes, for an agent pod's reported image and a job's harness_image
alike: ``default`` (no image, the installed image, or another tag of the
installed image's repository: an old SRW pod after an upgrade keeps the
stale-build skip), ``custom`` (an approved image other than the installed one:
serves only jobs of that image) and ``ineligible`` (anything else: serves no
job at all).
"""

from types import SimpleNamespace

import pytest

from orchestrator.services.manifest_experts import (
    approved_srw_images,
    harness_image_class,
    image_repository,
)

INSTALLED = "ghcr.io/superhuman-remote-worker/srw-agent:sha-new"
CUSTOM = "example.invalid/h@sha256:" + "b" * 64
APPROVED = ({"name": "cc", "image": CUSTOM},)


def _class(image, approved=APPROVED, installed=INSTALLED):
    return harness_image_class(image, approved=approved, installed=installed)


@pytest.mark.parametrize("image", [None, "", INSTALLED])
def test_no_image_or_the_installed_image_is_default(image):
    assert _class(image) == ("default", None)


@pytest.mark.parametrize(
    "image",
    [
        "ghcr.io/superhuman-remote-worker/srw-agent:sha-old",
        "ghcr.io/superhuman-remote-worker/srw-agent",
        "ghcr.io/superhuman-remote-worker/srw-agent:latest@sha256:" + "c" * 64,
    ],
)
def test_another_tag_of_the_installed_repository_is_default(image):
    assert _class(image) == ("default", None)


def test_an_approved_image_other_than_the_installed_one_is_custom():
    assert _class(CUSTOM) == ("custom", CUSTOM)


def test_the_installed_image_is_default_even_when_approved():
    approved = APPROVED + ({"name": "srw", "image": INSTALLED},)
    assert _class(INSTALLED, approved=approved) == ("default", None)


@pytest.mark.parametrize(
    "approved", [(), ({"name": "x", "image": "o@sha256:" + "d" * 64},)]
)
def test_an_image_neither_installed_nor_approved_is_ineligible(approved):
    assert _class(CUSTOM, approved=approved) == ("ineligible", CUSTOM)


def test_a_registry_port_is_not_mistaken_for_a_tag():
    installed = "registry.local:5000/srw-agent:sha-new"
    assert _class("registry.local:5000/srw-agent:sha-old", installed=installed) == (
        "default",
        None,
    )
    assert _class("registry.local:5000/other:sha-new", installed=installed) == (
        "ineligible",
        "registry.local:5000/other:sha-new",
    )


@pytest.mark.parametrize(
    ("image", "repository"),
    [
        ("srw-agent", "srw-agent"),
        ("srw-agent:sha-1", "srw-agent"),
        ("host:5000/srw-agent", "host:5000/srw-agent"),
        ("host:5000/srw-agent:tag@sha256:" + "e" * 64, "host:5000/srw-agent"),
        ("example.invalid/h@sha256:" + "b" * 64, "example.invalid/h"),
    ],
)
def test_image_repository_strips_the_digest_and_tag(image, repository):
    assert image_repository(image) == repository


def test_approved_srw_images_reads_the_store_binding():
    assert approved_srw_images(SimpleNamespace()) == ()
    assert approved_srw_images(SimpleNamespace(manifest_approved_images=None)) == ()
    assert (
        approved_srw_images(SimpleNamespace(manifest_approved_images=APPROVED))
        == APPROVED
    )
    assert approved_srw_images(
        SimpleNamespace(manifest_approved_images=list(APPROVED))
    ) == tuple(APPROVED)
