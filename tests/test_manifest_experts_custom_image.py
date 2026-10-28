"""Which harness image an agent pod counts as (swappable harness image).

A pod is custom-class only when it runs an image other than the installed one
and that image is on the installation's approved list. Every other pod,
including a default pod of an earlier release reporting its old tag, is
default-class: the stale-build skip and the drain still apply to it.
"""

from types import SimpleNamespace

from orchestrator.services.manifest_experts import (
    approved_srw_images,
    custom_agent_image,
)

INSTALLED = "ghcr.io/superhuman-remote-worker/srw-agent:sha-new"
CUSTOM = "example.invalid/h@sha256:" + "b" * 64
APPROVED = ({"name": "cc", "image": CUSTOM},)


def test_an_approved_image_other_than_the_installed_one_is_custom():
    assert custom_agent_image(CUSTOM, approved=APPROVED, installed=INSTALLED) == CUSTOM


def test_an_agent_without_an_image_is_default_class():
    assert custom_agent_image(None, approved=APPROVED, installed=INSTALLED) is None
    assert custom_agent_image("", approved=APPROVED, installed=INSTALLED) is None


def test_the_installed_image_is_default_class_even_when_approved():
    approved = APPROVED + ({"name": "srw", "image": INSTALLED},)
    assert custom_agent_image(INSTALLED, approved=approved, installed=INSTALLED) is None


def test_an_old_default_tag_is_default_class_not_custom():
    old = "ghcr.io/superhuman-remote-worker/srw-agent:sha-old"
    assert custom_agent_image(old, approved=APPROVED, installed=INSTALLED) is None


def test_an_image_taken_off_the_approved_list_is_default_class():
    assert custom_agent_image(CUSTOM, approved=(), installed=INSTALLED) is None


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
