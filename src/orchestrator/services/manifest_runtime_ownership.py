"""Keep the reference harness's control plane out of other harness executions."""

from collections.abc import Iterable, Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.manifest_approved_images import approved_image_entry


def uses_srw_runtime(work: Mapping[str, Any]) -> bool:
    """The marker is projected from the server's immutable execution record.

    Historical rows have no record and retain the reference runtime. Authored
    job context or private harness settings never supply this marker.
    """
    return work.get("execution_harness_adapter") in (None, "srw/v1")


def require_srw_runtime(work: Mapping[str, Any]) -> None:
    if not uses_srw_runtime(work):
        raise HTTPException(
            409,
            {
                "code": "manifest_runtime_owned",
                "message": "This execution is managed through the resource API. Report its outcome through /api/resources/{uid}/outcome.",
            },
        )


def custom_srw_image(runtime: Mapping[str, Any], *, trusted_image: str) -> str | None:
    """The explicit image when it names something other than the installed harness."""
    image = runtime.get("image")
    if isinstance(image, str) and image and image != trusted_image:
        return image
    return None


def require_srw_expert_configuration(
    expert: Mapping[str, Any] | None,
    *,
    interactive: bool = False,
    trusted_image: str | None = None,
    approved_images: Iterable[Mapping[str, Any]] = (),
) -> None:
    """A projected generic Expert must never enter the SRW private loader."""
    if (
        expert is not None
        and "harness_adapter" in expert
        and expert["harness_adapter"] != "srw/v1"
    ):
        raise HTTPException(
            409,
            (
                "Interactive sessions require the SRW adapter; use a manifest Job for this Expert."
                if interactive
                else "This Expert requires native manifest Job admission."
            ),
        )
    if expert is not None and trusted_image is not None and expert.get("manifest"):
        runtime = expert["manifest"]["spec"]["runtime"]
        if interactive and custom_srw_image(runtime, trusted_image=trusted_image):
            raise HTTPException(
                422,
                "This Expert's harness runs jobs only; sessions use the installed SRW harness.",
            )
        require_srw_launch_configuration(
            runtime, trusted_image=trusted_image, approved_images=approved_images
        )


def require_srw_launch_configuration(
    runtime: Mapping[str, Any],
    *,
    trusted_image: str,
    approved_images: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """An omitted SRW image follows installation upgrades; explicit ones constrain.

    Only this adapter uses the installation-managed pool. An omitted image must
    never turn an ordinary container into a trusted SRW harness.
    """
    if not isinstance(trusted_image, str) or not trusted_image.strip():
        raise HTTPException(503, "The installed SRW harness image is unavailable.")
    if (
        "image" in runtime
        and runtime["image"] != trusted_image
        and approved_image_entry(approved_images, runtime["image"]) is None
    ) or ("image" not in runtime and runtime.get("adapter") != "srw/v1"):
        raise HTTPException(
            422,
            "The explicit SRW image must match the installed harness or an image on the installation's approved list (manifestHosting.approvedImages). Omit image with adapter srw/v1 to follow installation upgrades, or use generic hosting for an arbitrary image.",
        )
    if any(key in runtime for key in ("command", "args", "env", "probes", "resources")):
        raise HTTPException(
            422,
            "The SRW adapter's launch envelope is installation-managed; use generic hosting for a custom launch.",
        )
    private = runtime.get("config", {})
    if not isinstance(private, dict):
        raise HTTPException(422, "SRW private configuration must be an object.")
    if set(private) - {"config_name", "asset_name", "config", "prompts", "layers"}:
        raise HTTPException(
            422,
            "The SRW adapter accepts private config_name, asset_name, config, layers and prompts settings.",
        )
    if not isinstance(private.get("config_name", "worker_base"), str) or any(
        not isinstance(private.get(key, {}), dict) for key in ("config", "prompts")
    ):
        raise HTTPException(422, "Invalid SRW private configuration.")
    return private
