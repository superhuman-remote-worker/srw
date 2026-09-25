"""Explicit composition bindings for existing catalogue integration tests.

These tests still exercise canonical main-owned policy with patched stores.
They call the new owner with its dependencies; no production compatibility
handlers or duplicate catalogue cache are kept merely to support the tests.
The independent HTTP tests mount their own factories instead.
"""

from collections.abc import Awaitable, Callable
from typing import Any
from orchestrator.application import catalogue as catalogue_composition


def catalogue_service():
    import orchestrator.main

    return catalogue_composition.expert_catalog_service(
        orchestrator.main.app.state.resources
    )


def authoring_service():
    import orchestrator.main

    return catalogue_composition.expert_catalog_dependencies(
        orchestrator.main.app.state.resources
    ).authoring


def catalogue_state():
    from orchestrator.main import app

    return app.state.expert_catalog_state


def catalogue_route(handler: Callable[..., Awaitable[Any]]):
    """Bind a new router handler to the current explicit application ports."""

    async def invoke(*args, **kwargs):
        import orchestrator.main

        return await handler(
            *args,
            deps=catalogue_composition.expert_catalog_dependencies(
                orchestrator.main.app.state.resources
            ),
            **kwargs,
        )

    return invoke


def patch_service_method(monkeypatch, service_type, name, replacement):
    """Preserve a collaborator double's arguments when patching a service method."""
    monkeypatch.setattr(
        service_type, name, lambda _self, *args, **kwargs: replacement(*args, **kwargs)
    )
