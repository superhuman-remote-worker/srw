"""Per-session K8s Service + Ingress lifecycle.

For each bound agent pod, the orchestrator creates one Service (selects the
pod by its `srw.io/thread-id` label) and one Ingress (path-based,
`/p/{thread_id}`, points at the Service). Both resources carry
``ownerReferences`` to the agent pod so K8s GC cleans them up if explicit
teardown is skipped (orchestrator crash, etc.).

See `knowledge-base/knowledge/features/direct_session_websockets.md` §Component details.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

from orchestrator.services.pinned_k8s_effect import (
    legacy_pinned_namespace_candidates,
    run_bounded_k8s_call,
)

try:
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config
    from kubernetes.client.exceptions import ApiException

    KUBERNETES_AVAILABLE = True
except ImportError:
    k8s_client = None  # type: ignore[assignment]
    k8s_config = None  # type: ignore[assignment]
    ApiException = Exception  # type: ignore[misc, assignment]  # fallback so isinstance/raise still work
    KUBERNETES_AVAILABLE = False

logger = logging.getLogger(__name__)
_KUBERNETES_NAMESPACE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")


class SessionRouteAuthorityError(RuntimeError):
    """The requested route is not bound to one exact live session Pod."""


def _value(obj: Any, *names: str) -> Any:
    """Read one Kubernetes model/dict field without trusting mock defaults."""

    if isinstance(obj, dict):
        for name in names:
            if name in obj:
                return obj[name]
        return None
    for name in names:
        value = getattr(obj, name, None)
        if value is not None and value.__class__.__module__ != "unittest.mock":
            return value
    return None


def _owner_reference_matches(resource: Any, *, pod_name: str, pod_uid: str) -> bool:
    metadata = _value(resource, "metadata")
    references = _value(metadata, "owner_references", "ownerReferences")
    if not isinstance(references, (list, tuple)) or len(references) != 1:
        return False
    reference = references[0]
    return bool(
        _value(reference, "api_version", "apiVersion") == "v1"
        and _value(reference, "kind") == "Pod"
        and _value(reference, "name") == pod_name
        and str(_value(reference, "uid") or "") == pod_uid
    )


def _is_k8s_status(exc: BaseException, status: int) -> bool:
    """Duck-type a K8s API exception by `.status`.

    Catching by ``ApiException`` class fails under test isolation problems —
    if another test file replaces ``kubernetes.client.exceptions`` in
    ``sys.modules`` after this module captured ``ApiException``, the running
    code's class binding diverges from what tests raise. Comparing by the
    K8s-API-error-shape attribute sidesteps the class identity hazard.
    """
    return getattr(exc, "status", None) == status


class SessionRouterService:
    """Idempotent K8s Service + Ingress lifecycle for sessions."""

    def __init__(
        self,
        namespace: str,
        ingress_host: str,
        ingress_class: str = "traefik",
        annotations: Optional[dict[str, str]] = None,
        tls_secret_name: Optional[str] = None,
        db: Any = None,
        # Injected for testability; lazy-resolved in production.
        core_api: Any = None,
        networking_api: Any = None,
        single_origin: bool = False,
    ) -> None:
        self._namespace = namespace
        self._ingress_host = ingress_host
        self._ingress_class = ingress_class
        self._annotations = annotations or {}
        self._single_origin = single_origin
        if single_origin:
            self._annotations = {
                **self._annotations,
                "kubernetes.io/ingress.class": ingress_class,
                "traefik.ingress.kubernetes.io/router.entrypoints": "websecure",
                "traefik.ingress.kubernetes.io/router.tls": "true",
            }
        # Local dev needs the per-session Ingress to be on the same TLS
        # entrypoint as the cockpit (mkcert/cert-manager). When set, the
        # Ingress gets a `tls:` block + websecure entrypoint annotation.
        self._tls_secret_name = None if single_origin else tls_secret_name
        self._db = db
        self._core_api = core_api
        self._networking_api = networking_api

    # --------------------------------------------------------------------- #
    # Lazy K8s client setup
    # --------------------------------------------------------------------- #

    def _lazy_init_apis(self) -> None:
        if self._core_api is not None and self._networking_api is not None:
            return
        if not KUBERNETES_AVAILABLE:
            raise RuntimeError(
                "session_router requires the 'kubernetes' package; "
                "set it in orchestrator/requirements.txt or inject test APIs."
            )
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()
        if self._core_api is None:
            self._core_api = k8s_client.CoreV1Api()
        if self._networking_api is None:
            self._networking_api = k8s_client.NetworkingV1Api()

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #

    async def ensure_route(
        self,
        thread_id: str,
        pod_name: str,
        pod_uid: str,
        runtime_generation: str | None = None,
    ) -> str:
        """Publish only the route for one exact reciprocal live runtime."""

        self._lazy_init_apis()
        thread_id = str(thread_id or "").strip()
        pod_name = str(pod_name or "").strip()
        pod_uid = str(pod_uid or "").strip()
        runtime_generation = str(runtime_generation or "").strip()
        if (
            not all((thread_id, pod_name, pod_uid, runtime_generation))
            or self._db is None
        ):
            raise SessionRouteAuthorityError("session route authority is unavailable")
        name = f"session-{thread_id}"

        binding = await self._require_current_binding(
            thread_id=thread_id,
            runtime_generation=runtime_generation,
            pod_name=pod_name,
            pod_uid=pod_uid,
        )
        pod_ip = binding.pod_ip
        namespace = binding.pod_namespace

        # A route with the same host/path in the release namespace can race or
        # conflict with a legacy-namespace route. Never publish a second route
        # until the prior deterministic resources have been removed through an
        # exact owner/generation-fenced lifecycle path.
        for candidate_namespace in legacy_pinned_namespace_candidates(self._namespace):
            if candidate_namespace == namespace:
                continue
            shadow_service = await self._read_or_none(
                self._core_api.read_namespaced_service,
                name,
                namespace=candidate_namespace,
            )
            shadow_ingress = await self._read_or_none(
                self._networking_api.read_namespaced_ingress,
                name,
                namespace=candidate_namespace,
            )
            if shadow_service is not None or shadow_ingress is not None:
                raise SessionRouteAuthorityError(
                    "session route shadow exists outside authoritative namespace"
                )

        # Refuse foreign or malformed deterministic resources before touching
        # the Pod. A predecessor route may omit G, but it must already have the
        # exact owner and immutable routing shape before it can be adopted.
        service = await self._read_or_none(
            self._core_api.read_namespaced_service,
            name,
            namespace=namespace,
        )
        ingress = await self._read_or_none(
            self._networking_api.read_namespaced_ingress,
            name,
            namespace=namespace,
        )
        if service is not None and not self._service_matches(
            service,
            thread_id=thread_id,
            name=name,
            pod_name=pod_name,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            namespace=namespace,
            allow_missing_generation=True,
        ):
            raise SessionRouteAuthorityError("existing session Service is not trusted")
        if ingress is not None and not self._ingress_matches(
            ingress,
            thread_id=thread_id,
            name=name,
            pod_name=pod_name,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            namespace=namespace,
            allow_missing_generation=True,
        ):
            raise SessionRouteAuthorityError("existing session Ingress is not trusted")

        pod = await self._read_exact_ready_pod(
            thread_id=thread_id,
            runtime_generation=runtime_generation,
            pod_name=pod_name,
            pod_uid=pod_uid,
            pod_ip=pod_ip,
            namespace=namespace,
        )
        resource_version = str(
            _value(_value(pod, "metadata"), "resource_version", "resourceVersion") or ""
        ).strip()
        if not resource_version:
            raise SessionRouteAuthorityError("session Pod version is unavailable")
        await self._require_current_binding(
            thread_id=thread_id,
            runtime_generation=runtime_generation,
            pod_name=pod_name,
            pod_uid=pod_uid,
            pod_ip=pod_ip,
            pod_namespace=namespace,
        )

        try:
            await self._call(
                self._core_api.patch_namespaced_pod,
                name=pod_name,
                namespace=namespace,
                body=[
                    {"op": "test", "path": "/metadata/uid", "value": pod_uid},
                    {
                        "op": "test",
                        "path": "/metadata/resourceVersion",
                        "value": resource_version,
                    },
                    {
                        "op": "add",
                        "path": "/metadata/labels/srw.io~1thread-id",
                        "value": thread_id,
                    },
                    {
                        "op": "add",
                        "path": "/metadata/labels/srw~1thread-id",
                        "value": thread_id[:12],
                    },
                    {
                        "op": "add",
                        "path": "/metadata/labels/srw~1purpose",
                        "value": "session",
                    },
                    {
                        "op": "add",
                        "path": "/metadata/labels/srw.io~1runtime-generation",
                        "value": runtime_generation,
                    },
                ],
            )
        except Exception as exc:
            if any(_is_k8s_status(exc, status) for status in (404, 409, 422)):
                raise SessionRouteAuthorityError(
                    "session Pod changed before route publication"
                ) from exc
            raise

        await self._read_exact_ready_pod(
            thread_id=thread_id,
            runtime_generation=runtime_generation,
            pod_name=pod_name,
            pod_uid=pod_uid,
            pod_ip=pod_ip,
            namespace=namespace,
            require_route_labels=True,
        )
        await self._require_current_binding(
            thread_id=thread_id,
            runtime_generation=runtime_generation,
            pod_name=pod_name,
            pod_uid=pod_uid,
            pod_ip=pod_ip,
            pod_namespace=namespace,
        )

        service = await self._publish_service(
            existing=service,
            thread_id=thread_id,
            name=name,
            pod_name=pod_name,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            namespace=namespace,
        )
        ingress = await self._publish_ingress(
            existing=ingress,
            thread_id=thread_id,
            name=name,
            pod_name=pod_name,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            namespace=namespace,
        )

        await self._require_current_binding(
            thread_id=thread_id,
            runtime_generation=runtime_generation,
            pod_name=pod_name,
            pod_uid=pod_uid,
            pod_ip=pod_ip,
            pod_namespace=namespace,
        )
        # Delete/recreate can replace a validated deterministic object between
        # any two awaits. The final API-server observations are the token
        # publication boundary used by the caller's final DB reread.
        service = await self._read_or_none(
            self._core_api.read_namespaced_service,
            name,
            namespace=namespace,
        )
        ingress = await self._read_or_none(
            self._networking_api.read_namespaced_ingress,
            name,
            namespace=namespace,
        )
        if service is None or not self._service_matches(
            service,
            thread_id=thread_id,
            name=name,
            pod_name=pod_name,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            namespace=namespace,
        ):
            raise SessionRouteAuthorityError("final session Service is not trusted")
        if ingress is None or not self._ingress_matches(
            ingress,
            thread_id=thread_id,
            name=name,
            pod_name=pod_name,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            namespace=namespace,
        ):
            raise SessionRouteAuthorityError("final session Ingress is not trusted")
        return f"/p/{thread_id}"

    async def _require_current_binding(
        self,
        *,
        thread_id: str,
        runtime_generation: str,
        pod_name: str,
        pod_uid: str,
        pod_ip: str | None = None,
        pod_namespace: str | None = None,
    ) -> Any:
        try:
            binding = await self._db.get_pinned_session_binding(
                thread_id,
                expected_runtime_generation=runtime_generation,
            )
        except Exception as exc:
            raise SessionRouteAuthorityError(
                "session route binding could not be verified"
            ) from exc
        if not (
            binding is not None
            and binding.thread_id == thread_id
            and binding.runtime_generation == runtime_generation
            and binding.agent_hostname == pod_name
            and isinstance(binding.pod_namespace, str)
            and _KUBERNETES_NAMESPACE.fullmatch(binding.pod_namespace) is not None
            and (pod_namespace is None or binding.pod_namespace == pod_namespace)
            and (not self._single_origin or binding.pod_namespace == self._namespace)
            and binding.pod_uid == pod_uid
            and binding.agent_status in {"booting", "ready", "working", "session"}
            and (pod_ip is None or binding.pod_ip == pod_ip)
        ):
            raise SessionRouteAuthorityError(
                "session route binding is no longer reciprocal"
            )
        return binding

    async def _read_exact_ready_pod(
        self,
        *,
        thread_id: str,
        runtime_generation: str,
        pod_name: str,
        pod_uid: str,
        pod_ip: str,
        namespace: str,
        require_route_labels: bool = False,
    ) -> Any:
        try:
            pod = await self._call(
                self._core_api.read_namespaced_pod,
                name=pod_name,
                namespace=namespace,
            )
        except Exception as exc:
            raise SessionRouteAuthorityError(
                "session Pod could not be verified"
            ) from exc

        metadata = _value(pod, "metadata")
        status = _value(pod, "status")
        labels = _value(metadata, "labels")
        container_statuses = _value(status, "container_statuses", "containerStatuses")
        component = labels.get("srw/component") if isinstance(labels, dict) else None
        managed_pool = bool(
            component == "agent"
            and labels.get("srw/managed-by") == "agent-provisioner"
            and labels.get("srw/purpose") in {"job", "session"}
        )
        # ``srw.io/thread-id`` is the canonical full-identity label.
        # ``srw/thread-id`` is the historical compatibility label: a dedicated
        # Pod carries the full UUID there until this route adopts it, and the
        # 12-character form afterwards, because the route selector needs a
        # value inside the 63-character label limit.  Reading the full UUID
        # back from the compatibility label after publication is what made a
        # dedicated Pod refuse its own freshly published route.  A short
        # compatibility value is therefore trusted only alongside an exact
        # canonical label, so the deterministic name never stands alone as
        # identity for a thread that merely shares its 12-character prefix.
        canonical_thread = (
            labels.get("srw.io/thread-id") if isinstance(labels, dict) else None
        )
        compatibility_thread = (
            labels.get("srw/thread-id") if isinstance(labels, dict) else None
        )
        dedicated_identity = compatibility_thread == thread_id or (
            compatibility_thread == thread_id[:12] and canonical_thread == thread_id
        )
        dedicated = bool(
            component == "persistent-agent"
            and dedicated_identity
            and pod_name == f"persistent-{thread_id[:12]}"
        )
        route_labels_match = bool(
            isinstance(labels, dict)
            and canonical_thread == thread_id
            and compatibility_thread == thread_id[:12]
            and labels.get("srw/purpose") == "session"
            and labels.get("srw.io/runtime-generation") == runtime_generation
        )
        ready_agent = bool(
            isinstance(container_statuses, (list, tuple))
            and any(
                str(_value(item, "name") or "") == "agent"
                and _value(item, "ready") is True
                for item in container_statuses
            )
        )
        if not (
            str(_value(metadata, "name") or "") == pod_name
            and str(_value(metadata, "namespace") or "") == namespace
            and str(_value(metadata, "uid") or "") == pod_uid
            and _value(metadata, "deletion_timestamp", "deletionTimestamp") is None
            and str(_value(status, "phase") or "") == "Running"
            and str(_value(status, "pod_ip", "podIP") or "") == pod_ip
            and ready_agent
            and (managed_pool or dedicated)
            and (not require_route_labels or route_labels_match)
        ):
            raise SessionRouteAuthorityError(
                "session Pod identity or readiness no longer matches"
            )
        return pod

    async def _publish_service(
        self,
        *,
        existing: Any | None,
        thread_id: str,
        name: str,
        pod_name: str,
        pod_uid: str,
        runtime_generation: str,
        namespace: str,
    ) -> Any:
        if existing is None:
            try:
                await self._call(
                    self._core_api.create_namespaced_service,
                    namespace=namespace,
                    body=self._service_body(
                        thread_id,
                        name,
                        pod_name,
                        pod_uid,
                        runtime_generation,
                        namespace=namespace,
                    ),
                )
            except Exception as exc:
                if not _is_k8s_status(exc, 409):
                    raise
        else:
            await self._call(
                self._core_api.patch_namespaced_service,
                name=name,
                namespace=namespace,
                body=self._route_authority_patch(
                    existing,
                    pod_name,
                    pod_uid,
                    runtime_generation,
                    service=True,
                ),
            )
        observed = await self._read_or_none(
            self._core_api.read_namespaced_service,
            name,
            namespace=namespace,
        )
        if observed is None or not self._service_matches(
            observed,
            thread_id=thread_id,
            name=name,
            pod_name=pod_name,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            namespace=namespace,
        ):
            raise SessionRouteAuthorityError("created session Service is not trusted")
        return observed

    async def _publish_ingress(
        self,
        *,
        existing: Any | None,
        thread_id: str,
        name: str,
        pod_name: str,
        pod_uid: str,
        runtime_generation: str,
        namespace: str,
    ) -> Any:
        if existing is None:
            try:
                await self._call(
                    self._networking_api.create_namespaced_ingress,
                    namespace=namespace,
                    body=self._ingress_body(
                        thread_id,
                        name,
                        pod_name,
                        pod_uid,
                        runtime_generation,
                        namespace=namespace,
                    ),
                )
            except Exception as exc:
                if not _is_k8s_status(exc, 409):
                    raise
        else:
            await self._call(
                self._networking_api.patch_namespaced_ingress,
                name=name,
                namespace=namespace,
                body=self._route_authority_patch(
                    existing,
                    pod_name,
                    pod_uid,
                    runtime_generation,
                    service=False,
                ),
            )
        observed = await self._read_or_none(
            self._networking_api.read_namespaced_ingress,
            name,
            namespace=namespace,
        )
        if observed is None or not self._ingress_matches(
            observed,
            thread_id=thread_id,
            name=name,
            pod_name=pod_name,
            pod_uid=pod_uid,
            runtime_generation=runtime_generation,
            namespace=namespace,
        ):
            raise SessionRouteAuthorityError("created session Ingress is not trusted")
        return observed

    @staticmethod
    def _route_generation_matches(
        labels: Any,
        selector: Any,
        *,
        thread_id: str,
        runtime_generation: str,
        allow_missing_generation: bool,
    ) -> bool:
        if not isinstance(labels, dict) or labels.get("srw.io/thread-id") != thread_id:
            return False
        label_generation = labels.get("srw.io/runtime-generation")
        if label_generation != runtime_generation and not (
            allow_missing_generation and label_generation in {None, ""}
        ):
            return False
        if selector is None:
            return True
        expected = {"srw.io/thread-id": thread_id}
        if selector == {**expected, "srw.io/runtime-generation": runtime_generation}:
            return True
        return allow_missing_generation and selector == expected

    def _service_matches(
        self,
        resource: Any,
        *,
        thread_id: str,
        name: str,
        pod_name: str,
        pod_uid: str,
        runtime_generation: str,
        namespace: str,
        allow_missing_generation: bool = False,
    ) -> bool:
        metadata = _value(resource, "metadata")
        spec = _value(resource, "spec")
        labels = _value(metadata, "labels")
        selector = _value(spec, "selector")
        ports = _value(spec, "ports")
        matching_port = bool(
            isinstance(ports, (list, tuple))
            and len(ports) == 1
            and _value(ports[0], "port") == 8001
            and _value(ports[0], "target_port", "targetPort") == 8001
        )
        return bool(
            _value(metadata, "name") == name
            and _value(metadata, "namespace") == namespace
            and isinstance(labels, dict)
            and labels.get("srw.io/managed-by") == "orchestrator"
            and self._route_generation_matches(
                labels,
                selector,
                thread_id=thread_id,
                runtime_generation=runtime_generation,
                allow_missing_generation=allow_missing_generation,
            )
            and _value(spec, "type") == "ClusterIP"
            and matching_port
            and _owner_reference_matches(resource, pod_name=pod_name, pod_uid=pod_uid)
        )

    def _ingress_matches(
        self,
        resource: Any,
        *,
        thread_id: str,
        name: str,
        pod_name: str,
        pod_uid: str,
        runtime_generation: str,
        namespace: str,
        allow_missing_generation: bool = False,
    ) -> bool:
        metadata = _value(resource, "metadata")
        spec = _value(resource, "spec")
        labels = _value(metadata, "labels")
        annotations = _value(metadata, "annotations") or {}
        rules = _value(spec, "rules")
        if not isinstance(rules, (list, tuple)) or len(rules) != 1:
            return False
        rule = rules[0]
        paths = _value(_value(rule, "http"), "paths")
        if not isinstance(paths, (list, tuple)) or len(paths) != 1:
            return False
        path = paths[0]
        service = _value(_value(path, "backend"), "service")
        port = _value(service, "port")
        actual_tls = _value(spec, "tls") or []
        if self._tls_secret_name:
            tls_matches = bool(
                isinstance(actual_tls, (list, tuple))
                and len(actual_tls) == 1
                and _value(actual_tls[0], "hosts") == [self._ingress_host]
                and _value(actual_tls[0], "secret_name", "secretName")
                == self._tls_secret_name
            )
        else:
            tls_matches = not actual_tls
        return bool(
            _value(metadata, "name") == name
            and _value(metadata, "namespace") == namespace
            and isinstance(labels, dict)
            and labels.get("srw.io/managed-by") == "orchestrator"
            and self._route_generation_matches(
                labels,
                None,
                thread_id=thread_id,
                runtime_generation=runtime_generation,
                allow_missing_generation=allow_missing_generation,
            )
            and annotations == self._annotations
            and _value(spec, "ingress_class_name", "ingressClassName")
            == (None if self._single_origin else self._ingress_class)
            and _value(rule, "host")
            == (None if self._single_origin else self._ingress_host)
            and _value(path, "path") == f"/p/{thread_id}"
            and _value(path, "path_type", "pathType") == "Prefix"
            and _value(service, "name") == name
            and _value(port, "number") == 8001
            and tls_matches
            and _owner_reference_matches(resource, pod_name=pod_name, pod_uid=pod_uid)
        )

    async def teardown_route(
        self,
        thread_id: str,
        *,
        expected_namespace: str,
        expected_runtime_generation: str | None = None,
        expected_owner_uid: str | None = None,
    ) -> bool:
        """Delete only the captured route generation. 404 is exact success."""
        self._lazy_init_apis()
        expected_namespace = str(expected_namespace or "").strip()
        if _KUBERNETES_NAMESPACE.fullmatch(expected_namespace) is None:
            raise SessionRouteAuthorityError(
                "session route namespace authority is unavailable"
            )
        name = f"session-{thread_id}"

        complete = True
        for read_fn, delete_fn in (
            (
                self._networking_api.read_namespaced_ingress,
                self._networking_api.delete_namespaced_ingress,
            ),
            (
                self._core_api.read_namespaced_service,
                self._core_api.delete_namespaced_service,
            ),
        ):
            try:
                resource = await self._call(
                    read_fn, name=name, namespace=expected_namespace
                )
            except Exception as e:
                if _is_k8s_status(e, 404):
                    continue
                complete = False
                continue
            if (
                expected_runtime_generation is not None
                or expected_owner_uid is not None
            ) and not self._resource_matches_authority(
                resource,
                pod_uid=expected_owner_uid,
                runtime_generation=expected_runtime_generation,
                require_generation=expected_runtime_generation is not None,
            ):
                # A replacement owns this deterministic name. Preserve it.
                continue
            resource_uid = str(_value(_value(resource, "metadata"), "uid") or "")
            if not resource_uid:
                complete = False
                continue
            try:
                await self._call(
                    delete_fn,
                    name=name,
                    namespace=expected_namespace,
                    body={"preconditions": {"uid": resource_uid}},
                )
            except Exception as e:
                if _is_k8s_status(e, 404) or _is_k8s_status(e, 409):
                    continue
                logger.warning(
                    "teardown_route: %s on %s returned %s",
                    delete_fn.__name__,
                    name,
                    getattr(e, "status", "?"),
                )
                complete = False
        return complete

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #

    async def _exists(self, read_fn: Any, name: str, *, namespace: str) -> bool:
        try:
            await self._call(read_fn, name=name, namespace=namespace)
            return True
        except Exception as e:
            if _is_k8s_status(e, 404):
                return False
            raise

    async def _read_or_none(
        self,
        read_fn: Any,
        name: str,
        *,
        namespace: str,
    ) -> Any | None:
        try:
            return await self._call(read_fn, name=name, namespace=namespace)
        except Exception as exc:
            if _is_k8s_status(exc, 404):
                return None
            raise

    @staticmethod
    def _resource_matches_authority(
        resource: Any,
        *,
        pod_uid: str | None,
        runtime_generation: str | None,
        require_generation: bool = False,
    ) -> bool:
        metadata = _value(resource, "metadata")
        labels_value = _value(metadata, "labels")
        labels = dict(labels_value) if isinstance(labels_value, dict) else {}
        owners_value = _value(metadata, "owner_references", "ownerReferences")
        owners = list(owners_value) if isinstance(owners_value, (list, tuple)) else []
        owner_uids = {str(_value(owner, "uid") or "") for owner in owners}
        if pod_uid and (len(owners) != 1 or str(pod_uid) not in owner_uids):
            return False
        observed_generation = labels.get("srw.io/runtime-generation")
        if runtime_generation:
            if observed_generation not in {None, "", runtime_generation}:
                return False
            if require_generation and observed_generation != runtime_generation:
                return False
        return True

    def _route_authority_patch(
        self,
        resource: Any,
        pod_name: str,
        pod_uid: str,
        runtime_generation: str,
        *,
        service: bool,
    ) -> list[dict[str, Any]]:
        """UID-fenced route adoption/refresh patch.

        Deterministic resource names are reusable across Resume.  A normal
        merge patch can therefore read G1 and overwrite replacement G2 after
        a delete/create race.  JSON Patch's immutable UID test makes that
        interleaving fail without mutating the replacement.
        """

        metadata = _value(resource, "metadata")
        resource_uid = str(_value(metadata, "uid") or "")
        if not resource_uid:
            raise RuntimeError("session route resource has no immutable UID")
        patch: list[dict[str, Any]] = [
            {"op": "test", "path": "/metadata/uid", "value": resource_uid},
            {
                "op": "add",
                "path": "/metadata/labels/srw.io~1runtime-generation",
                "value": runtime_generation,
            },
            {
                "op": "add",
                "path": "/metadata/ownerReferences",
                "value": [self._owner_ref(pod_name, pod_uid)],
            },
        ]
        if service:
            patch.append(
                {
                    "op": "add",
                    "path": "/spec/selector",
                    "value": {
                        "srw.io/thread-id": str(
                            (_value(metadata, "labels") or {}).get(
                                "srw.io/thread-id", ""
                            )
                        ),
                        "srw.io/runtime-generation": runtime_generation,
                    },
                }
            )
        return patch

    @staticmethod
    async def _call(fn: Any, **kwargs: Any) -> Any:
        return await run_bounded_k8s_call(fn, **kwargs)

    def _owner_ref(self, pod_name: str, pod_uid: str) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "name": pod_name,
            "uid": pod_uid,
            "controller": False,
            "blockOwnerDeletion": False,
        }

    def _service_body(
        self,
        thread_id: str,
        name: str,
        pod_name: str,
        pod_uid: str,
        runtime_generation: str | None = None,
        *,
        namespace: str | None = None,
    ) -> dict[str, Any]:
        namespace = namespace or self._namespace
        return {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    "srw.io/thread-id": thread_id,
                    "srw.io/managed-by": "orchestrator",
                    **(
                        {"srw.io/runtime-generation": runtime_generation}
                        if runtime_generation
                        else {}
                    ),
                },
                "ownerReferences": [self._owner_ref(pod_name, pod_uid)],
            },
            "spec": {
                "type": "ClusterIP",
                "selector": {
                    "srw.io/thread-id": thread_id,
                    "srw.io/runtime-generation": runtime_generation,
                },
                "ports": [{"port": 8001, "targetPort": 8001}],
                # The orchestrator is the authoritative readiness gate: it won't
                # mint a token / return 200 from /connection until probe_ready()
                # confirms the agent answers /ready on its pod IP. Gating the
                # Service *additionally* on the pod's k8s readinessProbe
                # (initialDelaySeconds=30) only opens a ~30s window where
                # /connection says "ready" but the Service has no endpoints, so
                # the cockpit's WS fails and reconnect-loops. Publishing
                # not-ready addresses closes that gap — traffic only arrives
                # after /connection 200 anyway.
                "publishNotReadyAddresses": True,
            },
        }

    def _ingress_body(
        self,
        thread_id: str,
        name: str,
        pod_name: str,
        pod_uid: str,
        runtime_generation: str | None = None,
        *,
        namespace: str | None = None,
    ) -> dict[str, Any]:
        namespace = namespace or self._namespace
        annotations = dict(self._annotations)
        spec: dict[str, Any] = {
            "ingressClassName": self._ingress_class,
            "rules": [
                {
                    "host": self._ingress_host,
                    "http": {
                        "paths": [
                            {
                                "path": f"/p/{thread_id}",
                                "pathType": "Prefix",
                                "backend": {
                                    "service": {
                                        "name": name,
                                        "port": {"number": 8001},
                                    }
                                },
                            }
                        ]
                    },
                }
            ],
        }
        if self._single_origin:
            del spec["ingressClassName"]
            del spec["rules"][0]["host"]
        if self._tls_secret_name:
            spec["tls"] = [
                {
                    "hosts": [self._ingress_host],
                    "secretName": self._tls_secret_name,
                }
            ]
        return {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    "srw.io/thread-id": thread_id,
                    "srw.io/managed-by": "orchestrator",
                    **(
                        {"srw.io/runtime-generation": runtime_generation}
                        if runtime_generation
                        else {}
                    ),
                },
                "annotations": annotations,
                "ownerReferences": [self._owner_ref(pod_name, pod_uid)],
            },
            "spec": spec,
        }
