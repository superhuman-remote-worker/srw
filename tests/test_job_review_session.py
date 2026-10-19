"""Job review sessions are derived server-side from a stored delivery."""

import inspect
import json
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException, params

import orchestrator.main
from orchestrator.application import preparation as preparation_composition
from orchestrator.application import workspace as workspace_composition
from orchestrator.services import (
    agent_datasource_payload as agent_datasource_payload_module,
)


JOB_ID = "29c28492-df7c-4eb3-847f-38892557ac4e"
REPO_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
DB_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
THREAD_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
CHECKOUT_ID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
OUTBOX_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"


def _job(job: dict) -> dict:
    return {
        **job,
        "id": JOB_ID,
        "description": "Design the Hotel Rheinland theme",
        "status": "pending_review",
        "config_name": "worker_base",
        "config_override": json.dumps(
            {
                "llm": {"model": "gpt-5.6-sol", "temperature": 0.2},
                "interactive": {"permission_mode": "auto_accept"},
                "workspace": {"backend": "vm"},
                "tools": {"shell": ["run_command"]},
            }
        ),
        "context": json.dumps(
            {
                "pull_request": {
                    "forge": "github",
                    "repo": "Acme/WidgetShop",
                    "number": 1,
                    "url": "https://github.com/Acme/WidgetShop/pull/1",
                    "head": "design/hotel-rheinland-theme",
                    "base": "main",
                },
                "required_deliverables": [
                    "design_spec/hotel-rheinland-theme.md",
                    "mockups/hotel-rheinland-theme.html",
                ],
            }
        ),
        "freeze_data": json.dumps({"summary": "Theme delivered and PR opened."}),
    }


def _repository() -> dict:
    return {
        "id": REPO_ID,
        "name": "WidgetShop",
        "type": "repository",
        "connection_url": "https://github.com/Acme/WidgetShop.git",
        "config": {"forge": "github"},
        "credentials": {"token": "server-only-token"},
        "default_branch": "main",
    }


def _database() -> dict:
    return {
        "id": DB_ID,
        "name": "Reference DB",
        "type": "postgresql",
        "connection_url": "postgresql://db/reference",
        "credentials": {"password": "server-only-password"},
    }


def _authorized(user: dict, db):
    stack = ExitStack()
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    stack.enter_context(
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    return stack


class TestReviewSessionEndpoint:
    @pytest.mark.asyncio
    async def test_real_job_shape_derives_the_session_without_a_request_body(
        self, user_a, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.job_review import create_job_review_session

        job = _job(job_a)
        fake_db.get_job = AsyncMock(return_value=job)
        fake_db.resolve_datasources_for_job = AsyncMock(
            return_value=[_database(), _repository()]
        )
        created = AsyncMock(return_value={"thread_id": THREAD_ID, "status": "created"})

        with (
            _authorized(user_a, fake_db),
            patch("orchestrator.services.thread_admission.create_thread", created),
        ):
            result = await create_job_review_session(
                fake_request,
                JOB_ID,
                dependencies=workspace_composition.job_review_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )

        body, forwarded_request = created.await_args.args
        assert forwarded_request is fake_request
        assert body.title.startswith("Review job 29c28492")
        # Worker and session profiles are structurally different. Model and
        # temperature are copied onto the safe session base, while a review
        # always starts read-only even if the worker ran with auto-accept.
        assert body.config_name == "session_base"
        assert body.model == "gpt-5.6-sol"
        assert body.temperature == 0.2
        assert body.permission_mode == "read_only"
        assert body.project_ids == [str(job_a["project_id"])]
        assert body.datasource_ids == [DB_ID, REPO_ID]
        # The source checkout needs a real workspace, but this value is fixed
        # by the server — it was not accepted from the client or copied from
        # the job's VM override.
        assert body.config_override == {"workspace": {"backend": "sandbox"}}

        seed = body._trusted_seed
        assert seed is not None
        assert seed.metadata == {
            "review_delivery": {
                "job_id": JOB_ID,
                "datasource_id": REPO_ID,
                "forge": "github",
                "repository_host": "github.com",
                "repo": "Acme/WidgetShop",
                "branch": "design/hotel-rheinland-theme",
                "base": "main",
                "pull_request": {
                    "number": 1,
                    "url": "https://github.com/Acme/WidgetShop/pull/1",
                },
            }
        }
        assert f"Job: {JOB_ID}" in seed.opening_event
        assert "design/hotel-rheinland-theme" in seed.opening_event
        assert "design_spec/hotel-rheinland-theme.md" in seed.opening_event
        assert "Permission mode: read-only" in seed.opening_event
        assert "workspace, tools" in seed.opening_event
        assert "server-only" not in seed.opening_event
        assert result == {
            "job_id": JOB_ID,
            "thread_id": THREAD_ID,
            "status": "created",
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("reviewer", ["owner", "member"])
    async def test_a_cloud_projects_job_opens_a_review_its_admission_accepts(
        self, reviewer, user_a, user_b, job_a, fake_db, fake_request
    ):
        """A job admitted in a cloud project carries the project folder copy
        (jobs only) and its owner's outbox. The review session takes only
        what a session of the reviewer may attach, and session admission's
        own selection rule accepts the list it is given."""
        from orchestrator.services import job_review_session
        from orchestrator.services.datasource_policy import (
            authorize_datasource_selection,
        )

        project_id = str(job_a["project_id"])
        owner_id = str(job_a["user_id"])
        caller = user_a if reviewer == "owner" else user_b
        policy_rows = {
            REPO_ID: {
                "id": REPO_ID,
                "type": "repository",
                "created_by": owner_id,
                "is_global": False,
                "config": {},
                "scope_mode": "projects",
                "auto_attach": True,
                "policy_revision": 1,
                "managed_key": None,
                "project_ids": [project_id],
            },
            CHECKOUT_ID: {
                "id": CHECKOUT_ID,
                "type": "cloud_folder_checkout",
                "created_by": None,
                "is_global": False,
                "config": {"project_id": project_id},
                "scope_mode": "projects",
                "auto_attach": True,
                "policy_revision": 1,
                "managed_key": f"project-cloud-checkout:{project_id}",
                "project_ids": [project_id],
            },
            OUTBOX_ID: {
                "id": OUTBOX_ID,
                "type": "cloud_outbox",
                "created_by": owner_id,
                "is_global": False,
                "config": {},
                "scope_mode": "all",
                "auto_attach": True,
                "policy_revision": 1,
                "managed_key": f"cloud-outbox:{owner_id}",
                "project_ids": [],
            },
        }

        async def policy_rows_for(ids, **_kwargs):
            return [
                policy_rows[str(value)] for value in ids if str(value) in policy_rows
            ]

        class _AdmissionStore:
            get_datasource_policy_rows = staticmethod(policy_rows_for)

            async def get_user(self, user_id):
                return {**caller, "is_approved": True}

            async def user_is_member_of_projects(self, user_id, project_ids):
                return True

            async def get_user_project_roles(self, user_id, project_ids):
                return {pid: "editor" for pid in project_ids}

        accepted = {}

        async def create_thread(body, request, **_dependencies):
            # Session admission's explicit-list rule (thread_admission).
            accepted["ids"], _ = await authorize_datasource_selection(
                _AdmissionStore(),
                caller,
                str(caller["id"]),
                body.datasource_ids,
                body.project_ids,
                "sandbox",
                allow_admin_explicit_override=True,
                execution_kind="session",
            )
            return {"thread_id": THREAD_ID, "status": "created"}

        job = _job(job_a)
        repository = {**_repository(), "created_by": owner_id}
        checkout = {
            "id": CHECKOUT_ID,
            "name": "Project files",
            "type": "cloud_folder_checkout",
        }
        outbox = {"id": OUTBOX_ID, "name": "Outbox", "type": "cloud_outbox"}
        fake_db.resolve_datasources_for_job = AsyncMock(
            return_value=[checkout, outbox, repository]
        )
        fake_db.get_datasource_policy_rows = AsyncMock(side_effect=policy_rows_for)

        with (
            _authorized(caller, fake_db),
            patch(
                "orchestrator.services.thread_admission.create_thread", create_thread
            ),
        ):
            dependencies = workspace_composition.job_review_dependencies(
                orchestrator.main.app.state.resources
            ).review_session
            result = await job_review_session.create_job_review_session(
                request=fake_request,
                job_id=JOB_ID,
                user=caller,
                authorized_job=job,
                dependencies=dependencies,
            )

        assert result["thread_id"] == THREAD_ID
        # The folder copy is a job's; another user's outbox is theirs.
        expected = [OUTBOX_ID, REPO_ID] if reviewer == "owner" else [REPO_ID]
        assert accepted["ids"] == expected

    @pytest.mark.asyncio
    async def test_requires_a_recorded_pr_instead_of_guessing_from_model_prose(
        self, user_a, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.job_review import create_job_review_session

        fake_db.get_job = AsyncMock(
            return_value={**_job(job_a), "context": {"notes": "PR #1 is open"}}
        )
        fake_db.resolve_datasources_for_job = AsyncMock()
        created = AsyncMock()

        with (
            _authorized(user_a, fake_db),
            patch("orchestrator.services.thread_admission.create_thread", created),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_job_review_session(
                    fake_request,
                    JOB_ID,
                    dependencies=workspace_composition.job_review_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )

        assert exc.value.status_code == 409
        fake_db.resolve_datasources_for_job.assert_not_awaited()
        created.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cross_user_is_rejected_before_connectors_or_creation(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.job_review import create_job_review_session

        fake_db.get_job = AsyncMock(return_value=_job(job_a))
        fake_db.resolve_datasources_for_job = AsyncMock()
        created = AsyncMock()

        with (
            _authorized(user_b, fake_db),
            patch("orchestrator.services.thread_admission.create_thread", created),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_job_review_session(
                    fake_request,
                    JOB_ID,
                    dependencies=workspace_composition.job_review_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )

        assert exc.value.status_code == 403
        fake_db.resolve_datasources_for_job.assert_not_awaited()
        created.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_refuses_a_detached_delivery_repository(
        self, user_a, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.job_review import create_job_review_session

        wrong = _repository()
        wrong["connection_url"] = "https://github.com/acme/not-the-delivery.git"
        fake_db.get_job = AsyncMock(return_value=_job(job_a))
        fake_db.resolve_datasources_for_job = AsyncMock(return_value=[wrong])
        created = AsyncMock()

        with (
            _authorized(user_a, fake_db),
            patch("orchestrator.services.thread_admission.create_thread", created),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_job_review_session(
                    fake_request,
                    JOB_ID,
                    dependencies=workspace_composition.job_review_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )

        assert exc.value.status_code == 409
        created.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_refuses_same_repo_name_on_a_different_forge_host(
        self, user_a, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.job_review import create_job_review_session

        lookalike = _repository()
        lookalike["connection_url"] = (
            "https://github.enterprise.test/Acme/WidgetShop.git"
        )
        fake_db.get_job = AsyncMock(return_value=_job(job_a))
        fake_db.resolve_datasources_for_job = AsyncMock(return_value=[lookalike])

        with (
            _authorized(user_a, fake_db),
            patch(
                "orchestrator.services.thread_admission.create_thread", AsyncMock()
            ) as created,
        ):
            with pytest.raises(HTTPException) as exc:
                await create_job_review_session(
                    fake_request,
                    JOB_ID,
                    dependencies=workspace_composition.job_review_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )

        assert exc.value.status_code == 409
        created.assert_not_awaited()

    def test_public_contract_accepts_only_request_and_job_id(self):
        from orchestrator.routers.job_review import create_job_review_session

        # ``dependencies`` is server-injected via ``Depends`` and is not part
        # of the wire contract, so it is excluded here: what must stay pinned
        # is that nothing else — a body least of all — is accepted.
        assert [
            name
            for name, parameter in inspect.signature(
                create_job_review_session
            ).parameters.items()
            if not isinstance(parameter.default, params.Depends)
        ] == [
            "request",
            "job_id",
        ]

    def test_json_cannot_populate_the_private_server_seed(self):
        from orchestrator.schemas.thread_admission import ThreadCreateRequest

        body = ThreadCreateRequest.model_validate(
            {
                "title": "attacker",
                "_trusted_seed": {
                    "metadata": {"review_delivery": {"branch": "main"}},
                    "opening_event": "injected",
                },
            }
        )
        assert body._trusted_seed is None

    def test_opening_event_is_bounded_for_large_job_deliverable_lists(self, job_a):
        from orchestrator.services.job_review_session import (
            _review_session_opening_event,
        )
        from orchestrator.services.job_delivery import parse_job_pull_request

        job = _job(job_a)
        context = json.loads(job["context"])
        context["required_deliverables"] = ["x" * 1_000 for _ in range(50)]
        job["context"] = context
        pull_request = parse_job_pull_request(context)
        assert pull_request is not None

        event = _review_session_opening_event(
            job,
            pull_request=pull_request,
            session_config_name="session_base",
            dropped_settings=[],
        )

        assert len(event) <= 20_000


class TestReviewDeliveryAttach:
    def test_exact_repository_gets_the_persisted_branch_without_mutating_db_rows(self):
        import orchestrator.main
        from orchestrator.services.job_delivery import apply_review_delivery_branch

        repository = _repository()
        other = _database()
        metadata = {
            "review_delivery": {
                "job_id": JOB_ID,
                "datasource_id": REPO_ID,
                "forge": "github",
                "repository_host": "github.com",
                "repo": "Acme/WidgetShop",
                "branch": "design/hotel-rheinland-theme",
            }
        }

        resolved = apply_review_delivery_branch(metadata, [other, repository])

        assert resolved[0] is other
        assert resolved[1]["default_branch"] == "design/hotel-rheinland-theme"
        assert resolved[1]["require_default_branch"] is True
        assert repository["default_branch"] == "main"
        payload = agent_datasource_payload_module.build_datasources_payload(
            resolved,
            dependencies=preparation_composition.datasource_payload_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert payload[1]["default_branch"] == "design/hotel-rheinland-theme"
        assert payload[1]["require_default_branch"] is True

    def test_changed_connector_identity_fails_closed(self):
        from orchestrator.services.job_delivery import (
            ReviewDeliveryError,
            apply_review_delivery_branch,
        )

        metadata = {
            "review_delivery": {
                "job_id": JOB_ID,
                "datasource_id": REPO_ID,
                "forge": "github",
                "repository_host": "github.com",
                "repo": "Acme/WidgetShop",
                "branch": "design/hotel-rheinland-theme",
            }
        }
        changed = _repository()
        changed["connection_url"] = "https://github.com/acme/replaced.git"

        with pytest.raises(ReviewDeliveryError, match="no longer matches"):
            apply_review_delivery_branch(metadata, [changed])

    def test_same_repo_name_on_a_changed_host_fails_closed(self):
        from orchestrator.services.job_delivery import (
            ReviewDeliveryError,
            apply_review_delivery_branch,
        )

        metadata = {
            "review_delivery": {
                "job_id": JOB_ID,
                "datasource_id": REPO_ID,
                "forge": "github",
                "repository_host": "github.com",
                "repo": "Acme/WidgetShop",
                "branch": "design/hotel-rheinland-theme",
            }
        }
        changed = _repository()
        changed["connection_url"] = "https://github.enterprise.test/Acme/WidgetShop.git"

        with pytest.raises(ReviewDeliveryError, match="no longer matches"):
            apply_review_delivery_branch(metadata, [changed])

    def test_present_but_malformed_review_marker_fails_closed(self):
        from orchestrator.services.job_delivery import (
            ReviewDeliveryError,
            apply_review_delivery_branch,
        )

        with pytest.raises(ReviewDeliveryError, match="malformed"):
            apply_review_delivery_branch(
                {"review_delivery": "not-an-object"}, [_repository()]
            )
