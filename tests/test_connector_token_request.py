"""Kubernetes TokenRequest minting for kubeconfig connectors (connector drivers C5).

The rules (``shared.connectors.token_request``), the three API calls against
a fake API server (``tests/_provider_fakes.py``) and the kubeconfig
driver's create, update, bind and Test.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml
from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    BindContext,
    ConnectorDraft,
    DeploymentGates,
)
from orchestrator.services.connector_drivers.credential_files import (
    KubeconfigDriver,
)
from orchestrator.services.connector_drivers.provider_http import ProviderError
from orchestrator.services.connector_drivers.token_request import (
    delete_bound_secret,
    delivered_kubeconfig_text,
    mint_token,
    parse_minting_kubeconfig,
)
from shared.connectors.builtin import KUBECONFIG_SPEC
from shared.connectors.token_request import (
    MIN_EXPIRATION_SECONDS,
    TokenRequestConfigError,
    TokenRequestOptions,
    bound_secret,
    parse_token_request,
    secret_name,
    token_request,
    token_request_options,
)
from tests._provider_fakes import (
    FAKE_CA,
    KUBE_SERVER,
    FakeKubeApi,
    ProviderRouter,
    install,
    minting_kubeconfig_yaml,
)

CONNECTOR = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
OPTIONS = {"namespace": "srw-identities", "service_account": "agent"}


@pytest.fixture
def kube(monkeypatch):
    api = FakeKubeApi()
    install(monkeypatch, ProviderRouter(kube=api))
    return api


def _draft(**over):
    fields = dict(
        name="cluster",
        connection_url=None,
        credentials=None,
        config=None,
        read_only=None,
        is_global=None,
        default_branch=None,
    )
    fields.update(over)
    supplied = frozenset(key for key, value in over.items())
    return ConnectorDraft(**fields, supplied=supplied)


def _files(contents: str) -> dict:
    return {"files": [{"contents": contents}]}


class TestOptions:
    def test_a_config_names_the_account_its_lifetime_and_audiences(self):
        options = parse_token_request(
            {**OPTIONS, "expiration_seconds": 900, "audiences": ["a", "b", "a"]}
        )
        assert options == TokenRequestOptions(
            "srw-identities", "agent", 900, ("a", "b")
        )
        assert options.as_config() == {
            **OPTIONS,
            "expiration_seconds": 900,
            "audiences": ["a", "b"],
        }

    def test_the_default_lifetime_is_an_hour(self):
        assert parse_token_request(OPTIONS).expiration_seconds == 3600

    def test_no_config_is_no_minting(self):
        assert token_request_options({}) is None
        assert token_request_options({"token_request": None}) is None
        assert token_request_options(None) is None

    @pytest.mark.parametrize(
        "value",
        [
            "x",
            {"service_account": "agent"},
            {**OPTIONS, "namespace": "Not_A_Label"},
            {**OPTIONS, "service_account": ""},
            {**OPTIONS, "expiration_seconds": MIN_EXPIRATION_SECONDS - 1},
            {**OPTIONS, "expiration_seconds": 86401},
            {**OPTIONS, "expiration_seconds": True},
            {**OPTIONS, "expiration_seconds": "3600"},
            {**OPTIONS, "audiences": "api"},
            {**OPTIONS, "audiences": ["has space"]},
            {**OPTIONS, "audiences": [str(n) for n in range(11)]},
            {**OPTIONS, "verbs": ["get"]},
        ],
    )
    def test_a_config_srw_cannot_use_is_refused(self, value):
        with pytest.raises(TokenRequestConfigError):
            parse_token_request(value)


class TestMintingKubeconfig:
    def test_a_bearer_token_kubeconfig_mints(self):
        minting = parse_minting_kubeconfig(
            minting_kubeconfig_yaml("tok-0123456789abcdef")
        )
        assert minting.server == KUBE_SERVER
        assert minting.token == "tok-0123456789abcdef"
        assert minting.ca_pem == FAKE_CA
        assert minting.context_namespace == "work"
        assert "tok-" not in repr(minting)

    @pytest.mark.parametrize(
        ("user", "cluster", "why"),
        [
            ({"exec": {"command": "aws"}}, {}, "exec plugin"),
            ({"auth-provider": {"name": "gcp"}}, {}, "auth-provider"),
            ({"tokenFile": "/var/run/token"}, {}, "token from a file"),
            ({"client-certificate-data": "eA=="}, {}, "client certificate"),
            ({"username": "u", "password": "p"}, {}, "basic auth"),
            ({"as": "admin"}, {}, "impersonates"),
            ({}, {"insecure-skip-tls-verify": True}, "skips TLS"),
            ({}, {"proxy-url": "http://proxy:3128"}, "proxy"),
            ({}, {"certificate-authority": "/ca.crt"}, "CA from a file"),
        ],
    )
    def test_what_srw_cannot_hold_as_data_is_refused(self, user, cluster, why):
        text = minting_kubeconfig_yaml(
            "tok-0123456789abcdef", user_extra=user, cluster_extra=cluster
        )
        with pytest.raises(TokenRequestConfigError, match=why):
            parse_minting_kubeconfig(text)

    @pytest.mark.parametrize(
        "text",
        [
            "",
            "::: not yaml :::\n\t- [",
            "[]",
            minting_kubeconfig_yaml("short"),
            minting_kubeconfig_yaml("tok-0123456789abcdef", server="http://kube:80"),
            minting_kubeconfig_yaml("tok-0123456789abcdef").replace(
                "current-context: fake", "current-context: other"
            ),
        ],
        # The fake CA is made per process: ids keep xdist's workers agreeing.
        ids=["empty", "not-yaml", "a-list", "short-token", "http", "no-context"],
    )
    def test_a_kubeconfig_without_a_usable_cluster_or_token_is_refused(self, text):
        with pytest.raises(TokenRequestConfigError):
            parse_minting_kubeconfig(text)

    @pytest.mark.parametrize(
        "server",
        ["https://kube.test:6443/prefix", "https://kube.test:6443/api/v1"],
    )
    def test_a_server_with_a_path_is_refused(self, server):
        with pytest.raises(TokenRequestConfigError, match="without a path"):
            parse_minting_kubeconfig(
                minting_kubeconfig_yaml("tok-0123456789abcdef", server=server)
            )

    def test_the_server_is_normalized_to_scheme_host_and_port(self):
        minting = parse_minting_kubeconfig(
            minting_kubeconfig_yaml(
                "tok-0123456789abcdef", server="https://KUBE.test:6443/"
            )
        )
        assert minting.server == KUBE_SERVER

    def test_the_delivered_kubeconfig_holds_the_minted_token_only(self):
        text = delivered_kubeconfig_text(
            server=KUBE_SERVER,
            ca_pem=FAKE_CA,
            tls_server_name=None,
            token="minted-token",
            context_namespace="work",
            name="fake",
        )
        doc = yaml.safe_load(text)
        assert doc["users"] == [{"name": "fake", "user": {"token": "minted-token"}}]
        assert doc["contexts"][0]["context"] == {
            "cluster": "fake",
            "user": "fake",
            "namespace": "work",
        }
        assert doc["current-context"] == "fake"
        assert "exec" not in text and "client-" not in text
        # The workspace's copy reads back as a minting-shaped kubeconfig.
        assert parse_minting_kubeconfig(text.replace("minted-token", "x" * 20)).server


class TestBodies:
    def test_the_secret_carries_no_data_and_srws_labels(self):
        body = bound_secret(
            name="srw-mint-1", credential_id="c-1", annotations={"a": "b"}
        )
        assert body["type"] == "Opaque" and body["immutable"] is True
        assert "data" not in body and "stringData" not in body
        assert body["metadata"]["labels"]["srw.io/minted-credential"] == "c-1"

    def test_the_token_is_bound_to_the_secret_by_name_and_uid(self):
        body = token_request(
            TokenRequestOptions("ns", "agent", 600, ("aud",)),
            secret="srw-mint-1",
            secret_uid="u-1",
        )
        assert body["spec"] == {
            "expirationSeconds": 600,
            "boundObjectRef": {
                "kind": "Secret",
                "apiVersion": "v1",
                "name": "srw-mint-1",
                "uid": "u-1",
            },
            "audiences": ["aud"],
        }

    def test_a_secret_is_named_after_its_credential(self):
        name = secret_name("12345678-1234-4234-8234-123456789abc")
        assert name == "srw-mint-12345678123442348234123456789abc"


def _minting(api: FakeKubeApi):
    return parse_minting_kubeconfig(minting_kubeconfig_yaml(api.minting_token))


class TestCalls:
    @pytest.mark.asyncio
    async def test_a_mint_creates_the_secret_then_a_token_bound_to_it(self, kube):
        minted = await mint_token(
            _minting(kube),
            parse_token_request({**OPTIONS, "expiration_seconds": 600}),
            secret="srw-mint-a",
            credential_id="c-a",
            annotations={"srw.io/connector": CONNECTOR},
        )
        assert minted.handle == kube.secrets["srw-mint-a"]["uid"]
        assert kube.authenticates(minted.token)
        lifetime = minted.expires_at - kube.tokens[minted.token]["expires_at"]
        assert abs(lifetime.total_seconds()) < 2
        methods = [(method, path) for method, path, _ in kube.requests]
        assert methods == [
            ("POST", "/api/v1/namespaces/srw-identities/secrets"),
            ("POST", "/api/v1/namespaces/srw-identities/serviceaccounts/agent/token"),
        ]
        assert "minting" not in repr(minted) and minted.token not in repr(minted)

    @pytest.mark.asyncio
    async def test_deleting_the_secret_revokes_the_token(self, kube):
        minting = _minting(kube)
        minted = await mint_token(
            minting,
            parse_token_request(OPTIONS),
            secret="srw-mint-b",
            credential_id="c-b",
            annotations={},
        )
        await delete_bound_secret(
            minting, namespace="srw-identities", name="srw-mint-b", uid=minted.handle
        )
        assert not kube.authenticates(minted.token)
        # Gone already, or replaced under the same name: revoked either way.
        await delete_bound_secret(
            minting, namespace="srw-identities", name="srw-mint-b", uid=minted.handle
        )
        kube.secrets["srw-mint-b"] = {"uid": "someone-else"}
        await delete_bound_secret(
            minting, namespace="srw-identities", name="srw-mint-b", uid=minted.handle
        )
        assert kube.secrets["srw-mint-b"] == {"uid": "someone-else"}

    @pytest.mark.asyncio
    async def test_a_refused_token_request_deletes_the_secret_again(self, kube):
        kube.forbidden.add("token")
        with pytest.raises(ProviderError, match="HTTP 403") as caught:
            await mint_token(
                _minting(kube),
                parse_token_request(OPTIONS),
                secret="srw-mint-c",
                credential_id="c-c",
                annotations={},
            )
        assert caught.value.transient is False
        assert "srw-mint-c" not in kube.secrets

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "transient"), [(401, False), (404, False), (500, True), (429, True)]
    )
    async def test_refusals_say_whether_a_retry_can_help(self, kube, status, transient):
        kube.fail["create-secret"] = [status]
        with pytest.raises(ProviderError) as caught:
            await mint_token(
                _minting(kube),
                parse_token_request(OPTIONS),
                secret="srw-mint-d",
                credential_id="c-d",
                annotations={},
            )
        assert caught.value.transient is transient
        assert kube.minting_token not in str(caught.value)

    @pytest.mark.asyncio
    async def test_the_api_servers_words_never_reach_the_error(self, kube):
        kube.fail["create-secret"] = [403]
        kube.leak = "secrets is forbidden: User internal-admin@10.0.0.7"
        with pytest.raises(ProviderError) as caught:
            await mint_token(
                _minting(kube),
                parse_token_request(OPTIONS),
                secret="srw-mint-l",
                credential_id="c-l",
                annotations={},
            )
        assert "HTTP 403" in str(caught.value)
        assert "internal-admin" not in str(caught.value)
        assert "10.0.0.7" not in str(caught.value)

    @pytest.mark.asyncio
    async def test_the_call_dials_the_resolved_address(self, monkeypatch):
        api = FakeKubeApi()
        router = install(monkeypatch, ProviderRouter(kube=api))
        await mint_token(
            _minting(api),
            parse_token_request(OPTIONS),
            secret="srw-mint-p",
            credential_id="c-p",
            annotations={},
        )
        assert router.dialled == [("203.0.113.10", "kube.test")] * 2

    @pytest.mark.asyncio
    async def test_a_cluster_address_needs_the_operators_listing(self, monkeypatch):
        api = FakeKubeApi()
        install(
            monkeypatch,
            ProviderRouter(kube=api),
            addresses={"kube.test": ("10.43.0.1",)},
        )
        with pytest.raises(ProviderError) as caught:
            await mint_token(
                _minting(api),
                parse_token_request(OPTIONS),
                secret="srw-mint-q",
                credential_id="c-q",
                annotations={},
                allow_private=True,
            )
        assert caught.value.reason == "address_refused"
        assert api.requests == []
        install(
            monkeypatch,
            ProviderRouter(kube=api),
            addresses={"kube.test": ("10.43.0.1",)},
            private_hosts=("kube.test:6443",),
        )
        minted = await mint_token(
            _minting(api),
            parse_token_request(OPTIONS),
            secret="srw-mint-q",
            credential_id="c-q",
            annotations={},
        )
        assert api.authenticates(minted.token)

    @pytest.mark.asyncio
    async def test_a_wrong_minting_credential_is_a_401(self, kube):
        minting = parse_minting_kubeconfig(
            minting_kubeconfig_yaml("wrong-token-0123456")
        )
        with pytest.raises(ProviderError, match="401"):
            await mint_token(
                minting,
                parse_token_request(OPTIONS),
                secret="srw-mint-e",
                credential_id="c-e",
                annotations={},
            )

    @pytest.mark.asyncio
    async def test_an_unreachable_server_is_transient(self, monkeypatch):
        install(monkeypatch, ProviderRouter())
        with pytest.raises(ProviderError) as caught:
            await mint_token(
                parse_minting_kubeconfig(minting_kubeconfig_yaml("t" * 20)),
                parse_token_request(OPTIONS),
                secret="srw-mint-f",
                credential_id="c-f",
                annotations={},
            )
        assert caught.value.transient is True

    @pytest.mark.asyncio
    async def test_a_delete_the_role_does_not_allow_is_an_error(self, kube):
        kube.secrets["srw-mint-g"] = {"uid": "u"}
        kube.forbidden.add("delete-secret")
        with pytest.raises(ProviderError, match="delete the bound Secret"):
            await delete_bound_secret(
                _minting(kube), namespace="srw-identities", name="srw-mint-g", uid="u"
            )


def _ctx():
    gates = DeploymentGates(lambda: True)
    return SimpleNamespace(
        environment=SimpleNamespace(gates=gates), can_autonomous_send=AsyncMock()
    )


class TestDriver:
    def test_the_kubeconfig_driver_mints_through_its_config(self):
        driver = KubeconfigDriver()
        assert driver.spec is KUBECONFIG_SPEC
        properties = KUBECONFIG_SPEC.config_schema["properties"]
        assert "token_request" in properties
        read_only = KUBECONFIG_SPEC.access_level("ReadOnly")
        assert "RBAC" in read_only.enforced_by and read_only.advisory

    @pytest.mark.asyncio
    async def test_a_create_with_token_request_checks_the_minting_kubeconfig(self):
        driver = KubeconfigDriver()
        normalized = await driver.validate(
            _draft(
                credentials=_files(minting_kubeconfig_yaml("tok-0123456789abcdef")),
                config={"token_request": {**OPTIONS, "expiration_seconds": 600}},
            ),
            existing=None,
            ctx=_ctx(),
        )
        assert normalized.config == {
            "token_request": {**OPTIONS, "expiration_seconds": 600}
        }
        with pytest.raises(HTTPException, match="exec plugin"):
            await driver.validate(
                _draft(
                    credentials=_files(
                        minting_kubeconfig_yaml(
                            "tok-0123456789abcdef",
                            user_extra={"exec": {"command": "x"}},
                        )
                    ),
                    config={"token_request": OPTIONS},
                ),
                existing=None,
                ctx=_ctx(),
            )

    @pytest.mark.asyncio
    async def test_a_static_kubeconfig_is_delivered_as_before_even_with_exec(self):
        driver = KubeconfigDriver()
        text = minting_kubeconfig_yaml("", user_extra={"exec": {"command": "aws"}})
        normalized = await driver.validate(
            _draft(credentials=_files(text)), existing=None, ctx=_ctx()
        )
        assert normalized.config == {}
        assert normalized.credentials["files"][0]["contents"] == text

    @pytest.mark.asyncio
    async def test_a_config_other_than_token_request_is_refused(self):
        with pytest.raises(HTTPException, match="token_request only"):
            await KubeconfigDriver().validate(
                _draft(
                    credentials=_files(minting_kubeconfig_yaml("t" * 20)),
                    config={"other": 1},
                ),
                existing=None,
                ctx=_ctx(),
            )

    @pytest.mark.asyncio
    async def test_an_update_is_checked_as_it_leaves_the_connector(self):
        driver = KubeconfigDriver()
        existing = {
            "id": CONNECTOR,
            "name": "cluster",
            "config": {},
            "credentials": _files(minting_kubeconfig_yaml("tok-0123456789abcdef")),
        }
        # Turning minting on with an exec kubeconfig is refused.
        with pytest.raises(HTTPException, match="exec plugin"):
            await driver.validate(
                _draft(
                    config={"token_request": OPTIONS},
                    credentials=_files(
                        minting_kubeconfig_yaml(
                            "", user_extra={"exec": {"command": "aws"}}
                        )
                    ),
                ),
                existing=existing,
                ctx=_ctx(),
            )
        # A bad CA in the stored kubeconfig is refused before any delivery
        # (a lifetime change keeps the stored kubeconfig in use).
        existing["config"] = {"token_request": OPTIONS}
        existing["credentials"] = _files(
            minting_kubeconfig_yaml("tok-0123456789abcdef").replace(
                "certificate-authority-data: ",
                "certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg== #",
            )
        )
        with pytest.raises(HTTPException, match="certificate-authority-data"):
            await driver.validate(
                _draft(
                    config={"token_request": {**OPTIONS, "expiration_seconds": 900}}
                ),
                existing=existing,
                ctx=_ctx(),
            )
        # A name-only edit of a minting connector is not checked again.
        normalized = await driver.validate(
            _draft(name="renamed"), existing=existing, ctx=_ctx()
        )
        assert normalized.config is None and normalized.credentials is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("stored", "edited"),
        [
            # Minting turned on over a kubeconfig delivered as it is.
            ({}, {"token_request": OPTIONS}),
            # Minting turned off: the minting credential would be delivered.
            ({"token_request": OPTIONS}, {}),
            ({"token_request": OPTIONS}, {"token_request": None}),
            # Another target ServiceAccount or namespace.
            (
                {"token_request": OPTIONS},
                {"token_request": {**OPTIONS, "service_account": "admin"}},
            ),
            (
                {"token_request": OPTIONS},
                {"token_request": {**OPTIONS, "namespace": "kube-system"}},
            ),
            # Tokens for other services.
            (
                {"token_request": OPTIONS},
                {"token_request": {**OPTIONS, "audiences": ["vault"]}},
            ),
            (
                {"token_request": {**OPTIONS, "audiences": ["vault"]}},
                {"token_request": OPTIONS},
            ),
        ],
        ids=[
            "on",
            "off-empty",
            "off-null",
            "other-account",
            "other-namespace",
            "audiences-added",
            "audiences-dropped",
        ],
    )
    async def test_an_edit_may_not_retarget_the_stored_kubeconfig(self, stored, edited):
        driver = KubeconfigDriver()
        kubeconfig = _files(minting_kubeconfig_yaml("tok-0123456789abcdef"))
        existing = {
            "id": CONNECTOR,
            "name": "cluster",
            "config": stored,
            "credentials": kubeconfig,
        }
        with pytest.raises(HTTPException, match="send the kubeconfig again") as caught:
            await driver.validate(_draft(config=edited), existing=existing, ctx=_ctx())
        assert caught.value.status_code == 400
        # With the kubeconfig sent again, the same edit is fine.
        normalized = await driver.validate(
            _draft(config=edited, credentials=kubeconfig),
            existing=existing,
            ctx=_ctx(),
        )
        assert normalized.credentials is not None

    @pytest.mark.asyncio
    async def test_a_lifetime_edit_keeps_the_stored_kubeconfig(self):
        normalized = await KubeconfigDriver().validate(
            _draft(
                config={
                    "token_request": {
                        **OPTIONS,
                        "expiration_seconds": 900,
                        "audiences": ["api", "b"],
                    }
                }
            ),
            existing={
                "id": CONNECTOR,
                "name": "cluster",
                "config": {"token_request": {**OPTIONS, "audiences": ["b", "api"]}},
                "credentials": _files(minting_kubeconfig_yaml("tok-0123456789abcdef")),
            },
            ctx=_ctx(),
        )
        assert normalized.config["token_request"]["expiration_seconds"] == 900
        assert normalized.credentials is None

    @pytest.mark.asyncio
    async def test_minting_is_refused_where_the_deployment_turned_it_off(
        self, monkeypatch
    ):
        from orchestrator.services import connector_minted_credentials as minted

        monkeypatch.setitem(minted._state, "enabled", False)
        driver = KubeconfigDriver()
        with pytest.raises(HTTPException, match="providerMinting") as caught:
            await driver.validate(
                _draft(
                    credentials=_files(minting_kubeconfig_yaml("tok-0123456789abcdef")),
                    config={"token_request": OPTIONS},
                ),
                existing=None,
                ctx=_ctx(),
            )
        assert caught.value.status_code == 403
        # A kubeconfig delivered as it is stays available.
        normalized = await driver.validate(
            _draft(credentials=_files(minting_kubeconfig_yaml("tok-0123456789abcdef"))),
            existing=None,
            ctx=_ctx(),
        )
        assert normalized.config == {}

    def test_a_minting_connector_binds_without_its_kubeconfig(self):
        driver = KubeconfigDriver()
        ctx = BindContext(
            gates=DeploymentGates(lambda: True),
            logger=logging.getLogger("test"),
            default_known_hosts="",
        )
        secret_text = minting_kubeconfig_yaml("tok-0123456789abcdef")
        row = {
            "id": CONNECTOR,
            "type": "kubeconfig",
            "name": "cluster",
            "config": {"token_request": OPTIONS},
        }
        credentials = {
            "files": [
                {
                    "name": "cluster.yaml",
                    "contents": secret_text,
                    "target_path": "/home/srw/.kube/configs/cluster.yaml",
                    "mode": "0600",
                }
            ]
        }
        entry = driver.bind(row, credentials, ctx=ctx)
        assert entry["credentials"] == {}
        assert entry["minted"] == {
            "provider": "kubernetes",
            "connector_id": CONNECTOR,
            "read_only": False,
            "file": {
                "name": "cluster.yaml",
                "target_path": "/home/srw/.kube/configs/cluster.yaml",
                "mode": "0600",
            },
        }
        assert "tok-0123456789abcdef" not in repr(entry)
        # Without minting, the stored file rides the entry as before (D1d).
        static = driver.bind({**row, "config": {}}, credentials, ctx=ctx)
        assert static["credentials"] == credentials and "minted" not in static

    @pytest.mark.asyncio
    async def test_test_mints_and_revokes(self, kube):
        result = await KubeconfigDriver().check(
            {"id": CONNECTOR, "config": {"token_request": OPTIONS}},
            _files(minting_kubeconfig_yaml(kube.minting_token)),
            ctx=SimpleNamespace(),
        )
        assert result["status"] == "ok", result
        assert "revoked it" in result["message"]
        assert kube.secrets == {}
        assert all(not kube.authenticates(token) for token in kube.tokens)
        assert kube.minting_token not in str(result)

    @pytest.mark.asyncio
    async def test_test_reports_a_role_that_cannot_mint(self, kube):
        kube.forbidden.add("token")
        result = await KubeconfigDriver().check(
            {"id": CONNECTOR, "config": {"token_request": OPTIONS}},
            _files(minting_kubeconfig_yaml(kube.minting_token)),
            ctx=SimpleNamespace(),
        )
        assert result["status"] == "error" and "HTTP 403" in result["message"]
        assert kube.secrets == {}
