"""Several named manifest texts in one request (``sources``).

A command given several files sends them as ``sources: [{name, source,
format}]`` in place of ``source``. Each text is parsed by itself, so YAML and
JSON can be mixed; together they are one bundle under the limits of a single
source. An error names the text it is in and counts documents inside it, also
when it was raised after parsing, where documents are counted across the
bundle.
"""

import json
from unittest.mock import AsyncMock, Mock

from httpx import ASGITransport, AsyncClient
import pytest

from orchestrator.schemas.manifests import ManifestInput
from shared.manifests import (
    ManifestError,
    ParsedSources,
    parse_documents,
    parse_sources,
)
from shared.manifests.errors import ManifestIssue
from shared.manifests.validation import MAX_DOCUMENTS, MAX_SOURCE_BYTES
from tests.test_manifest_http import make_app, manifest

SCOPE = {"kind": "Account", "name": "personal"}


def expert(name):
    doc = manifest()
    doc["metadata"]["name"] = name
    return doc


def as_yaml(name):
    return (
        "apiVersion: srw/v1alpha1\nkind: Expert\n"
        f"metadata:\n  name: {name}\n"
        "spec:\n  runtime:\n    image: example/custom:1\n"
    )


def json_source(name, *documents):
    payload = documents[0] if len(documents) == 1 else list(documents)
    return {"name": name, "source": json.dumps(payload), "format": "json"}


def yaml_source(name, text):
    return {"name": name, "source": text, "format": "yaml"}


TWO_FILES = [
    json_source("a.json", expert("alpha")),
    yaml_source("b.yaml", as_yaml("beta")),
]


class TestParseSources:
    def test_mixed_formats_become_one_bundle_in_order(self):
        bundle = parse_sources(TWO_FILES)
        assert [doc["metadata"]["name"] for doc in bundle.documents] == [
            "alpha",
            "beta",
        ]
        assert bundle.origins == (("a.json", 1), ("b.yaml", 1))

    def test_the_bundle_stands_in_for_a_source_text(self):
        bundle = parse_sources(TWO_FILES)
        documents = parse_documents(bundle, format="yaml")
        assert documents == bundle.documents
        documents[0]["metadata"]["name"] = "changed"
        assert bundle.documents[0]["metadata"]["name"] == "alpha"

    @pytest.mark.parametrize(
        ("sources", "code", "source", "document"),
        [
            # A syntax error in the second document of the second text.
            (
                [
                    json_source("a.json", expert("alpha")),
                    yaml_source("b.yaml", as_yaml("beta") + "---\nkind: [\n"),
                ],
                "InvalidSyntax",
                "b.yaml",
                2,
            ),
            # A schema error is found after all texts are read, and is still
            # counted inside its own text.
            (
                [
                    json_source("a.json", expert("alpha"), expert("gamma")),
                    yaml_source(
                        "b.yaml",
                        as_yaml("beta").replace("kind: Expert", "kind: Unknown"),
                    ),
                ],
                "InvalidManifest",
                "b.yaml",
                1,
            ),
            (
                [
                    json_source("a.json", expert("alpha")),
                    yaml_source("b.yaml", "# none\n"),
                ],
                "EmptySource",
                "b.yaml",
                1,
            ),
            (
                [
                    json_source("same", expert("alpha")),
                    json_source("same", expert("beta")),
                ],
                "DuplicateSource",
                "same",
                1,
            ),
        ],
    )
    def test_an_error_names_its_source(self, sources, code, source, document):
        with pytest.raises(ManifestError) as error:
            parse_sources(sources)
        issue = error.value.as_dict()
        assert (issue["code"], issue["source"], issue["document"]) == (
            code,
            source,
            document,
        )

    def test_the_size_limit_holds_for_the_texts_together(self):
        filler = "#" * (MAX_SOURCE_BYTES // 2)
        sources = [
            yaml_source("a.yaml", as_yaml("alpha") + filler),
            yaml_source("b.yaml", as_yaml("beta") + filler),
        ]
        with pytest.raises(ManifestError) as error:
            parse_sources(sources)
        assert error.value.issue.code == "InputLimitExceeded"
        assert error.value.issue.source == "b.yaml"

    def test_the_document_limit_holds_for_the_texts_together(self):
        half = [expert(f"e{number}") for number in range(MAX_DOCUMENTS // 2 + 1)]
        other = [expert(f"f{number}") for number in range(MAX_DOCUMENTS // 2 + 1)]
        with pytest.raises(ManifestError) as error:
            parse_sources([json_source("a.json", *half), json_source("b.json", *other)])
        assert error.value.issue.code == "DocumentLimit"

    def test_one_source_text_keeps_its_error_shape(self):
        with pytest.raises(ManifestError) as error:
            parse_documents("kind: [\n")
        assert "source" not in error.value.as_dict()

    def test_a_later_error_is_traced_back_to_its_source(self):
        origins = (("a.json", 1), ("a.json", 2), ("b.yaml", 1))
        late = ManifestError(ManifestIssue("DuplicateResource", "x", document=3))
        assert late.located(origins).as_dict() == {
            "code": "DuplicateResource",
            "message": "x",
            "document": 1,
            "path": "/",
            "source": "b.yaml",
        }
        # Outside the bundle, or already located: unchanged.
        beyond = ManifestError(ManifestIssue("X", "x", document=9))
        assert beyond.located(origins) is beyond
        named = late.located(origins)
        assert named.located(origins) is named


class TestRequestModel:
    @pytest.mark.parametrize(
        "body",
        [
            {},
            {"source": "{}", "sources": [{"name": "a", "source": "{}"}]},
            {"sources": []},
            {"sources": [{"name": "a", "source": "{}"}], "format": "json"},
            {"sources": [{"name": "a\nb", "source": "{}"}]},
            {"sources": [{"name": "a", "source": "{}", "extra": 1}]},
            {
                "sources": [
                    {"name": "a", "source": "#" * MAX_SOURCE_BYTES},
                    {"name": "b", "source": "#"},
                ]
            },
        ],
    )
    def test_refused(self, body):
        with pytest.raises(ValueError):
            ManifestInput(**body)

    def test_either_form_is_accepted(self):
        assert ManifestInput(source="{}").sources is None
        assert ManifestInput(sources=[{"name": "a", "source": "{}"}]).source is None


async def _post(app, path, body):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.post(path, json=body)


class TestRoutes:
    @pytest.mark.asyncio
    async def test_validate_reports_each_document_with_its_source(self):
        response = await _post(
            make_app(), "/api/manifests/validate", {"sources": TWO_FILES}
        )
        assert response.status_code == 200, response.text
        assert response.json()["documents"] == [
            {"kind": "Expert", "name": "alpha", "source": "a.json"},
            {"kind": "Expert", "name": "beta", "source": "b.yaml"},
        ]

    @pytest.mark.asyncio
    async def test_one_text_reports_no_source(self):
        response = await _post(
            make_app(),
            "/api/manifests/validate",
            {"source": json.dumps(expert("alpha")), "format": "json"},
        )
        assert response.json()["documents"] == [{"kind": "Expert", "name": "alpha"}]

    @pytest.mark.asyncio
    async def test_a_syntax_error_names_the_file(self):
        sources = [TWO_FILES[0], yaml_source("b.yaml", "kind: Expert\nkind: SENTINEL")]
        response = await _post(
            make_app(), "/api/manifests/validate", {"sources": sources}
        )
        assert response.status_code == 422
        assert response.json()["detail"] == {
            "code": "DuplicateKey",
            "message": "Duplicate object key.",
            "document": 1,
            "path": "/",
            "source": "b.yaml",
        }
        assert "SENTINEL" not in response.text

    @pytest.mark.asyncio
    async def test_an_error_found_across_the_bundle_names_the_file(self):
        # The same Expert in two files collides only once both are resolved
        # together, where documents are counted across the bundle.
        sources = [
            json_source("a.json", expert("other"), expert("alpha")),
            yaml_source("b.yaml", as_yaml("alpha")),
        ]
        response = await _post(
            make_app(),
            "/api/manifests/preview",
            {"sources": sources, "default_scope": SCOPE},
        )
        assert response.status_code == 422, response.text
        detail = response.json()["detail"]
        assert detail["code"] == "DuplicateResource"
        assert (detail["source"], detail["document"]) == ("b.yaml", 1)

    @pytest.mark.asyncio
    async def test_preview_and_export_read_the_bundle(self):
        app = make_app()
        body = {"sources": TWO_FILES, "default_scope": SCOPE}
        preview = await _post(app, "/api/manifests/preview", body)
        assert preview.status_code == 200, preview.text
        assert [doc["metadata"]["name"] for doc in preview.json()["resolved"]] == [
            "alpha",
            "beta",
        ]
        exported = await _post(
            app, "/api/manifests/export", {**body, "output_format": "json"}
        )
        assert exported.status_code == 200, exported.text
        names = [
            doc["metadata"]["name"] for doc in json.loads(exported.json()["source"])
        ]
        assert names == ["alpha", "beta"]

    @pytest.mark.asyncio
    async def test_apply_hands_the_service_the_bundle(self):
        resources = Mock()
        resources.apply = AsyncMock(return_value={"resources": []})
        response = await _post(
            make_app(resources=resources),
            "/api/manifests/apply",
            {"sources": TWO_FILES},
        )
        assert response.status_code == 200, response.text
        bundle = resources.apply.await_args.args[0]
        assert isinstance(bundle, ParsedSources)
        assert [doc["metadata"]["name"] for doc in bundle.documents] == [
            "alpha",
            "beta",
        ]

    @pytest.mark.asyncio
    async def test_an_error_from_the_apply_names_the_file(self):
        resources = Mock()
        resources.apply = AsyncMock(
            side_effect=ManifestError(ManifestIssue("UnknownRef", "x", document=2))
        )
        response = await _post(
            make_app(resources=resources),
            "/api/manifests/apply",
            {"sources": TWO_FILES},
        )
        assert response.status_code == 422
        detail = response.json()["detail"]
        assert (detail["source"], detail["document"]) == ("b.yaml", 1)

    @pytest.mark.asyncio
    async def test_a_refused_request_shape_reaches_no_service(self):
        resources = Mock()
        response = await _post(
            make_app(resources=resources),
            "/api/manifests/apply",
            {"source": "{}", "sources": TWO_FILES},
        )
        assert response.status_code == 422
        assert not resources.mock_calls
