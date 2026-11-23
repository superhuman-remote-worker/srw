"""Inputs to manifest resource and execution operations."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from shared.manifests.parsing import MAX_SOURCES
from shared.manifests.validation import MAX_SOURCE_BYTES


class ManifestScope(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: Literal["Account", "Project", "Catalog"]
    name: str = Field(pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$", max_length=63)


class ManifestSource(BaseModel):
    """One named manifest text, usually one file."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str = Field(
        min_length=1,
        max_length=255,
        pattern=r"^[^\x00-\x1f\x7f]+$",
        description=(
            "What to call this source in an error, usually the file's path. "
            "Names must differ within one request."
        ),
    )
    source: str = Field(min_length=1, max_length=MAX_SOURCE_BYTES)
    format: Literal["yaml", "json"] = "yaml"


class ManifestInput(BaseModel):
    """A manifest as one text, or as several named texts read as one bundle."""

    model_config = ConfigDict(extra="forbid", strict=True)

    source: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_SOURCE_BYTES,
        description="The manifest text. Give this or `sources`, not both.",
    )
    format: Literal["yaml", "json"] = Field(
        default="yaml", description="The format of `source`."
    )
    sources: list[ManifestSource] | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_SOURCES,
        description=(
            "Several manifest texts as one bundle, each with its own format, "
            "in place of `source`. Together they obey the limits of one "
            "source. An error then carries `source`, the name of the text it "
            "is in, and `document` counts inside that text."
        ),
    )

    @model_validator(mode="after")
    def one_input(self) -> "ManifestInput":
        if (self.source is None) == (self.sources is None):
            raise ValueError("Give either `source` or `sources`.")
        if self.sources is not None:
            if "format" in self.model_fields_set:
                raise ValueError(
                    "`format` belongs to `source`; with `sources`, give each "
                    "source its own format."
                )
            # Characters never outnumber bytes, so this refuses early what the
            # parser would refuse by its byte count.
            if sum(len(item.source) for item in self.sources) > MAX_SOURCE_BYTES:
                raise ValueError("The manifest sources together exceed 1 MiB.")
        return self


class ManifestPreviewInput(ManifestInput):
    default_scope: ManifestScope | None = None
    resolution: Literal["bundle", "stored"] = "bundle"


class ManifestExportInput(ManifestPreviewInput):
    output_format: Literal["yaml", "json"] = "yaml"


class ManifestApplyInput(ManifestInput):
    default_scope: ManifestScope | None = None
    expected_versions: dict[str, int] = Field(default_factory=dict, max_length=100)
    plan_revision: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)


class ManifestSecretInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    scope: ManifestScope | None = None
    values: dict[str, SecretStr] = Field(min_length=1, max_length=100)
    expected_version: int | None = Field(default=None, ge=1)


class ManifestOutcomeInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    attempt: int = Field(ge=1)
    outcome: Literal["Succeeded", "Failed"]
