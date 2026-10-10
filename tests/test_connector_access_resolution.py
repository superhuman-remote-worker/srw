"""The level an execution binds a connector at (main cloud slice 5, 5.1).

``resolve_access`` is the strict rule (``strict_access_resolution``) with an
execution's inputs: a level it asks for, the project link's default and a
ceiling. A level asked for binds as asked or is refused; a default is only
ever lowered. Drivers that are not strict keep ``access_resolution``'s rule
exactly.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from orchestrator.services.connector_drivers.base import bound_access
from orchestrator.services.connector_drivers.cloud_folder import CloudFolderDriver
from shared.connectors.builtin import (
    BUILTIN_SPECS,
    CLOUD_FOLDER_CHECKOUT_SPEC,
    CLOUD_FOLDER_SPEC,
    spec_for_type,
)
from shared.connectors.contract import (
    ACCESS_ORIGINS,
    ACCESS_REFUSALS,
    AccessContext,
    AccessLevel,
    AccessResolution,
    access_resolution,
    read_only_ceiling,
    resolve_access,
    strict_access,
)

SPEC = CLOUD_FOLDER_SPEC
ALL = ("read_only", "protected", "read_write")


class TestExplicitLevels:
    @pytest.mark.parametrize("level", ALL)
    def test_an_allowed_level_binds_as_asked(self, level):
        resolved = resolve_access({}, SPEC, requested=level)
        assert resolved == AccessResolution(level, origin="explicit")

    def test_a_stored_level_says_it_was_materialized(self):
        resolved = resolve_access(
            {}, SPEC, requested="protected", requested_origin="materialized"
        )
        assert resolved == AccessResolution("protected", origin="materialized")

    def test_a_level_above_the_ceiling_is_refused_never_lowered(self):
        resolved = resolve_access({}, SPEC, requested="read_write", ceiling="read_only")
        assert resolved.level is None
        assert resolved.code == "above_ceiling"
        assert resolved.reason == (
            "Cloud folder binds this connector at most at read_only here; "
            "'read_write' is above it"
        )

    def test_a_level_not_offered_is_refused_with_the_strict_rules_words(self):
        resolved = resolve_access(
            {}, SPEC, offered=("read_only", "read_write"), requested="protected"
        )
        assert resolved == access_resolution(
            {}, SPEC, offered=("read_only", "read_write"), requested="protected"
        )
        assert resolved.code == "level_not_offered"

    def test_an_unknown_level_is_refused(self):
        assert resolve_access({}, SPEC, requested="admin").code == ("level_not_offered")

    def test_a_read_only_binding_refuses_a_level_that_writes(self):
        for level in ("protected", "read_write"):
            resolved = resolve_access(
                {"project_read_only": True}, SPEC, requested=level
            )
            assert resolved.code == "above_ceiling"
        assert resolve_access(
            {"project_read_only": True}, SPEC, requested="read_only"
        ) == AccessResolution("read_only", origin="explicit")

    def test_an_explicit_level_wins_over_the_link_default(self):
        assert resolve_access(
            {}, SPEC, requested="read_write", link_default="protected"
        ) == AccessResolution("read_write", origin="explicit")


class TestDefaults:
    def test_the_link_default_binds_when_allowed(self):
        assert resolve_access({}, SPEC, link_default="protected") == (
            AccessResolution("protected", origin="link_default")
        )

    def test_without_a_link_default_the_driver_default_binds(self):
        assert resolve_access({}, SPEC) == AccessResolution(
            "read_write", origin="driver_default"
        )
        spec = replace(SPEC, default_access="protected")
        assert resolve_access({}, spec) == AccessResolution(
            "protected", origin="driver_default"
        )

    def test_a_default_off_this_tier_is_lowered_to_the_next_allowed(self):
        resolved = resolve_access(
            {}, SPEC, offered=("read_only", "read_write"), link_default="protected"
        )
        assert resolved.level == "read_only"
        assert resolved.origin == "clamped"
        assert not resolved.refused
        assert resolved.reason == (
            "the default 'protected' is not allowed here; lowered to 'read_only'"
        )

    def test_a_default_above_the_ceiling_is_lowered(self):
        resolved = resolve_access({}, SPEC, ceiling="read_only")
        assert (resolved.level, resolved.origin) == ("read_only", "clamped")
        resolved = resolve_access(
            {}, SPEC, ceiling="protected", link_default="read_write"
        )
        assert (resolved.level, resolved.origin) == ("protected", "clamped")

    def test_a_default_is_never_raised(self):
        # The default is the lowest level and it is not offered: nothing
        # above it takes its place.
        resolved = resolve_access(
            {}, SPEC, offered=("protected", "read_write"), link_default="read_only"
        )
        assert resolved.level is None
        assert resolved.code == "level_not_offered"
        assert resolved.reason == (
            "Cloud folder does not offer the access level 'read_only' or any "
            "level below it here (offered: protected, read_write)"
        )

    def test_a_default_with_every_offered_level_above_the_ceiling_is_refused(self):
        # A viewer on a provider that offers no read-only level.
        resolved = resolve_access(
            {}, SPEC, offered=("protected", "read_write"), ceiling="read_only"
        )
        assert resolved.level is None
        assert resolved.code == "above_ceiling"

    def test_an_unknown_link_default_is_refused_not_mapped(self):
        resolved = resolve_access({}, SPEC, link_default="bogus")
        assert resolved.code == "level_not_offered"

    def test_a_read_only_binding_takes_the_strict_rules_floor(self):
        resolved = resolve_access({"project_read_only": True}, SPEC)
        assert (resolved.level, resolved.origin) == ("read_only", "clamped")
        resolved = resolve_access(
            {"project_read_only": True}, SPEC, link_default="read_only"
        )
        assert resolved == AccessResolution("read_only", origin="link_default")

    def test_a_read_only_binding_never_lands_on_protected(self):
        for offered in (("protected",), ("protected", "read_write")):
            resolved = resolve_access(
                {"project_read_only": True},
                SPEC,
                offered=offered,
                link_default="protected",
            )
            assert resolved.level is None
            assert resolved.code == "read_only_unavailable"
            for ceiling in (None, "read_only", "protected"):
                assert (
                    resolve_access(
                        {"project_read_only": True},
                        SPEC,
                        offered=offered,
                        ceiling=ceiling,
                    ).level
                    is None
                )

    def test_nothing_offered_is_refused_as_before(self):
        assert resolve_access({}, SPEC, offered=()) == access_resolution(
            {}, SPEC, offered=()
        )

    def test_an_unknown_ceiling_allows_nothing(self):
        resolved = resolve_access({}, SPEC, ceiling="bogus")
        assert resolved.level is None and resolved.refused


class TestVocabulary:
    def test_codes_and_origins_are_closed(self):
        assert "above_ceiling" in ACCESS_REFUSALS
        for kwargs in (
            {},
            {"requested": "read_write", "ceiling": "read_only"},
            {"offered": ("protected", "read_write"), "link_default": "read_only"},
            {"link_default": "protected", "offered": ("read_only",)},
        ):
            resolved = resolve_access({}, SPEC, **kwargs)
            assert resolved.code is None or resolved.code in ACCESS_REFUSALS
            assert resolved.origin is None or resolved.origin in ACCESS_ORIGINS

    def test_the_read_only_ceiling_is_the_highest_level_that_writes_nothing(self):
        assert read_only_ceiling(CLOUD_FOLDER_SPEC) == "read_only"
        assert read_only_ceiling(CLOUD_FOLDER_CHECKOUT_SPEC) == "read_only"
        assert read_only_ceiling(spec_for_type("postgresql")) is None
        two = replace(
            SPEC,
            access_levels=(
                AccessLevel("listing", 5, "Names only.", writes="none"),
                *SPEC.access_levels,
            ),
        )
        assert read_only_ceiling(two) == "read_only"


class TestOtherDriversKeepTodaysRule:
    @pytest.mark.parametrize(
        "spec", [spec for spec in BUILTIN_SPECS if not strict_access(spec)]
    )
    @pytest.mark.parametrize(
        "entry",
        [{}, {"project_read_only": True}, {"config": {"access": "bogus"}}],
    )
    def test_link_default_and_ceiling_change_nothing(self, spec, entry):
        expected = access_resolution(entry, spec)
        assert (
            resolve_access(entry, spec, link_default="read_only", ceiling="read_only")
            == expected
        )
        assert expected.origin is None


class TestBoundAccess:
    @staticmethod
    def _row(**changes):
        row = {
            "id": "11111111-1111-4111-8111-111111111111",
            "type": "cloud_folder",
            "managed_key": "project-cloud:22222222-2222-4222-8222-222222222222",
            "config": {
                "folder_kind": "project",
                "project_id": "22222222-2222-4222-8222-222222222222",
                "backend_id": "nextcloud",
                "backend_instance_id": "33333333-3333-4333-8333-333333333333",
                "folder_handle": "7",
            },
            "read_only": None,
            "is_global": False,
        }
        row.update(changes)
        return row

    def test_the_link_default_reaches_the_binding(self):
        driver = CloudFolderDriver()
        ctx = AccessContext(workspace_backend="sandbox")
        assert bound_access(driver, self._row(), ctx=ctx).level == "read_write"
        resolved = bound_access(driver, self._row(project_access="protected"), ctx=ctx)
        assert resolved == AccessResolution("protected", origin="link_default")

    def test_a_protected_default_is_lowered_where_the_lane_is_closed(self):
        driver = CloudFolderDriver()
        ctx = AccessContext(workspace_backend="sandbox", protected_mode_enabled=False)
        resolved = bound_access(driver, self._row(project_access="protected"), ctx=ctx)
        from orchestrator.services.cloud.nextcloud import NEXTCLOUD_CAPABILITIES

        if NEXTCLOUD_CAPABILITIES.offers(
            "cloud_folder", "project", "read_only", workspace_backend="sandbox"
        ):
            # Lowered to the reader account's level, never left protected.
            assert (resolved.level, resolved.origin) == ("read_only", "clamped")
        else:
            assert resolved.level is None
            assert resolved.code == "level_not_offered"

    def test_a_stored_level_binds_or_is_refused(self):
        driver = CloudFolderDriver()
        ctx = AccessContext(workspace_backend="sandbox")
        mount = driver.mount_binding(self._row(), ctx=ctx, requested="protected")
        assert mount.access == "protected"
        refused = driver.mount_binding(
            self._row(), ctx=ctx, requested="read_write", ceiling="read_only"
        )
        assert refused.reason == "access_unavailable"
        assert "'read_write' is above it" in refused.detail
