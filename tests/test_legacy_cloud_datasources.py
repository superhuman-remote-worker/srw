"""Nextcloud admin's home is no seeded connector's URL (main cloud slice 8).

The retire pass itself runs on a real PostgreSQL in
``test_legacy_cloud_datasources_real_postgres``.
"""

from __future__ import annotations

import logging
import os
from unittest.mock import AsyncMock

import pytest

from orchestrator.services.legacy_cloud_datasources import (
    is_nextcloud_admin_home_url,
)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("http://srw-nextcloud/remote.php/dav/files/admin/", True),
        ("http://srw-nextcloud/remote.php/dav/files/admin", True),
        (" https://cloud.example/nc/remote.php/dav/files/admin/ ", True),
        ("http://srw-nextcloud/remote.php/dav/files/ada/", False),
        ("http://srw-nextcloud/remote.php/dav/files/admin/Team/", False),
        ("https://dav.example/remote.php/webdav/", False),
        ("", False),
        (None, False),
    ],
)
def test_admins_home_is_recognised(url, expected):
    assert is_nextcloud_admin_home_url(url) is expected


@pytest.fixture
def _env(monkeypatch):
    for name in list(os.environ):
        if name.startswith("DEFAULT_DS_"):
            monkeypatch.delenv(name)
    return monkeypatch


@pytest.mark.asyncio
async def test_init_refuses_to_seed_admins_home(_env, caplog):
    """An old ConfigMap still naming the bundled row seeds nothing."""
    from orchestrator import init

    _env.setenv(
        "DEFAULT_DS_WEBDAV_URL", "http://srw-nextcloud/remote.php/dav/files/admin/"
    )
    _env.setenv("DEFAULT_DS_WEBDAV_NAME", "Nextcloud")
    _env.setenv("DEFAULT_DS_WEBDAV_USERNAME", "admin")
    _env.setenv("DEFAULT_DS_WEBDAV_PASSWORD", "secret")
    db = AsyncMock()
    caplog.set_level(logging.WARNING)

    await init._seed_default_datasources(db)

    db.upsert_default_datasource.assert_not_awaited()
    assert "Nextcloud admin's home" in caplog.text
    assert "secret" not in caplog.text


@pytest.mark.asyncio
async def test_init_still_seeds_an_external_webdav_server(_env):
    from orchestrator import init

    _env.setenv("DEFAULT_DS_WEBDAV_URL", "https://dav.example/remote.php/webdav/")
    _env.setenv("DEFAULT_DS_WEBDAV_NAME", "External WebDAV")
    db = AsyncMock()

    await init._seed_default_datasources(db)

    (call,) = db.upsert_default_datasource.await_args_list
    assert call.kwargs["name"] == "External WebDAV"
    assert call.kwargs["ds_type"] == "webdav"
