"""Prepared receipt size is bounded by its immutable artifact disk capacity."""

import pytest
from shared.vm_creation_issuance import validate_rootdisk_source
from shared.workspace_preparation_settings import disk_bytes
from tests.test_vm_creation_preparation import prepared_case as _prepared_case_fixture

prepared_case = _prepared_case_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size",
    ["minimum", "capacity", "oversized", "unbounded", "zero", "negative", "boolean"],
)
async def test_prepared_receipt_disk_size_bounds(prepared_case, size):
    _, source, request, configuration = prepared_case
    capacity = disk_bytes(configuration["preparation"]["disk_size"])
    source["receipt"]["diskBytes"] = {
        "minimum": 1,
        "capacity": capacity,
        "oversized": capacity + 1,
        "unbounded": 2**100,
        "zero": 0,
        "negative": -1,
        "boolean": True,
    }[size]
    if size in {"minimum", "capacity"}:
        validate_rootdisk_source(
            source, request=request, configuration=configuration, expected_pvc_uid=None
        )
    else:
        with pytest.raises(ValueError, match="Prepared artifact receipt changed"):
            validate_rootdisk_source(
                source,
                request=request,
                configuration=configuration,
                expected_pvc_uid=None,
            )
