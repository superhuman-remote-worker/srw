"""Migrations an old-head upgrade stage must still carry.

The forward-upgrade suites migrate a database to an old head, then run
today's code against it to build their fixtures. Today's retirement Begin,
cancel and delete revoke credential leases (and request the revoke of
provider-minted credentials, C5, connector drivers C2), so those tables must
exist at every head those suites stage. The migrations only create new tables (on
``datasources``, ``jobs`` and ``threads``, all present since cutover, or on
nothing), so applying them early changes no other object.
"""

LEASE_TABLE_MIGRATIONS = ("0346_", "0347_", "0430_")

# Today's catalog reads select ``models.served_model_id`` (0552): a fixture
# that creates a thread resolves the default chat model through them. 0552
# adds one nullable column to ``models`` (present since cutover) and renames
# duplicated model ids, which an old-head fixture database does not have.
CATALOG_COLUMN_MIGRATIONS = ("0552_",)

# Today's ``get_job`` selects ``jobs.harness_image`` (0559): every fixture
# that reads a job back goes through it. 0559 adds one nullable column each to
# ``agents`` and ``jobs`` (both present since cutover) and nothing else.
HARNESS_IMAGE_COLUMN_MIGRATIONS = ("0559_",)


def is_lease_table_migration(name: str) -> bool:
    return name.startswith(
        LEASE_TABLE_MIGRATIONS
        + CATALOG_COLUMN_MIGRATIONS
        + HARNESS_IMAGE_COLUMN_MIGRATIONS
    )
