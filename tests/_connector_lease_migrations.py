"""Migrations an old-head upgrade stage must still carry (connector drivers C2).

The forward-upgrade suites migrate a database to an old head, then run
today's code against it to build their fixtures. Today's retirement Begin,
cancel and delete revoke credential leases, so the lease tables must exist
at every head those suites stage. The two migrations only create new tables
(on ``datasources``, ``jobs`` and ``threads``, all present since cutover), so
applying them early changes no other object.
"""

LEASE_TABLE_MIGRATIONS = ("0346_", "0347_")


def is_lease_table_migration(name: str) -> bool:
    return name.startswith(LEASE_TABLE_MIGRATIONS)
