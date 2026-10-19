"""sql_query stays read-only against a real PostgreSQL.

The read-only guarantee is a ``SET TRANSACTION READ ONLY`` before the model's
text. A text carrying its own transaction control (``COMMIT; DELETE …``) used
to end that transaction and write through it, because a parameterless query
went over the simple protocol, which runs every statement it is given.
"""

from types import SimpleNamespace

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

from agent.tools.sql.postgresql import create_postgresql_tools

_ROWS = list(range(1, 6))


@pytest.fixture(scope="module")
def pg_url():
    with PostgresContainer("postgres:16", driver=None) as pg:
        yield pg.get_connection_url()


@pytest.fixture
def table(pg_url):
    with psycopg.connect(pg_url, autocommit=True) as admin:
        admin.execute("DROP TABLE IF EXISTS t")
        admin.execute("CREATE TABLE t (id int PRIMARY KEY)")
        admin.execute("INSERT INTO t SELECT generate_series(1, 5)")


@pytest.fixture
def sql_query(pg_url, table):
    # Same connection shape as agent.connectors.connections.
    conn = psycopg.connect(pg_url, autocommit=False)
    tools = create_postgresql_tools(SimpleNamespace(connection_for=lambda _: conn))
    yield {t.name: t for t in tools}["sql_query"]
    conn.close()


def _ids(pg_url):
    with psycopg.connect(pg_url, autocommit=True) as admin:
        return [r[0] for r in admin.execute("SELECT id FROM t ORDER BY id")]


class TestSqlQueryRefusesWrites:
    @pytest.mark.parametrize(
        "query",
        [
            "COMMIT; DELETE FROM t WHERE id = 1",
            "COMMIT; DELETE FROM t WHERE id = 1; SELECT 1 AS x",
            "ROLLBACK; INSERT INTO t VALUES (99)",
            "SET TRANSACTION READ WRITE; DELETE FROM t WHERE id = 1",
            "DELETE FROM t WHERE id = 1",
            "WITH d AS (DELETE FROM t WHERE id = 1 RETURNING id) SELECT * FROM d",
        ],
    )
    def test_write_attempt_changes_nothing(self, sql_query, pg_url, query):
        out = sql_query.invoke({"query": query})

        assert _ids(pg_url) == _ROWS
        assert out.startswith(("Error executing query", "Query executed but"))

    def test_a_lone_commit_does_not_open_the_next_call(self, sql_query, pg_url):
        sql_query.invoke({"query": "COMMIT"})
        out = sql_query.invoke({"query": "DELETE FROM t WHERE id = 1"})

        assert "read-only transaction" in out
        assert _ids(pg_url) == _ROWS


class TestSqlQueryStillReads:
    def test_select(self, sql_query):
        out = sql_query.invoke({"query": "SELECT count(*) AS n FROM t"})

        assert out == 'Columns: n\n\nRow 1: {"n": 5}'

    def test_trailing_semicolon(self, sql_query):
        out = sql_query.invoke({"query": "SELECT id FROM t WHERE id = 2;"})

        assert 'Row 1: {"id": 2}' in out

    def test_semicolon_and_percent_inside_literals(self, sql_query):
        out = sql_query.invoke({"query": "SELECT 'a;b' AS s, '100%' AS p"})

        assert 'Row 1: {"s": "a;b", "p": "100%"}' in out

    def test_read_cte(self, sql_query):
        out = sql_query.invoke(
            {"query": "WITH c AS (SELECT id FROM t WHERE id > 3) SELECT * FROM c"}
        )

        assert 'Row 2: {"id": 5}' in out

    def test_same_query_twice(self, sql_query):
        first = sql_query.invoke({"query": "SELECT max(id) AS m FROM t"})
        second = sql_query.invoke({"query": "SELECT max(id) AS m FROM t"})

        assert first == second == 'Columns: m\n\nRow 1: {"m": 5}'

    def test_truncation_note(self, sql_query):
        out = sql_query.invoke({"query": "SELECT generate_series(1, 150) AS g"})

        assert out.endswith("... showing 100 of 150 rows")
