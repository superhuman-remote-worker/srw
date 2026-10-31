"""mongo_aggregate refuses pipelines with write stages before reaching MongoDB."""

import json
from types import SimpleNamespace

import pytest

from agent.tools.mongodb.mongo import create_mongo_tools


class _FakeCollection:
    def __init__(self):
        self.pipelines = []

    def aggregate(self, pipeline):
        self.pipelines.append(pipeline)
        return iter([{"_id": "a", "n": 2}])


class _FakeDb:
    def __init__(self):
        self.collection = _FakeCollection()

    def __getitem__(self, name):
        return self.collection


@pytest.fixture
def db():
    return _FakeDb()


@pytest.fixture
def mongo_aggregate(db):
    tools = create_mongo_tools(SimpleNamespace(connection_for=lambda _: db))
    return {t.name: t for t in tools}["mongo_aggregate"]


def _run(tool, pipeline):
    return tool.invoke({"collection": "c", "pipeline": json.dumps(pipeline)})


@pytest.mark.parametrize(
    ("pipeline", "stage"),
    [
        ([{"$match": {}}, {"$out": "copy"}], "$out"),
        ([{"$merge": {"into": "copy"}}], "$merge"),
        ([{"$facet": {"x": [{"$out": "copy"}]}}], "$out"),
        (
            [{"$lookup": {"from": "o", "as": "j", "pipeline": [{"$merge": "copy"}]}}],
            "$merge",
        ),
        ([{"$unionWith": {"coll": "o", "pipeline": [{"$out": "copy"}]}}], "$out"),
    ],
)
def test_write_stage_is_refused_before_mongo_sees_it(
    mongo_aggregate, db, pipeline, stage
):
    out = _run(mongo_aggregate, pipeline)

    assert out.startswith(f"Error: {stage} writes to the database")
    assert db.collection.pipelines == []


def test_read_pipeline_runs(mongo_aggregate, db):
    pipeline = [
        {"$match": {"status": "active"}},
        {"$group": {"_id": "$type", "n": {"$sum": 1}}},
    ]

    out = _run(mongo_aggregate, pipeline)

    assert db.collection.pipelines == [pipeline]
    assert out.startswith("Aggregation results (1 total):")


def test_out_as_a_string_value_is_not_a_stage(mongo_aggregate, db):
    pipeline = [{"$match": {"note": "$out"}}]

    _run(mongo_aggregate, pipeline)

    assert db.collection.pipelines == [pipeline]
