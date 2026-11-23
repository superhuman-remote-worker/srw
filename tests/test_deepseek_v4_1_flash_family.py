"""DeepSeek V4.1 Flash takes images; the rest of the V4 line does not.

V4.1 Flash (DeepSeek's API: ``deepseek-flash``; OpenRouter:
``deepseek/deepseek-v4.1-flash``) has its own family, ``deepseek-v4.1-flash``,
which is the ``deepseek`` entry plus vision. V4 Pro and the April/July V4
Flash checkpoints stay on ``deepseek``, text-only. Migration 0570 moves the
catalog rows and the administrator's overrides stored under the old family.
"""

from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
import yaml
from testcontainers.postgres import PostgresContainer

from orchestrator.services.discovery import _is_multimodal_chat
from orchestrator.services.family_matcher import detect_family
from shared.runtime.core import loader
from shared.runtime.core.image_tokens import estimate_image_block_tokens
from shared.runtime.core.model_registry import family_of

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_0570 = (
    ROOT
    / "src/orchestrator/database/migrations/app/0570_deepseek_v4_1_flash_family.sql"
)

VISION = "deepseek-v4.1-flash"
TEXT = "deepseek"

#: (name, family) — the names each provider uses, and near-misses.
NAMES = [
    ("deepseek-flash", VISION),  # DeepSeek's API, today's V4.1 Flash
    ("DeepSeek-Flash", VISION),
    ("deepseek-flash:free", VISION),
    ("deepseek-flash-api", VISION),  # catalog id with a served-name suffix
    ("deepseek-v4.1-flash", VISION),  # OpenRouter / Hugging Face
    ("DeepSeek-V4.1-Flash-1017", VISION),
    ("deepseek-v4-flash-vision-exp", VISION),  # retired preview, routed to 4.1
    ("deepseek-v4-flash", TEXT),  # OpenRouter's April checkpoint
    ("deepseek-v4-flash-0731", TEXT),
    ("deepseek-v4-pro", TEXT),
    ("deepseek-v4-pro-0813", TEXT),
    ("deepseek-chat", TEXT),
    ("deepseek-reasoner", TEXT),
    ("deepseek-flashy", TEXT),
    ("deepseek-v4.10-flash", TEXT),
    ("deepseek-flash.2", TEXT),
]


@pytest.mark.parametrize(
    "prefix", ["", "deepseek/", "openrouter/deepseek/", "deepseek-ai/"]
)
@pytest.mark.parametrize("model,family", NAMES)
def test_detectors_agree(prefix, model, family):
    assert family_of(prefix + model) == family
    assert detect_family(prefix + model).family == family


def test_discovery_tags_only_the_vision_model():
    assert _is_multimodal_chat("deepseek/deepseek-v4.1-flash")
    assert _is_multimodal_chat("deepseek-flash")
    assert not _is_multimodal_chat("deepseek/deepseek-v4-flash")
    assert not _is_multimodal_chat("deepseek/deepseek-v4-pro")


def _load(tmp_path, model, *, role="worker_base"):
    path = tmp_path / f"{role}-{model.replace('/', '_')}.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "$extends": role,
                "agent_id": "deepseek-test",
                "display_name": "DeepSeek Test",
                "llm": {"model": model},
            }
        )
    )
    return loader.load_agent_config(str(path))


@pytest.mark.parametrize("role", ["worker_base", "session_base"])
def test_flash_is_the_deepseek_entry_with_vision(tmp_path, role):
    pro = _load(tmp_path, "deepseek-v4-pro", role=role)
    flash = _load(tmp_path, "deepseek-flash", role=role)

    assert pro.llm.multimodal is False
    assert flash.llm.multimodal is True
    assert flash.limits.image_tokens == {"mode": "flat", "flat": 1024}
    assert pro.limits.image_tokens != flash.limits.image_tokens

    def rest(config):
        llm = {
            k: v
            for k, v in vars(config.llm).items()
            if k not in ("model", "multimodal")
        }
        limits = {k: v for k, v in vars(config.limits).items() if k != "image_tokens"}
        return llm, limits, config.extra

    assert rest(flash) == rest(pro)
    assert flash.llm.compaction == {"strategy": "native", "recipe": "deepseek"}
    assert flash.llm.replay_reasoning_content is True


@pytest.mark.parametrize(
    "name,filename",
    [
        ("systemprompt", "systemprompt_deepseek.txt"),
        ("systemprompt_interactive", "systemprompt_interactive_deepseek.txt"),
        ("persona", "persona_deepseek.txt"),
        ("memory_extraction", "memory_extraction_prompt_deepseek.txt"),
        ("curation", "curation_prompt_deepseek.txt"),
    ],
)
def test_flash_keeps_the_deepseek_prompts(name, filename):
    for family in (TEXT, VISION):
        resolver = loader.PromptMatrixResolver(model_family=family)
        assert resolver.resolve_filename(name) == filename


def test_flash_counts_each_image_at_the_measured_cap():
    matrix = yaml.safe_load((ROOT / "config/model_config_matrix.yaml").read_text())
    config = matrix[VISION]["settings"]["image_tokens"]
    block = {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}}
    assert estimate_image_block_tokens(block, config) == 1024
    assert matrix[TEXT]["settings"]["multimodal"] is False


# --- migration 0570, against real PostgreSQL -------------------------------


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def schema_applied(pg_dsn):
    # Its own connection: the dump empties search_path on the one it runs on.
    connection = await asyncpg.connect(pg_dsn)
    try:
        await connection.execute(
            (ROOT / "src/orchestrator/database/schema_current.sql").read_text()
        )
    finally:
        await connection.close()


@pytest_asyncio.fixture
async def conn(pg_dsn, schema_applied):
    connection = await asyncpg.connect(pg_dsn)
    try:
        await connection.execute("TRUNCATE models, config_overrides CASCADE")
        yield connection
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_0570_moves_the_rows_family_of_puts_on_vision(conn):
    rows = [
        ("deepseek-flash", None),
        ("deepseek-flash-api", "deepseek-flash"),
        ("openrouter/deepseek/deepseek-v4.1-flash", None),
        ("DeepSeek-V4.1-Flash-1017", None),
        ("deepseek-v4-flash-vision-exp", None),
        ("deepseek/deepseek-v4-flash", None),
        ("deepseek-v4-flash-0731", None),
        ("deepseek-v4-pro", None),
        ("deepseek-flashy", None),
        ("deepseek-v4.10-flash", None),
    ]
    for model_id, served in rows:
        await conn.execute(
            """
            INSERT INTO models (provider_kind, provider_ref, model_id, display_label,
                                capabilities, family, served_model_id)
            VALUES ('system', 'deepseek', $1, $1, '{chat}', 'deepseek', $2)
            """,
            model_id,
            served,
        )
    # A row someone already set to another family keeps it.
    await conn.execute(
        """
        INSERT INTO models (provider_kind, provider_ref, model_id, display_label,
                            capabilities, family)
        VALUES ('system', 'deepseek', 'deepseek-flash-custom', 'c', '{chat}', 'default')
        """
    )

    await conn.execute(MIGRATION_0570.read_text())

    families = {
        r["model_id"]: r["family"]
        for r in await conn.fetch("SELECT model_id, family FROM models")
    }
    for model_id, served in rows:
        assert families[model_id] == family_of(served or model_id), model_id
    assert families["deepseek-flash-custom"] == "default"
    assert sum(f == VISION for f in families.values()) == 5


@pytest.mark.asyncio
async def test_0570_copies_the_overrides_except_vision(conn):
    await conn.execute(
        """
        INSERT INTO config_overrides (family, kind, name, content, value_json)
        VALUES ('deepseek', 'prompts', 'persona', 'You are terse.', NULL),
               ('deepseek', 'settings', 'temperature', NULL, '0.3'),
               ('deepseek', 'settings', 'multimodal', NULL, 'false'),
               ('deepseek', 'settings', 'image_tokens.flat', NULL, '500'),
               ('deepseek', 'settings', 'parallel_tool_calls', NULL, 'true'),
               ('deepseek-v4.1-flash', 'settings', 'parallel_tool_calls', NULL, 'false'),
               (NULL, 'settings', 'top_p', NULL, '0.5'),
               ('glm', 'settings', 'temperature', NULL, '0.9')
        """
    )

    await conn.execute(MIGRATION_0570.read_text())

    copied = {
        (r["kind"], r["name"]): r["content"] or r["value_json"]
        for r in await conn.fetch(
            "SELECT kind, name, content, value_json::text FROM config_overrides"
            " WHERE family = 'deepseek-v4.1-flash'"
        )
    }
    assert copied == {
        ("prompts", "persona"): "You are terse.",
        ("settings", "temperature"): "0.3",
        # The flash override that already existed is kept.
        ("settings", "parallel_tool_calls"): "false",
    }
    assert await conn.fetchval("SELECT count(*) FROM config_overrides") == 10
