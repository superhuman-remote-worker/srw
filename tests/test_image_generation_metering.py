"""Metering generate_image calls (image generation WP4): one audit row per call
becomes one ``unit='image'`` usage event priced from the provider's token usage
at list price; subscription images are flagged (notional, decision D7); a row
without a list price or usage stays unpriced for an admin rate. Also the
contact sheet helper the tool and compose_images share."""

from __future__ import annotations

import asyncio
import io
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from PIL import Image

from agent.services.image_compose import (
    MAX_DECODE_PIXELS,
    ImageComposeError,
    SheetTile,
    contact_sheet,
    fit_reference,
    sniff_mime,
)
from orchestrator.services.audit_usage import materialize_llm_usage_from_audit
from shared.image_generation import image_list_price_usd
from tests.test_audit_usage import AppConn, AuditConn, FakeLedger, FakePool, _row

T0 = datetime(2026, 11, 17, 20, 0, tzinfo=timezone.utc)
JOB = "11111111-1111-1111-1111-111111111111"
THREAD = "22222222-2222-2222-2222-222222222222"


def test_list_price_from_tokens():
    # gpt-image-2.x: $30/M image out, $8/M image in, $5/M text in.
    cost = image_list_price_usd(
        "gpt-image-2.6-flare",
        {
            "input_text_tokens": 100,
            "input_image_tokens": 1000,
            "output_image_tokens": 6000,
        },
    )
    assert cost == pytest.approx(100 * 5e-6 + 1000 * 8e-6 + 6000 * 30e-6)
    # Gemini 2.1: 1,120 image tokens at $30/M = $0.0336 for a 1K image.
    assert image_list_price_usd(
        "gemini-nano-banana-2.1", {"output_image_tokens": 1120}
    ) == pytest.approx(0.0336)
    assert image_list_price_usd(
        "openai/gpt-image-1.5", {"output_image_tokens": 1000}
    ) == (pytest.approx(0.032))
    assert image_list_price_usd("flux-pro", {"output_image_tokens": 10}) is None
    assert image_list_price_usd("gpt-image-2", {}) is None


def _image_row(id, *, job_id, agent_type, meta, model="gpt-image-2.6-flare"):
    row = _row(
        id,
        job_id=job_id,
        agent_type=agent_type,
        call_type="image_generation",
        model=model,
        ts=T0,
    )
    row["image_meta"] = json.dumps(meta)
    return row


def _materialize(rows, *, jobs=None, threads=None):
    ledger = FakeLedger()
    asyncio.run(
        materialize_llm_usage_from_audit(
            FakePool(AuditConn(rows)),
            FakePool(AppConn(jobs=jobs or {}, threads=threads or {})),
            ledger,
            since_ts=T0 - timedelta(hours=1),
            min_age_s=0,
            now=T0 + timedelta(minutes=5),
        )
    )
    return ledger.events


def test_an_image_call_becomes_one_priced_image_event():
    meta = {
        "count": 2,
        "billing": "api",
        "provider": "openai-images",
        "quality": "high",
        "size": "1024x1024",
        "usage": {"output_image_tokens": 8000},
    }
    events = _materialize(
        [_image_row("a1", job_id=JOB, agent_type="universal", meta=meta)],
        jobs={JOB: ("u1", "p1")},
    )
    assert len(events) == 1
    event = events[0]
    assert (event.category, event.unit, event.quantity) == ("llm", "image", 2)
    assert event.ref_kind == "job" and event.ref_id == JOB
    assert event.user_id == "u1"
    assert event.cost_usd == Decimal("0.24")
    assert event.rate_usd == Decimal("0.12")
    assert event.details["billing"] == "api"
    assert event.details["priced_from"] == "list_price_tokens"


def test_a_subscription_session_image_is_flagged_and_attributed_to_the_thread():
    meta = {
        "count": 1,
        "billing": "subscription",
        "usage": {"output_image_tokens": 1000},
    }
    events = _materialize(
        [
            _image_row(
                "b1",
                job_id=THREAD,
                agent_type="persistent",
                meta=meta,
                model="gpt-image-2",
            )
        ],
        threads={THREAD: ("u2", None)},
    )
    assert events[0].ref_kind == "thread"
    assert events[0].details["billing"] == "subscription"
    assert events[0].cost_usd == Decimal("0.03")


def test_without_usage_or_list_price_the_event_stays_unpriced():
    events = _materialize(
        [
            _image_row("c1", job_id=JOB, agent_type="universal", meta={"count": 1}),
            _image_row(
                "c2",
                job_id=JOB,
                agent_type="universal",
                meta={"count": 1, "usage": {"output_image_tokens": 5}},
                model="flux-pro",
            ),
        ]
    )
    assert [e.cost_usd for e in events] == [None, None]
    assert all(e.unit == "image" for e in events)


def test_an_image_row_is_never_metered_as_tokens():
    row = _image_row("d1", job_id=JOB, agent_type="universal", meta={"count": 1})
    row["m_prompt"] = "500"
    row["m_completion"] = "800"
    events = _materialize([row])
    assert [e.unit for e in events] == ["image"]


# --- contact sheets -------------------------------------------------------------


def _png(color, size=(40, 30)):
    out = io.BytesIO()
    Image.new("RGB", size, color).save(out, format="PNG")
    return out.getvalue()


def test_contact_sheet_is_a_numbered_grid():
    tiles = [SheetTile(_png((i * 40, 0, 0)), f"img-{i}.png") for i in range(5)]
    sheet = Image.open(io.BytesIO(contact_sheet(tiles, cell=128)))
    # 5 tiles → 3 columns × 2 rows of 128 px cells plus captions and padding.
    assert sheet.width > 3 * 128 and sheet.height > 2 * 128
    with pytest.raises(ImageComposeError):
        contact_sheet([])


def test_decoding_is_capped(monkeypatch):
    import agent.services.image_compose as compose

    monkeypatch.setattr(compose, "MAX_DECODE_PIXELS", 100)
    with pytest.raises(ImageComposeError, match="pixel limit"):
        contact_sheet([SheetTile(_png((0, 0, 0), size=(20, 20)), "big")])
    assert MAX_DECODE_PIXELS == 40_000_000


def test_fit_reference_and_sniffing():
    data, mime = fit_reference(_png((1, 2, 3), size=(4000, 1000)), max_edge=1000)
    assert mime == "image/jpeg"
    assert max(Image.open(io.BytesIO(data)).size) == 1000
    assert sniff_mime(_png((0, 0, 0))) == "image/png"
    assert sniff_mime(b"<svg") is None
