"""The WP0 bench profiles: what each one imitates and what it asserts.

A profile is one phone call: the caller dials the profile's business number
on the bench PBX, the PBX rings the AI extension through the profile's
endpoint (``pbx/pjsip.conf.in``), and the checks below read what happened from
the SIP messages at the device under test (DUT), the parties' event logs and
the transcripts of their recordings.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from tests.voice_bench.analysis import (
    SipMessage,
    final_status,
    initial_invite,
    missing_keywords,
    response_to,
)

DTMF_DIGITS = "2580#"
TRANSFER_DIGIT = "9"
HUMAN_EXTENSION = "3000"


@dataclass
class Check:
    name: str
    ok: bool | None  # None = skipped (not observable in this mode)
    detail: str = ""

    def as_dict(self) -> dict:
        status = "skip" if self.ok is None else ("pass" if self.ok else "fail")
        return {"name": self.name, "status": status, "detail": self.detail}


@dataclass
class Window:
    """A stretch of one party's recording to transcribe and search."""

    role: str
    start: float | None  # wall clock; None = start of the call
    end: float | None  # wall clock; None = end of the call
    keywords: list[str]
    proves: str


@dataclass
class CallData:
    profile: Profile
    out: Path
    standin: bool
    events: dict[str, list[dict]]
    summary: dict[str, dict]
    dut_msgs: list[SipMessage]
    keywords: dict[str, list[str]]  # role -> keywords of that party's own WAV
    heard_from_dut: list[str]  # what the caller must hear from the DUT
    contacts_before: str = ""
    # standin | external | srw (SRW's addon + gateway; ``driver`` is the
    # harness's log of what SRW's side saw through the gateway's API)
    dut_mode: str = "standin"
    driver: list[dict] = field(default_factory=list)
    windows: dict[str, Window] = field(default_factory=dict)
    transcripts: dict[str, str] = field(default_factory=dict)
    audio: dict[str, dict] = field(default_factory=dict)  # window -> level stats
    observed: dict = field(default_factory=dict)

    def first_event(self, role: str, kind: str, **match) -> dict | None:
        for event in self.events.get(role, []):
            if event.get("type") == kind and all(
                event.get(k) == v for k, v in match.items()
            ):
                return event
        return None

    def t_confirmed(self, role: str) -> float | None:
        event = self.first_event(role, "call_state", state="CONFIRMED")
        return event["t"] if event else None

    def t_disconnected(self, role: str) -> float | None:
        event = self.first_event(role, "call_state", state="DISCONNECTED")
        return event["t"] if event else None

    def driver_seen(self, kind: str, **match) -> list[dict]:
        return [
            e
            for e in self.driver
            if e.get("kind") == kind and all(e.get(k) == v for k, v in match.items())
        ]


@dataclass
class Profile:
    name: str
    number: str
    imitates: str
    hangup_after: float = 12.0
    caller_ext: str = "1000"
    dut_registers: bool = True
    slow: bool = False
    caller_steps: list[dict] = field(default_factory=list)
    transfer_on_dtmf: bool = False
    approximation: str | None = None
    checks: Callable[[CallData], list[Check]] | None = None
    windows: Callable[[CallData], dict[str, Window]] | None = None


# -- shared checks ---------------------------------------------------------------------


def _audio_check(data: CallData, window: str, label: str) -> Check:
    w = data.windows.get(window)
    if w is None:
        return Check(label, False, f"window {window} could not be placed")
    text = data.transcripts.get(window, "")
    stats = data.audio.get(window, {})
    missing = missing_keywords(text, w.keywords)
    voiced = stats.get("voiced", 0.0)
    ok = not missing and voiced >= 0.15
    detail = f"voiced {voiced:.0%}, transcript: {text[:160]!r}"
    if missing:
        detail = f"missing {missing}; " + detail
    return Check(label, ok, detail)


def common_checks(data: CallData) -> list[Check]:
    p = data.profile
    checks = []
    t_conf = data.t_confirmed("caller")
    t_disc = data.t_disconnected("caller")
    invite = initial_invite(data.dut_msgs)
    if data.standin:
        dut_ok = data.t_confirmed("dut") is not None
    else:
        dut_ok = invite is not None and final_status(data.dut_msgs, invite) == 200
    checks.append(
        Check(
            "call_established",
            t_conf is not None and dut_ok,
            f"caller confirmed={t_conf is not None}, DUT answered={dut_ok}",
        )
    )
    if t_conf and t_disc:
        lasted = t_disc - t_conf
        hung_up = data.first_event("caller", "command", do="hangup") is not None
        checks.append(
            Check(
                "call_lasted",
                lasted >= p.hangup_after - 1.0 and hung_up,
                f"{lasted:.1f} s, ended by the caller's scheduled hangup={hung_up}",
            )
        )
    else:
        checks.append(
            Check("call_lasted", False, "no confirmed/disconnected pair on the caller")
        )
    if "caller_full" in data.windows:
        checks.append(_audio_check(data, "caller_full", "caller_hears_dut"))
    if data.standin:
        checks.append(_audio_check(data, "dut_full", "dut_hears_caller"))
    elif data.dut_mode == "srw":
        checks.append(
            Check(
                "dut_hears_caller",
                None,
                "SRW keeps no audio; the caller hearing its own words proves it",
            )
        )
        checks += gateway_checks(data)
    else:
        checks.append(
            Check("dut_hears_caller", None, "external device: check its own transcript")
        )
    return checks


def gateway_checks(data: CallData) -> list[Check]:
    """SRW's side of the call, from the driver harness's log (``--dut srw``)."""
    connector = "bench-peer" if not data.profile.dut_registers else "bench-register"
    hooks = data.driver_seen("webhook")
    names = {h["name"] for hook in hooks for h in hook.get("sip_headers", [])}
    accepted = data.driver_seen("accepted", status=200)
    started = data.driver_seen("event", event="session.started")
    closed = data.driver_seen("event", event="session.closed")
    reasons = [e.get("reason") for e in closed]
    data.observed["gateway"] = {
        "webhooks": [h["connector"] for h in hooks],
        "headers": sorted(names),
        "closed": reasons,
    }
    return [
        Check(
            "gateway_webhook",
            len(hooks) == 1
            and hooks[0]["connector"] == connector
            and {"From", "To"} <= names,
            f"{len(hooks)} signed webhook(s) for {[h['connector'] for h in hooks]}, "
            f"headers {sorted(names)}",
        ),
        Check(
            "gateway_session",
            len(accepted) == 1 and len(started) == 1,
            f"accepted {len(accepted)}, session.started {len(started)}",
        ),
        Check(
            "gateway_closed",
            reasons == ["remote_hangup"],
            f"session.closed reasons {reasons}",
        ),
    ]


def _driver_digits(data: CallData) -> str:
    return "".join(
        e.get("digit") or ""
        for e in data.driver_seen("event", event="transport.dtmf.received")
    )


def default_windows(data: CallData) -> dict[str, Window]:
    windows = {
        "caller_full": Window(
            "caller", None, None, data.heard_from_dut, "caller hears the DUT"
        )
    }
    if data.standin:
        windows["dut_full"] = Window(
            "dut", None, None, data.keywords["caller"], "DUT hears the caller"
        )
    return windows


# -- per-profile checks ----------------------------------------------------------------


def _sdp_of_invite(data: CallData):
    invite = initial_invite(data.dut_msgs)
    return invite, (invite.sdp() if invite else None)


def check_register(data: CallData) -> list[Check]:
    msgs = data.dut_msgs
    regs = [
        m
        for m in msgs
        if m.is_request and m.method == "REGISTER" and m.direction == "tx"
    ]
    challenged = any(
        m.status == 401 and "digest" in m.first("WWW-Authenticate").lower()
        for r in regs
        for m in response_to(msgs, r)
    )
    authorized = [
        r for r in regs if r.first("Authorization").lower().startswith("digest")
    ]
    accepted = any(final_status(msgs, r) == 200 for r in authorized)
    if data.standin or regs:
        ok = challenged and accepted
        detail = f"REGISTER challenged with Digest={challenged}, accepted with credentials={accepted}"
    else:
        ok = "2000/" in data.contacts_before
        detail = "registration predates the call; PBX lists contact 2000: " + str(ok)
    return [Check("registered_with_digest", ok, detail)]


def check_peer(data: CallData) -> list[Check]:
    regs = [m for m in data.dut_msgs if m.is_request and m.method == "REGISTER"]
    invite = initial_invite(data.dut_msgs)
    checks = []
    if data.standin:
        checks.append(
            Check(
                "no_registration",
                not regs,
                f"{len(regs)} REGISTER request(s) from the DUT",
            )
        )
    else:
        checks.append(
            Check(
                "no_registration",
                None,
                "external device may also register; not asserted",
            )
        )
    target = invite.start.split(" ")[1] if invite else ""
    checks.append(
        Check("invite_to_peer_address", bool(invite), f"Request-URI {target}")
    )
    return checks


def check_alaw(data: CallData) -> list[Check]:
    invite, offer = _sdp_of_invite(data)
    answer = None
    if invite:
        for resp in response_to(data.dut_msgs, invite):
            if resp.status == 200 and resp.sdp():
                answer = resp.sdp()
    data.observed["offer"] = offer.codecs if offer else None
    data.observed["answer"] = answer.codecs if answer else None
    return [
        Check(
            "offer_alaw_only",
            bool(offer) and offer.voice_codecs == ["PCMA"],
            f"offered {offer.codecs if offer else None}",
        ),
        Check(
            "answer_alaw",
            bool(answer) and answer.voice_codecs == ["PCMA"],
            f"answered {answer.codecs if answer else None}",
        ),
    ]


def _dtmf_digits(data: CallData) -> list[dict]:
    return [e for e in data.events.get("dut", []) if e.get("type") == "dtmf"]


def _info_requests(data: CallData) -> list[SipMessage]:
    return [
        m
        for m in data.dut_msgs
        if m.is_request
        and m.method == "INFO"
        and m.direction == "rx"
        and "dtmf" in m.first("Content-Type").lower()
    ]


def _signal(msg: SipMessage) -> str:
    for line in msg.body.splitlines():
        key, _, value = line.partition("=")
        if key.strip().lower() == "signal":
            return value.strip()
    return ""


def check_dtmf_info(data: CallData) -> list[Check]:
    invite, offer = _sdp_of_invite(data)
    infos = _info_requests(data)
    signals = "".join(_signal(m) for m in infos)
    answered = all(final_status(data.dut_msgs, m) == 200 for m in infos)
    data.observed["info_signals"] = signals
    checks = [
        Check(
            "no_telephone_event_offered",
            bool(offer) and not offer.telephone_event,
            f"offered {offer.codecs if offer else None}",
        ),
        Check(
            "dtmf_as_sip_info",
            signals == DTMF_DIGITS and answered,
            f"INFO signals {signals!r} (sent {DTMF_DIGITS!r}), all answered 200={answered}",
        ),
    ]
    if data.standin:
        seen = _dtmf_digits(data)
        digits = "".join(e["digit"] for e in seen)
        methods = sorted({e["method"] for e in seen})
        data.observed["dtmf_seen"] = digits
        checks.append(
            Check(
                "dut_saw_digits",
                digits == DTMF_DIGITS and methods == ["info"],
                f"DUT saw {digits!r} via {methods}",
            )
        )
    elif data.dut_mode == "srw":
        digits = _driver_digits(data)
        data.observed["dtmf_seen"] = digits
        checks.append(
            Check(
                "dut_saw_digits",
                digits == DTMF_DIGITS,
                f"transport.dtmf.received at SRW's driver: {digits!r}",
            )
        )
    else:
        checks.append(
            Check("dut_saw_digits", None, "external device: check its own DTMF log")
        )
    return checks


def check_dtmf_rfc4733(data: CallData) -> list[Check]:
    invite, offer = _sdp_of_invite(data)
    infos = _info_requests(data)
    checks = [
        Check(
            "telephone_event_offered",
            bool(offer) and offer.telephone_event,
            f"offered {offer.codecs if offer else None}",
        ),
        Check("no_sip_info", not infos, f"{len(infos)} INFO request(s)"),
    ]
    if data.standin:
        seen = _dtmf_digits(data)
        digits = "".join(e["digit"] for e in seen)
        methods = sorted({e["method"] for e in seen})
        data.observed["dtmf_seen"] = digits
        checks.append(
            Check(
                "dut_saw_digits",
                digits == DTMF_DIGITS and methods == ["rfc4733"],
                f"DUT saw {digits!r} via {methods}",
            )
        )
    elif data.dut_mode == "srw":
        digits = _driver_digits(data)
        data.observed["dtmf_seen"] = digits
        checks.append(
            Check(
                "dut_saw_digits",
                digits == DTMF_DIGITS,
                f"transport.dtmf.received at SRW's driver: {digits!r}",
            )
        )
    else:
        checks.append(
            Check(
                "dut_saw_digits",
                None,
                "RFC 4733 events travel in RTP; check the device's log",
            )
        )
    return checks


def _reinvites(data: CallData) -> list[SipMessage]:
    """In-dialog INVITEs with SDP the DUT received while the call was up."""
    invite = initial_invite(data.dut_msgs)
    t_disc = data.t_disconnected("caller")
    if not invite:
        return []
    return [
        m
        for m in data.dut_msgs
        if m.is_request
        and m.method == "INVITE"
        and m.direction == "rx"
        and m.call_id == invite.call_id
        and m.has_to_tag
        and m.sdp() is not None
        and (t_disc is None or m.t < t_disc - 0.5)
    ]


def windows_reinvite(data: CallData) -> dict[str, Window]:
    windows = default_windows(data)
    re_invites = _reinvites(data)
    if re_invites:
        start = re_invites[-1].t + 1.0
        windows["caller_after_reinvite"] = Window(
            "caller",
            start,
            None,
            data.heard_from_dut,
            "caller hears the DUT after the last re-INVITE",
        )
        if data.standin:
            windows["dut_after_reinvite"] = Window(
                "dut",
                start,
                None,
                data.keywords["caller"],
                "DUT hears the caller after the last re-INVITE",
            )
    return windows


def check_reinvite(data: CallData) -> list[Check]:
    invite, offer = _sdp_of_invite(data)
    re_invites = _reinvites(data)
    t_conf = data.t_confirmed("caller") or (re_invites[0].t if re_invites else 0.0)
    timeline = [
        {
            "after_s": round(m.t - t_conf, 1),
            "media": m.sdp().media_target,
            "status": final_status(data.dut_msgs, m),
        }
        for m in re_invites
    ]
    data.observed["initial_media"] = offer.media_target if offer else None
    data.observed["reinvites"] = timeline
    checks = [
        Check(
            "reinvites_mid_call",
            len(re_invites) >= 2,
            f"{len(re_invites)} in-dialog INVITE(s) with SDP before the hangup",
        )
    ]
    if not re_invites or not offer:
        return checks
    targets = [t["media"] for t in timeline]
    moved_away = any(t != offer.media_target for t in targets)
    pulled_back = any(
        before != offer.media_target and after == offer.media_target
        for before, after in zip(targets, targets[1:])
    )
    checks += [
        Check(
            "media_reanchored",
            moved_away and pulled_back,
            f"{offer.media_target} -> {targets} (moved away={moved_away}, pulled back through the PBX={pulled_back})",
        ),
        Check(
            "reinvites_accepted",
            all(t["status"] == 200 for t in timeline),
            f"final responses {[t['status'] for t in timeline]}",
        ),
        _audio_check(data, "caller_after_reinvite", "caller_hears_dut_after_reinvite"),
    ]
    if data.standin:
        checks.append(
            _audio_check(data, "dut_after_reinvite", "dut_hears_caller_after_reinvite")
        )
    return checks


def _refreshes(data: CallData) -> list[SipMessage]:
    invite = initial_invite(data.dut_msgs)
    if not invite:
        return []
    return [
        m
        for m in data.dut_msgs
        if m.is_request
        and m.method in ("INVITE", "UPDATE")
        and m.call_id == invite.call_id
        and m.has_to_tag
        and m.first("Session-Expires")
    ]


def windows_session_timer(data: CallData) -> dict[str, Window]:
    windows = default_windows(data)
    t_disc = data.t_disconnected("caller")
    if t_disc:
        windows["caller_tail"] = Window(
            "caller",
            t_disc - 10.0,
            t_disc,
            data.heard_from_dut,
            "caller hears the DUT after 90 s",
        )
    return windows


def check_session_timer(data: CallData) -> list[Check]:
    msgs = data.dut_msgs
    invite = initial_invite(msgs)
    offered = invite.first("Session-Expires") if invite else ""
    agreed = ""
    if invite:
        for resp in response_to(msgs, invite):
            if resp.status == 200:
                agreed = resp.first("Session-Expires")
    t_conf = data.t_confirmed("caller")
    t_disc = data.t_disconnected("caller")
    refreshes = _refreshes(data)
    in_time = [m for m in refreshes if t_conf and 30.0 <= m.t - t_conf <= 89.0]
    data.observed["session_expires"] = {"offered": offered, "agreed": agreed}
    data.observed["refreshes"] = [
        {
            "method": m.method,
            "by": "PBX" if m.direction == "rx" else "DUT",
            "after_s": round(m.t - t_conf, 1) if t_conf else None,
            "status": final_status(msgs, m),
        }
        for m in refreshes
    ]
    early_bye = [
        m
        for m in msgs
        if m.is_request
        and m.method == "BYE"
        and t_disc is not None
        and m.t < t_disc - 0.5
        and m.direction == "rx"
    ]
    lasted = (t_disc - t_conf) if (t_conf and t_disc) else 0.0
    return [
        Check(
            "timer_requested",
            offered.split(";")[0].strip() == "90",
            f"INVITE Session-Expires: {offered!r}",
        ),
        Check(
            "timer_agreed",
            agreed.split(";")[0].strip() == "90",
            f"200 OK Session-Expires: {agreed!r}",
        ),
        Check(
            "refresh_seen",
            bool(in_time) and all(final_status(msgs, m) == 200 for m in in_time),
            f"refreshes {data.observed['refreshes']}",
        ),
        Check(
            "call_survived_timer",
            lasted >= 95.0 and not early_bye,
            f"call lasted {lasted:.1f} s; BYE before the caller's hangup: {len(early_bye)}",
        ),
        _audio_check(data, "caller_tail", "caller_hears_dut_after_90s"),
    ]


def _refer(data: CallData) -> SipMessage | None:
    for m in data.dut_msgs:
        if m.is_request and m.method == "REFER" and m.direction == "tx":
            return m
    return None


def windows_refer_accept(data: CallData) -> dict[str, Window]:
    windows = {}
    refer = _refer(data)
    t_human = data.t_confirmed("human")
    if refer:
        windows["caller_before_transfer"] = Window(
            "caller",
            None,
            refer.t,
            data.heard_from_dut,
            "caller hears the DUT before the transfer",
        )
    if t_human:
        windows["caller_after_transfer"] = Window(
            "caller",
            t_human + 0.5,
            None,
            data.keywords["human"],
            "caller hears the human after the transfer",
        )
        windows["human_full"] = Window(
            "human", None, None, data.keywords["caller"], "human hears the caller"
        )
    if data.standin:
        windows["dut_full"] = Window(
            "dut", None, None, data.keywords["caller"], "DUT hears the caller"
        )
    return windows


def check_refer_accept(data: CallData) -> list[Check]:
    msgs = data.dut_msgs
    refer = _refer(data)
    checks = [
        Check(
            "refer_sent",
            bool(refer),
            f"Refer-To: {refer.first('Refer-To') if refer else None}",
        )
    ]
    if not refer:
        return checks
    code = final_status(msgs, refer)
    notifies = [
        m
        for m in msgs
        if m.is_request
        and m.method == "NOTIFY"
        and m.direction == "rx"
        and m.call_id == refer.call_id
    ]
    fragments = [m.body.splitlines()[0] for m in notifies if m.body]
    data.observed["refer_response"] = code
    data.observed["notify_sipfrag"] = fragments
    t_human = data.t_confirmed("human")
    checks += [
        Check(
            "refer_target_is_human",
            HUMAN_EXTENSION in refer.first("Refer-To"),
            refer.first("Refer-To"),
        ),
        Check("refer_accepted", code == 202, f"REFER answered {code}"),
        Check(
            "transfer_completed",
            any(f.startswith("SIP/2.0 200") for f in fragments),
            f"NOTIFY sipfrag {fragments}",
        ),
        Check(
            "human_answered",
            t_human is not None,
            "human extension confirmed the call" if t_human else "no call",
        ),
        _audio_check(
            data, "caller_before_transfer", "caller_hears_dut_before_transfer"
        ),
        _audio_check(
            data, "caller_after_transfer", "caller_hears_human_after_transfer"
        ),
        _audio_check(data, "human_full", "human_hears_caller"),
    ]
    if data.standin:
        checks.append(_audio_check(data, "dut_full", "dut_hears_caller"))
    if data.dut_mode == "srw":
        checks.append(_driver_referred(data))
    return checks


def _driver_referred(data: CallData) -> Check:
    referred = data.driver_seen("referred")
    return Check(
        "driver_referred",
        [e.get("status") for e in referred] == [200],
        f"refer requests from SRW's driver answered {[e.get('status') for e in referred]}",
    )


def _fallback_invites(data: CallData) -> list[SipMessage]:
    """The device's own INVITEs to the human extension (a new dialog; the
    first is usually challenged, the retry carries credentials)."""
    return [
        m
        for m in data.dut_msgs
        if m.is_request
        and m.method == "INVITE"
        and m.direction == "tx"
        and not m.has_to_tag
        and f"sip:{HUMAN_EXTENSION}@" in m.start
    ]


def windows_refer_refuse(data: CallData) -> dict[str, Window]:
    windows = default_windows(data)
    refer = _refer(data)
    t_human = data.t_confirmed("human")
    if data.dut_mode == "srw" and t_human:
        # SRW dials the target itself once the PBX refuses (§4.2 step 7).
        windows["caller_after_fallback"] = Window(
            "caller",
            t_human + 0.5,
            None,
            data.keywords["human"],
            "caller hears the human after the fallback transfer",
        )
        restart = _gateway_restart(data)
        if restart.get("finished"):
            windows["caller_after_restart"] = Window(
                "caller",
                restart["finished"] + 0.5,
                None,
                data.keywords["human"],
                "caller still hears the human after the gateway restarted",
            )
        windows["human_full"] = Window(
            "human", None, None, data.keywords["caller"], "human hears the caller"
        )
    if refer:
        refusal = [
            m for m in response_to(data.dut_msgs, refer) if m.status and m.status >= 300
        ]
        start = (refusal[-1].t if refusal else refer.t) + 0.5
        windows["caller_after_refusal"] = Window(
            "caller",
            start,
            None,
            data.heard_from_dut,
            "caller still hears the DUT after the refusal",
        )
    return windows


def _gateway_restart(data: CallData) -> dict:
    """What the bench's gateway restart did during this call (``--dut srw``)."""
    path = data.out / "gateway_restart.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _restart_checks(data: CallData, t_human: float | None) -> list[Check]:
    """A handed-over call lives in the addon's dialplan, outside the
    gateway's app: restarting the gateway (a rollout) must not drop it."""
    restart = _gateway_restart(data)
    if not restart:
        return []
    t_disc = data.t_disconnected("caller")
    t_conf = data.t_confirmed("caller")
    scheduled = t_conf + data.profile.hangup_after if t_conf else None
    ok = (
        restart.get("ok")
        and t_human is not None
        and restart["started"] > t_human
        and t_disc is not None
        and t_disc > restart["finished"]
        and scheduled is not None
        and t_disc >= scheduled - 1.0
    )
    detail = (
        restart.get("error")
        or f"restart {restart['started'] - t_human:.1f}-{restart['finished'] - t_human:.1f} s "
        f"after the human answered; caller hung up "
        f"{(t_disc or 0) - (t_conf or 0):.1f} s into the call "
        f"(scheduled {data.profile.hangup_after:.0f} s)"
        if t_human
        else "the human never answered"
    )
    return [
        Check("handed_over_call_survives_gateway_restart", bool(ok), detail),
        _audio_check(data, "caller_after_restart", "caller_hears_human_after_restart"),
    ]


def check_refer_refuse(data: CallData) -> list[Check]:
    msgs = data.dut_msgs
    refer = _refer(data)
    checks = [
        Check(
            "refer_sent",
            bool(refer),
            f"Refer-To: {refer.first('Refer-To') if refer else None}",
        )
    ]
    if not refer:
        return checks
    responses = [m for m in response_to(msgs, refer) if m.status and m.status >= 200]
    code = responses[-1].status if responses else None
    reason = responses[-1].start.split(" ", 2)[-1] if responses else ""
    data.observed["refer_response"] = f"{code} {reason}".strip()
    t_disc = data.t_disconnected("caller")
    early_bye = [
        m
        for m in msgs
        if m.is_request and m.method == "BYE" and t_disc and m.t < t_disc - 0.5
    ]
    human_called = data.first_event("human", "call_state") is not None
    checks += [
        Check(
            "refer_refused",
            bool(code) and code >= 400,
            f"REFER answered {code} {reason}",
        ),
        Check(
            "call_stays_up",
            not early_bye,
            f"BYE before the caller's hangup: {len(early_bye)}",
        ),
    ]
    if data.dut_mode != "srw":
        checks += [
            Check(
                "human_not_called",
                not human_called,
                "human extension rang" if human_called else "human untouched",
            ),
            _audio_check(
                data, "caller_after_refusal", "caller_hears_dut_after_refusal"
            ),
        ]
        return checks
    # SRW falls back to dialling the target through its own line and bridging
    # it with the caller, who never leaves (local_voice_gateway.md §4.2).
    statuses = [final_status(data.dut_msgs, m) for m in _fallback_invites(data)]
    t_human = data.t_confirmed("human")
    data.observed["fallback_invites"] = statuses
    checks += [
        _driver_referred(data),
        Check(
            "fallback_dialled",
            200 in statuses,
            f"INVITEs from the device to {HUMAN_EXTENSION} answered {statuses}",
        ),
        Check(
            "human_answered",
            t_human is not None,
            "human extension confirmed the call" if t_human else "no call",
        ),
        _audio_check(
            data, "caller_after_fallback", "caller_hears_human_after_fallback"
        ),
        _audio_check(data, "human_full", "human_hears_caller"),
    ]
    checks += _restart_checks(data, t_human)
    return checks


def _webhook_headers(data: CallData, name: str) -> list[str]:
    return [
        h["value"]
        for hook in data.driver_seen("webhook")
        for h in hook.get("sip_headers", [])
        if h["name"].lower() == name.lower()
    ]


def _diversion_checks(data: CallData, expect: bool, number: str) -> list[Check]:
    checks = _diversion_sip_checks(data, expect, number)
    if data.dut_mode == "srw":
        diversion = _webhook_headers(data, "Diversion")
        history = _webhook_headers(data, "History-Info")
        found = any(number in d for d in diversion) and any(
            number in h for h in history
        )
        checks.append(
            Check(
                "webhook_carries_diversion" if expect else "webhook_without_diversion",
                found if expect else not (diversion or history),
                f"webhook sip_headers: Diversion {diversion}, History-Info {history}",
            )
        )
    return checks


def _diversion_sip_checks(data: CallData, expect: bool, number: str) -> list[Check]:
    invite = initial_invite(data.dut_msgs)
    diversion = invite.header("Diversion") if invite else []
    history = invite.header("History-Info") if invite else []
    data.observed["headers"] = {
        "Diversion": diversion,
        "History-Info": history,
        "To": invite.first("To") if invite else None,
        "From": invite.first("From") if invite else None,
    }
    if expect:
        return [
            Check(
                "diversion_present",
                any(number in d for d in diversion),
                f"Diversion: {diversion}",
            ),
            Check(
                "history_info_present",
                any(number in h for h in history),
                f"History-Info: {history}",
            ),
        ]
    return [
        Check(
            "diversion_absent",
            bool(invite) and not diversion,
            f"Diversion: {diversion}",
        ),
        Check(
            "history_info_absent",
            bool(invite) and not history,
            f"History-Info: {history}",
        ),
    ]


PROFILES: dict[str, Profile] = {
    p.name: p
    for p in (
        Profile(
            name="register",
            number="100",
            imitates="The DUT registers as extension 2000 with digest auth (any PBX); G.722, A-law, u-law offered.",
            checks=check_register,
        ),
        Profile(
            name="peer",
            number="101",
            imitates="The PBX sends the call to the DUT by IP without registration (IP peer/trunk to the device).",
            dut_registers=False,
            checks=check_peer,
        ),
        Profile(
            name="alaw-only",
            number="102",
            imitates="The PBX offers G.711 A-law only (typical German PBX trunk/extension setting).",
            checks=check_alaw,
        ),
        Profile(
            name="dtmf-info",
            number="103",
            imitates="DTMF reaches the DUT as SIP INFO only, no telephone-event in SDP (Swyx registered devices).",
            caller_steps=[
                {"at": 4.0, "do": "dtmf", "method": "rfc2833", "digits": DTMF_DIGITS}
            ],
            checks=check_dtmf_info,
        ),
        Profile(
            name="dtmf-rfc4733",
            number="104",
            imitates="DTMF reaches the DUT as RFC 2833/4733 telephone-events only (OpenScape Business).",
            caller_steps=[
                {"at": 4.0, "do": "dtmf", "method": "rfc2833", "digits": DTMF_DIGITS}
            ],
            checks=check_dtmf_rfc4733,
        ),
        Profile(
            name="reinvite",
            number="105",
            imitates="Mid-call re-INVITEs that move the DUT's media away and back through the PBX (AGFEO).",
            caller_ext="1001",
            hangup_after=16.0,
            caller_steps=[{"at": 5.0, "do": "hold"}, {"at": 8.0, "do": "unhold"}],
            approximation=(
                "Asterisk moves a call's media only through direct media. After answer the PBX re-INVITEs the "
                "DUT to send RTP straight to the caller; the caller then holds, so the PBX re-INVITEs again and "
                "pulls the media back through itself (what AGFEO does), and re-anchors after unhold. A PBX that "
                "pulls media back on its own, without a hold, cannot be configured in Asterisk."
            ),
            checks=check_reinvite,
            windows=windows_reinvite,
        ),
        Profile(
            name="session-timer",
            number="106",
            imitates="90 s session timer (RFC 4028) on the call; a refresh re-INVITE/UPDATE must keep it alive.",
            hangup_after=100.0,
            slow=True,
            checks=check_session_timer,
            windows=windows_session_timer,
        ),
        Profile(
            name="refer-accept",
            number="107",
            imitates="The DUT blind-transfers (REFER) to the human extension 3000 and the PBX carries it out.",
            hangup_after=22.0,
            caller_steps=[
                {"at": 6.5, "do": "dtmf", "method": "rfc2833", "digits": TRANSFER_DIGIT}
            ],
            transfer_on_dtmf=True,
            checks=check_refer_accept,
            windows=windows_refer_accept,
        ),
        Profile(
            name="refer-refuse",
            number="108",
            imitates="The DUT blind-transfers (REFER) and the PBX refuses it; the call must stay up.",
            hangup_after=18.0,
            caller_steps=[
                {"at": 6.5, "do": "dtmf", "method": "rfc2833", "digits": TRANSFER_DIGIT}
            ],
            transfer_on_dtmf=True,
            approximation="Asterisk refuses REFER with 603 Decline; real PBXs also answer 403, 405 or 501.",
            checks=check_refer_refuse,
            windows=windows_refer_refuse,
        ),
        Profile(
            name="diversion-on",
            number="200",
            imitates="Forwarded hunt-group call (200 -> 2000) with Diversion and History-Info headers.",
            checks=lambda data: _diversion_checks(data, True, "200"),
        ),
        Profile(
            name="diversion-off",
            number="201",
            imitates="Forwarded hunt-group call (201 -> 2000) without Diversion or History-Info (3CX, STARFACE).",
            checks=lambda data: _diversion_checks(data, False, "201"),
        ),
    )
}
