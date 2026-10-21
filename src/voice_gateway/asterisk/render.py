"""Render the Asterisk addon's /etc/asterisk; knowledge-base/knowledge/features/local_voice_gateway.md §4.4.

    python -m voice_gateway.asterisk.render --values addon.yaml --out /etc/asterisk

One YAML file describes the addon and its lines (``addon.example.yaml`` next
to this module); passwords come from the files it names, relative to it. The
output is the complete config directory: one registration or IP identify and
one endpoint per line, a dialplan that hands every call to the gateway's
Stasis app with the line's name, ARI on a private address, the gateway's
media connection, and modules that cannot write audio files. Files are 0640
in a 0750 directory; nothing is printed but file names.

A line's ``connector_id`` and ``webhook_secret_file`` belong to the gateway's
config and are ignored here, so one list of lines can feed both.
"""

from __future__ import annotations

import argparse
import ipaddress
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import yaml

from voice_gateway.asterisk import (
    DEFAULT_APP,
    DEFAULT_MEDIA_CONNECTION,
    ENDPOINT_PREFIX,
    HANDOVER_CONTEXT,
    HANDOVER_VAR,
    LINE_VAR,
    PBX_VAR,
    REFER_TO_VAR,
    TRANSFER_CONTEXT,
)
from voice_gateway.errors import ConfigError

HEADER = "; Rendered by python -m voice_gateway.asterisk.render; do not edit.\n"
CODECS = ("alaw", "ulaw", "g722")
DTMF_MODES = ("auto", "auto_info", "rfc4733", "info", "inband")
_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,30}")
_EXTENSION = re.compile(r"[0-9A-Za-z+*#][0-9A-Za-z+*#.-]{0,31}")
_USERNAME = re.compile(r"[0-9A-Za-z+*#._@-]{1,64}")
_HOSTNAME = re.compile(r"[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?")
_APP = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
_GATEWAY_KEYS = {"connector_id", "webhook_secret_file"}


@dataclass(frozen=True)
class Host:
    host: str  # IPv6 in brackets
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class LineValues:
    name: str
    mode: str
    pbx: Host
    domain: str
    transport: str
    extension: str | None
    username: str | None
    password: str | None
    codecs: tuple[str, ...]
    dtmf_mode: str
    match: tuple[str, ...]

    @property
    def endpoint(self) -> str:
        return ENDPOINT_PREFIX + self.name

    @property
    def context(self) -> str:
        return f"srw-line-{self.name}"


@dataclass(frozen=True)
class AddonValues:
    sip_bind: str
    sip_port: int
    local_net: tuple[str, ...]
    external_address: str | None
    rtp_start: int
    rtp_end: int
    ari_host: str
    ari_port: int
    ari_user: str
    password: str
    app: str
    media_uri: str
    media_connection: str
    lines: tuple[LineValues, ...]


# -- reading the values -----------------------------------------------------------------


def _mapping(value, label: str, allowed: set[str]) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{label} must be a mapping")
    unknown = value.keys() - allowed
    if unknown:
        raise ConfigError(f"{label} has unknown keys: {', '.join(sorted(unknown))}")
    return value


def _text(value, label: str, pattern: re.Pattern | None = None) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        value = str(value)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{label} must be a nonempty string")
    value = value.strip()
    if pattern is not None and not pattern.fullmatch(value):
        raise ConfigError(f"{label} has characters Asterisk config cannot carry")
    return value


def _port(value, label: str) -> int:
    if type(value) is not int or not 0 < value < 65536:
        raise ConfigError(f"{label} must be a port number")
    return value


def _host_text(value: str, label: str) -> str:
    try:
        address = ipaddress.ip_address(value.strip("[]"))
    except ValueError:
        if not _HOSTNAME.fullmatch(value):
            raise ConfigError(f"{label} must be an IP address or host name") from None
        return value
    return f"[{address}]" if address.version == 6 else str(address)


def _host(value, label: str, default_port: int) -> Host:
    text = _text(value, label)
    host, port = text, default_port
    if text.startswith("["):
        host, _, rest = text.partition("]")
        host += "]"
        if rest:
            if not rest.startswith(":"):
                raise ConfigError(f"{label} must be host[:port]")
            port = _port(int(rest[1:]) if rest[1:].isdigit() else -1, label)
    elif text.count(":") == 1:
        host, _, rest = text.partition(":")
        port = _port(int(rest) if rest.isdigit() else -1, label)
    return Host(_host_text(host, label), port)


def _secret(base: Path, value, label: str) -> str:
    path = base / _text(value, label)
    try:
        secret = path.read_text().strip()
    except (OSError, UnicodeError):
        raise ConfigError(f"cannot read {label}") from None
    # ';' starts a comment in Asterisk config; never echo the value.
    if not secret or any(c in secret for c in ';"\\') or not secret.isprintable():
        raise ConfigError(f'{label} must hold printable characters without ; " or \\')
    return secret


def _list(value, label: str) -> list:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ConfigError(f"{label} must be a list")
    return value


def _codecs(value, label: str, default: tuple[str, ...]) -> tuple[str, ...]:
    if value is None:
        return default
    codecs = tuple(_text(c, label) for c in _list(value, label))
    if (
        not codecs
        or any(c not in CODECS for c in codecs)
        or len(set(codecs)) < len(codecs)
    ):
        raise ConfigError(f"{label} must list some of {', '.join(CODECS)}")
    return codecs


def _line(raw, base: Path, codecs: tuple[str, ...], dtmf: str) -> LineValues:
    raw = _mapping(
        raw,
        "each line",
        {
            "name",
            "mode",
            "pbx",
            "domain",
            "transport",
            "extension",
            "username",
            "password_file",
            "codecs",
            "dtmf_mode",
            "match",
            *_GATEWAY_KEYS,
        },
    )
    name = _text(raw.get("name"), "line name", _NAME)
    label = f"line {name}"
    mode = raw.get("mode", "register")
    if mode not in ("register", "peer"):
        raise ConfigError(f"{label}: mode must be register or peer")
    transport = raw.get("transport", "udp")
    if transport not in ("udp", "tcp"):
        raise ConfigError(f"{label}: transport must be udp or tcp")
    pbx = _host(raw.get("pbx"), f"{label}: pbx", 5060)
    domain = (
        _host_text(_text(raw["domain"], f"{label}: domain"), f"{label}: domain")
        if raw.get("domain") is not None
        else pbx.host
    )
    extension = username = password = None
    if raw.get("extension") is not None:
        extension = _text(raw["extension"], f"{label}: extension", _EXTENSION)
    if raw.get("username") is not None:
        username = _text(raw["username"], f"{label}: username", _USERNAME)
    if raw.get("password_file") is not None:
        password = _secret(base, raw["password_file"], f"{label}: password_file")
    if mode == "register" and (extension is None or password is None):
        raise ConfigError(
            f"{label}: a registered line needs extension and password_file"
        )
    if password is not None and username is None:
        username = extension
    if password is not None and username is None:
        raise ConfigError(f"{label}: password_file needs a username or extension")
    match = tuple(
        _match(m, f"{label}: match")
        for m in (_list(raw.get("match"), f"{label}: match") or [pbx.host])
    )
    if mode == "register" and raw.get("match") is not None:
        raise ConfigError(
            f"{label}: a registered line is matched by its registration, not match"
        )
    dtmf_mode = raw.get("dtmf_mode", dtmf)
    if dtmf_mode not in DTMF_MODES:
        raise ConfigError(f"{label}: dtmf_mode must be one of {', '.join(DTMF_MODES)}")
    return LineValues(
        name,
        mode,
        pbx,
        domain,
        transport,
        extension,
        username,
        password,
        _codecs(raw.get("codecs"), f"{label}: codecs", codecs),
        dtmf_mode,
        match,
    )


def _match(value, label: str) -> str:
    text = _text(value, label)
    try:
        return str(ipaddress.ip_network(text.strip("[]"), strict=False))
    except ValueError:
        return _host_text(text, label)


def load_values(path: str | Path, *, ari_bind: str | None = None) -> AddonValues:
    """Read and check the addon values; secrets resolve next to the file.

    ``ari_bind`` overrides ``ari.bind``: the pod's own address is known only
    at start (an init container passes it from the downward API)."""
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text())
    except (OSError, UnicodeError, yaml.YAMLError):
        raise ConfigError("cannot read the values as YAML") from None
    raw = _mapping(
        raw, "values", {"sip", "rtp", "ari", "gateway", "codecs", "dtmf_mode", "lines"}
    )
    base = path.parent
    sip = _mapping(
        raw.get("sip"), "sip", {"bind", "port", "local_net", "external_address"}
    )
    rtp = _mapping(raw.get("rtp"), "rtp", {"start", "end"})
    ari = _mapping(raw.get("ari"), "ari", {"bind", "user", "password_file", "app"})
    gateway = _mapping(raw.get("gateway"), "gateway", {"media_uri", "connection"})
    bind = _host_text(_text(sip.get("bind", "0.0.0.0"), "sip.bind"), "sip.bind")
    local_net = []
    for net in _list(sip.get("local_net"), "sip.local_net"):
        try:
            local_net.append(
                str(ipaddress.ip_network(_text(net, "sip.local_net"), strict=False))
            )
        except ValueError:
            raise ConfigError("sip.local_net must list networks in CIDR form") from None
    external = None
    if sip.get("external_address") is not None:
        external = _host_text(
            _text(sip["external_address"], "sip.external_address"),
            "sip.external_address",
        )
        if not local_net:
            raise ConfigError(
                "sip.external_address needs sip.local_net (the networks reached without NAT)"
            )
    rtp_start = _port(rtp.get("start", 10000), "rtp.start")
    rtp_end = _port(rtp.get("end", 10099), "rtp.end")
    if rtp_end - rtp_start < 3:
        raise ConfigError(
            "rtp.end must leave room for two ports per call above rtp.start"
        )
    ari_bind = _host(ari_bind or ari.get("bind", "127.0.0.1:8088"), "ari.bind", 8088)
    if ari_bind.host in ("0.0.0.0", "[::]"):
        raise ConfigError(
            "ari.bind must be loopback or the pod's own address, never every interface"
        )
    media_uri = _text(
        gateway.get("media_uri", "ws://127.0.0.1:18441/media"), "gateway.media_uri"
    )
    try:
        parsed = urlsplit(media_uri)
        if (
            parsed.scheme not in ("ws", "wss")
            or not parsed.hostname
            or parsed.username
            or parsed.query
            or parsed.fragment
            or any(c in media_uri for c in ';"\\ ')
        ):
            raise ValueError
        _ = parsed.port
    except ValueError:
        raise ConfigError("gateway.media_uri must be a ws:// or wss:// URL") from None
    codecs = _codecs(raw.get("codecs"), "codecs", CODECS)
    dtmf = raw.get("dtmf_mode", "auto")
    if dtmf not in DTMF_MODES:
        raise ConfigError(f"dtmf_mode must be one of {', '.join(DTMF_MODES)}")
    lines = tuple(
        _line(item, base, codecs, dtmf) for item in _list(raw.get("lines"), "lines")
    )
    if not lines:
        raise ConfigError("lines must be a nonempty list")
    if len({line.name for line in lines}) < len(lines):
        raise ConfigError("line names must be unique")
    peers = [m for line in lines if line.mode == "peer" for m in line.match]
    if len(set(peers)) < len(peers):
        raise ConfigError("two peer lines cannot match the same address")
    return AddonValues(
        sip_bind=bind,
        sip_port=_port(sip.get("port", 5060), "sip.port"),
        local_net=tuple(local_net),
        external_address=external,
        rtp_start=rtp_start,
        rtp_end=rtp_end,
        ari_host=ari_bind.host,
        ari_port=ari_bind.port,
        ari_user=_text(ari.get("user", "srw"), "ari.user", _APP),
        password=_secret(base, ari.get("password_file"), "ari.password_file"),
        app=_text(ari.get("app", DEFAULT_APP), "ari.app", _APP),
        media_uri=media_uri,
        media_connection=_text(
            gateway.get("connection", DEFAULT_MEDIA_CONNECTION),
            "gateway.connection",
            _APP,
        ),
        lines=lines,
    )


# -- the files ----------------------------------------------------------------------------


def _section(name: str, **options) -> str:
    lines = [f"[{name}]"]
    for key, value in options.items():
        if value is None:
            continue
        for item in value if isinstance(value, (list, tuple)) else [value]:
            lines.append(f"{key} = {item}")
    return "\n".join(lines) + "\n"


def _sip_uri(line: LineValues, user: str | None = None) -> str:
    uri = f"sip:{user + '@' if user else ''}{line.pbx}"
    return uri + (";transport=tcp" if line.transport == "tcp" else "")


def pjsip_conf(v: AddonValues) -> str:
    parts = [
        HEADER,
        "; Registered lines are matched by the ;line= parameter of their Contact\n"
        "; (always tried first), peer lines by the PBX's address; nothing else.\n",
        _section(
            "global",
            type="global",
            user_agent="SRW voice gateway",
            endpoint_identifier_order="ip",
        ),
    ]
    external = {}
    if v.external_address:
        external = {
            "external_signaling_address": v.external_address,
            "external_media_address": v.external_address,
        }
    for protocol in ("udp", "tcp"):
        parts.append(
            _section(
                f"srw-{protocol}",
                type="transport",
                protocol=protocol,
                bind=f"{v.sip_bind}:{v.sip_port}",
                local_net=v.local_net or None,
                **external,
            )
        )
    for line in v.lines:
        what = (
            f"registers as extension {line.extension} at {line.pbx}"
            if line.mode == "register"
            else f"takes calls the PBX at {', '.join(line.match)} sends"
        )
        parts.append(f"; ---- line {line.name}: {what}\n")
        auth = None
        if line.password is not None:
            auth = line.endpoint + "-auth"
            parts.append(
                _section(
                    auth,
                    type="auth",
                    auth_type="digest",
                    username=line.username,
                    password=line.password,
                )
            )
        # The PBX's address, for the fallback transfer dial.
        parts.append(_section(line.endpoint, type="aor", contact=_sip_uri(line)))
        parts.append(
            _section(
                line.endpoint,
                type="endpoint",
                transport=f"srw-{line.transport}",
                context=line.context,
                aors=line.endpoint,
                outbound_auth=auth,
                from_user=line.extension,
                from_domain=line.domain if line.extension else None,
                disallow="all",
                allow=",".join(line.codecs),
                dtmf_mode=line.dtmf_mode,
                direct_media="no",
                rtp_symmetric="yes",
                force_rport="yes",
                rewrite_contact="yes",
                timers="yes",
                # The PBX may not transfer the addon; the gateway transfers callers.
                allow_transfer="no",
                send_pai="no",
                inband_progress="no",
            )
        )
        if line.mode == "register":
            parts.append(
                _section(
                    line.endpoint,
                    type="registration",
                    transport=f"srw-{line.transport}",
                    outbound_auth=auth,
                    server_uri=_sip_uri(line),
                    client_uri=f"sip:{line.extension}@{line.domain}",
                    contact_user=line.extension,
                    line="yes",
                    endpoint=line.endpoint,
                    expiration=600,
                    retry_interval=30,
                    forbidden_retry_interval=120,
                    fatal_retry_interval=120,
                    max_retries=1000000,
                    auth_rejection_permanent="no",
                )
            )
        else:
            parts.append(
                _section(
                    line.endpoint,
                    type="identify",
                    endpoint=line.endpoint,
                    match=list(line.match),
                )
            )
    return "\n".join(parts)


def extensions_conf(v: AddonValues) -> str:
    parts = [
        HEADER,
        "; Every call goes to the voice gateway's Stasis app with its line's\n"
        "; name. Nothing here records or plays audio.\n",
        "[general]\nstatic = yes\nwriteprotect = yes\nautofallthrough = yes\n",
    ]
    for line in v.lines:
        extra = ""
        if line.extension and not line.extension.isdigit():
            extra = f"exten => {line.extension},1,Goto(s,1)\n"
        parts.append(
            f"[{line.context}]\n"
            "exten => _X!,1,Goto(s,1)\n"
            "exten => _+!,1,Goto(s,1)\n"
            f"{extra}"
            f"exten => s,1,Set({LINE_VAR}={line.name})\n"
            f" same => n,Set({PBX_VAR}={line.pbx})\n"
            f" same => n,Stasis({v.app},{line.name})\n"
            "; Reached only while the gateway is not connected: 480 to the PBX.\n"
            " same => n,Hangup(19)\n"
        )
    parts.append(
        f"[{TRANSFER_CONTEXT}]\n"
        "; Blind REFER on the caller's own dialog; the gateway gets the caller\n"
        "; back with TRANSFERSTATUS (SUCCESS/FAILURE) and the SIP code.\n"
        f"exten => s,1,Transfer(PJSIP/${{{REFER_TO_VAR}}})\n"
        f" same => n,Stasis({v.app},${{{LINE_VAR}}},transfer,${{TRANSFERSTATUS}},"
        "${TRANSFERSTATUSPROTOCOL})\n"
        " same => n,Hangup()\n"
    )
    parts.append(
        f"[{HANDOVER_CONTEXT}]\n"
        "; The fallback transfer's target answered: the caller leaves the\n"
        "; gateway's app and bridges to it here (x: the target's leg ends with\n"
        "; the bridge), so a gateway restart never drops the two parties.\n"
        f"exten => s,1,Bridge(${{{HANDOVER_VAR}}},x)\n"
        " same => n,Hangup()\n"
    )
    return "\n".join(parts)


def render(v: AddonValues) -> dict[str, str]:
    """Every file of the addon's /etc/asterisk, by name."""
    tls = "yes" if v.media_uri.startswith("wss:") else "no"
    return {
        "asterisk.conf": HEADER
        + _section(
            "directories",
            astetcdir="/etc/asterisk",
            astmoddir="/usr/lib/asterisk/modules",
            astvarlibdir="/var/lib/asterisk",
            astdbdir="/var/lib/asterisk",
            astkeydir="/var/lib/asterisk",
            astdatadir="/var/lib/asterisk",
            astagidir="/var/lib/asterisk/agi-bin",
            astspooldir="/var/spool/asterisk",
            astrundir="/var/run/asterisk",
            astlogdir="/var/log/asterisk",
            astsbindir="/usr/sbin",
        )
        + "\n"
        + _section(
            "options",
            verbose=0,
            debug=0,
            nofork="yes",
            transmit_silence="no",
            hideconnect="yes",
        ),
        "modules.conf": HEADER
        + "; Without file formats Asterisk cannot write a recording (§2.4: no\n"
        "; audio on disk). The addon registers to the PBX and is nobody's\n"
        "; registrar.\n"
        + _section(
            "modules",
            autoload="yes",
            noload=[
                "format_wav.so",
                "format_pcm.so",
                "format_sln.so",
                "res_ari_recordings.so",
                "app_playback.so",
                "app_echo.so",
                "res_pjsip_registrar.so",
                "res_pjsip_config_wizard.so",
            ],
        ),
        "logger.conf": HEADER
        + "; Warnings and errors only: no caller numbers or SIP messages.\n"
        + "[general]\ndateformat = %F %T.%3q\nqueue_log = no\n\n[logfiles]\n"
        + "console => notice,warning,error\n",
        "pjsip.conf": pjsip_conf(v),
        "extensions.conf": extensions_conf(v),
        "http.conf": HEADER
        + "; Asterisk's HTTP server carries ARI only, on a private address.\n"
        + _section(
            "general",
            enabled="yes",
            bindaddr=v.ari_host.strip("[]"),
            bindport=v.ari_port,
            enablestatic="no",
            sessionlimit=100,
        ),
        "ari.conf": HEADER
        + _section("general", enabled="yes", pretty="no", websocket_write_timeout=1000)
        + "\n"
        + _section(
            v.ari_user,
            type="user",
            read_only="no",
            password=v.password,
            password_format="plain",
        ),
        "websocket_client.conf": HEADER
        + "; Asterisk dials this entry for a call's audio (ARI externalMedia,\n"
        "; external_host = the entry's name); a new connection per call.\n"
        + _section(
            v.media_connection,
            type="websocket_client",
            connection_type="per_call_config",
            uri=v.media_uri,
            protocols="media",
            username=v.ari_user,
            password=v.password,
            connection_timeout=2000,
            reconnect_interval=500,
            reconnect_attempts=3,
            tls_enabled=tls,
        ),
        "chan_websocket.conf": HEADER
        + _section("global", type="global", control_message_format="json"),
        "rtp.conf": HEADER
        + _section("general", rtpstart=v.rtp_start, rtpend=v.rtp_end),
        # No call records either.
        "cdr.conf": HEADER + _section("general", enable="no"),
        "cel.conf": HEADER + _section("general", enable="no"),
    }


def write(files: dict[str, str], out: Path) -> None:
    """Write the files 0640 into a 0750 directory, replacing older copies."""
    out.mkdir(parents=True, exist_ok=True)
    out.chmod(0o750)
    for name, text in files.items():
        path = out / name
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o640)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o640)
            handle.write(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m voice_gateway.asterisk.render",
        description="Render the Asterisk addon's config directory from line values.",
    )
    parser.add_argument("--values", required=True, help="addon and line values (YAML)")
    parser.add_argument(
        "--out", required=True, help="directory to write, e.g. /etc/asterisk"
    )
    parser.add_argument(
        "--ari-bind", help="HOST:PORT for ARI, overriding ari.bind (the pod's address)"
    )
    args = parser.parse_args(argv)
    try:
        files = render(load_values(args.values, ari_bind=args.ari_bind))
    except ConfigError as exc:
        print(f"render: {exc}", file=sys.stderr)
        return 2
    write(files, Path(args.out))
    print(f"render: wrote {', '.join(sorted(files))} to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
