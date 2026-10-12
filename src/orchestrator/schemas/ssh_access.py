"""Request contracts for SSH key registration and gateway attachment audit.

Every field cap in this module is a security control, not tidiness — each
one is annotated with the sink it bounds. Read the annotation before
relaxing one.
"""

from pydantic import BaseModel, Field, field_validator


class VMGatewayAccessRequest(BaseModel):
    """Bounded signed gateway proof; identity is resolved from key and handle."""

    proof: dict[str, str | int] = Field(...)


class SshKeyCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Display name")
    public_key: str = Field(..., min_length=1, max_length=8192)
    # Capped like its siblings: `challenge` is the field that reaches hmac.new()
    # over the full head and .encode("utf-8"). A minted token is ~120 bytes.
    challenge: str = Field(..., min_length=1, max_length=1024)
    signature: str = Field(..., min_length=1, max_length=8192)


class SshKeyUsedRequest(BaseModel):
    """Body for the gateway's post-authentication ``last_used_at`` bump.

    ``fingerprint`` is capped: it flows straight into a SQL predicate
    (``resolve_user_by_ssh_fingerprint``, then ``mark_ssh_key_used``'s
    second WHERE clause) with no length check downstream, so an unbounded
    body would let any ``X-Internal-Key`` holder push an arbitrarily large
    value into that query. A SHA256 fingerprint is ``"SHA256:"`` plus 43
    base64 characters (50 total); 128 is headroom, not a tight fit.
    """

    fingerprint: str = Field(..., min_length=1, max_length=128)


class SshAttachmentCreate(BaseModel):
    """Body for opening an SSH-attachment audit row.

    Deliberately does NOT carry ``thread_id``/``user_id``/``ssh_key_id``.
    ``get_ssh_target``'s docstring states the invariant this body must not
    violate: "this codebase does not accept an internal key plus an
    asserted user identity." An earlier draft of this endpoint took those
    three as asserted fields — any ``X-Internal-Key`` holder (every agent
    pod) could then write an audit row attributing an SSH attach to any
    user on any thread. That the value is only ever recorded, never used
    for an authorization decision, is a mitigation, not a justification —
    the audit table's whole purpose is a trustworthy record of who reached
    a workspace over SSH, and a forgeable one is worth less than it looks.

    ``thread_id``, ``user_id`` and ``ssh_key_id`` are resolved server-side
    instead, by ``internal_create_ssh_attachment``, from ``fingerprint`` and
    ``handle`` — the same two values ``get_ssh_target`` resolves identity
    from.

    ``fingerprint`` is capped for the same reason as
    ``SshKeyUsedRequest.fingerprint``.
    """

    fingerprint: str = Field(..., min_length=1, max_length=128)
    handle: str
    client_ip: str | None = None


class SshAttachmentClose(BaseModel):
    """Body for closing an SSH-attachment audit row.

    ``fingerprint`` (fix round 2): before this field existed, any
    ``X-Internal-Key`` holder (every agent pod) could close ANY attachment
    row it could name — there was no identity in this body at all, only an
    opaque path-param UUID. Unlike ``SshAttachmentCreate`` there was no
    identity to *forge*, so the risk isn't access escalation, but a
    forensics table any internal-key holder can silently corrupt (mark
    detached, stamp fabricated ``channels``) is weaker evidence, and being
    evidence is this table's whole purpose. ``internal_close_ssh_attachment``
    resolves this fingerprint to a user and checks that user against the
    NAMED attachment's own thread — see that function's docstring. Capped
    for the same reason as the other two fingerprint fields.

    ``channels`` is capped in both count and per-entry length: it is written
    straight into a ``text[]`` column (migration 0204) with no size limit of
    its own, so an unbounded body would let any ``X-Internal-Key`` holder
    inflate an audit row arbitrarily. Two channel types exist today
    ("session", "sftp"), so 8 entries of up to 32 characters each is
    generous headroom, not a tight fit.
    """

    fingerprint: str = Field(..., min_length=1, max_length=128)
    channels: list[str] = Field(default_factory=list, max_length=8)

    @field_validator("channels")
    @classmethod
    def _cap_channel_name_length(cls, value: list[str]) -> list[str]:
        for channel in value:
            if len(channel) > 32:
                raise ValueError(f"channel name too long: {channel[:40]!r}...")
        return value
