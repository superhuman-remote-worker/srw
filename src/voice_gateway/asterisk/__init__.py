"""Asterisk as the gateway's call source; knowledge-base/knowledge/features/local_voice_gateway.md §4.2, §4.4.

``ari`` talks to Asterisk's REST interface, ``media`` carries a call's audio
over chan_websocket, ``source`` is the CallSource that runs §4.2's call
flow, and ``render`` writes the addon's /etc/asterisk from line values. The
names below tie the rendered config to the source.
"""

#: Stasis application the dialplan hands calls to.
DEFAULT_APP = "srw-voice"
#: websocket_client.conf entry Asterisk dials for a call's audio.
DEFAULT_MEDIA_CONNECTION = "srw-gateway"
#: PJSIP endpoint of a line; the fallback transfer dials through it.
ENDPOINT_PREFIX = "srw-"
#: Dialplan context that sends a blind REFER and returns to Stasis with
#: ``transfer,<TRANSFERSTATUS>,<TRANSFERSTATUSPROTOCOL>``.
TRANSFER_CONTEXT = "srw-transfer"
#: Stasis argument of the fallback transfer's dial leg; upper case, so it
#: can never be a line name.
DIAL_ARG = "SRW_DIAL"
#: Channel variables the source sets or reads.
REFER_TO_VAR = "SRW_REFER_TO"
LINE_VAR = "SRW_LINE"
PBX_VAR = "SRW_PBX"
