# Voice bench (WP0): an imitation customer phone system

SRW's local voice path attaches to a customer's existing phone system (PBX):
an Asterisk addon registers as an extension on the PBX and hands the audio to
SRW's voice gateway. This bench imitates the common German PBXs and their
quirks so that the addon (and anything else that answers calls) can be tested
against all of them on one workstation, without the real systems.

```
 caller (pjsua)                bench PBX (Asterisk 22)                 device under test
 plays caller.wav  ──SIP/RTP──▶  dials the profile's endpoint  ──SIP/RTP──▶  stand-in (pjsua) or
 records what it hears          for extension 2000                         WP2's Asterisk addon
 sends DTMF                     ◀── blind REFER to 3000 ────────────────── plays dut.wav, records,
                                     │                                     transfers on DTMF 9
                                     ▼
                                human extension 3000 (pjsua): plays human.wav
```

Each **profile** is one phone call. The caller dials the profile's business
number, the PBX rings the AI extension 2000 with that profile's settings, and
the runner checks the SIP messages at the device, the parties' event logs and
the transcripts (whisper.cpp) of what each side recorded.

## Run it

Needs Docker with Compose v2 or later and Python 3.10+ with PyYAML (the repo's
virtual environment has it; `SRW_PYTHON` picks another interpreter).

```bash
scripts/voice-bench.sh run                    # every profile against the stand-in
scripts/voice-bench.sh run --skip-slow        # without session-timer
scripts/voice-bench.sh run --profile dtmf-info,refer-accept
scripts/voice-bench.sh run --dut srw          # SRW's addon + gateway + echo engine (WP2)
scripts/voice-bench.sh list                   # profiles and what they imitate
```

The first run downloads the pinned models, builds four images and renders the
German WAVs; later runs reuse all of it. The run writes
`tests/voice_bench/results/<UTC time>/report.json` plus one directory per
profile, prints a table and **exits 1 if any profile fails** (0 when all pass,
2 on bad arguments). Everything the bench generates (`.cache/`, `.run/`,
`results/`, `.env`) is gitignored.

Other commands: `models` (download), `audio [--force]` (render WAVs),
`build`, `up` (PBX only, for an external device), `standin up|down`, `down`.

## Profiles

| Profile | Dials | Imitates | Asserts (on top of the common checks) |
|---|---|---|---|
| `register` | 100 | The device registers as an extension with digest auth (any PBX); G.722, A-law, µ-law offered | REGISTER challenged (401 Digest) and accepted with credentials |
| `peer` | 101 | The PBX sends the call to the device by IP, no registration (IP peer/trunk) | no REGISTER from the device; INVITE goes to the peer address |
| `alaw-only` | 102 | Only G.711 A-law offered | offer and answer contain PCMA (+ telephone-event) only |
| `dtmf-info` | 103 | DTMF as SIP INFO only, no telephone-event in SDP (Swyx registered devices) | INFO `Signal=` carries `2580#`, each answered 200; the device saw `2580#` via INFO |
| `dtmf-rfc4733` | 104 | DTMF as RFC 2833/4733 only (OpenScape Business) | telephone-event offered, no INFO; the device saw `2580#` via RFC 4733 |
| `reinvite` | 105 | Mid-call re-INVITEs that move the media away and back through the PBX (AGFEO) | ≥2 mid-call re-INVITEs, media moved away *and* pulled back, all answered 200, audio both ways after the last one |
| `session-timer` (slow) | 106 | 90 s session timer (RFC 4028) | `Session-Expires: 90` offered and agreed, a refresh (re-INVITE or UPDATE; Asterisk uses UPDATE when the device allows it) 30–89 s in, answered 200; the call lasts 100 s with no early BYE; audio in the last 10 s |
| `refer-accept` | 107 | The device blind-transfers to the human extension; the PBX carries it out | REFER to 3000 answered 202, NOTIFY sipfrag 200, the human answers; the caller hears the device before and the human after |
| `refer-refuse` | 108 | Same REFER, the PBX refuses it | REFER refused (Asterisk: 603 Decline; recorded in the report), no BYE, the human is never rung, the caller still hears the device |
| `diversion-on` | 200 | Hunt-group call forwarded 200 → 2000 with `Diversion` and `History-Info` | both headers present and name 200 |
| `diversion-off` | 201 | Same forward without either header (3CX, STARFACE, OpenScape hunt groups) | both headers absent |

**Common checks** for every profile: the call is established on both sides;
it lasts until the caller's scheduled hangup; the caller's recording is
non-silent and its transcript contains the device's keywords; the device's
recording contains the caller's keywords (stand-in only).

**Approximations** (also in each profile's `approximation` field):

- `reinvite`: Asterisk moves a call's media only through *direct media*.
  After answer the PBX re-INVITEs the device to send RTP straight to the
  caller; the caller then holds, so the PBX re-INVITEs again and pulls the
  media back through itself (what AGFEO does); after unhold it re-anchors once
  more. A PBX that pulls media back on its own, without a hold, cannot be
  configured in Asterisk.
- `refer-refuse`: Asterisk refuses REFER with 603 Decline. Real PBXs also
  answer 403, 405 or 501, or accept with 202 and report a failure in the
  NOTIFY; the device must treat any of them as "transfer failed".
- Licence and seat limits (FRITZ!Box: 10 IP phones; per-user or per-channel
  licences) and TLS/SRTP are not imitated.

## The parts

| Part | What | Source (pinned, checksum-verified) |
|---|---|---|
| PBX | `srw-asterisk:22.11.0`, built by `docker/Dockerfile.asterisk` (also the WP2 addon image); bench config in `pbx/`, one endpoint and one dialplan context per profile | downloads.asterisk.org tarball, sha256 + GPG signature of the Asterisk Development Team key |
| Caller, stand-in device, human | `srw-voice-bench-ua:pjsua-2.17`: pjsua (pjproject 2.17, GPL-2.0, test container only) driven by `ua/ua_agent.py` | pjproject tarball from Asterisk's third-party mirror (sha256) |
| Transcription | `srw-voice-bench-whisper:1.9.5`: whisper.cpp v1.9.5 (MIT), CPU, model `ggml-small.bin` (multilingual) | GitHub release tag archive (sha256); model from huggingface.co/ggerganov/whisper.cpp at a pinned commit (sha256) |
| Test voices | `srw-voice-bench-piper:2023.11.14-2`: Piper (MIT) + sox; voice `de_DE-mls-medium` | GitHub release (sha256); voice from huggingface.co/rhasspy/piper-voices at a pinned commit (sha256) |
| SRW gateway and driver (`--dut srw`) | `srw-voice-bench-gateway:py3.12`: python:3.12-slim (digest-pinned) with aiohttp and PyYAML; the code is mounted from the repository | PyPI wheels, hashes from `requirements/locks/vm-controller-py312.txt` |

**Why pjsua.** It is pjproject's reference user agent: unattended
auto-answer, looping WAV playback, recording of the far end, RFC 4733 and SIP
INFO DTMF, blind REFER, hold/unhold, session timers, and a log line for every
SIP message and received DTMF digit (with its method). The controller feeds
its console commands on a timeline and writes JSON events, so one image plays
all three parties, built from the same pjproject release Asterisk bundles.
SIPp would only cover signalling; baresip and VoIP Patrol were not needed.

**Voice credit.** The bench WAVs and the test set are spoken by Piper's
`de_DE-mls-medium` voice (rhasspy/piper-voices, MIT), trained from scratch on
the German part of Multilingual LibriSpeech (Pratap et al., 2020,
openslr.org/94, CC BY 4.0). Most other Piper German voices are fine-tuned
from non-commercial checkpoints and are not used. The mls voice garbles some
sentences (questions, anything starting with "Hallo"), so the three bench
lines in `testset/de/voices.yaml` were picked because whisper transcribes
them reliably after a G.711 round trip.

## SRW's own stack as the device (WP2)

```bash
scripts/voice-bench.sh run --dut srw                 # all 11 profiles
scripts/voice-bench.sh run --dut srw --skip-slow     # without session-timer
```

Runs every profile against SRW's local voice path instead of the stand-in
(`srw.py`, `compose.srw.yaml`):

- **addon** (`.50` on the bench network): `srw-asterisk` with the
  `/etc/asterisk` that `python -m voice_gateway.asterisk.render` writes from
  bench line values: line `bench` registers as extension 2000, line
  `bench-peer` takes the peer profile's INVITE at the addon's address.
- **gateway**: `python -m voice_gateway` with `source: {kind: asterisk}` and
  the `echo` profile, so the caller hears its own words back.
- **driver**: `srw_driver.py` in SRW's role: it receives the gateway's signed
  webhook (verified with the orchestrator's own code), accepts with `echo`,
  attaches, sends a greeting append like the gpt-live-sip driver, and on
  DTMF 9 asks the gateway to transfer to `sip:3000@<PBX>`.

The gateway and the driver share the addon's network namespace, so ARI, the
media WebSocket and the webhook stay on loopback. They run from the
repository (mounted read-only) in a test-only Python image
(`docker/Dockerfile.gateway`, hash-pinned aiohttp and PyYAML). All secrets
are fresh throwaway values under `.run/srw/`.

On top of the external-device checks, the runner reads the driver's log
(`driver.events.jsonl` per profile) and asserts one signed webhook for the
right line with From/To (and Diversion/History-Info for `diversion-on`, none
for `diversion-off`), accept + `session.started`, `session.closed` with
`remote_hangup`, the DTMF digits SRW received (`2580#` for both DTMF
profiles), and the transfer request. `refer-refuse` asserts SRW's fallback
instead of the stand-in's behaviour: after the 603 the addon dials 3000
itself through its line, the human answers, and the caller, who never left,
hears the human (the bench PBX lets extension 2000 call 3000, as any PBX
lets an internal phone call the reception). Once the human answered, the
runner restarts the gateway (`gateway_restart.json` per profile): the
handed-over call lives in the addon's dialplan, outside the gateway's app, so
the caller must still hear the human after the restart and hang up on
schedule (`handed_over_call_survives_gateway_restart`). After the last profile,
`no_audio_written` in the report lists every file the addon, the gateway and
the driver changed in their containers (`docker diff`): the gateway and the
driver run read-only, and the addon may only keep its astdb and control
socket. The services' logs are saved as `srw-*.log` in the results.

## One scripted call through SRW's live stack (A1)

```bash
scripts/voice-bench.sh addon up --line fake        # PBX + SRW's addon only
# start the gateway with the printed source settings, then:
scripts/voice-bench.sh call --script tests/voice_bench/calls/a1-de.yaml
scripts/voice-bench.sh down
```

For the acceptance call (local_voice_gateway.md §5 A1) the gateway runs
outside the bench, on the host: it needs the model servers on host loopback
and must be reachable by the line's driver pod on k3d, while SRW's real live
job (orchestrator, driver pod, back office) answers its webhook.

- **`addon up`** starts the PBX and only the addon (`srw.py::prepare_addon`).
  The addon registers the gateway's line (`--line`, the name in the gateway
  config) as extension 2000, serves ARI on its bench address (`.50`) and
  sends each call's audio to `ws://<bench gateway address>:18441/media`,
  i.e. the host (`--media-host` for another address). It prints the
  gateway's `source:` block (also `.run/srw/gateway/source.yaml`, with the
  shared Asterisk secret beside it).
- **`call`** places one call from a script (`call.py`, example
  `calls/a1-de.yaml`): the number to dial (107 accepts a blind REFER to the
  human extension 3000), the caller's lines at fixed seconds after the
  answer (test-set ids or free text, rendered with the bench's Piper voice),
  the hangup time, and `expect` windows with keywords the caller must hear
  (`from: human` starts at the moment 3000 answered). Afterwards whisper
  transcribes what the caller heard, each voiced stretch of the recording
  on its own (one long clip makes whisper loop over the silences and blur
  the times); a window's text is the stretches that start inside it.
  `call.json` holds that timeline, the windows, the PBX's view of a
  transfer (REFER, its answer, the NOTIFY, when the human answered) and
  `no_audio_written` for the addon (`docker diff`). `call --analyze DIR`
  repeats only the analysis on an earlier call's results.

The script's times are fixed, so leave room for the back office: a line
spoken while the voice still talks may barge in. whisper (small) is a weak
judge of the cascade's Piper voice after G.711: compare a failed keyword
with SRW's own transcript (the job's live session) before blaming the call.

## Plugging in a real device (WP2)

Run the profiles against another device (or a manually run addon) instead of
the stand-in:

```bash
scripts/voice-bench.sh up --dut-peer <device-ip>[:port]       # start the PBX, print the settings
# configure the device (below), let it register, then:
scripts/voice-bench.sh run --dut external --dut-peer <device-ip>[:port] --wait-registration 60
```

What the device needs:

- **Register** as extension `2000`, user `2000`, password
  `VOICE_BENCH_AI_PASSWORD` from `tests/voice_bench/.env` (created on the
  first run with throwaway values), registrar = the bench PBX, UDP or TCP 5060.
- **Answer** the `peer` profile's INVITE at the address given with
  `--dut-peer` (no registration involved).
- **Transfer on DTMF 9**: in `refer-accept`/`refer-refuse` the caller presses
  9; the device must then blind-transfer (REFER) to `sip:3000@<PBX address>`.
- **Audio**: with `--dut external` the caller expects to hear **its own words
  back** (`--dut-expect echo`, the WP2 echo engine). Use `--dut-expect
  standin` or `--dut-expect word1,word2` for other engines.

Two ways to connect it:

- **A: put the device on the bench network** (recommended for k3d). The bench
  network is `172.30.86.0/24` (`--subnet` to change it): PBX `.10`, stand-in
  `.20`, human `.30`, caller `.40`. `docker network connect
  srw-voice-bench_bench <k3d node container>` gives the node an address there;
  a `hostNetwork` pod (or a NodePort) on that node is then reachable from the
  PBX, and pod traffic to `172.30.86.x` leaves through the node. No NAT, so
  every profile works, including `reinvite`, where direct media makes the
  device send RTP straight to the caller (`172.30.86.40`, UDP 4000–4010).
- **B: publish the PBX on the host** for a device elsewhere on the LAN:
  add `--external-address <host LAN IP>` (and `--sip-port` if 5060 is taken)
  to both commands. `compose.external.yaml` publishes SIP 5060/udp+tcp and
  RTP `10000–10099/udp` 1:1; the PBX advertises the address
  (`external_signaling_address`, `external_media_address`) to everything
  outside `local_net` (the bench subnet). The device must advertise an address
  the PBX can reach in its Contact and SDP. `reinvite` needs the device to
  reach the caller at `172.30.86.40` directly, which a LAN device usually
  cannot; leave that profile out in this mode.

In external mode the runner reads the device's side of every call from the
PBX's SIP log (`pjsip.conf` has `debug = yes`; each profile's slice is saved as
`pbx.log`). Checks that need the device's own view (the DTMF digits its
application saw, what it recorded) are reported as `skip`, to be verified in
the device's own logs. The PBX stays up between external runs (the device's
registration lives there); `scripts/voice-bench.sh down` stops it.

**Self-test of the external path**: `scripts/voice-bench.sh standin up`
starts the stand-in as a long-running "external" device; then
`scripts/voice-bench.sh run --dut external --dut-peer 172.30.86.20
--dut-expect standin` must pass like the normal run.

## Results

`report.json` lists the tools, the device mode, and per profile: `status`
(`pass`/`fail`/`error`), `checks` (name, `pass`/`fail`/`skip`, detail),
`observed` values (codecs, DTMF, REFER response, headers, refresh timeline),
`transcripts` per audio window, `audio` levels, `approximation` and the
artifact directory. Per profile the directory holds each party's scenario
(`*.json`), `*.events.jsonl` (every SIP message with headers and body, call
states, DTMF, transfer results), pjsua console and debug logs, recordings
(`*.wav`, synthetic test speech only), the transcribed windows with whisper's
JSON, and the PBX log slice.

The bench PBX records nothing; only the test parties record, and only
synthetic speech.

## German test set (`testset/de/`)

- `facts.md`: a **fictional** facts sheet (Hotel Beispielhof) for the voice
  layer: what it answers itself and what it hands to the back office.
- `utterances.yaml`: 31 caller lines with the expected handling (`answer`,
  `delegate`, `transfer`, `message`) and keywords, for the later "answer or
  delegate" evaluation of the voice LLM (WP3/WP4).
- `voices.yaml`: the bench lines and the speaker pool.

`scripts/voice-bench.sh audio` renders every line as 16 kHz PCM and 8 kHz
G.711 A-law into `.cache/audio/{bench,testset}/` (gitignored, never
committed).
