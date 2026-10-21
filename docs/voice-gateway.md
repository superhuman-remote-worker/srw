---
title: Local phone lines
description: Answer calls on your own phone system with SRW's voice gateway and Asterisk addon instead of a hosted voice engine.
---

# Local phone lines

A phone line in SRW is a connector of the `srw.gpt-live-sip/v1` driver: a call
starts a live job, a voice talks with the caller, and the job's back office
answers what the voice cannot. By default the voice is OpenAI's GPT-Live, which
answers calls that reach OpenAI's SIP service.

The **voice gateway** and the **Asterisk addon** answer calls on your own phone
system (PBX) instead, with an engine you run:

```
caller ── PSTN ── your PBX ──SIP/RTP (LAN)──▶ Asterisk addon
                                                │ ARI + each call's audio
                                                ▼
          phone-line driver pod ───────────▶ voice gateway ── STT, voice LLM, TTS
                                                │              (OpenAI-compatible URLs)
                       signed webhook ◀─────────┘
          orchestrator ─▶ automation ─▶ live job (back office)
```

- **Asterisk** (the `srw-asterisk` image, Asterisk 22 LTS) registers as an
  extension on your PBX, or takes calls the PBX sends to its address, and
  converts every codec to 16 kHz audio.
- **The voice gateway** (`srw-voice-gateway`) speaks the slice of the GPT-Live
  API the phone-line driver uses, so the driver, the jobs and the Cockpit work
  as with GPT-Live. Each of its **profiles** is one engine: `cascade` (voice
  activity detection and turn detection in the gateway; speech-to-text, a voice
  LLM and text-to-speech over OpenAI-compatible HTTP), or `echo` and `scripted`
  for tests.
- The model servers are yours: a profile names their URLs, like any endpoint
  in the model catalog. They can run on a GPU node, another machine or a hosted
  service.

Both components are off by default (`voiceGateway.enabled`, `asterisk.enabled`
in the Helm values).

## Before you start

- **The phone-line driver**: `connectors.servicePods.enabled` and
  `connectors.drivers.gptLiveSip.enabled` (see the values' comments).
- **One phone-line connector per line**, created in the Cockpit. Its model is
  one of the gateway's profiles (they appear as `live_voice` models once the
  chart has seeded them), and it holds a webhook secret (`whsec_…`) that the
  line's values name too.
- **An extension per registered line** on your PBX (user, password), or for a
  peer line a trunk or route that sends calls to Asterisk's address.
- **A warm back-office agent per line that may ring at the same time.** Every
  call is a live job, and a live job waits for an agent like any job. Set
  `agent.pool.buffer` and `agent.pool.minAgents` to at least the number of lines
  and `agent.pool.maxAgents` above them plus what your other jobs and chat
  sessions use. Without a warm agent the caller hears the line's no-job
  fallback text until an agent has started.

## Configure

Put the line secrets in a Secret of your own (or let your secret manager
create it):

```bash
kubectl -n srw create secret generic srw-voice-lines \
  --from-file=reception-sip-password=./reception-sip-password \
  --from-file=reception-webhook=./reception-webhook-secret
```

Then add the lines and the profiles to your values:

```yaml
voiceGateway:
  enabled: true
  lines:
    - name: reception
      mode: register                 # an extension on the PBX
      pbx: 192.0.2.10
      extension: "2000"
      passwordSecret: {name: srw-voice-lines, key: reception-sip-password}
      connectorId: 00000000-0000-0000-0000-000000000001   # the phone-line connector
      webhookSecret: {name: srw-voice-lines, key: reception-webhook}
  profiles:
    cascade-gpu:
      engine: cascade
      language: de
      stt: {base_url: "http://stt.voice.svc:8000/v1", model: Systran/faster-whisper-small}
      llm: {base_url: "http://llm.voice.svc:8080/v1", model: gemma-4-12b-it}
      tts: {base_url: "http://tts.voice.svc:8000/v1", model: de_DE-mls-medium, voice: "10"}
      turn: {vad_model_file: /opt/srw-voice/models/silero_vad-v6.2.3.onnx}
asterisk:
  enabled: true
  network:
    mode: hostNetwork
  nodeSelector:
    kubernetes.io/hostname: voice-node
```

A stage that needs an API key reads it from a file: list the key under
`voiceGateway.profileSecrets` and name `/run/srw-voice/profiles/<name>` as the
stage's `api_key_file`.

### How the PBX reaches Asterisk

The PBX sends SIP (port 5060, UDP and TCP) and RTP (a port range, two ports per
call) to Asterisk, so Asterisk needs an address on your LAN:

- **`hostNetwork`** (default): Asterisk runs on the node named by
  `asterisk.nodeSelector` and uses that node's addresses. The gateway runs on
  the same node. Asterisk's ARI port (8088) listens on the node's address too
  and is protected by its password only: keep it off the LAN with the node's
  firewall. Set `network.externalAddress` and `network.localNets` when the PBX
  reaches the node through address translation.
- **`loadBalancer`**: a LoadBalancer Service (MetalLB or your cloud's) publishes
  SIP and the RTP range at `network.externalAddress`, which Asterisk advertises
  to the PBX. `network.allowedSources` limits who may send SIP and RTP. Asterisk
  sends its own packets from the node's address, so the PBX must accept RTP
  from an address other than the one it sends to (most do).

### What the chart sets up

- A Secret with the gateway's API key and Asterisk's password, generated once
  and kept (or `voiceGateway.existingSecret`).
- With `voiceGateway.catalog.seed` (default), the llm-seed Job adds the gateway
  as a `live_voice` endpoint and one catalog row per profile; the model id is
  the profile's name. Insert-only: rows you changed in Admin → Models stay as
  they are. Per profile you can set a display name, a price per minute for the
  usage ledger (`ratePerMinuteUsd`, default 0) and the voice an expert that
  names none gets.
- The phone-line driver's pods reach the gateway's in-cluster Service: the
  Service admits `srw.gpt-live-sip/v1` (annotation `srw.io/driver-egress`) and
  the driver pod's egress is pinned to the gateway's pods. The gateway admits
  only those pods and the orchestrator; its audio port admits only Asterisk.

In the phone-line connector, set the **greeting delay** (`greeting_delay_ms`) to
0: the gateway speaks the greeting as soon as it gets it, while GPT-Live needs
the default wait.

## Check a line

```bash
kubectl -n srw exec deploy/<release>-asterisk -c asterisk -- asterisk -rx "pjsip show registrations"
kubectl -n srw logs deploy/<release>-voice-gateway
```

A registered line shows `Registered`. Call the extension: the gateway's log
shows the call ringing, answered and closed (session ids and lines only, never
numbers or what was said), and the Cockpit shows the live job like any phone
call. An `echo` profile plays the caller back: a way to test a line without
model servers.

## Calls and transfers

- **DTMF** arrives whichever way the PBX sends it (RFC 4733, SIP INFO or
  in-band). Set `dtmfMode: auto_info` on a line when callers' speech could be
  read as digits.
- **Transfers** are a blind REFER on the caller's own call. When the PBX refuses
  it, Asterisk dials the target itself through the same line while the caller
  stays with the voice, then bridges the two in its own dialplan: a gateway
  restart does not drop that call.
- **Updates** end the calls in progress (the gateway holds them in memory and
  runs as one replica); roll out between calls.

## Privacy

Nothing in the path writes audio to disk: Asterisk loads no file formats and
writes no call records, the gateway keeps a call's audio in memory, and both
run on a read-only root with memory volumes. The model servers are yours to
configure the same way.

## Licences

The Asterisk addon is GPL-2.0 software running as its own process in its own
image; the image carries Asterisk's and pjproject's licences and the exact
source tarballs it was built from under `/usr/share/doc/srw-asterisk`. See
[THIRD_PARTY_LICENSES.md](https://github.com/superhuman-remote-worker/srw/blob/main/THIRD_PARTY_LICENSES.md)
for the gateway image's components.

## Not supported yet

- SIP over TLS and SRTP.
- More than one gateway replica.
- The PBX registering to SRW (SRW as the registrar), outbound calls, attended
  transfers.
- Managing lines in the Cockpit: lines are Helm values for now.
