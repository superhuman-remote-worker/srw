# SSH access to a session workspace

Every persistent session has a workspace container behind it, and you can open a real
`ssh` connection into that container: run shell commands, `scp` files in and
out, or point an editor at it. This page covers registering a key, connecting with
plain `ssh`, VS Code Remote-SSH and JetBrains Gateway, what the gateway deliberately
refuses, how this interacts with the agent working in the same workspace, and how to
read the errors you'll actually hit.

Two things here are counter-intuitive enough to cause support tickets if left
unstated, so read at least these before anything else:

- **Connecting needs a Personal Access Token, not just a registered key.** See
  [Connect](#2-connect) below — this is easy to miss because the config block cockpit
  gives you doesn't mention it.
- **JetBrains Gateway cannot use the config block at all**, and downloads a large IDE
  backend into your workspace the first time you attach. See
  [JetBrains Gateway](#3-jetbrains-gateway).

## 1. Register a key

Go to **Settings → SSH Keys** (`/settings/ssh-keys`) and click **Add SSH key**.

Registration is a two-step possession challenge, not a simple "paste your public
key" form:

1. **Prove you hold the private key.** Clicking "Add SSH key" mints a one-time
   challenge string tied to your account (valid 5 minutes) and shows you the exact
   command to run locally:

   ```bash
   f=$(mktemp) && echo -n '<challenge>' > "$f" && \
     ssh-keygen -Y sign -f ~/.ssh/id_ed25519 -n srw-ssh-key-registration "$f" && \
     cat "$f.sig"
   ```

   (The command cockpit actually renders uses `mktemp` rather than a fixed path,
   and escapes the challenge string for the shell — shown simplified here.)

2. **Register the key.** Paste the public key and the signature that command
   produced back into the form, give the key a name, and submit.

This two-step dance exists because public keys aren't secret — copy-pasting one you
saw somewhere would otherwise be enough to claim it — and SSH key fingerprints are
globally unique, so letting anyone register a key they merely observed would
permanently lock the real owner out of registering their own. The signature proves
you hold the matching private key before the server accepts the claim.

A key added to your account is a security-relevant event: you'll get a notification
("New SSH key added: `<name>`") so a key added by someone else — a stolen session, a
shared account — doesn't go unnoticed.

## 2. Connect

### The credential: a PAT, exchanged for a short-lived attach token

Registering a key proves *identity*. Actually opening a connection also needs a
**Personal Access Token (PAT)**, because the connection is a browser-less client
talking to an authenticated API, and the helper needs something to authenticate
that first hop with. The plan's own config block doesn't mention this step, so it's
easy to register a key, paste the config, and get an opaque failure — do this first:

1. Go to **Settings → API Keys**, create a token with the **`chat:write`** scope
   (the default `jobs:read` + `chat:read` is not enough — an interactive shell is
   a write), and copy it. It looks like `ak_…` and is shown exactly once. A token
   without `chat:write` fails the exchange with
   `token exchange refused (403): PAT lacks the chat:write scope`. The shell you
   get runs with your SSH key's authority, not the token's: for an admin's
   account, a `chat:write` token (even without the `admin` scope) opens every
   workspace an admin's registered key can reach.
2. Save it to `~/.config/srw/token` (create the file with mode `0600` — the helper
   warns on stderr if it's group- or world-readable, since it holds a bearer
   credential), **or** export it as `$SRW_TOKEN` in your shell.

From there it's automatic: on **every** connection, `srw-ssh-proxy` exchanges that
PAT for a fresh, short-lived *attach token* (`POST /api/ssh/attach-token`) before
opening the WebSocket. That attach token lives **300 seconds** server-side and is
never written to disk — you never see it, and there's nothing to copy.

There is a second, different environment variable, `$SRW_SSH_TOKEN`, which is **not**
your PAT — it's a way to hand the helper an *already-minted* attach token directly
(for example one pasted from a debugging session), skipping the exchange entirely.
Because that token expires in five minutes, **`$SRW_SSH_TOKEN` is useless in a config
file or a script meant to be reused** — it works once, then mysteriously stops. Use
`$SRW_TOKEN` (or the token file) for anything you intend to keep working.

**`$SRW_TOKEN` is also used, for a different purpose, by this repository's `bench/`
scripts** (an MCP token there, not a PAT — see `bench/README.md`). If you work in
`bench/` too, don't assume one exported value is correct for both; check which tool
you're about to run before trusting `$SRW_TOKEN` in your shell.

### Get the helper

Cockpit's connect panel expects a single file, `srw-ssh-proxy`, on your `$PATH`. It's
a dependency-free Python 3 script — no packaging, no `pip install` — that speaks the
gateway's `wss://` protocol directly, because there's no WebSocket client in the
Python standard library. Get it from this repository's `scripts/` directory and put
it somewhere on your `PATH`, e.g.:

```bash
install -m 755 scripts/srw-ssh-proxy ~/.local/bin/srw-ssh-proxy
```

**The helper always dials TCP port 443** — it's hardcoded, not derived from the API
hostname or any config. If your deployment serves the API (or the ssh-gateway
Ingress behind it) on a different port, `srw-ssh-proxy` cannot reach it at all; there
is currently no flag to override this.

### Paste the config, then connect

Open a session and click **SSH** in the header (only shown when the deployment has
SSH access configured). The panel renders a block like:

```
Host srw-s-7f3a91c2
    HostName            ssh.srw.works
    User                s-7f3a91c2
    ProxyCommand        srw-ssh-proxy --stdio api.srw.works --origin https://cockpit.srw.works
    IdentityFile        ~/.ssh/id_ed25519
    IdentitiesOnly      yes
    PreferredAuthentications publickey
    ServerAliveInterval 30
    ServerAliveCountMax 3
    ControlMaster       auto
    ControlPath         ~/.ssh/srw-%C
    ControlPersist      10m
```

Append it to `~/.ssh/config`, then:

```bash
ssh srw-s-7f3a91c2
```

A few things worth knowing about this block:

- **`HostName` is never dialled.** `ProxyCommand` short-circuits DNS entirely — the
  proxy connects over `wss://` to `apiHost`, not `sshHost`. `HostName` exists purely
  as the `known_hosts` key, so the gateway's host key lands under one stable name.
  Don't expect `ssh.srw.works` to resolve or be pingable.
- **`--origin` is always present**, and it's the exact origin your browser was
  serving cockpit from when the panel rendered. The gateway checks it. See
  [Troubleshooting](#7-troubleshooting) if this 403s.
- **`IdentitiesOnly yes`** stops your client from offering every key in your agent —
  offering the wrong keys first risks `MaxAuthTries` lockout and would mis-attribute
  which key's `last_used_at` gets bumped.
- **`IdentityFile` assumes `~/.ssh/id_ed25519`.** That's the only path the config
  block generates, but the server accepts seven key types — Ed25519, ECDSA, RSA and
  FIDO2 hardware-backed (`sk-*`) variants (`src/orchestrator/services/ssh_public_keys.py`).
  If the key you registered lives somewhere else — a different filename, a different
  algorithm — edit `IdentityFile` in the block above to point at it before
  connecting. Combined with `IdentitiesOnly yes`, a wrong or missing path here means
  `ssh` offers nothing and fails with `Permission denied (publickey)` — see
  [Troubleshooting](#7-troubleshooting).
- **`ControlMaster`/`ControlPath`/`ControlPersist`** let one authenticated attachment
  serve a follow-up `ssh`, `scp`, or `-L` for 10 minutes without re-authenticating —
  this is also what keeps you under the gateway's per-workspace attachment cap if you
  open several tools at once.
- The panel also shows the **gateway host key fingerprint** — verify it on first
  connect, the same as you would for any new host.

`scp` and SFTP both work through this block unchanged (verified end-to-end: upload
and download both round-tripped correctly), since the gateway implements a real
`sftp` subsystem rather than refusing it.

**`rsync` does not work — the workspace image doesn't ship it.** `rsync` execs a
remote `rsync --server` over the connection, and there is no `rsync` binary in the
workspace container (confirmed live: `which rsync` inside a running workspace pod
exits 1; the package list `docker/Dockerfile.workspace` installs doesn't include it
either). It fails immediately with "command not found" rather than anything
gateway-specific. Use `scp` instead.

## 3. JetBrains Gateway

**Do not use the config block above for JetBrains Gateway.** Gateway ignores
`IdentityFile`, `User`, `Port`, and `ControlMaster` — they're simply not in its
supported-directive list — and there is no "use system OpenSSH executable" setting
to fall back on. Trying to point Gateway at the `Host srw-s-…` alias will fail in
confusing ways.

Instead, run the helper as a **local listener** — the connect panel's copy button
renders the exact command for your deployment, `--origin` included (see below):

```bash
srw-ssh-proxy --listen 127.0.0.1:2222 api.srw.works --origin https://cockpit.srw.works
```

Then in JetBrains Gateway: connect via SSH to `127.0.0.1:2222`, authentication type
**Key pair**, pointed at your registered private key, and switch **"Parse config
file" off**. This was verified end-to-end here with a plain `ssh` client standing in
for Gateway: the listener accepts a connection with no credential of its own beyond
the PAT set in its own environment and mints an authenticated tunnel — which is
exactly the property to be aware of below.

**`--listen` is an unauthenticated local door.** Anything on your machine that can
reach `127.0.0.1:2222` gets a fully authenticated tunnel into your workspace, using
the PAT from `srw-ssh-proxy`'s own environment — no further credential is asked for.
Don't bind it to anything but loopback, and don't run it as a long-lived background
service on a shared or multi-user machine.

**Windows is not supported.** The helper uses `select()` on stdin and `os.read()` on
a raw socket file descriptor, both POSIX-only — it will not run under a plain Windows
Python (WSL is fine). This matters more here than in the plain-`ssh` case: JetBrains
Gateway's user base skews Windows, and `--listen` is the flag that exists specifically
for Gateway. State this plainly rather than let a Windows user discover it as a
traceback.

**The first attach downloads a large IDE backend.** Gateway itself only uploads a
small worker over SFTP (roughly 2.4 MB, from the design estimate — not re-measured
here) — but that worker then has **the workspace** download the JetBrains IDE
backend itself from JetBrains' own CDN. Expect roughly 5 GB (also from the design
estimate, not measured in this deployment — no JetBrains client was available to
trigger a real attach and watch it happen). Two consequences either way:

- The workspace needs egress to JetBrains' CDN. If your deployment restricts
  workspace egress, this will hang or fail rather than error clearly.
- The workspace needs several GB of free disk, on top of whatever your repo
  checkouts and caches already use, against workspaces provisioned with a 10Gi
  volume. Check free space before the first attach, or it can fill the volume.

**The command the panel's copy button gives you already includes `--origin`** — it
carries the exact origin your browser used to load cockpit, the same value the
config block in [Connect](#2-connect) carries. You only hit the guess described
below if you type or script the command yourself instead of copying it.

If you do, and omit `--origin`, `srw-ssh-proxy` falls back to guessing
`https://cockpit.<domain>` from `api.<domain>`. **That guess is wrong on this
chart's own default topology**: cockpit defaults to the apex domain
(`global.hostnames.cockpit` is unset → `https://<domain>`, not
`https://cockpit.<domain>` — `helm/values.yaml`), so the guess produces an origin
that doesn't exist and the gateway's exact-match Origin check fails closed with a
bare 403 — for the one client here that can't fall back to the config block. Always
pass `--origin <your-cockpit-origin>` explicitly if you're not copying the panel's
command verbatim; this flag works identically in `--listen` mode.

## 4. VS Code Remote-SSH

The config block from [Connect](#2-connect) works verbatim — Remote-SSH shells out to
your system's real `ssh` client, so nothing about it is JetBrains-shaped. Its host
requirements are different, though: the workspace needs `bash`, `tar`, and either
`curl` or `wget`, with a glibc of 2.17 or newer.

## 5. What does not work, and why

The gateway refuses several things `ssh` can normally do. The first four rows below
were reproduced live against a running gateway, not just read out of the source; the
VM-tier row is a code-level fact only (this deployment had no VM-tier workspace to
test against):

| Feature | Behaviour | Why |
|---|---|---|
| `ssh -R` (remote port forwarding) | Refused. The client prints `Warning: remote port forwarding failed for listen port N` and the rest of the session continues normally | Not implemented — asyncssh's `tcpip-forward` handler is never installed |
| SSH agent forwarding (`-A`) | Refused. `$SSH_AUTH_SOCK` is simply never set in the remote shell | Means **no `git push` using your local key** from inside the workspace — the agent socket never reaches it |
| `ssh -J` (jump host / `ProxyJump`) | Cannot work at all | `ProxyJump` needs a `direct-tcpip` channel to an arbitrary destination host, and the gateway only permits a `direct-tcpip` destination of `127.0.0.1`/`localhost` — anything else is declined outright, with no dial attempted at all. (Confirmed live: a forward aimed at a real external address came back `connect failed: Connection refused`. That text is not evidence of an attempted-and-failed dial — it's asyncssh's fixed, generic literal for *any* declined channel-open request, emitted identically regardless of why. The gateway doesn't redirect the destination anywhere; it just says no.) |
| `ssh -L` / `-D` to a service *inside* the workspace | Works | Same `direct-tcpip` path, but the destination you ask for already *is* loopback, so the permit check passes |
| VM-tier workspaces | Refused with exit 77, `"this workspace is VM-tier - SSH access is not supported"` | Not implemented for that backend |

The refusal for a non-loopback `-L`/`-J` destination surfaces at **first use of the
forward**, not at `ssh` startup — `-L` itself will appear to succeed silently; the
`Connection refused` only shows up once something tries to use the tunnel.

## 6. Working alongside the agent

The workspace you're SSHing into is the same one the session's agent is working in.
Two seams follow directly from that, and they're easy to get burned by if you don't
expect them:

- **Edits you make over SSH land in the agent's next per-turn commit**, and show up
  in review as the agent's own changes — there's no separate "user edit" marker.
- **A session rewind discards them.** Rewinding a session resets the workspace to an
  earlier point, the same as it would any other uncommitted or since-superseded
  agent work — your SSH edits are not special-cased.

This isn't a bug to work around; it's documented behaviour. If you're editing files
by hand alongside an active agent, treat it the same way you would editing a
teammate's uncommitted branch: coordinate, or expect your changes to become "agent
changes" in the diff.

## 7. Troubleshooting

**`token exchange refused (403): PAT lacks the chat:write scope`.** The PAT is
valid but was created without `chat:write` — the default `jobs:read` + `chat:read`
set cannot open a shell. Create a new token with `chat:write` in **Settings → API
Keys** and replace `~/.config/srw/token` (or `$SRW_TOKEN`). A plain
`token exchange refused (401|403): bad or unscoped PAT` instead means the token
itself was rejected — revoked, expired, mistyped, or its owner is no longer
approved.

**403 at the WebSocket upgrade, before any SSH banner at all.** This is an Origin
problem, not a credential problem. The gateway enforces an exact-match allow-list
with no default in either direction, and it's stricter than "misconfigured at
connect time" — a *completely empty* `sshGateway.allowedOrigins` can't ship at all:
the Helm chart refuses to render (`helm template` with an empty list fails with
"sshGateway.enabled requires a non-empty sshGateway.allowedOrigins", confirmed live),
and even past that guard the gateway's own `load_config()` raises at boot, before the
`/api/ssh/attach` route exists. So an *empty* list is never what you're looking at in
production. What reaches this 403 in practice is a **non-empty but wrong** entry —
the allow-listed origin has the wrong scheme (`http` vs `https`), the wrong port, or
doesn't match the exact subdomain cockpit is actually served from. `srw-ssh-proxy`
prints the Origin it sent on this error, which is the fastest way to compare it
against what the operator configured:

```
srw-ssh-proxy: upgrade refused: HTTP/1.1 403 Forbidden (sent Origin: 'https://cockpit.srw.works'; a
bare 403 here means the attach token was expired/invalid, the Origin was rejected, or the
handshake rate limit was hit -- retry for a fresh token, or pass --origin if the Origin is wrong)
```

The gateway deliberately doesn't distinguish an expired/invalid attach token, a
rejected Origin, and the handshake rate limit in this response — all three render as
the same bare 403 — so if retrying (which gets a fresh attach token) doesn't help,
suspect Origin next.

**`Permission denied (publickey)` from your own `ssh` client.** Two different causes
produce this exact message, and `ssh` gives no way to tell them apart:

- **No key at the `IdentityFile` path.** The config block assumes `~/.ssh/id_ed25519`
  (see [the note above](#2-connect)); with `IdentitiesOnly yes` also set, a missing
  or wrong path means your client offers no key at all. If you registered a key
  stored somewhere else, edit `IdentityFile` to match. This is the most common
  real-world cause — check it first.
- **The handle in `User s-…` is not even a syntactically valid handle** — a typo, a
  stale copy-paste, or a hand-edited config — so the gateway declines to attempt key
  authentication at all for that username.

Neither of these means "your key isn't registered" — see the next entry for that
case, which looks different.

**`srw: no such workspace, or you do not have access to it` (exit 77).** This is the
gateway's *authorization* refusal, and it's deliberately worded identically for two
different situations: your key isn't registered to any account, **or** the handle is
syntactically valid but doesn't belong to you (or doesn't exist at all). Both come
back as this exact message and exit code — the gateway does not give a prober any
signal to tell "wrong key" apart from "not your session."

**Exit codes from inside the SSH channel.** Once a key authenticates, a refusal is
reported by the gateway itself — inside the encrypted SSH channel, as a
`srw: <reason>` line on stderr plus `ssh`'s own exit code. `srw-ssh-proxy` has
nothing to do with these; it only relays opaque frames below this layer and cannot
read them:

| Exit | Meaning | Reasons |
|---|---|---|
| **75** | Retry later — nothing else needs to change | workspace suspended, reclaimed while idle, shutting down, still restoring, a stale SSH binding, or **too many SSH connections to this workspace already** — close one and reconnect |
| **69** | Gone — retrying alone won't help | workspace failed, deleted, this session ended, never had a workspace, is unreachable right now, or **the gateway itself is misconfigured** — report this to an administrator, since reconnecting can't fix it |
| **77** | Denied | unknown/unauthorized handle, unregistered key, or a VM-tier workspace |

The attachment-cap and misconfiguration rows aren't tied to workspace state the way
the others are — they come from the gateway's own resource limits and boot-time
config, respectively — but they use the same exit-code channel and are worth
recognizing if you hit them.

The one you'll hit most often is a **suspended workspace**:

```
srw: workspace suspended - resume the session in cockpit, then reconnect
```

This is not an error to debug — resume the session in cockpit (which restores the
workspace from its snapshot if it had been reclaimed) and reconnect; it just works.

**Idle disconnects.** If a connection drops after sitting quiet for a while, check
that `ServerAliveInterval 30` / `ServerAliveCountMax 3` are still in your config
block — they're there specifically to survive infrastructure that closes idle
connections after roughly a minute or two, and if you hand-edited or partially
copied the block, it's easy to drop them by accident. (The helper itself also sends
its own keepalive frames on the WebSocket independently of this setting, so this
mainly bites hand-trimmed configs.)
