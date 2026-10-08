# Build your own connector driver

A connector gives an agent access to something outside SRW: a database, a
repository, an API key. Every connector runs a *driver*. SRW ships drivers for
its built-in connector types; this guide takes you from "I need to hand my
agents a credential SRW has no connector for" to a connector of your own
driver in a Session: write the driver, check it with the test kit, build an
image, register it, and attach a connector of it.

The guide follows SRW's example driver, `example.env/v1` in
[`drivers/example`](../drivers/example). It holds a token, mints a credential
from it for each Job or Session, and puts that credential in the workspace as
an environment variable and a file.

## When to build one

Use a built-in connector first. **Credentials** or **Generic** puts environment
variables in the workspace and **Generic file** puts a file there; neither
needs an image.

Build a driver when the value the workspace receives should be *computed*:
a short-lived token minted for each execution, a credential scoped down from
the one you store, a config file assembled from several settings. Your driver
runs at bind time: once for each Job or Session that attaches its connector,
in a short-lived pod SRW starts for that purpose.

What a driver can do in this release:

- **Return data, never commands.** A bind returns environment variables and
  credential files for the workspace. SRW writes them over its own channel,
  and no driver image gets a shell in a workspace. What it returns is still
  data the workspace's programs read: SRW refuses the variable names on its
  list of known tool hooks and the file locations whose formats run a
  command (see [what a binding may hold](#what-a-binding-may-hold)), but that
  list is a best-effort lint, not a sandbox. **The author of a driver image is
  the trust boundary**, as the author of a workspace image is: register only
  images whose authors you trust with the connector's credentials and with
  what lands in your workspaces.
- **Run unprivileged.** Its pod runs as the image's own user (root included),
  with every Linux capability dropped, no privilege escalation, seccomp
  `RuntimeDefault`, no ServiceAccount token and no ingress. Its egress is
  limited to the hosts its spec declares (see [Egress](#egress)).
- **See only its own connector's credentials.** The pod gets the connector's
  config and credentials in its own Secret and nothing of SRW's.

## How a driver works

SRW runs the driver once per operation. Each run is a pod that starts, runs
the driver to completion and is deleted:

1. SRW writes the request to `/run/srw/request.json` (also named by
   `SRW_REQUEST_FILE`).
2. SRW's shim runs your image's entrypoint. Your program reads the request and
   writes one JSON object per line to **stdout**. Anything on stderr goes to
   the pod log only.
3. The shim posts the lines and your exit code to SRW. Exit 0 means you wrote
   exactly one `result` line; a non-zero exit means exactly one `error` line.

The operations:

| Operation | When SRW runs it | Your answer |
| --- | --- | --- |
| `spec` | At registration, only when the image has no spec label | The spec, as the result |
| `check` | **Test connection** on a connector | `{"status": "SUCCEEDED" or "FAILED", "message": ...}`. A wrong config is `FAILED`, never an error |
| `bind` | Each Job or Session that attaches the connector, as soon as it is created or the connector is selected | `{"binding": {...}}`, optionally with a `driver_state` (below) |
| `revoke` | When that Job or Session ends, the connector is detached from it, changed or deleted, or its registration is disabled | `{}`. It must succeed when the binding is already gone |
| `gc` | Never, in this release: the test kit checks it when you declare it in `operations` | `{}`; retire anything you minted that `live_binding_ids` doesn't name |

A request looks like this:

```json
{
  "protocol_version": "1.0",
  "operation": "bind",
  "binding_id": "6f0d…",
  "connector": {"config": {"file": true}, "access": "ReadWrite"},
  "credentials": {"token": "…"},
  "execution": {"kind": "session", "id": "…", "project_id": "…", "workspace_backend": "sandbox"}
}
```

`binding_id` is stable for one binding: a `bind` may run again for the same
id, and must answer the same way. `driver_state` is an opaque string your
`bind` may return. SRW stores it encrypted, never shows it to the agent, and
hands it back to `revoke`, so a driver can revoke what it minted after its
bind pod is long gone. **Put everything your `revoke` needs into
`driver_state`**: the upstream id of what you minted, which upstream it lives
on. `revoke` also receives the connector's config and credentials as they were
at that bind (kept encrypted on the binding until it is revoked), but the
connector may have changed or be gone by then, and its revoke still runs.

An error line names a class:

| Class | Meaning | SRW does |
| --- | --- | --- |
| `config` | The connector's config is wrong (`field` names it, a JSON pointer) | Shows it on the connector |
| `credentials` | The stored credentials are wrong (`field` names the slot) | Shows it on the connector |
| `permission` | The upstream refused | Shows it |
| `transient` | Try again later (`retry_after_s`) | Retries with backoff: a bind up to 6 times, a revoke up to 12 |
| `unsupported` | You don't implement the operation | Never retries |
| `system` | Your driver broke | Shows it |

A bind that fails with any class but `transient` (or gives up retrying) is
final for that Job or Session until the connector, its registration or its
access changes: a Job fails before it starts; a Session goes on without the
connector, and its workspace README and the connector say why. `message` is
what the connector's owner and administrators see, with control characters
removed and cut at 500 characters; everyone else (another project member's
Job or Session, and its agent) reads "its driver reported an error" instead,
so your text never reaches a workspace you were not trusted with. `detail`
goes to the operator log only; never put a secret in either.

The JSON Schemas of these messages ship with SRW:
[`request.schema.json`](../src/shared/connectors/request.schema.json),
[`output-line.schema.json`](../src/shared/connectors/output-line.schema.json),
[`spec.schema.json`](../src/shared/connectors/spec.schema.json) and
[`binding.schema.json`](../src/shared/connectors/binding.schema.json).

## 1. Write the spec

The spec says what your driver is. SRW reads it from the image's
`io.srw.driver.spec` label without running the image, so put it in a file and
add it as a label when you build. The example's
[`spec.json`](../drivers/example/spec.json), shortened:

```json
{
  "name": "example.env/v1",
  "title": "Example environment driver",
  "protocol_version": "1.0",
  "plane": "bind_time",
  "delivery_forms": ["env_file", "credential_file"],
  "env_names": ["EXAMPLE_TOKEN", "EXAMPLE_TOKEN_FILE", "EXAMPLE_DRIVER_PROCESS"],
  "config_schema": {
    "type": "object",
    "additionalProperties": false,
    "properties": {"file": {"type": "boolean"}}
  },
  "credential_slots": [
    {
      "name": "token",
      "kind": "secret_string",
      "schema": {"type": "object", "properties": {"token": {"type": "string", "writeOnly": true}}},
      "required": true
    }
  ],
  "access_levels": [
    {"id": "ReadOnly", "rank": 0, "enforced_by": "Told to the agent only.", "advisory": true},
    {"id": "ReadWrite", "rank": 1, "enforced_by": "The upstream credential decides."}
  ],
  "supported_backends": ["sandbox", "vm"],
  "operations": ["gc"],
  "egress": []
}
```

The rules SRW applies when you register:

- **`name`** is `<namespace>.<driver>/v<major>`, lowercase. Use a namespace
  you own, such as your company's. `srw.` is SRW's own and is refused. The
  major is your config contract: change it when a connector's stored config
  would no longer work.
- **`plane`** is `bind_time`. (`service` drivers are managed MCP servers, which
  you import from a `server.json`; see [MCP servers](#mcp-servers).)
- **`delivery_forms`** lists what your bind returns: `env_file`,
  `credential_file` or both.
- **`env_names`** lists every environment variable your bind may set, a
  file's `env_var` included, each at most 128 characters. A driver that
  returns `env_file` must declare them: they are shown on the driver, on each
  of its connectors and in the attach picker, before anyone attaches one.
  None may be a variable a driver may not set (see
  [what a binding may hold](#what-a-binding-may-hold)).
- **`config_schema`** is a JSON Schema (2020-12) for the connector's config.
  SRW validates every connector against it when it is saved, on its own
  servers, so a registered schema holds no regular expression anywhere in
  the document (`pattern`, `patternProperties`, `format: regex`, whatever key
  holds them), a `$ref` points only at `#/$defs/<name>` or
  `#/definitions/<name>` of the same document, no `$id`, `$anchor`,
  `$dynamicAnchor`, `$dynamicRef` or `$recursiveRef` appears, `$schema` is
  2020-12 or absent, and it stays under 32 KiB and 12 levels deep. Check a
  pattern in your `check`.
- **`credential_slots`** name the parts of the connector's credentials. Each
  slot's `schema` lists the keys it owns (the same schema rules apply); mark
  secrets `writeOnly`. A connector may only store keys some slot owns, and
  must store a `required` slot.
- **`access_levels`** say what `ReadOnly` and `ReadWrite` mean for your
  driver, and `enforced_by` says what makes each true. Mark a level `advisory`
  when nothing enforces it.
- **`supported_backends`** is a subset of `sandbox` and `vm`: what your driver
  returns goes into a shell workspace.
- **`egress`** lists the hosts your driver's pod connects to; see
  [Egress](#egress).

Every key you write is read; an unknown key is refused, never ignored.

## 2. Write the driver

The example is [`srw_example_driver.py`](../drivers/example/srw_example_driver.py),
Python's standard library only. Its bind derives a credential for this binding
from the connector's token and returns it:

```python
def bind(request):
    token = request["credentials"]["token"]
    value = minted(token, request["binding_id"])  # never the token itself
    entries = [variable("EXAMPLE_TOKEN", value)]
    if request["connector"]["config"].get("file"):
        entries.append(
            credential_file("~/.srw-files/example/token", value, "EXAMPLE_TOKEN_FILE")
        )
    return result(
        {
            "binding": {
                "driver": "example.env/v1",
                "name": "example",
                "access": request["connector"]["access"],
                "entries": entries,
            }
        },
        # What revoke needs to revoke this credential upstream.
        driver_state=json.dumps({"minted": fingerprint(value)}),
    )
```

### What a binding may hold

- `env_file` entries: `{"name": ..., "value": ...}`. The name is one your spec
  declares in `env_names`. SRW refuses the names on its list of known tool
  hooks, compared in any case: SRW's own (`PATH`, `HOME`, `SRW_*`, `LD_*`,
  `PYTHON*`…), the `GIT_*` and `SSH_*` families, every `*_PROXY`, the CA and
  TLS variables (`SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`,
  `NODE_EXTRA_CA_CERTS`…), `KUBECONFIG`, `DOCKER_*`, `XDG_*`, `TMPDIR`,
  `HISTFILE`, shell prompts, editors and pagers (`PS1`, `EDITOR`, `*PAGER`,
  `*EDITOR`, `LESSOPEN`, `*BROWSER`), `*ASKPASS`, every name ending in
  `_OPTS`, `_OPTIONS`, `FLAGS`, `_COMMAND`, `_ARGS`, `RC`, `RCPATH`,
  `_CONFIG`, `_CONFIG_FILE`, `_CONFIG_PATH`, `_CONFIG_DIR` or `_HOME`, and the
  settings of the common runtimes, build tools and package managers
  (`NODE_*`, `NPM_CONFIG_*`, `PIP_*`, `UV_*`, `CARGO_*`, `GRADLE_*`,
  `MAVEN_*`, `YARN_*`, `COREPACK_*`, `ERL_*`, `ELIXIR_*`, `CMAKE_*`, `TF_*`,
  `ANSIBLE_*`, `CLOUDSDK_*`, `JULIA_*`, `JUPYTER_*`, `DENO_*`, `BASH_*`,
  `PERL5*`, `RUBY*`, `DOTNET_*`, `BUN_*`, `JAVA_HOME`, `CC`, `GOPROXY`…). The
  full list is [`env_names.py`](../src/shared/connectors/env_names.py), the
  one SRW's managed MCP servers are checked against too. It is a
  best-effort lint against known hooks, not a sandbox: a tool it does not
  know may read a name it does not list. Credential-shaped names
  (`AWS_ACCESS_KEY_ID`, `PGPASSWORD`, `DATABASE_URL`, `GITHUB_TOKEN`,
  `OPENAI_API_KEY`…) are yours to set. A value is a string of at most 64 KiB
  without NUL bytes.
- `credential_file` entries: `{"path": ..., "content": ..., "mode": 384,
  "env_var": ...}`. The path is `~/.srw-files/…`, `~/.netrc` or `~/.pgpass`,
  at most 255 characters, and one binding writes each path once. The other
  places a built-in credential file may land (`~/.kube/`, `~/.aws/`,
  `~/.docker/`, `~/.config/<app>/`) hold formats that run a command (a
  kubeconfig's `exec`, an AWS `credential_process`), which a driver's output
  may not carry in this release; for the same reason a `~/.netrc` may not
  define a macro (`macdef`). A file is never executable. `env_var`, when
  set, names the file in the environment and follows the variable rules.
- Every entry's `recipient` is `workspace`.

A variable two connectors of one Job or Session would set is never
delivered twice: when your binding sets a name an environment connector, a
credential file's `env_var` or another registered driver's connector (the
one with the lower connector id) already sets, a Session goes on without
your connector and its README says which name and which connector; a Job is
refused before it starts with the same reason.

SRW checks the binding before anything reaches the workspace. A binding it
won't deliver fails the bind with the reason on the connector, and SRW revokes
what your bind minted (with its `driver_state`).

## 3. Check it with the test kit

`scripts/srw-driver-test.py` runs your driver the way SRW does, one request
file per operation, and checks spec, check, bind, revoke, revoke again and gc
against the same rules SRW applies: the bind's output goes through exactly
SRW's own check (and the message schemas, when `jsonschema` is installed).
Give it a fixture: the config and credentials a connector would hold.

```json
{
  "config": {"file": true},
  "credentials": {"token": "a-test-token"},
  "expect_check": "SUCCEEDED"
}
```

Run it against your program while you write it:

```bash
python3 scripts/srw-driver-test.py \
    --spec-file drivers/example/spec.json --fixture drivers/example/fixture.json \
    -- python3 drivers/example/srw_example_driver.py
```

and against your image once it is built (step 4); it runs the image with
docker, without network, with the request mounted read-only:

```bash
python3 scripts/srw-driver-test.py --image "$IMAGE" --fixture fixture.json
```

Each step prints `PASS` or `FAIL` with the reasons; the exit status is 0 only
when all pass. Use test credentials in the fixture: they go to your driver.

## 4. Build and push the image

Any base works; pin it by digest so a rebuild is the image you tested. The
example's [`Dockerfile.driver-example`](../docker/Dockerfile.driver-example)
copies the program onto a pinned `python:3.12-slim` and runs it as an
unprivileged user:

```dockerfile
FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de
COPY drivers/example/srw_example_driver.py drivers/example/spec.json /driver/
USER 10001:10001
ENTRYPOINT ["python3", "/driver/srw_example_driver.py"]
```

Set `ENTRYPOINT` (or `CMD`) to your program: SRW's shim runs exactly that. Add
the spec as a label when you build; a JSON label is safest on the command
line:

```bash
IMAGE=registry.example/team/example-driver:1.0
docker build -f docker/Dockerfile.driver-example \
    --label "io.srw.driver.spec=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])), separators=(",", ":")))' drivers/example/spec.json)" \
    -t "$IMAGE" .
docker push "$IMAGE"
```

Without the label (an empty one counts as none), SRW runs your image's `spec`
operation once at registration, in a pod with no secret and no egress, and
uses its answer.

Push it to a registry SRW can read without credentials, or one your operator
configured. On the local cluster from [Local Kubernetes with k3d](local-kubernetes.md),
push to `localhost:5005/<name>:<tag>` and register
`srw-registry:5000/<name>:<tag>`.

### Versions

SRW never rewrites the reference you register. Each bind resolves it to a
digest and records it with the spec it ran with:

- A digest (`…@sha256:…`, or `…:1.0@sha256:…`) is exact.
- A tag is looked up at each bind. A version tag stays put as long as nobody
  pushes it again; a moving tag such as `latest` follows your releases. SRW
  can't tell them apart: only a digest is a guarantee.
- When a tag moves, the new image's label is compared with the spec the
  connector last bound with. The next bind (and **Test connection**) is
  refused with "The image behind … changed its contract (…): pin a digest or
  register a new driver major." when the new image has no label, declares
  another name or plane, changes the protocol major, drops a credential slot,
  an access level or a workspace backend, adds a required slot or changes
  one's schema, sets new environment names, returns new forms, reaches new
  egress or newly needs DNS, or the connector's stored config no longer
  validates. Release breaking changes under a new name (`…/v2`). An image
  without a label binds only while the tag still points at the registered
  digest: SRW cannot read a moved image's contract, so it refuses it; pin a
  digest, or add the label.
- When the registry can't be reached, a bind reuses the digest it resolved
  last, and the connector's `driver_status.last_bind.stale` says so.

## 5. Register it

Register the image where its connectors' owners can use it:

| Where | Who may register, disable and delete | Who may use it |
| --- | --- | --- |
| Your Account | You | You |
| A Project | The project's owners and editors | The project's members |
| The shared Catalog | Administrators | Everyone |

In the cockpit, open **Settings → Connector drivers**, paste the reference under
**Register a driver image**, choose **Mine** or (administrators) **Shared**, and
click **Register**. Over the API:

```bash
curl -X POST "$SRW/api/connector-drivers" -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d '{"image": "registry.example/team/example-driver:1.0",
         "scope": {"kind": "Project", "name": "<project UUID>"}}'
```

Leave `scope` out for your Account. `GET /api/connector-drivers` lists the
registrations you can see (an administrator may also read another user's by
id, for support); each says whether you may manage it (`can_manage`) and,
if you may, how many connectors use it and how many of its bindings are live
(`usage`). `POST /api/connector-drivers/<id>/disable` is the kill switch: the
driver binds nothing new, SRW revokes every live binding of it, and its
connectors show "registration disabled"; `/enable` undoes it. The cockpit
offers Disable and Delete only to those who may use them and asks before a
Disable, naming the live bindings it revokes.
`DELETE /api/connector-drivers/<id>` removes a registration once SRW revoked
all its bindings, if no connector uses it or it is disabled.

Names, per scope:

- One registration per name in each Account, Project and in the Catalog.
- A registration in an Account or a Project may not reuse a name the Catalog
  has: nobody shadows what administrators curated.
- A connector pins the registration it was created with. A connector created
  by driver name takes the Catalog's; otherwise the name must match exactly one
  registration in the project it is created for and your Account, or the
  request is refused as ambiguous with the ids to pick from
  (`driver_registration_id`).

The driver then appears on **Settings → Connector drivers** with its access
levels, credential slots, egress and the variables it sets. Unless your
operator trusts its repository, every claim there is marked as declared by its
author.

## 6. Create a connector and use it

Create a connector of type `image_driver` that names the registration (or the
driver's name), with config and credentials for your spec:

```bash
curl -X POST "$SRW/api/datasources" -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d '{"name": "example", "type": "image_driver",
         "driver_registration_id": "<registration id>",
         "config": {"file": true},
         "credentials": {"token": "…"}}'
```

Select it on a new Job or Session like any connector. SRW runs your bind as
soon as the Job or Session is created (or the connector is selected): a Job
starts once it is bound, a Session's first attach waits for it briefly. Its
variables are sourced for every command in the workspace and its files are
linked at their paths. Detach it from a live Session and SRW runs your
`revoke` within a reconciler pass; the workspace drops its files, and a
pinned Session stops setting its variables for new commands (a stateless
Session keeps them set, holding the revoked credential, until it ends).
Change its config or credentials and every binding of it is revoked; the next
delivery binds again. A Job or Session that no longer selects the connector,
or may no longer use it (its owner left the project, the connector's project
link was removed), has its binding revoked within a reconciler pass
(`access_lost`), a paused Job's included. **Test connection** runs your
`check`.
`GET /api/datasources/<id>` shows the registration and how the last bind went
in `driver_status`.

## Egress

A driver pod reaches nothing but SRW's result endpoint and the hosts it
declares:

```json
"egress": [{"host": "${config.host}", "ports": [443], "protocol": "tcp"}],
"needs_dns": null
```

`host` is a literal host, a CIDR, or `${config.<key>}`; ports may be
`${config.<key>}` too. SRW resolves each host once when it starts the pod,
refuses cluster-internal addresses (and private ones unless every project of
the connector allows them), and writes the answer into the pod's
NetworkPolicy and `/etc/hosts`. The pod has no DNS unless you set
`needs_dns` to a reason, which also lifts the name restriction.

## MCP servers

A packaged MCP server from the MCP Registry registers from its `server.json`:

```bash
curl -X POST "$SRW/api/connector-drivers/import" -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' -d "{\"server\": $(cat server.json)}"
```

SRW maps its `oci` package to a managed MCP driver: the image, the transport,
port and path, and non-secret environment variables as config. npm, PyPI and
mcpb packages, remote servers and secret environment variables are refused
with the reason. In this release an imported server is registered and listed;
binding its connectors comes with the service plane's support for
registrations.

## When it doesn't work

| Symptom | Cause | Fix |
| --- | --- | --- |
| Registration answers "The image has no io.srw.driver.spec label, and this installation runs no driver pod…" | No label, and driver pods are off. | Build with the label (step 4), or ask your operator to turn on `connectors.servicePods.enabled`. |
| "The driver image's spec is refused: …" | The spec breaks a rule of step 1 (an undeclared or forbidden variable name, a `pattern` in a schema…). | Fix what it names; run the test kit. |
| "driver names under srw. are SRW's own" | The name uses SRW's namespace. | Use your own namespace. |
| 409 "The shared Catalog has a driver named …" | The name exists in the Catalog. | Use the Catalog's driver, or another name. |
| 409 "Ambiguous driver name …" | Your Account and the project both register the name. | Pass `driver_registration_id`. |
| A Job waits before it starts, or a Session's README says "Not delivered yet: its driver is still binding"; `driver_status.last_bind.status` is `pending` | The bind pod is starting (the image is being pulled the first time). | Wait; a Session gets it at its next attach or connector change. |
| `last_bind` says "The image behind … changed its contract …" | The tag moved to an incompatible image. | Register a digest, or a new major. |
| `last_bind` shows your driver's own message; a Job failed with it, or a Session's README says "Not delivered: …" | Your bind answered a final error. | Fix the config or credentials it names (that retries the bind); check the pod log for `detail`. |
| "The driver returned a binding SRW will not deliver: …" | An entry breaks the rules in step 2. | Fix what it names; run the test kit. |
| "the installation runs its cap of … bind-time driver pods" | Many binds at once. | Wait (SRW retries), or ask your operator to raise `connectors.servicePods.quota.bindTimePods`. |
| "the driver image could not be pulled (ErrImagePull)" | The registry refused the pull, or the image is gone. | Check the reference and that the registry serves it without credentials. |
| "The connector driver did not answer: …" | The pod never posted: the program crashed before writing, or the deadline passed. | Read the pod's events and log in the connector namespace; run the image with the test kit. |
| `driver_status.notice` is "registration disabled" | Someone disabled the registration. | Ask its owner, or move the connector to another driver. |
| A Session's README says "Not delivered: it sets X, which connector Y sets too", or a Job failed with it | Two connectors of the execution set the same variable. | Detach one, or change the variable one of them sets. |
| `last_bind.message` says "its driver reported an error (the connector's owner sees …)" | The driver's own message is shown to the connector's owner and administrators only. | Ask the connector's owner. |
| A `connector_driver_revoke_abandoned` security event; a binding's `revoke_error` says "gave up after …" | SRW could not revoke what the driver minted (12 transient failures, or a final error). It may still be live upstream. | Revoke it upstream by hand; the event names the binding, connector and registration. |

## For operators

| Chart key | Default | Effect |
| --- | --- | --- |
| `connectors.servicePods.enabled` | `false` | Driver pods run at all (the namespace, its baseline and quotas). |
| `connectors.drivers.trustedRepositories` | `[]` | Repositories whose images are trusted, on a path boundary (`ghcr.io/acme` trusts `ghcr.io/acme/driver`, never `ghcr.io/acme-evil`; a registry host alone or a bare name trusts no organisation). |
| `connectors.customDrivers.privileged` | `false` | Lets images outside the trusted list have privilege too (the in-pod plane, when it arrives). |
| `connectors.customDrivers.bindDeadlineSeconds` | `120` | A driver pod's `activeDeadlineSeconds`. |
| `connectors.customDrivers.bindWaitSeconds` | `20` | How long a Session's attach or claim waits for a new bind (at most 25, under the agent's 30 s request). A Job waits in the dispatcher instead, which never blocks. |
| `connectors.customDrivers.specPodsPerUser` | `2` | Spec-operation pods (registering an unlabelled image) one user may run at once. |
| `connectors.servicePods.quota.bindTimePods` | `10` | Live bind-time pods; the orchestrator refuses past it with a clear message, and the namespace quota is the backstop. |

Registration is never an admission control: any image may be a driver, as any
image may be a workspace. The controls are privilege, which custom images don't
get, and a best-effort check of what a driver's output may set in a
workspace; beyond them the image's author is the trust boundary, as a
workspace image's is. Disable is the kill switch when one turns out not to
be trustworthy.
