# Third-Party Licenses

This product (the "Software", licensed under FSL-1.1-ALv2 — see `LICENSE`) bundles third-party
open-source components. Their licenses are permissive or weak-copyleft and require
that we **reproduce their copyright and license notices** when we distribute the
Software. This file discharges that obligation for every bundled dependency.

> **This file is generated** from the locked dependency tree — do not hand-edit the
> inventory sections. Regenerate it whenever dependencies change (see
> [How to (re)generate](#how-to-regenerate)). The curated callouts at the top flag
> the components with extra obligations and must be kept in sync by review.

---

## Scope — what is and isn't covered here

**Covered** (libraries we *convey* — i.e. ship inside our distributed artifacts):

- **Backend:** Python packages installed into the orchestrator container image
  (`requirements.txt`, `src/orchestrator/requirements.txt`, `src/mcp_server/requirements.txt`).
- **Frontend:** npm **production** dependencies bundled into the cockpit browser/SSR
  build (`cockpit/package.json` → `dependencies`; `devDependencies` are build-time
  only and are not shipped).

**NOT covered, by design** — server software we deploy but do **not** redistribute:

- **Neo4j, PostgreSQL, MongoDB server images** are pulled by the customer's cluster
  **directly from their official public registries** via our Helm chart. We reference
  them; we never make or transfer a copy, so we are not "conveying" them under their
  licenses (this is why Neo4j Community Edition's GPLv3 does not reach us). Their
  notices travel with the official images.
- We **do** bundle their **client drivers** (`neo4j`, `psycopg`/`psycopg2-binary`,
  `pymongo`/`motor`) into our image — those are libraries we convey, so they **are**
  listed below.
- ⚠️ This exemption holds **only** while the customer pulls from the public registry.
  If you ever mirror, re-tag, cache, or ship those server images yourself (e.g. an
  **air-gapped install**), you become a distributor of that server and must add its
  source offer + notices for that delivery.

---

## Components with NOTICE-file obligations (Apache-2.0 §4(d))

Apache-2.0 requires that, where a dependency provides a `NOTICE` file, its contents be
reproduced in our distribution. The generator collects them via `--with-notice-file`
and reproduces them verbatim under [Backend NOTICE files](#backend-notice-files) — that
section is generated, not hand-maintained.

At the current pins, nine bundled packages actually ship a `NOTICE`: `aiofiles`,
`boto3`, `botocore`, `langdetect`, `neo4j`, `propcache`, `s3transfer`,
`sentence-transformers`, `yarl`.

> Being Apache-2.0 is **not** the same as shipping a NOTICE — §4(d) only bites where
> the dependency actually provides one. `aiohttp`, `kubernetes`, `motor`, `opentelemetry-api`,
> `pymongo` and `tenacity` are Apache-2.0 but ship no `NOTICE` in their wheels; `torch`
> ships a `licenses/` tree (LICENSE + `third_party/`) rather than a NOTICE. The generated
> section below is the source of truth — re-read it after every dependency bump.

---

## Weak-copyleft dependencies — know these

These are **not** permissive. They are fine to bundle in a source-available/proprietary
product **because we use them as unmodified, dynamically-importable libraries** (the
LGPL's relink/replace condition is satisfied by Python's import model; MPL-2.0 and
EPL-2.0 are file-level and only reach files we would have modified). Do **not** vendor a
*modified* copy without releasing those modifications. Flag all of these to counsel
before the on-prem launch:

| Component | License | Notes |
|---|---|---|
| `psycopg` (psycopg3) + `psycopg-binary` / `psycopg-pool` / `psycopg2-binary` | LGPL-3.0 | PostgreSQL driver. `psycopg2-binary`/`psycopg-binary` also bundle `libpq` (PostgreSQL License, permissive). Used via public API only. |
| `paramiko` | LGPL-2.1-or-later | SSH/SFTP client for the remote workspace backend. Used via public API only. |
| `asyncssh` | EPL-2.0 **OR** GPL-2.0-or-later | Dual-licensed; **we take the EPL-2.0 option** — the GPL operand never binds us. Host-key-pinned SFTP for the Dynamic Canvas workspace gateway. Used unmodified. |
| `jwcrypto` | LGPL-3.0-or-later | JOSE/JWT crypto, pulled in by `python-keycloak`. Used via public API only. |
| `certifi` | MPL-2.0 | CA bundle. File-level copyleft over a data file we never modify. |
| `tqdm` | MPL-2.0 AND MIT | Progress bars, transitive via `sentence-transformers`/`huggingface-hub`. Unmodified. |
| `orjson` | MPL-2.0 AND (Apache-2.0 OR MIT) | JSON codec, transitive via the LangChain stack. Unmodified. |

---

## Adapted prompt text

Prompt text taken from other projects, with the changes we made (Apache-2.0 §4(b)).
Hand-maintained; add a row whenever a prompt adopts third-party text.

| Our files | Source | License | Changes |
|---|---|---|---|
| The two compaction paragraphs ("When you run out of context…", "Compaction does not end the task…") in `config/prompts/systemprompt_gpt_5.txt`, `systemprompt_interactive_gpt_5.txt`, `systemprompt_gpt_6_1_sol.txt` and `systemprompt_interactive_gpt_6_1_sol.txt` | [OpenAI Codex](https://github.com/openai/codex), `codex-rs/models-manager/models.json`, the GPT-6 `instructions_template` ("Working with the user"), commit `a6baf8867cb4` | Apache-2.0 | Removed the clause "but you will still see all prior user requests" (SRW does not keep them) |
| `config/prompts/compaction/codex/prompt.md` (the native compaction instruction for GPT models) | [OpenAI Codex](https://github.com/openai/codex), `codex-rs/prompts/templates/compact/prompt.md`, commit `a6baf8867cb4` | Apache-2.0 | Added a last line asking for the summary only, without tool calls (SRW keeps the tools in the request) |

NOTICE of OpenAI Codex (the part that applies):

```
OpenAI Codex
Copyright 2025 OpenAI
```

---

## How to (re)generate

CI keeps this file current automatically — see [`.github/workflows`](.github/workflows):
the `dependency-audit` job runs the policy **gate** (fails the build on a denied
license), and the develop `license-inventory` job **regenerates and commits** the
sections below. Both call one script, the single source of truth for policy:

```bash
# Gate only (what the dependency-audit job runs):
python scripts/check_licenses.py --check

# Regenerate the sections below in place (what license-inventory commits):
python scripts/check_licenses.py --write
```

To run it locally: backend licenses are read from **installed package metadata**, so a
requirements file you did not install is invisible to both the gate and the inventory.
Install all three runtime files, and install CPU-only PyTorch first exactly as
`docker/Dockerfile.agent` does — otherwise `sentence-transformers → torch` drags in ~18
proprietary `nvidia-*`/`cuda-*` CUDA wheels that the shipped image never contains, and
the gate fails on packages we do not convey. The frontend inventory is read directly
from `cockpit/package-lock.json` — no `npm install` needed.

```bash
pip install pip-licenses
pip install torch --index-url https://download.pytorch.org/whl/cpu   # matches the agent image
pip install -r requirements.txt \
            -r src/orchestrator/requirements.txt \
            -r src/mcp_server/requirements.txt
python scripts/check_licenses.py --write   # gate + regenerate
```

Exit codes: `0` clean, `1` policy failure (a denied license — a real finding), `2`
generation failure (the inventory could not be built, so nothing is known). A caller
that tolerates `1` must still fail on `2`.

> Policy lives in `scripts/check_licenses.py` (ALLOW / WEAK / DENY token lists +
> per-package `OVERRIDES`). A new dependency under GPL/AGPL/SSPL/BUSL — or any
> UNKNOWN license under `--strict` — fails the gate.

---

## Backend (Python) — full inventory

<!-- BEGIN: backend-inventory -->
| Package | Version | License | Category |
|---|---|---|---|
| [aiofile](https://github.com/mosquito/aiofile) | 3.12.3 | Apache Software License | ALLOW |
| [aiofiles](https://github.com/Tinche/aiofiles) | 25.1.0 | Apache Software License | ALLOW |
| [aiohappyeyeballs](https://github.com/aio-libs/aiohappyeyeballs) | 2.7.1 | Python Software Foundation License | ALLOW |
| [aiohttp](https://github.com/aio-libs/aiohttp) | 3.14.3 | Apache-2.0 AND MIT | ALLOW |
| [aiohttp_socks](https://github.com/romis2012/aiohttp-socks) | 0.12.0 | Apache-2.0 | ALLOW |
| [aiosignal](https://gitter.im/aio-libs/Lobby) | 1.4.0 | Apache Software License | ALLOW |
| [aiosmtplib](https://aiosmtplib.readthedocs.io/en/stable/) | 5.1.2 | MIT | ALLOW |
| [aiosqlite](https://aiosqlite.omnilib.dev) | 0.22.1 | MIT License | ALLOW |
| [altair](https://github.com/vega/altair) | 6.2.2 | BSD License | ALLOW |
| [annotated-doc](https://github.com/fastapi/annotated-doc) | 0.0.5 | MIT | ALLOW |
| [annotated-types](https://github.com/annotated-types/annotated-types) | 0.8.0 | MIT License | ALLOW |
| [anthropic](https://github.com/anthropics/anthropic-sdk-python) | 1.2.0 | MIT License | ALLOW |
| [anyio](https://github.com/agronholm/anyio) | 4.14.2 | MIT | ALLOW |
| [argon2-cffi](https://argon2-cffi.readthedocs.io/) | 25.1.0 | MIT | ALLOW |
| [argon2-cffi-bindings](https://tidelift.com/?utm_source=lifter&utm_medium=referral&utm_campaign=hynek) | 26.1.0 | MIT | ALLOW |
| [arxiv](https://github.com/lukasschwab/arxiv.py) | 4.0.1 | MIT License | ALLOW |
| [asyncpg](https://github.com/MagicStack/asyncpg) | 0.31.0 | Apache-2.0 | ALLOW |
| [asyncssh](http://asyncssh.timeheart.net) | 2.24.0 | EPL-2.0 OR GPL-2.0-or-later | WEAK |
| [attrs](https://www.attrs.org/) | 26.1.0 | MIT | ALLOW |
| [Authlib](https://github.com/authlib/authlib) | 1.7.2 | BSD License | ALLOW |
| [bcrypt](https://github.com/pyca/bcrypt/) | 5.0.0 | Apache Software License | ALLOW |
| [beartype](https://beartype.readthedocs.io) | 0.22.9 | MIT License | ALLOW |
| [beautifulsoup4](https://www.crummy.com/software/BeautifulSoup/bs4/) | 4.15.0 | MIT License | ALLOW |
| [blinker](https://github.com/pallets-eco/blinker/) | 1.9.0 | MIT License | ALLOW |
| [boto3](https://github.com/boto/boto3) | 1.43.82 | Apache-2.0 | ALLOW |
| botocore | 1.43.82 | Apache-2.0 | ALLOW |
| [cachetools](https://github.com/tkem/cachetools/) | 7.1.7 | MIT | ALLOW |
| [caio](https://github.com/mosquito/caio/) | 0.12.2 | Apache-2.0 | ALLOW |
| [certifi](https://github.com/certifi/python-certifi) | 2026.7.22 | Mozilla Public License 2.0 (MPL 2.0) | WEAK |
| [cffi](https://github.com/python-cffi/cffi) | 2.1.1 | MIT-0 | ALLOW |
| [charset-normalizer](https://github.com/jawah/charset_normalizer/blob/master/CHANGELOG.md) | 3.5.1 | MIT | ALLOW |
| [claude-agent-sdk](https://github.com/anthropics/claude-agent-sdk-python) | 0.2.147 | MIT License | ALLOW |
| [click](https://github.com/pallets/click/) | 8.5.0 | BSD-3-Clause | ALLOW |
| [croniter](https://github.com/pallets-eco/croniter) | 6.2.4 | MIT License | ALLOW |
| [cryptography](https://github.com/pyca/cryptography) | 50.0.1 | Apache-2.0 OR BSD-3-Clause | ALLOW |
| [cyclopts](https://github.com/BrianPugh/cyclopts) | 4.23.3 | Apache Software License | ALLOW |
| [deprecation](https://github.com/briancurtin/deprecation) | 2.1.0 | Apache Software License | ALLOW |
| distro | 1.9.0 | Apache Software License | ALLOW |
| [dnspython](https://www.dnspython.org) | 2.8.0 | ISC License (ISCL) | ALLOW |
| [docstring_parser](https://github.com/rr-/docstring_parser) | 0.18.0 | MIT License | ALLOW |
| docx2txt | 0.9 | UNKNOWN | ALLOW |
| durationpy | 0.11 | MIT | ALLOW |
| email-reply-parser | 0.5.12 | MIT | ALLOW |
| email-validator | 2.3.0 | The Unlicense (Unlicense) | ALLOW |
| [et_xmlfile](https://foss.heptapod.net/openpyxl/et_xmlfile) | 2.0.0 | MIT License | ALLOW |
| [exceptiongroup](https://github.com/agronholm/exceptiongroup) | 1.3.1 | MIT License | ALLOW |
| [fastapi](https://github.com/fastapi/fastapi) | 0.141.1 | MIT | ALLOW |
| [fastmcp](https://gofastmcp.com) | 3.4.4 | Apache Software License | ALLOW |
| [fastmcp-slim](https://gofastmcp.com) | 3.4.4 | Apache Software License | ALLOW |
| [filelock](https://github.com/tox-dev/py-filelock) | 3.32.4 | MIT License | ALLOW |
| filetype | 1.2.0 | MIT License | ALLOW |
| [frozenlist](https://matrix.to/#/#aio-libs:matrix.org) | 1.8.0 | Apache-2.0 | ALLOW |
| [fsspec](https://github.com/fsspec/filesystem_spec) | 2026.7.0 | BSD-3-Clause | ALLOW |
| google-auth | 2.57.0 | Apache Software License | ALLOW |
| [google-genai](https://github.com/googleapis/python-genai) | 2.20.0 | Apache-2.0 | ALLOW |
| [greenlet](https://greenlet.readthedocs.io) | 3.5.5 | MIT AND PSF-2.0 | ALLOW |
| griffelib | 2.2.0 | ISC | ALLOW |
| [groq](https://github.com/groq/groq-python) | 0.37.1 | Apache Software License | ALLOW |
| h11 | 0.16.0 | MIT License | ALLOW |
| [hf-xet](https://github.com/huggingface/xet-core) | 1.6.0 | Apache Software License | ALLOW |
| [httpcore](https://www.encode.io/httpcore/) | 1.0.9 | BSD License | ALLOW |
| [httpcore2](https://github.com/pydantic/httpx2) | 2.12.0 | BSD License | ALLOW |
| [httptools](https://github.com/MagicStack/httptools) | 0.8.0 | MIT | ALLOW |
| [httpx](https://github.com/encode/httpx) | 0.28.1 | BSD License | ALLOW |
| [httpx-sse](https://github.com/florimondmanca/httpx-sse) | 0.4.3 | MIT | ALLOW |
| [httpx2](https://github.com/pydantic/httpx2) | 2.12.0 | BSD License | ALLOW |
| huggingface_hub | 1.29.0 | Apache Software License | ALLOW |
| [idna](https://github.com/kjd/idna) | 3.19 | BSD-3-Clause | ALLOW |
| imap-tools | 1.15.0 | Apache Software License | ALLOW |
| [invoke](https://github.com/pyinvoke/invoke) | 3.0.3 | BSD-2-Clause | ALLOW |
| [itsdangerous](https://github.com/pallets/itsdangerous/) | 2.2.0 | BSD License | ALLOW |
| jaraco.classes | 3.4.0 | MIT License | ALLOW |
| [jaraco.context](https://github.com/jaraco/jaraco.context) | 6.1.2 | MIT | ALLOW |
| [jaraco.functools](https://github.com/jaraco/jaraco.functools) | 4.6.0 | MIT | ALLOW |
| [jeepney](https://gitlab.com/takluyver/jeepney) | 0.9.0 | MIT | ALLOW |
| [Jinja2](https://github.com/pallets/jinja/) | 3.1.6 | BSD License | ALLOW |
| jiter | 0.16.0 | MIT | ALLOW |
| jmespath | 1.1.0 | MIT License | ALLOW |
| [joblib](https://joblib.readthedocs.io) | 1.5.3 | BSD-3-Clause | ALLOW |
| [joserfc](https://github.com/authlib/joserfc) | 1.7.4 | BSD License | ALLOW |
| [jsonpatch](https://github.com/stefankoegl/python-json-patch.git) | 1.33 | BSD License | ALLOW |
| jsonpointer | 3.1.1 | BSD License | ALLOW |
| [jsonref](https://github.com/gazpachoking/jsonref) | 1.1.0 | MIT | ALLOW |
| [jsonschema](https://github.com/python-jsonschema/jsonschema) | 4.26.0 | MIT | ALLOW |
| [jsonschema-path](https://github.com/p1c2u/jsonschema-path) | 0.5.0 | Apache Software License | ALLOW |
| [jsonschema-specifications](https://github.com/python-jsonschema/jsonschema-specifications) | 2025.9.1 | MIT | ALLOW |
| [jwcrypto](https://github.com/latchset/jwcrypto) | 1.5.9 | LGPL-3.0-or-later | WEAK |
| [keyring](https://github.com/jaraco/keyring) | 25.7.0 | MIT | ALLOW |
| kubernetes | 35.0.0 | Apache Software License | ALLOW |
| [langchain](https://docs.langchain.com/) | 1.3.14 | MIT License | ALLOW |
| [langchain-anthropic](https://docs.langchain.com/oss/python/integrations/providers/anthropic) | 1.7.0 | MIT License | ALLOW |
| [langchain-classic](https://docs.langchain.com/) | 1.0.8 | MIT License | ALLOW |
| [langchain-community](https://docs.langchain.com/) | 0.4.2 | MIT | ALLOW |
| [langchain-core](https://docs.langchain.com/) | 1.6.1 | MIT License | ALLOW |
| [langchain-google-genai](https://docs.langchain.com/oss/python/integrations/providers/google) | 4.3.7 | MIT | ALLOW |
| [langchain-groq](https://docs.langchain.com/oss/python/integrations/providers/groq) | 1.1.3 | MIT License | ALLOW |
| [langchain-mcp-adapters](https://www.github.com/langchain-ai/langchain-mcp-adapters) | 0.1.14 | MIT | ALLOW |
| [langchain-openai](https://docs.langchain.com/oss/python/integrations/providers/openai) | 1.6.0 | MIT License | ALLOW |
| langchain-postgres | 0.0.17 | MIT | ALLOW |
| [langchain-protocol](https://github.com/langchain-ai/agent-protocol/tree/main/streaming) | 0.0.19 | MIT License | ALLOW |
| [langchain-tavily](https://github.com/tavily-ai/langchain-tavily) | 0.2.18 | MIT License | ALLOW |
| [langchain-text-splitters](https://docs.langchain.com/) | 1.1.2 | MIT License | ALLOW |
| langdetect | 1.0.9 | Apache Software License | ALLOW |
| [langgraph](https://docs.langchain.com/oss/python/langgraph/overview) | 1.2.10 | MIT | ALLOW |
| [langgraph-checkpoint](https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint) | 4.1.1 | MIT | ALLOW |
| [langgraph-checkpoint-postgres](https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint-postgres) | 3.1.1 | MIT | ALLOW |
| [langgraph-checkpoint-sqlite](https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint-sqlite) | 3.1.1 | MIT | ALLOW |
| [langgraph-prebuilt](https://github.com/langchain-ai/langgraph/tree/main/libs/prebuilt) | 1.1.0 | MIT | ALLOW |
| [langgraph-sdk](https://github.com/langchain-ai/langgraph/tree/main/libs/sdk-py) | 0.4.4 | MIT | ALLOW |
| [langsmith](https://smith.langchain.com/) | 0.11.2 | MIT | ALLOW |
| [lxml](https://github.com/lxml/lxml) | 6.1.2 | BSD-3-Clause | ALLOW |
| [markdown-it-py](https://github.com/executablebooks/markdown-it-py) | 4.2.0 | MIT License | ALLOW |
| [MarkupSafe](https://github.com/pallets/markupsafe/) | 3.0.3 | BSD-3-Clause | ALLOW |
| [mcp](https://modelcontextprotocol.io) | 1.29.0 | MIT License | ALLOW |
| [mdurl](https://github.com/executablebooks/mdurl) | 0.1.2 | MIT License | ALLOW |
| [more-itertools](https://github.com/more-itertools/more-itertools) | 11.1.0 | MIT | ALLOW |
| [motor](https://www.mongodb.org) | 3.7.1 | Apache Software License | ALLOW |
| [mpmath](https://github.com/fredrik-johansson/mpmath) | 1.3.0 | BSD License | ALLOW |
| [multidict](https://matrix.to/#/#aio-libs:matrix.org) | 6.7.1 | Apache License 2.0 | ALLOW |
| [narwhals](https://github.com/narwhals-dev/narwhals) | 2.25.0 | MIT | ALLOW |
| [nats-py](https://github.com/nats-io/nats.py) | 2.15.0 | Apache-2.0 | ALLOW |
| [neo4j](https://neo4j.com/) | 6.3.0 | Apache-2.0 AND Python-2.0 | ALLOW |
| [networkx](https://networkx.org/) | 3.6.1 | BSD-3-Clause | ALLOW |
| [numpy](https://numpy.org) | 2.5.2 | BSD-3-Clause AND 0BSD AND MIT AND Zlib AND CC0-1.0 | ALLOW |
| oauthlib | 3.3.1 | BSD-3-Clause | ALLOW |
| [openai](https://github.com/openai/openai-python) | 3.5.0 | Apache Software License | ALLOW |
| [openapi-pydantic](https://github.com/mike-oakley/openapi-pydantic) | 0.5.1 | MIT License | ALLOW |
| [openpyxl](https://foss.heptapod.net/openpyxl/openpyxl) | 3.1.5 | MIT License | ALLOW |
| [opentelemetry-api](https://github.com/open-telemetry/opentelemetry-python/tree/main/opentelemetry-api) | 1.44.0 | Apache-2.0 | ALLOW |
| [orjson](https://github.com/ijl/orjson) | 3.12.0 | Apache Software License; MIT License; Mozilla Public License 2.0 (MPL 2.0) | WEAK |
| [ormsgpack](https://github.com/ormsgpack/ormsgpack) | 1.12.2 | Apache Software License; MIT License | ALLOW |
| [packaging](https://github.com/pypa/packaging) | 26.3 | Apache-2.0 OR BSD-2-Clause | ALLOW |
| [pandas](https://pandas.pydata.org) | 3.0.5 | BSD License | ALLOW |
| [paramiko](https://github.com/paramiko/paramiko) | 5.0.0 | LGPL-2.1 | WEAK |
| [pathable](https://github.com/p1c2u/pathable) | 0.6.0 | Apache Software License | ALLOW |
| pdf2image | 1.17.0 | MIT License | ALLOW |
| [pdfminer.six](https://github.com/pdfminer/pdfminer.six) | 20260107 | MIT | ALLOW |
| pdfplumber | 0.11.10 | MIT License | ALLOW |
| [pgvector](https://github.com/pgvector/pgvector-python) | 0.3.6 | MIT | ALLOW |
| [pillow](https://python-pillow.github.io) | 12.3.0 | MIT-CMU | ALLOW |
| [platformdirs](https://github.com/tox-dev/platformdirs) | 4.11.5 | MIT License | ALLOW |
| [propcache](https://matrix.to/#/#aio-libs:matrix.org) | 0.5.2 | Apache Software License | ALLOW |
| protobuf | 7.36.0 | 3-Clause BSD License | ALLOW |
| psutil | 7.2.2 | BSD-3-Clause | ALLOW |
| [psycopg](https://psycopg.org/) | 3.3.4 | LGPL-3.0-only | WEAK |
| [psycopg-binary](https://psycopg.org/) | 3.3.4 | LGPL-3.0-only | WEAK |
| [psycopg-pool](https://psycopg.org/) | 3.3.1 | LGPL-3.0-only | WEAK |
| [psycopg2-binary](https://psycopg.org/) | 2.9.12 | GNU Library or Lesser General Public License (LGPL) | WEAK |
| py-key-value-aio | 0.4.5 | Apache-2.0 | ALLOW |
| [pyarrow](https://arrow.apache.org/) | 25.0.1 | Apache-2.0 | ALLOW |
| [pyasn1](https://github.com/pyasn1/pyasn1) | 0.6.4 | BSD-2-Clause | ALLOW |
| [pyasn1_modules](https://github.com/pyasn1/pyasn1-modules) | 0.4.2 | BSD License | ALLOW |
| [pycparser](https://github.com/eliben/pycparser) | 3.0 | BSD-3-Clause | ALLOW |
| [pydantic](https://github.com/pydantic/pydantic) | 2.13.4 | MIT | ALLOW |
| [pydantic-settings](https://github.com/pydantic/pydantic-settings) | 2.15.0 | MIT License | ALLOW |
| [pydantic_core](https://github.com/pydantic) | 2.46.4 | MIT | ALLOW |
| pydeck | 0.9.3 | Apache License 2.0 | ALLOW |
| [Pygments](https://pygments.org) | 2.21.0 | BSD-2-Clause | ALLOW |
| [PyJWT](https://github.com/jpadilla/pyjwt) | 2.13.0 | MIT | ALLOW |
| [pymongo](https://www.mongodb.org) | 4.17.0 | Apache-2.0 | ALLOW |
| [PyNaCl](https://github.com/pyca/pynacl) | 1.6.2 | Apache Software License | ALLOW |
| [pypdf](https://github.com/py-pdf/pypdf) | 6.16.2 | BSD-3-Clause | ALLOW |
| [pypdfium2](https://github.com/pypdfium2-team/pypdfium2) | 5.13.0 | BSD-3-Clause, Apache-2.0, dependency licenses | ALLOW |
| [pyperclip](https://github.com/asweigart/pyperclip) | 1.11.0 | BSD License | ALLOW |
| [python-dateutil](https://github.com/dateutil/dateutil) | 2.9.0.post0 | BSD License; Apache Software License | ALLOW |
| [python-docx](https://github.com/python-openxml/python-docx) | 1.2.0 | MIT License | ALLOW |
| [python-dotenv](https://github.com/theskumar/python-dotenv) | 1.2.3 | BSD-3-Clause | ALLOW |
| [python-keycloak](https://raw.githubusercontent.com/marcospereirampj/python-keycloak/master/CHANGELOG.md) | 7.1.1 | MIT License | ALLOW |
| python-magic | 0.4.27 | MIT License | ALLOW |
| [python-multipart](https://github.com/Kludex/python-multipart) | 0.0.32 | Apache Software License | ALLOW |
| [python-pptx](https://github.com/scanny/python-pptx) | 1.0.2 | MIT License | ALLOW |
| [python-socks](https://github.com/romis2012/python-socks) | 3.0.0 | Apache-2.0 | ALLOW |
| [pytz](https://github.com/stub42/pytz.git) | 2026.3.post1 | MIT License | ALLOW |
| [PyYAML](https://github.com/yaml/pyyaml) | 6.0.3 | MIT License | ALLOW |
| [referencing](https://github.com/python-jsonschema/referencing) | 0.37.0 | MIT | ALLOW |
| [regex](https://github.com/mrabarnett/mrab-regex) | 2026.7.19 | Apache-2.0 AND CNRI-Python | ALLOW |
| [requests](https://github.com/psf/requests) | 2.34.2 | Apache Software License | ALLOW |
| requests-oauthlib | 2.0.0 | BSD License | ALLOW |
| [requests-toolbelt](https://github.com/requests/toolbelt) | 1.0.0 | Apache Software License | ALLOW |
| [rich](https://github.com/Textualize/rich) | 15.0.0 | MIT License | ALLOW |
| [rich-rst](https://github.com/wasi-master/rich-rst) | 2.1.0 | MIT | ALLOW |
| [rpds-py](https://github.com/crate-py/rpds) | 2026.6.3 | MIT | ALLOW |
| s3transfer | 0.19.2 | Apache Software License | ALLOW |
| [safetensors](https://github.com/huggingface/safetensors) | 0.8.0 | Apache Software License | ALLOW |
| [scikit-learn](https://scikit-learn.org) | 1.9.0 | BSD-3-Clause | ALLOW |
| [scipy](https://scipy.org/) | 1.18.1 | BSD License | ALLOW |
| [SecretStorage](https://github.com/mitya57/secretstorage) | 3.5.0 | BSD-3-Clause | ALLOW |
| [sentence-transformers](https://www.SBERT.net) | 6.0.0 | Apache-2.0 | ALLOW |
| [setuptools](https://github.com/pypa/setuptools) | 84.0.0 | MIT | ALLOW |
| shellingham | 1.5.4 | ISC License (ISCL) | ALLOW |
| six | 1.17.0 | MIT License | ALLOW |
| [sniffio](https://github.com/python-trio/sniffio) | 1.3.1 | MIT License; Apache Software License | ALLOW |
| [soupsieve](https://github.com/facelessuser/soupsieve) | 2.9.2 | MIT License | ALLOW |
| [SQLAlchemy](https://docs.sqlalchemy.org) | 2.0.52 | MIT | ALLOW |
| sqlite-vec | 0.1.9 | MIT License, Apache License, Version 2.0 | ALLOW |
| [sse-starlette](https://github.com/sysid/sse-starlette) | 3.4.8 | BSD-3-Clause | ALLOW |
| [starlette](https://github.com/Kludex/starlette) | 1.6.0 | BSD-3-Clause | ALLOW |
| [streamlit](https://streamlit.io) | 1.62.0 | Apache-2.0 | ALLOW |
| [sympy](https://github.com/sympy/sympy) | 1.14.0 | BSD License | ALLOW |
| tenacity | 9.1.4 | Apache Software License | ALLOW |
| threadpoolctl | 3.6.0 | BSD License | ALLOW |
| [tiktoken](https://github.com/openai/tiktoken) | 0.14.0 | MIT License Copyright (c) 2022 OpenAI, Shantanu Jain Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. | ALLOW |
| [tokenizers](https://github.com/huggingface/tokenizers) | 0.23.1 | Apache Software License | ALLOW |
| toml | 0.10.2 | MIT License | ALLOW |
| [torch](https://pytorch.org) | 2.13.0+cpu | Apache-2.0 AND Apache-2.0 WITH LLVM-exception AND BSD-2-Clause AND BSD-3-Clause AND BSL-1.0 AND MIT | ALLOW |
| [tqdm](https://tqdm.github.io) | 4.70.0 | MPL-2.0 AND MIT | WEAK |
| transformers | 5.16.1 | Apache 2.0 License | ALLOW |
| triton | 3.7.1 | MIT License | ALLOW |
| [truststore](https://github.com/sethmlarson/truststore) | 0.10.4 | MIT | ALLOW |
| [typer](https://github.com/fastapi/typer) | 0.27.2 | MIT | ALLOW |
| [typing-inspection](https://github.com/pydantic/typing-inspection) | 0.4.4 | MIT | ALLOW |
| [typing_extensions](https://github.com/python/typing_extensions) | 4.16.0 | PSF-2.0 | ALLOW |
| [uncalled-for](https://github.com/chrisguidry/uncalled-for) | 0.4.0 | MIT License | ALLOW |
| [urllib3](https://github.com/urllib3/urllib3/blob/main/CHANGES.rst) | 2.7.0 | MIT | ALLOW |
| [uuid_utils](https://github.com/aminalaee/uuid-utils) | 0.17.0 | BSD-3-Clause | ALLOW |
| [uvicorn](https://uvicorn.dev/) | 0.52.4 | BSD-3-Clause | ALLOW |
| [uvloop](https://github.com/MagicStack/uvloop) | 0.22.1 | Apache Software License; MIT License | ALLOW |
| [watchdog](https://github.com/gorakhargosh/watchdog/) | 6.0.0 | Apache Software License | ALLOW |
| [watchfiles](https://github.com/samuelcolvin/watchfiles) | 1.2.0 | MIT License | ALLOW |
| [webdavclient3](https://github.com/ezhov-evgeny/webdav-client-python-3) | 3.14.7 | MIT | ALLOW |
| [websocket-client](https://github.com/websocket-client/websocket-client/) | 1.9.0 | Apache Software License | ALLOW |
| [websockets](https://github.com/python-websockets/websockets) | 16.1.1 | BSD-3-Clause | ALLOW |
| xlsxwriter | 3.2.9 | BSD License | ALLOW |
| xxhash | 4.0.1 | BSD-2-Clause | ALLOW |
| [yarl](https://matrix.to/#/#aio-libs:matrix.org) | 1.24.5 | Apache-2.0 | ALLOW |
| [zstandard](https://github.com/indygreg/python-zstandard) | 0.25.0 | BSD-3-Clause | ALLOW |
<!-- END: backend-inventory -->

### Backend NOTICE files

<!-- BEGIN: backend-notices -->
### aiofiles 25.1.0

```
Asyncio support for files
Copyright 2016 Tin Tvrtkovic
```

### boto3 1.43.82

```
boto3
Copyright 2013-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
```

### botocore 1.43.82

```
Botocore
Copyright 2012-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

----

Botocore includes vendorized parts of the requests python library for backwards compatibility.

Requests License
================

Copyright 2013 Kenneth Reitz

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

Botocore includes vendorized parts of the urllib3 library for backwards compatibility.

Urllib3 License
===============

This is the MIT license: http://www.opensource.org/licenses/mit-license.php

Copyright 2008-2011 Andrey Petrov and contributors (see CONTRIBUTORS.txt),
Modifications copyright 2012 Kenneth Reitz.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.

Bundle of CA Root Certificates
==============================

***** BEGIN LICENSE BLOCK *****
This Source Code Form is subject to the terms of the
Mozilla Public License, v. 2.0. If a copy of the MPL
was not distributed with this file, You can obtain
one at http://mozilla.org/MPL/2.0/.

***** END LICENSE BLOCK *****
```

### langdetect 1.0.9

```
language-detection license
==========================

    Copyright (c) 2010-2014 Cybozu Labs, Inc. All rights reserved.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
```

### neo4j 6.3.0

```
Neo4j
Copyright (c) Neo4j Sweden AB (referred to in this notice as "Neo4j") [https://neo4j.com]

This product includes software ("Software") developed by Neo4j
```

### propcache 0.5.2

```
Copyright 2016-2021, Andrew Svetlov and aio-libs team

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
```

### s3transfer 0.19.2

```
s3transfer
Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
```

### sentence-transformers 6.0.0

```
-------------------------------------------------------------------------------
Sentence Transformers

Copyright 2019-2025
Ubiquitous Knowledge Processing (UKP) Lab
Technische Universität Darmstadt

Copyright 2025-present
Hugging Face, Inc.
-------------------------------------------------------------------------------
```

### yarl 1.24.5

```
Copyright 2016-2021, Andrew Svetlov and aio-libs team

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
```
<!-- END: backend-notices -->

---

## Frontend (JavaScript/TypeScript) — full inventory

<!-- BEGIN: frontend-inventory -->
| Package | Version | License | Category |
|---|---|---|---|
| [@angular-devkit/core](https://www.npmjs.com/package/@angular-devkit/core) | 21.2.19 | MIT | ALLOW |
| [@angular-devkit/schematics](https://www.npmjs.com/package/@angular-devkit/schematics) | 21.2.19 | MIT | ALLOW |
| [@angular/cdk](https://www.npmjs.com/package/@angular/cdk) | 21.2.14 | MIT | ALLOW |
| [@angular/common](https://www.npmjs.com/package/@angular/common) | 21.2.19 | MIT | ALLOW |
| [@angular/compiler](https://www.npmjs.com/package/@angular/compiler) | 21.2.19 | MIT | ALLOW |
| [@angular/core](https://www.npmjs.com/package/@angular/core) | 21.2.19 | MIT | ALLOW |
| [@angular/forms](https://www.npmjs.com/package/@angular/forms) | 21.2.19 | MIT | ALLOW |
| [@angular/platform-browser](https://www.npmjs.com/package/@angular/platform-browser) | 21.2.19 | MIT | ALLOW |
| [@angular/platform-server](https://www.npmjs.com/package/@angular/platform-server) | 21.2.19 | MIT | ALLOW |
| [@angular/pwa](https://www.npmjs.com/package/@angular/pwa) | 21.2.19 | MIT | ALLOW |
| [@angular/router](https://www.npmjs.com/package/@angular/router) | 21.2.19 | MIT | ALLOW |
| [@angular/service-worker](https://www.npmjs.com/package/@angular/service-worker) | 21.2.19 | MIT | ALLOW |
| [@angular/ssr](https://www.npmjs.com/package/@angular/ssr) | 21.2.19 | MIT | ALLOW |
| [@babel/code-frame](https://www.npmjs.com/package/@babel/code-frame) | 7.29.7 | MIT | ALLOW |
| [@babel/helper-validator-identifier](https://www.npmjs.com/package/@babel/helper-validator-identifier) | 7.29.7 | MIT | ALLOW |
| [@jridgewell/sourcemap-codec](https://www.npmjs.com/package/@jridgewell/sourcemap-codec) | 1.5.5 | MIT | ALLOW |
| [@jsverse/transloco](https://www.npmjs.com/package/@jsverse/transloco) | 8.4.0 | MIT | ALLOW |
| [@jsverse/transloco-locale](https://www.npmjs.com/package/@jsverse/transloco-locale) | 8.4.0 | MIT | ALLOW |
| [@jsverse/transloco-utils](https://www.npmjs.com/package/@jsverse/transloco-utils) | 8.4.0 | MIT | ALLOW |
| [@jsverse/utils](https://www.npmjs.com/package/@jsverse/utils) | 1.0.0-beta.5 | MIT | ALLOW |
| [@monaco-editor/loader](https://www.npmjs.com/package/@monaco-editor/loader) | 1.7.0 | MIT | ALLOW |
| [@schematics/angular](https://www.npmjs.com/package/@schematics/angular) | 21.2.19 | MIT | ALLOW |
| [@standard-schema/spec](https://www.npmjs.com/package/@standard-schema/spec) | 1.1.0 | MIT | ALLOW |
| [@types/trusted-types](https://www.npmjs.com/package/@types/trusted-types) | 2.0.7 | MIT | ALLOW |
| [accepts](https://www.npmjs.com/package/accepts) | 2.0.0 | MIT | ALLOW |
| [ajv](https://www.npmjs.com/package/ajv) | 8.18.0 | MIT | ALLOW |
| [ajv-formats](https://www.npmjs.com/package/ajv-formats) | 3.0.1 | MIT | ALLOW |
| [angular-split](https://www.npmjs.com/package/angular-split) | 20.0.0 | Apache-2.0 | ALLOW |
| [ansi-regex](https://www.npmjs.com/package/ansi-regex) | 6.2.2 | MIT | ALLOW |
| [argparse](https://www.npmjs.com/package/argparse) | 2.0.1 | Python-2.0 | ALLOW |
| [body-parser](https://www.npmjs.com/package/body-parser) | 2.3.0 | MIT | ALLOW |
| [bytes](https://www.npmjs.com/package/bytes) | 3.1.2 | MIT | ALLOW |
| [call-bind-apply-helpers](https://www.npmjs.com/package/call-bind-apply-helpers) | 1.0.2 | MIT | ALLOW |
| [call-bound](https://www.npmjs.com/package/call-bound) | 1.0.4 | MIT | ALLOW |
| [callsites](https://www.npmjs.com/package/callsites) | 3.1.0 | MIT | ALLOW |
| [chalk](https://www.npmjs.com/package/chalk) | 5.6.2 | MIT | ALLOW |
| [cli-cursor](https://www.npmjs.com/package/cli-cursor) | 5.0.0 | MIT | ALLOW |
| [cli-spinners](https://www.npmjs.com/package/cli-spinners) | 3.4.0 | MIT | ALLOW |
| [clipboard](https://www.npmjs.com/package/clipboard) | 2.0.11 | MIT | ALLOW |
| [commander](https://www.npmjs.com/package/commander) | 8.3.0 | MIT | ALLOW |
| [content-disposition](https://www.npmjs.com/package/content-disposition) | 1.1.0 | MIT | ALLOW |
| [content-type](https://www.npmjs.com/package/content-type) | 2.0.0 | MIT | ALLOW |
| [content-type](https://www.npmjs.com/package/content-type) | 1.0.5 | MIT | ALLOW |
| [cookie](https://www.npmjs.com/package/cookie) | 0.7.2 | MIT | ALLOW |
| [cookie-signature](https://www.npmjs.com/package/cookie-signature) | 1.2.2 | MIT | ALLOW |
| [cose-base](https://www.npmjs.com/package/cose-base) | 2.2.0 | MIT | ALLOW |
| [cosmiconfig](https://www.npmjs.com/package/cosmiconfig) | 8.3.6 | MIT | ALLOW |
| [cron-parser](https://www.npmjs.com/package/cron-parser) | 4.9.0 | MIT | ALLOW |
| [cronstrue](https://www.npmjs.com/package/cronstrue) | 2.59.0 | MIT | ALLOW |
| [cytoscape](https://www.npmjs.com/package/cytoscape) | 3.34.0 | MIT | ALLOW |
| [cytoscape-fcose](https://www.npmjs.com/package/cytoscape-fcose) | 2.2.0 | MIT | ALLOW |
| [debug](https://www.npmjs.com/package/debug) | 4.4.3 | MIT | ALLOW |
| [delegate](https://www.npmjs.com/package/delegate) | 3.2.0 | MIT | ALLOW |
| [depd](https://www.npmjs.com/package/depd) | 2.0.0 | MIT | ALLOW |
| [dexie](https://www.npmjs.com/package/dexie) | 4.4.4 | Apache-2.0 | ALLOW |
| [dompurify](https://www.npmjs.com/package/dompurify) | 3.4.13 | (MPL-2.0 OR Apache-2.0) | ALLOW |
| [dompurify](https://www.npmjs.com/package/dompurify) | 3.2.7 | (MPL-2.0 OR Apache-2.0) | ALLOW |
| [dunder-proto](https://www.npmjs.com/package/dunder-proto) | 1.0.1 | MIT | ALLOW |
| [ee-first](https://www.npmjs.com/package/ee-first) | 1.1.1 | MIT | ALLOW |
| [encodeurl](https://www.npmjs.com/package/encodeurl) | 2.0.0 | MIT | ALLOW |
| [entities](https://www.npmjs.com/package/entities) | 6.0.1 | BSD-2-Clause | ALLOW |
| [entities](https://www.npmjs.com/package/entities) | 8.0.0 | BSD-2-Clause | ALLOW |
| [error-ex](https://www.npmjs.com/package/error-ex) | 1.3.4 | MIT | ALLOW |
| [es-define-property](https://www.npmjs.com/package/es-define-property) | 1.0.1 | MIT | ALLOW |
| [es-errors](https://www.npmjs.com/package/es-errors) | 1.3.0 | MIT | ALLOW |
| [es-object-atoms](https://www.npmjs.com/package/es-object-atoms) | 1.1.2 | MIT | ALLOW |
| [escape-html](https://www.npmjs.com/package/escape-html) | 1.0.3 | MIT | ALLOW |
| [etag](https://www.npmjs.com/package/etag) | 1.8.1 | MIT | ALLOW |
| [express](https://www.npmjs.com/package/express) | 5.2.1 | MIT | ALLOW |
| [fast-deep-equal](https://www.npmjs.com/package/fast-deep-equal) | 3.1.3 | MIT | ALLOW |
| [fast-uri](https://www.npmjs.com/package/fast-uri) | 3.1.5 | BSD-3-Clause | ALLOW |
| [finalhandler](https://www.npmjs.com/package/finalhandler) | 2.1.1 | MIT | ALLOW |
| [forwarded](https://www.npmjs.com/package/forwarded) | 0.2.0 | MIT | ALLOW |
| [fresh](https://www.npmjs.com/package/fresh) | 2.0.0 | MIT | ALLOW |
| [function-bind](https://www.npmjs.com/package/function-bind) | 1.1.2 | MIT | ALLOW |
| [get-east-asian-width](https://www.npmjs.com/package/get-east-asian-width) | 1.6.0 | MIT | ALLOW |
| [get-intrinsic](https://www.npmjs.com/package/get-intrinsic) | 1.3.0 | MIT | ALLOW |
| [get-proto](https://www.npmjs.com/package/get-proto) | 1.0.1 | MIT | ALLOW |
| [good-listener](https://www.npmjs.com/package/good-listener) | 1.2.2 | MIT | ALLOW |
| [gopd](https://www.npmjs.com/package/gopd) | 1.2.0 | MIT | ALLOW |
| [has-symbols](https://www.npmjs.com/package/has-symbols) | 1.1.0 | MIT | ALLOW |
| [hasown](https://www.npmjs.com/package/hasown) | 2.0.4 | MIT | ALLOW |
| [http-errors](https://www.npmjs.com/package/http-errors) | 2.0.1 | MIT | ALLOW |
| [iconv-lite](https://www.npmjs.com/package/iconv-lite) | 0.7.3 | MIT | ALLOW |
| [import-fresh](https://www.npmjs.com/package/import-fresh) | 3.3.1 | MIT | ALLOW |
| [inherits](https://www.npmjs.com/package/inherits) | 2.0.4 | ISC | ALLOW |
| [ipaddr.js](https://www.npmjs.com/package/ipaddr.js) | 1.9.1 | MIT | ALLOW |
| [is-arrayish](https://www.npmjs.com/package/is-arrayish) | 0.2.1 | MIT | ALLOW |
| [is-interactive](https://www.npmjs.com/package/is-interactive) | 2.0.0 | MIT | ALLOW |
| [is-promise](https://www.npmjs.com/package/is-promise) | 4.0.0 | MIT | ALLOW |
| [is-unicode-supported](https://www.npmjs.com/package/is-unicode-supported) | 2.1.0 | MIT | ALLOW |
| [js-tokens](https://www.npmjs.com/package/js-tokens) | 4.0.0 | MIT | ALLOW |
| [js-yaml](https://www.npmjs.com/package/js-yaml) | 4.3.1 | MIT | ALLOW |
| [json-parse-even-better-errors](https://www.npmjs.com/package/json-parse-even-better-errors) | 2.3.1 | MIT | ALLOW |
| [json-schema-traverse](https://www.npmjs.com/package/json-schema-traverse) | 1.0.0 | MIT | ALLOW |
| [jsonc-parser](https://www.npmjs.com/package/jsonc-parser) | 3.3.1 | MIT | ALLOW |
| [katex](https://www.npmjs.com/package/katex) | 0.16.47 | MIT | ALLOW |
| [layout-base](https://www.npmjs.com/package/layout-base) | 2.0.1 | MIT | ALLOW |
| [lines-and-columns](https://www.npmjs.com/package/lines-and-columns) | 1.2.4 | MIT | ALLOW |
| [log-symbols](https://www.npmjs.com/package/log-symbols) | 7.0.1 | MIT | ALLOW |
| [luxon](https://www.npmjs.com/package/luxon) | 3.7.2 | MIT | ALLOW |
| [magic-string](https://www.npmjs.com/package/magic-string) | 0.30.21 | MIT | ALLOW |
| [marked](https://www.npmjs.com/package/marked) | 17.0.6 | MIT | ALLOW |
| [marked](https://www.npmjs.com/package/marked) | 14.0.0 | MIT | ALLOW |
| [math-intrinsics](https://www.npmjs.com/package/math-intrinsics) | 1.1.0 | MIT | ALLOW |
| [media-typer](https://www.npmjs.com/package/media-typer) | 1.1.1 | MIT | ALLOW |
| [merge-descriptors](https://www.npmjs.com/package/merge-descriptors) | 2.0.0 | MIT | ALLOW |
| [mime-db](https://www.npmjs.com/package/mime-db) | 1.54.0 | MIT | ALLOW |
| [mime-types](https://www.npmjs.com/package/mime-types) | 3.0.2 | MIT | ALLOW |
| [mimic-function](https://www.npmjs.com/package/mimic-function) | 5.0.1 | MIT | ALLOW |
| [monaco-editor](https://www.npmjs.com/package/monaco-editor) | 0.55.1 | MIT | ALLOW |
| [ms](https://www.npmjs.com/package/ms) | 2.1.3 | MIT | ALLOW |
| [negotiator](https://www.npmjs.com/package/negotiator) | 1.0.0 | MIT | ALLOW |
| [ngx-markdown](https://www.npmjs.com/package/ngx-markdown) | 21.3.0 | MIT | ALLOW |
| [object-inspect](https://www.npmjs.com/package/object-inspect) | 1.13.4 | MIT | ALLOW |
| [on-finished](https://www.npmjs.com/package/on-finished) | 2.4.1 | MIT | ALLOW |
| [once](https://www.npmjs.com/package/once) | 1.4.0 | ISC | ALLOW |
| [onetime](https://www.npmjs.com/package/onetime) | 7.0.0 | MIT | ALLOW |
| [ora](https://www.npmjs.com/package/ora) | 9.3.0 | MIT | ALLOW |
| [parent-module](https://www.npmjs.com/package/parent-module) | 1.0.1 | MIT | ALLOW |
| [parse-json](https://www.npmjs.com/package/parse-json) | 5.2.0 | MIT | ALLOW |
| [parse5](https://www.npmjs.com/package/parse5) | 8.0.1 | MIT | ALLOW |
| [parse5-html-rewriting-stream](https://www.npmjs.com/package/parse5-html-rewriting-stream) | 8.0.0 | MIT | ALLOW |
| [parse5-sax-parser](https://www.npmjs.com/package/parse5-sax-parser) | 8.0.0 | MIT | ALLOW |
| [parseurl](https://www.npmjs.com/package/parseurl) | 1.3.3 | MIT | ALLOW |
| [path-to-regexp](https://www.npmjs.com/package/path-to-regexp) | 8.4.2 | MIT | ALLOW |
| [path-type](https://www.npmjs.com/package/path-type) | 4.0.0 | MIT | ALLOW |
| [picocolors](https://www.npmjs.com/package/picocolors) | 1.1.1 | ISC | ALLOW |
| [picomatch](https://www.npmjs.com/package/picomatch) | 4.0.5 | MIT | ALLOW |
| [prismjs](https://www.npmjs.com/package/prismjs) | 1.30.0 | MIT | ALLOW |
| [proxy-addr](https://www.npmjs.com/package/proxy-addr) | 2.0.7 | MIT | ALLOW |
| [qs](https://www.npmjs.com/package/qs) | 6.15.3 | BSD-3-Clause | ALLOW |
| [range-parser](https://www.npmjs.com/package/range-parser) | 1.3.0 | MIT | ALLOW |
| [raw-body](https://www.npmjs.com/package/raw-body) | 3.0.2 | MIT | ALLOW |
| [require-from-string](https://www.npmjs.com/package/require-from-string) | 2.0.2 | MIT | ALLOW |
| [resolve-from](https://www.npmjs.com/package/resolve-from) | 4.0.0 | MIT | ALLOW |
| [restore-cursor](https://www.npmjs.com/package/restore-cursor) | 5.1.0 | MIT | ALLOW |
| [router](https://www.npmjs.com/package/router) | 2.2.0 | MIT | ALLOW |
| [rxjs](https://www.npmjs.com/package/rxjs) | 7.8.2 | Apache-2.0 | ALLOW |
| [safer-buffer](https://www.npmjs.com/package/safer-buffer) | 2.1.2 | MIT | ALLOW |
| [select](https://www.npmjs.com/package/select) | 1.1.2 | MIT | ALLOW |
| [send](https://www.npmjs.com/package/send) | 1.2.1 | MIT | ALLOW |
| [serve-static](https://www.npmjs.com/package/serve-static) | 2.2.1 | MIT | ALLOW |
| [setprototypeof](https://www.npmjs.com/package/setprototypeof) | 1.2.0 | ISC | ALLOW |
| [side-channel](https://www.npmjs.com/package/side-channel) | 1.1.1 | MIT | ALLOW |
| [side-channel-list](https://www.npmjs.com/package/side-channel-list) | 1.0.1 | MIT | ALLOW |
| [side-channel-map](https://www.npmjs.com/package/side-channel-map) | 1.0.1 | MIT | ALLOW |
| [side-channel-weakmap](https://www.npmjs.com/package/side-channel-weakmap) | 1.0.2 | MIT | ALLOW |
| [signal-exit](https://www.npmjs.com/package/signal-exit) | 4.1.0 | ISC | ALLOW |
| [source-map](https://www.npmjs.com/package/source-map) | 0.7.6 | BSD-3-Clause | ALLOW |
| [state-local](https://www.npmjs.com/package/state-local) | 1.0.7 | MIT | ALLOW |
| [statuses](https://www.npmjs.com/package/statuses) | 2.0.2 | MIT | ALLOW |
| [stdin-discarder](https://www.npmjs.com/package/stdin-discarder) | 0.3.2 | MIT | ALLOW |
| [string-width](https://www.npmjs.com/package/string-width) | 8.2.2 | MIT | ALLOW |
| [strip-ansi](https://www.npmjs.com/package/strip-ansi) | 7.2.0 | MIT | ALLOW |
| [tiny-emitter](https://www.npmjs.com/package/tiny-emitter) | 2.1.0 | MIT | ALLOW |
| [toidentifier](https://www.npmjs.com/package/toidentifier) | 1.0.1 | MIT | ALLOW |
| [tslib](https://www.npmjs.com/package/tslib) | 2.8.1 | 0BSD | ALLOW |
| [type-is](https://www.npmjs.com/package/type-is) | 2.1.0 | MIT | ALLOW |
| [unpipe](https://www.npmjs.com/package/unpipe) | 1.0.0 | MIT | ALLOW |
| [vary](https://www.npmjs.com/package/vary) | 1.1.2 | MIT | ALLOW |
| [wrappy](https://www.npmjs.com/package/wrappy) | 1.0.2 | ISC | ALLOW |
| [xhr2](https://www.npmjs.com/package/xhr2) | 0.2.1 | MIT | ALLOW |
| [yoctocolors](https://www.npmjs.com/package/yoctocolors) | 2.2.0 | MIT | ALLOW |
| [zone.js](https://www.npmjs.com/package/zone.js) | 0.16.2 | MIT | ALLOW |
<!-- END: frontend-inventory -->

---

## This project's own license

The Software itself is licensed under the **Functional Source License v1.1 (FSL-1.1-ALv2)** — see `LICENSE`.
This file concerns only the third-party components distributed alongside it.
