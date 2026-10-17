---
title: Models
description: The four model roles SRW needs before jobs and sessions can start, why embeddings must be 4096-dimensional, and the one-key OpenRouter setup.
---

# Models

SRW keeps its models in a catalog in its database. Each catalog row names a
model, the provider or endpoint that serves it, and the roles (capabilities) it
fills. Until four roles each have an enabled model and a default, SRW refuses
to create jobs and sessions: `POST /api/jobs` and `POST /api/persistent/threads`
answer `503` with `system_not_ready`, and the Cockpit shows what is missing.

The shortest way to satisfy all four is one [OpenRouter](https://openrouter.ai)
API key. The local development overlay ships with that setup. You only paste the
key.

## The four required roles

| Role | What SRW uses it for | What the model must do |
| --- | --- | --- |
| `chat` | The model an expert works with in jobs and sessions: the conversation, tool calls and code. | Any chat model with tool calling. |
| `auxiliary` | Background work on its own budget: extracting memories, curating knowledge, naming sessions, and summarizing a conversation that has grown too long. If a separate auxiliary model is unreachable, SRW falls back to the main model. | Any chat model. The same row can serve `chat` and `auxiliary`. |
| `embedding` | Vectors for memory recall, knowledge-base search and citations. | Return exactly 4096-dimensional vectors (see below). |
| `rerank` | Reorders recalled memories by relevance before they reach the model. | Serve a Cohere-shaped `POST <base_url>/rerank` that takes `{model, query, documents}` and returns `results[].index` and `results[].relevance_score`. |

Rerank is required for the same reason embedding is. The memory pipeline always
runs the reranker, and a reranker that cannot reach its endpoint fails every
turn. SRW therefore refuses to start work without one. The same check also
wants an application default expert for jobs and one for sessions. SRW seeds
both at startup.

Other roles are optional:

- `vision` describes images. Without a vision model, SRW uses the chat model
  (the `llm.fallback_optional_capabilities_to_chat` setting, on by default).
- `whisper` (speech to text) and `tts` (read aloud). The audio features stay off
  until these exist.
- `search` and `fetch` are web research providers, not language models. The
  chart registers the bundled SearXNG (and Crawl4AI, when enabled) by itself.

## Why embeddings must be 4096-dimensional

Every vector column in SRW's vector database is `vector(4096)`. This covers
memories, knowledge-base chunks and citation sources. A model that returns any
other size cannot be stored or searched. When SRW sees the first wrong-sized
vector, it logs `EMBEDDING DIMENSION MISMATCH` and switches dense search off.
This keeps every write from failing on its own.

SRW does not send a `dimensions` parameter. The model must return 4096 values
by default. An installation cannot change the column size today.

- `qwen/qwen3-embedding-8b` (Qwen3 Embedding 8B) returns 4096 dimensions. It is
  the model SRW is configured and tested with.
- OpenAI's `text-embedding-3-large` returns at most 3072. It cannot be used,
  either directly or through OpenRouter.

Choose the embedding model before you store data. SRW stamps each vector with
the embedding profile (model and endpoint) that produced it. Vectors from
different models are not comparable, so switching models later means
re-embedding what is already stored.

## One OpenRouter key

OpenRouter serves chat models from many vendors. It also serves
`qwen/qwen3-embedding-8b` on `/api/v1/embeddings` and `qwen/qwen3-reranker-8b`
on `/api/v1/rerank`. One key therefore covers all four roles. Create a key at
<https://openrouter.ai/keys>.

SRW registers OpenRouter as a provider key, not as a custom endpoint. Chat then
goes through SRW's OpenRouter client, which sends OpenRouter's reasoning
settings and rotates between comma-separated keys. For embedding and rerank,
SRW sends the same key to `https://openrouter.ai/api/v1`.

Provider-key OpenRouter model IDs carry an `openrouter/` routing prefix, for
example `openrouter/qwen/qwen3-embedding-8b`. SRW removes the prefix before it
calls OpenRouter.

### With Helm values

The chart's seed Job writes keys, catalog rows and default pins. It runs after
every install and upgrade.

```yaml
llm:
  seed:
    enabled: true
    systemApiKeys:
      openrouter:
        secretName: srw-llm-seed       # a Secret you create in the release namespace
        key: openrouter_api_key
    systemModels:
      # chat + auxiliary: any OpenRouter chat model with tool calling
      - provider: openrouter
        id: "openrouter/anthropic/claude-sonnet-5.5"
        displayName: "Claude Sonnet 5.5 (OpenRouter)"
        capabilities: [chat, auxiliary]
      - provider: openrouter
        id: "openrouter/qwen/qwen3-embedding-8b"
        displayName: "Qwen3 Embedding 8B (OpenRouter)"
        capability: embedding
      - provider: openrouter
        id: "openrouter/qwen/qwen3-reranker-8b"
        displayName: "Qwen3 Reranker 8B (OpenRouter)"
        capability: rerank
    defaults:
      chat: "openrouter/anthropic/claude-sonnet-5.5"
      auxiliary: "openrouter/anthropic/claude-sonnet-5.5"
      embedding: "openrouter/qwen/qwen3-embedding-8b"
      rerank: "openrouter/qwen/qwen3-reranker-8b"
```

```bash
kubectl -n srw create secret generic srw-llm-seed \
  --from-literal=openrouter_api_key='sk-or-v1-...'
```

Claude Sonnet 5.5 is only an example. Use any chat model OpenRouter lists, and
write its ID with the `openrouter/` prefix.

How the seed behaves:

- It is insert-only. Keys, rows and pins that already exist are left alone, so
  changes made in the Cockpit survive upgrades. Add `reconcile: true` to an
  entry to make Helm the source of truth for that entry.
- An empty or missing key is skipped, together with the models that need it.
  Add the key and run `helm upgrade`, and the next run seeds them.
- Without `defaults`, a required role that has models but no pin gets one
  automatically: the first model by display label, which is the only one when
  you seed one model per role. Declare `defaults` to choose among several. A
  declared default replaces such an automatic pin, but never one an
  administrator chose.
- In chart-managed development mode (`secrets.create: true`) the key may be
  written inline as `value: "sk-or-v1-..."` instead of `secretName`/`key`.
  `deployment/values-local.yaml.example` does this, with an empty value for you
  to fill in.

`helm/values.yaml` documents every field under `llm.seed`.

### In the Cockpit

An administrator can do the same after login. The model pages are under
**Settings → Models**, in the Administration group.

1. On the **Providers** tab, under **Provider API Keys**, choose **OpenRouter**,
   paste the key and select **Save Key**. SRW then lists OpenRouter's models
   so that you can add chat models from the list.
2. On the **Catalog** tab, use **Add model** with the provider `openrouter`:
   - a chat model with the capabilities `chat` and `auxiliary`, unless you
     added one from the list in step 1;
   - model ID `qwen/qwen3-embedding-8b` with only the `embedding` capability;
   - model ID `qwen/qwen3-reranker-8b` with only the `rerank` capability.

   The Cockpit adds the `openrouter/` prefix to the ID. The family setting
   matters only for chat models; `default` is fine for the other two.
3. On the **Defaults** tab, check **Chat**, **Auxiliary**, **Embedding** and
   **Rerank**. SRW pins a role automatically when its first model is added, and
   the tab marks such pins as picked automatically. Choose another model where
   you want one.

When all four have a default, the readiness check passes and new sessions and
jobs start.

## Changing the chat model

- Add another chat row: a `systemModels` entry, or **Add model** on the Catalog
  tab.
- Change the deployment default on the **Defaults** tab. Editing
  `llm.seed.defaults` later moves only an automatic pin, not one an
  administrator chose, because the seed is insert-only. To make Helm own the
  pin, use the mapping form `chat: {model: "<id>", reconcile: true}`.
- Each user can pick a personal default under **Settings → Defaults → Default
  Model**.
- The auxiliary role does not have to use the chat model. A cheaper chat model
  with only the `auxiliary` capability, pinned as **Auxiliary**, keeps
  background work off the main model's budget.

SRW tunes prompts, context limits and reasoning settings per model family
(`config/model_config_matrix.yaml`). A model from a family SRW does not know
gets the `default` settings.

## What other providers can supply

| Source | `chat` and `auxiliary` | 4096-dim `embedding` | `rerank` |
| --- | --- | --- | --- |
| OpenRouter | Yes | Yes: `qwen/qwen3-embedding-8b` | Yes: `qwen/qwen3-reranker-8b` |
| OpenAI key | Yes | No. Its largest embedding model returns 3072 dimensions. | No `/rerank` route |
| Anthropic key | Yes | No embedding API | No |
| Groq key | Yes | No | No |
| AI subscriptions (the subscription proxy) | Yes | No. SRW imports only chat models from it. | No |
| Self-hosted OpenAI-compatible server | Whatever it serves | If it serves a 4096-dim model | If it serves a Cohere-shaped `/rerank` |

Providers can be mixed. For example, chat can come from an Anthropic key or an
AI subscription while embedding and rerank come from OpenRouter or a
self-hosted server. Before you use any other provider's embedding model,
confirm that it returns 4096 dimensions without a `dimensions` parameter.

## Self-hosted embedding and rerank

Any server that speaks the OpenAI embeddings API can serve the embedding role,
if its model returns 4096 dimensions. The rerank role needs the Cohere-shaped
`/rerank` route described above. One example is vLLM serving Qwen3 Embedding 8B
and Qwen3 Reranker 8B:

```yaml
llm:
  seed:
    enabled: true
    systemEndpoints:
      - label: "Local Qwen"
        baseUrl: "http://vllm.ai.svc.cluster.local:8000/v1"
        # apiKeyRef: {secretName: srw-llm-seed, key: vllm_api_key}  # if it needs one
        models:
          - id: "Qwen/Qwen3-Embedding-8B"
            capability: embedding
          - id: "Qwen/Qwen3-Reranker-8B"
            capability: rerank
```

In the Cockpit, add the server under **Providers → System Endpoints → Add
Endpoint**. Then add its models on the Catalog tab with that endpoint as the
provider. Endpoint model IDs are sent unchanged.

- SRW sends at most 64 texts per embedding request. Larger batches are split.
- The orchestrator and the agent pods both call the server. If you enable
  `agent.networkPolicy`, add an `agent.networkPolicy.extraEgress` rule for a
  server on a cluster or private address.

## Upgraded installations

Releases before the rerank role used the embedding host's `/rerank` route
implicitly. On upgrade, a migration adds a `Qwen3 Reranker 8B (auto)` row on
each embedding provider and pins it, so that existing installations stay ready.
That row works only where the embedding host really serves `/rerank` (for
example OpenRouter). Replace it on the Catalog tab when it does not. A fresh
installation does not get this row.
