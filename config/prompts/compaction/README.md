# Native compaction recipes

One directory per recipe; `prompt.md` is the instruction the working model gets
as the final user message of the fork (`agent.core.native_compaction`). The
family matrix picks the recipe (`settings.compaction`).

| Recipe | Source | Change |
|---|---|---|
| `codex` | OpenAI Codex, `codex-rs/prompts/templates/compact/prompt.md` at `a6baf8867cb4` (Apache-2.0, see `THIRD_PARTY_LICENSES.md`) | Added "Do not call any tools. Reply with the summary only." |
| `claude` | Anthropic, "Prompting Claude Fable 5.1": the summarization instruction for client-side compaction (platform.claude.com/docs/en/build-with-claude/prompt-engineering/prompting-claude-fable-5-1) | Added "Do not call any tools." The reply's `<summary>` block is the summary |

The fork keeps the tools in the request so it stays a cache hit; the added
line is why.
