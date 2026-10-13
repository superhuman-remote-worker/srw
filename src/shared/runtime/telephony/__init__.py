"""Phone calls: the engine-neutral call contract and the GPT-Live client.

``contract`` is what a job, its phone subagent and the cockpit see, whichever
voice engine carries the call. ``gpt_live`` speaks OpenAI's Live API (REST call
control plus the sideband WebSocket) and maps its events onto the contract.
``webhook`` verifies and parses the incoming-call webhook.

Design: knowledge-base/knowledge/features/phone_calls_as_subagents.md (§3.1.1
wire protocol, §4.4 contract).
"""
