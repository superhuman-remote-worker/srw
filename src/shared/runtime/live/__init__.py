"""Live sessions: the engine-neutral session contract and the webhook.

``contract`` is what a job, its voice subagent and the cockpit see, whichever
engine carries the session. ``webhook`` verifies and parses the provider's
incoming-session webhook (OpenAI's Standard Webhooks). The engine's wire
protocol lives in the live connector's pod (``drivers/gpt-live-sip``).

Design: knowledge-base/knowledge/features/live_session_jobs.md.
"""
