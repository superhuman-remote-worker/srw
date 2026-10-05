"""Stable markers for transient message injection.

Readers can recognize injected messages without importing the services that
construct their content. Keep these values in sync through imports, not copies.
"""

INSTRUCTION_TOOL_CALL_ID_PREFIX = "instruction_inject_"
TODOS_INJECTION_CONTENT_PREFIX = "<active_tasks>\n"
MEMORY_TOOL_CALL_ID_PREFIX = "memory_inject_"
KNOWLEDGE_TOOL_CALL_ID_PREFIX = "knowledge_inject_"
CHARTER_TOOL_CALL_ID_PREFIX = "charter_inject_"
CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX = "citation_feedback_inject_"
GUIDANCE_TOOL_CALL_ID_PREFIX = "guidance_inject_"

# Transient HumanMessages of the legacy tail. The active-subagent status is
# rendered by ``SubagentRuntime.active_subagents_block``; the App Guide turn
# boundary by ``skill_resolution.managed_product_guide_turn_boundary``, whose
# opening tag may carry a digest attribute, so its prefix stops before ``>``.
ACTIVE_SUBAGENTS_CONTENT_PREFIX = "<active_subagents>\n"
PRODUCT_GUIDE_TURN_BOUNDARY_CONTENT_PREFIX = "<managed_product_guide_turn_boundary"
