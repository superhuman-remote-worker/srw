"""Parked: instruction-builder artifact-tool schemas (Dynamic Canvas seed).

Reference snapshot, NOT live code. The instruction builder was removed in the
builder -> sessions consolidation (knowledge-base/knowledge/features/builder_to_sessions_consolidation.md).
These five LLM tool schemas -- the ones that let the builder author a job's
``instructions`` / ``config`` / ``description`` -- are preserved verbatim because
the Dynamic Canvas (knowledge-base/knowledge/features/dynamic_canvas.md) will reuse them for its
job/expert authoring operations.

These tools were applied CLIENT-side: the orchestrator emitted each call as a
``tool_call`` SSE event and the cockpit's ``JobArtifactService.applyToolCall()``
mutated the form. That application logic is parked alongside this file's
counterpart at ``cockpit/src/app/core/services/_parked/job-artifact.service.ts``.
There were no server-side dispatch handlers for these five (unlike the operator
tools, which lived in the now-deleted ``builder_dispatch.py``).

Nothing imports this module; it is reference only.
"""

# Verbatim from the removed services/builder_tools.py BUILDER_TOOLS list.
ARTIFACT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "update_instructions",
            "description": (
                "Replace the full instructions content. Use this when making large changes "
                "or when starting fresh. The entire instructions.md will be overwritten."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The complete new instructions content (markdown)",
                    },
                },
                "required": ["content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "edit_instructions",
            "description": (
                "Find and replace text within the instructions. Use this for targeted edits "
                "like fixing a section, renaming terms, or adjusting specific parts. "
                "The old_text must match exactly (including whitespace)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "old_text": {
                        "type": "string",
                        "description": "The exact text to find in the instructions",
                    },
                    "new_text": {
                        "type": "string",
                        "description": "The replacement text",
                    },
                },
                "required": ["old_text", "new_text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "insert_instructions",
            "description": (
                "Insert text at a specific line in the instructions, or append to the end. "
                "Use this for adding new sections or requirements without disturbing existing content."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The text to insert (markdown)",
                    },
                    "line": {
                        "type": "integer",
                        "description": "Line number to insert at (1-indexed). Omit to append to end.",
                    },
                },
                "required": ["content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_config",
            "description": (
                "Update the agent configuration. Objects merge recursively, arrays replace entirely. "
                "Use this to change the model, temperature, reasoning level, tool availability, "
                "autonomy level, scholar/verification phases, or memory settings."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "llm": {
                        "type": "object",
                        "description": "LLM settings to update",
                        "properties": {
                            "model": {
                                "type": "string",
                                "description": "Model name (e.g. 'gpt-4o', 'claude-sonnet-4-5-20250929')",
                            },
                            "temperature": {
                                "type": "number",
                                "description": "Temperature (0.0 to 2.0)",
                            },
                            "reasoning_level": {
                                "type": "string",
                                "description": "Reasoning level (low, medium, high)",
                            },
                            "strategic": {
                                "type": "object",
                                "description": "Overrides for strategic (planning) phases",
                                "properties": {
                                    "model": {"type": "string"},
                                    "temperature": {"type": "number"},
                                    "reasoning_level": {
                                        "type": "string",
                                        "enum": ["low", "medium", "high"],
                                    },
                                },
                            },
                            "tactical": {
                                "type": "object",
                                "description": "Overrides for tactical (execution) phases",
                                "properties": {
                                    "model": {"type": "string"},
                                    "temperature": {"type": "number"},
                                    "reasoning_level": {
                                        "type": "string",
                                        "enum": ["low", "medium", "high"],
                                    },
                                },
                            },
                        },
                    },
                    "tools": {
                        "type": "object",
                        "description": "Tool category overrides. Set a category to [] to disable it.",
                        "additionalProperties": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "scholar": {
                        "type": "object",
                        "description": "Scholar (research phase) settings. The scholar runs before the main job to gather background information.",
                        "properties": {
                            "enabled": {
                                "type": "boolean",
                                "description": "Enable/disable the scholar research phase (default: true)",
                            },
                        },
                    },
                    "verification": {
                        "type": "object",
                        "description": "Verification (critic) phase settings. The critic reviews deliverables after job completion.",
                        "properties": {
                            "enabled": {
                                "type": "boolean",
                                "description": "Enable/disable the critic verification phase (default: true)",
                            },
                            "max_rounds": {
                                "type": "integer",
                                "description": "Max feedback round-trips before auto-accepting (0 = unlimited, default: 5)",
                            },
                        },
                    },
                    "autonomy": {
                        "type": "string",
                        "enum": ["full", "review", "partial", "guided", "dependent"],
                        "description": (
                            "Controls when the agent pauses for human review. "
                            "full=never pauses, review=pauses at completion, "
                            "partial=pauses after the first strategic phase + completion, "
                            "guided=pauses after every strategic phase + completion, "
                            "dependent=pauses after every phase"
                        ),
                    },
                    "memory": {
                        "type": "object",
                        "description": "Memory settings for cross-context recall",
                        "properties": {
                            "project_scoped": {
                                "type": "boolean",
                                "description": "When true, memories are shared with other jobs in the same project",
                            },
                        },
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_description",
            "description": (
                "Replace the job description. Use this when the user wants to change "
                "what the agent should accomplish."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The new job description",
                    },
                },
                "required": ["content"],
            },
        },
    },
]
