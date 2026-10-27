"""Strict parsing of the harness configuration (``/run/srw/config.json``).

Standard library only. Nothing here reaches code execution: every key is typed,
bounded and checked against an allow-list; unknown keys are refused.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from typing import Literal

TOOL_NAMES = ("Bash", "Read", "Write", "Edit")
EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")
_ENV_NAME = re.compile(r"[A-Z_][A-Z0-9_]*")
_CREDENTIAL_ENV = ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY")
# The SDK passes each prompt text as one CLI argument; Linux caps a single
# argument at 131072 bytes, so a larger value would fail the launch opaquely.
SYSTEM_PROMPT_MAX_BYTES = 65536
MODEL_MAX_CHARS = 256
THINKING_BUDGET_MIN = 1024  # the API's minimum
THINKING_BUDGET_MAX = 1_000_000

FIXED_SETTINGS = {
    "permissionMode": "default",
    "settingSources": [],
    "toolSearch": False,
    "autoMemory": False,
    "claudeMd": False,
    "nonessentialTraffic": False,
    "fallbackModel": None,
    "outputLimitChars": 30000,
    "mcpServerName": "ws",
}


class ConfigError(ValueError):
    """The harness configuration, task or bindings are invalid."""


@dataclass(frozen=True)
class Limits:
    max_turns: int = 200
    max_budget_usd: float = 20.0
    timeout_seconds: int = 3600


@dataclass(frozen=True)
class SystemPrompt:
    kind: Literal["preset", "preset_append", "custom"]
    text: str | None = None


@dataclass(frozen=True)
class HarnessConfig:
    model: str
    system_prompt: SystemPrompt
    tools: tuple[str, ...]
    limits: Limits
    effort: str | None
    thinking: dict | None
    auto_compact: bool
    workspace_env: tuple[str, ...]


def _object(value: object, name: str) -> dict:
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be an object")
    return value


def _reject_unknown(data: dict, allowed: set[str], prefix: str = "") -> None:
    for key in data:
        if key not in allowed:
            raise ConfigError(f"unknown setting: {prefix}{key}")


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_prompt_size(text: str, name: str) -> None:
    try:
        size = len(text.encode("utf-8"))
    except UnicodeEncodeError:  # lone surrogates from JSON escapes
        raise ConfigError(f"{name} must be valid UTF-8 text") from None
    if size > SYSTEM_PROMPT_MAX_BYTES:
        raise ConfigError(
            f"{name} must be at most {SYSTEM_PROMPT_MAX_BYTES} bytes (UTF-8)"
        )


def _valid_model(value: object) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= MODEL_MAX_CHARS
        and not any(
            ch.isspace() or unicodedata.category(ch) in ("Cc", "Cs") for ch in value
        )
    )


def _parse_system_prompt(value: object) -> SystemPrompt:
    obj = _object(value, "systemPrompt")
    _reject_unknown(obj, {"type", "append", "text"}, "systemPrompt.")
    kind = obj.get("type")
    if kind == "preset":
        if "text" in obj:
            raise ConfigError("systemPrompt: text is only valid for type custom")
        if "append" not in obj:
            return SystemPrompt("preset")
        append = obj["append"]
        if not isinstance(append, str) or not append:
            raise ConfigError("systemPrompt.append must be a non-empty string")
        _check_prompt_size(append, "systemPrompt.append")
        return SystemPrompt("preset_append", append)
    if kind == "custom":
        if "append" in obj:
            raise ConfigError("systemPrompt: append is only valid for type preset")
        text = obj.get("text")
        if not isinstance(text, str) or not text:
            raise ConfigError("systemPrompt: custom requires a non-empty text")
        _check_prompt_size(text, "systemPrompt.text")
        return SystemPrompt("custom", text)
    raise ConfigError("systemPrompt.type must be 'preset' or 'custom'")


def _parse_limits(value: object) -> Limits:
    obj = _object(value, "limits")
    _reject_unknown(obj, {"maxTurns", "maxBudgetUsd", "timeoutSeconds"}, "limits.")
    defaults = Limits()
    max_turns = obj.get("maxTurns", defaults.max_turns)
    if not _is_int(max_turns) or not 1 <= max_turns <= 10000:
        raise ConfigError("limits.maxTurns must be an integer between 1 and 10000")
    budget = obj.get("maxBudgetUsd", defaults.max_budget_usd)
    if not _is_number(budget) or not 0 < budget <= 1000:
        raise ConfigError(
            "limits.maxBudgetUsd must be a number above 0 and at most 1000"
        )
    timeout = obj.get("timeoutSeconds", defaults.timeout_seconds)
    if not _is_int(timeout) or not 60 <= timeout <= 604800:
        raise ConfigError(
            "limits.timeoutSeconds must be an integer between 60 and 604800"
        )
    return Limits(max_turns, float(budget), timeout)


def _parse_tools(value: object) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(t, str) or t not in TOOL_NAMES for t in value)
        or len(set(value)) != len(value)
    ):
        raise ConfigError(
            f"tools must be a non-empty list of distinct names from {list(TOOL_NAMES)}"
        )
    return tuple(value)


def _parse_thinking(value: object) -> dict:
    obj = _object(value, "thinking")
    kind = obj.get("type")
    if kind in ("adaptive", "disabled"):
        _reject_unknown(obj, {"type"}, "thinking.")
        return {"type": kind}
    if kind == "enabled":
        _reject_unknown(obj, {"type", "budget_tokens"}, "thinking.")
        budget = obj.get("budget_tokens")
        if (
            not _is_int(budget)
            or not THINKING_BUDGET_MIN <= budget <= THINKING_BUDGET_MAX
        ):
            raise ConfigError(
                "thinking.budget_tokens must be an integer between "
                f"{THINKING_BUDGET_MIN} and {THINKING_BUDGET_MAX}"
            )
        return {"type": "enabled", "budget_tokens": budget}
    raise ConfigError("thinking.type must be 'adaptive', 'enabled' or 'disabled'")


def _parse_workspace_env(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(n, str) or not _ENV_NAME.fullmatch(n) for n in value
    ):
        raise ConfigError(
            "workspaceEnv must be a list of upper-case environment variable names"
        )
    for name in value:
        if name in _CREDENTIAL_ENV:
            raise ConfigError("workspaceEnv must not expose the model credential")
    return tuple(value)


def parse_config(data: object) -> HarnessConfig:
    obj = _object(data, "config")
    _reject_unknown(
        obj,
        {
            "version",
            "model",
            "systemPrompt",
            "tools",
            "limits",
            "effort",
            "thinking",
            "autoCompact",
            "workspaceEnv",
        },
    )
    version = obj.get("version")
    if not _is_int(version) or version != 1:
        raise ConfigError("version must be 1")
    model = obj.get("model")
    if not _valid_model(model):
        raise ConfigError(
            f"model is required: a non-empty string of at most {MODEL_MAX_CHARS} "
            "characters without whitespace or control characters"
        )
    system_prompt = (
        _parse_system_prompt(obj["systemPrompt"])
        if "systemPrompt" in obj
        else SystemPrompt("preset")
    )
    tools = _parse_tools(obj["tools"]) if "tools" in obj else TOOL_NAMES
    limits = _parse_limits(obj["limits"]) if "limits" in obj else Limits()
    effort = obj.get("effort")
    if "effort" in obj and (not isinstance(effort, str) or effort not in EFFORT_LEVELS):
        raise ConfigError(f"effort must be one of {list(EFFORT_LEVELS)}")
    thinking = _parse_thinking(obj["thinking"]) if "thinking" in obj else None
    auto_compact = obj.get("autoCompact", True)
    if not isinstance(auto_compact, bool):
        raise ConfigError("autoCompact must be a boolean")
    workspace_env = (
        _parse_workspace_env(obj["workspaceEnv"]) if "workspaceEnv" in obj else ()
    )
    return HarnessConfig(
        model=model,
        system_prompt=system_prompt,
        tools=tools,
        limits=limits,
        effort=effort,
        thinking=thinking,
        auto_compact=auto_compact,
        workspace_env=workspace_env,
    )


def effective_settings(config: HarnessConfig) -> dict:
    """Every effective setting (configured and fixed) as a JSON-able dict."""
    prompt = config.system_prompt
    if prompt.kind == "preset":
        system_prompt: dict = {"type": "preset"}
    elif prompt.kind == "preset_append":
        system_prompt = {"type": "preset", "append": prompt.text}
    else:
        system_prompt = {"type": "custom", "text": prompt.text}
    return {
        "model": config.model,
        "systemPrompt": system_prompt,
        "tools": list(config.tools),
        "limits": {
            "maxTurns": config.limits.max_turns,
            "maxBudgetUsd": config.limits.max_budget_usd,
            "timeoutSeconds": config.limits.timeout_seconds,
        },
        "effort": config.effort,
        "thinking": dict(config.thinking) if config.thinking is not None else None,
        "autoCompact": config.auto_compact,
        "workspaceEnv": list(config.workspace_env),
        **{
            k: (list(v) if isinstance(v, list) else v)
            for k, v in FIXED_SETTINGS.items()
        },
    }


def config_hash(data: object) -> str:
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
