"""Pointers to exact preserve cleanup; a receipt never grants authority alone."""

from dataclasses import asdict, dataclass
from typing import Any, Mapping
from uuid import UUID


@dataclass(frozen=True)
class ContainerRecoveryCleanup:
    job_id: str
    command_id: str
    hold_id: str
    runtime_incarnation: str
    intent_id: str
    intent_generation: int
    phase: str = "pending"
    version: int = 1

    @classmethod
    def parse(cls, value: Any) -> "ContainerRecoveryCleanup | None":
        if not isinstance(value, Mapping) or set(value) != set(
            cls.__dataclass_fields__
        ):
            return None
        if (
            type(value.get("version")) is not int
            or value["version"] != 1
            or not isinstance(value.get("phase"), str)
            or value.get("phase") not in {"pending", "settled"}
            or type(value.get("intent_generation")) is not int
            or value["intent_generation"] <= 0
        ):
            return None
        try:
            for key in (
                "job_id",
                "command_id",
                "hold_id",
                "runtime_incarnation",
                "intent_id",
            ):
                if str(UUID(value[key])) != value[key]:
                    return None
        except (TypeError, ValueError, AttributeError):
            return None
        return cls(**value)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def matches_intent(self, intent: Mapping[str, Any]) -> bool:
        return bool(
            str(intent.get("id")) == self.intent_id
            and intent.get("owner_kind") == "job"
            and str(intent.get("owner_id")) == self.job_id
            and intent.get("scope") == "workspace_container"
            and str(intent.get("runtime_incarnation")) == self.runtime_incarnation
            and intent.get("intent_generation") == self.intent_generation
            and intent.get("target_disposition") == "deleted"
            and intent.get("resource_policy") == "preserve"
            and intent.get("reclaim_shared_resources") is False
        )

    def matches_owner(self, status: str, context: Mapping[str, Any]) -> bool:
        workspace = context.get("workspace_container") or {}
        hold = context.get("_operator_pause_hold") or {}
        if not isinstance(workspace, dict):
            return False
        current = self.parse(workspace.get("recovery_cleanup"))
        return bool(
            status == "paused"
            and isinstance(hold, dict)
            and hold.get("hold_id") == self.hold_id
            and hold.get("source") == "workspace_recovery_unavailable"
            and workspace.get("_runtime_incarnation") == self.runtime_incarnation
            and workspace.get("recovery_completion_command_id") == self.command_id
            and current is not None
            and {**current.as_dict(), "phase": self.phase} == self.as_dict()
        )


def container_recovery_resume_allowed_sql(
    context: str, owner_id: str = "jobs.id"
) -> str:
    """Fail closed before consuming a hold or shedding its exact runtime.

    The native cleanup row, not a context phase string, proves settlement.
    Keeping this on the pre-update owner projection also covers old reads
    waiting behind the recovery admission transaction.
    """
    receipt = f"{context}->'workspace_container'->'recovery_cleanup'"
    keys = ",".join(f"'{key}'" for key in ContainerRecoveryCleanup.__dataclass_fields__)
    uuid_fields = (
        "job_id",
        "command_id",
        "hold_id",
        "runtime_incarnation",
        "intent_id",
    )
    uuid_shape = " AND ".join(
        f"jsonb_typeof({receipt}->'{key}') = 'string' AND "
        f"{receipt}->>'{key}' ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'"
        for key in uuid_fields
    )
    return f"""(
        NOT (COALESCE({context}->'workspace_container','{{}}'::jsonb) ? 'recovery_cleanup')
        OR CASE WHEN jsonb_typeof({receipt}) = 'object' THEN (
            ({receipt}) ?& ARRAY[{keys}]
            AND (({receipt}) - ARRAY[{keys}]) = '{{}}'::jsonb
            AND {uuid_shape}
            AND jsonb_typeof({receipt}->'version') = 'number'
            AND {receipt}->>'version' = '1'
            AND jsonb_typeof({receipt}->'intent_generation') = 'number'
            AND {receipt}->>'phase' = 'settled'
            AND {receipt}->>'command_id' = {context}->'workspace_container'->>'recovery_completion_command_id'
            AND {receipt}->>'hold_id' = COALESCE(
                {context}->'_operator_pause_hold', {context}->'last_operator_pause_hold'
            )->>'hold_id'
            AND {receipt}->>'job_id' = {owner_id}::text
            AND {receipt}->>'runtime_incarnation' = {context}->'workspace_container'->>'_runtime_incarnation'
            AND EXISTS (
                SELECT 1 FROM managed_repository_workspace_cleanup_intents recovery_intent
                WHERE recovery_intent.owner_kind='job'
                  AND recovery_intent.owner_id={owner_id}
                  AND recovery_intent.scope='workspace_container'
                  AND recovery_intent.id::text={receipt}->>'intent_id'
                  AND recovery_intent.intent_generation::text={receipt}->>'intent_generation'
                  AND recovery_intent.runtime_incarnation::text={receipt}->>'runtime_incarnation'
                  AND recovery_intent.target_disposition='deleted'
                  AND recovery_intent.resource_policy='preserve'
                  AND recovery_intent.reclaim_shared_resources=false
                  AND recovery_intent.result_kind='settled'
                  AND recovery_intent.settled_at IS NOT NULL
            )
            AND NOT EXISTS (
                SELECT 1 FROM managed_repository_workspace_cleanup_intents newer_recovery
                WHERE newer_recovery.owner_kind='job' AND newer_recovery.owner_id={owner_id}
                  AND newer_recovery.scope='workspace_container'
                  AND newer_recovery.settled_at IS NULL
            )
        ) ELSE false END
    )"""
