-- Durable acknowledgement of controller-side recovery retention pins.

ALTER TABLE vm_workspace_recovery_retention_pins
    ADD COLUMN controller_pinned_at timestamptz,
    ADD COLUMN controller_pin_uid text,
    ADD COLUMN controller_pin_resource_version text,
    ADD COLUMN controller_release_requested_at timestamptz,
    ADD COLUMN controller_released_at timestamptz,
    ADD COLUMN controller_sync_attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN controller_sync_error text,
    ADD COLUMN controller_sync_after timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD CONSTRAINT vm_workspace_recovery_controller_pin_uid_nonempty
        CHECK (controller_pin_uid IS NULL OR controller_pin_uid <> ''),
    ADD CONSTRAINT vm_workspace_recovery_controller_pin_version_nonempty
        CHECK (
            controller_pin_resource_version IS NULL
            OR controller_pin_resource_version <> ''
        ),
    ADD CONSTRAINT vm_workspace_recovery_controller_pin_ack_shape
        CHECK (
            (controller_pinned_at IS NULL AND controller_pin_uid IS NULL
                AND controller_pin_resource_version IS NULL)
            OR
            (controller_pinned_at IS NOT NULL AND controller_pin_uid IS NOT NULL
                AND controller_pin_resource_version IS NOT NULL)
        ),
    ADD CONSTRAINT vm_workspace_recovery_controller_release_ack_shape
        CHECK (
            controller_released_at IS NULL
            OR (released_at IS NOT NULL AND controller_pinned_at IS NOT NULL)
        );

CREATE INDEX vm_workspace_recovery_retention_pin_sync_due_idx
    ON vm_workspace_recovery_retention_pins (controller_sync_after)
    WHERE controller_released_at IS NULL;
