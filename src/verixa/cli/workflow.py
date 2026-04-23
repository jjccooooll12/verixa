"""Shared workflow command metadata for CLI runners."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


CaptureMode = Literal["snapshot", "plan", "test"]


@dataclass(frozen=True, slots=True)
class WorkflowCommandSpec:
    """Normalized command metadata shared across CLI runners."""

    display_command: str
    capture_mode: CaptureMode
    query_tag: str


def resolve_workflow_command(command: str) -> WorkflowCommandSpec:
    """Normalize a public command name into its workflow semantics."""

    normalized = command.lower()
    if normalized == "snapshot":
        return WorkflowCommandSpec(
            display_command="snapshot",
            capture_mode="snapshot",
            query_tag="verixa:snapshot",
        )
    if normalized in {"diff", "plan"}:
        return WorkflowCommandSpec(
            display_command="diff",
            capture_mode="plan",
            query_tag="verixa:diff",
        )
    if normalized == "check":
        return WorkflowCommandSpec(
            display_command="check",
            capture_mode="plan",
            query_tag="verixa:check",
        )
    if normalized in {"validate", "test"}:
        return WorkflowCommandSpec(
            display_command="validate",
            capture_mode="test",
            query_tag="verixa:validate",
        )
    raise ValueError(f"Unsupported workflow command '{command}'.")


def query_tag_for_command(command: str) -> str:
    """Return the Snowflake query tag used for one workflow command."""

    return resolve_workflow_command(command).query_tag
