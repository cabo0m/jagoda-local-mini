from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SandmanPolicy:
    name: str = "shared_sandman_policy_v1"
    preview_every_interactions: int = 20
    max_auto_write_runs_per_day: int = 3
    max_consolidation_runs_per_day: int = 1
    min_write_run_interval_seconds: int = 2 * 60 * 60
    memory_linking_default_limit: int = 20
    memory_linking_safe_limit: int = 35
    memory_linking_hard_limit: int = 50
    allow_auto_archive: bool = False
    allow_auto_demotion: bool = False
    allow_auto_conflict_resolution: bool = False
    require_preview_before_write: bool = True

    def clamp_linking_limit(self, requested: int | None) -> int:
        raw = self.memory_linking_default_limit if requested is None else int(requested)
        return max(1, min(raw, self.memory_linking_hard_limit))

    def auto_write_allowed(self, *, write_runs_today: int, seconds_since_last_write: int | None) -> tuple[bool, str]:
        if write_runs_today >= self.max_auto_write_runs_per_day:
            return False, "daily_write_run_limit_reached"
        if seconds_since_last_write is not None and seconds_since_last_write < self.min_write_run_interval_seconds:
            return False, "write_run_interval_too_short"
        return True, "allowed"

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "preview_every_interactions": self.preview_every_interactions,
            "max_auto_write_runs_per_day": self.max_auto_write_runs_per_day,
            "max_consolidation_runs_per_day": self.max_consolidation_runs_per_day,
            "min_write_run_interval_seconds": self.min_write_run_interval_seconds,
            "memory_linking_default_limit": self.memory_linking_default_limit,
            "memory_linking_safe_limit": self.memory_linking_safe_limit,
            "memory_linking_hard_limit": self.memory_linking_hard_limit,
            "allow_auto_archive": self.allow_auto_archive,
            "allow_auto_demotion": self.allow_auto_demotion,
            "allow_auto_conflict_resolution": self.allow_auto_conflict_resolution,
            "require_preview_before_write": self.require_preview_before_write,
        }


DEFAULT_SANDMAN_POLICY = SandmanPolicy()
