from __future__ import annotations

from typing import Any, Mapping, Optional

from pydantic import BaseModel, Field, field_validator

LAYER_CODES = frozenset({"core", "identity", "autobio", "projects", "working", "buffer"})
AREA_CODES = frozenset({"identity", "relation", "projects", "knowledge", "preferences", "history", "rumination", "meta"})
STATE_CODES = frozenset({"candidate", "active", "validated", "conflicted", "archived", "superseded"})
SCOPE_CODES = frozenset({"global", "user", "project", "conversation", "system"})
DEFAULT_LAYER_CODE = "buffer"
DEFAULT_AREA_CODE = "knowledge"
DEFAULT_STATE_CODE = "active"
DEFAULT_SCOPE_CODE = "global"

# Hierarchia warstw od najniższej (buffer) do najwyższej (core).
# Używana w promote_memory / demote_memory do walidacji kierunku przejścia.
LAYER_ORDER: list[str] = ["buffer", "working", "projects", "autobio", "identity", "core"]

# Warstwy chronione przed automatyczną archiwizacją i downgrade'em przez Sandmana.
SANDMAN_PROTECTED_LAYERS: frozenset[str] = frozenset({"core", "identity"})

# State codes chronione przed downgrade'em przez Sandmana.
SANDMAN_PROTECTED_STATES: frozenset[str] = frozenset({"validated", "canonical"})


def _norm_code(value: str | None, allowed: frozenset[str], field_name: str) -> str | None:
    if value is None:
        return None
    value = value.strip().lower().replace("-", "_").replace(" ", "_")
    if not value:
        return None
    if value not in allowed:
        raise ValueError(f"{field_name} must be one of: {', '.join(sorted(allowed))}")
    return value


def normalize_layer_code(value: str | None) -> str | None:
    return _norm_code(value, LAYER_CODES, "layer_code")


def normalize_area_code(value: str | None) -> str | None:
    return _norm_code(value, AREA_CODES, "area_code")


def normalize_state_code(value: str | None) -> str | None:
    return _norm_code(value, STATE_CODES, "state_code")


def normalize_scope_code(value: str | None) -> str | None:
    return _norm_code(value, SCOPE_CODES, "scope_code")


def normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def normalize_required_text(value: str, field_name: str) -> str:
    value = normalize_optional_text(value)
    if value is None:
        raise ValueError(f"{field_name} cannot be empty")
    return value


def derive_state_code(raw_state_code: str | None, activity_state: str | None = None, contradiction_flag: Any | None = None) -> str:
    state = normalize_state_code(raw_state_code)
    if state is not None:
        return state
    if bool(contradiction_flag):
        return "conflicted"
    if normalize_optional_text(activity_state) == "archived":
        return "archived"
    return DEFAULT_STATE_CODE


def enrich_memory_dict(memory: Mapping[str, Any] | dict[str, Any]) -> dict[str, Any]:
    item = dict(memory)
    item["layer_code"] = normalize_layer_code(item.get("layer_code")) or DEFAULT_LAYER_CODE
    item["area_code"] = normalize_area_code(item.get("area_code")) or DEFAULT_AREA_CODE
    item["scope_code"] = normalize_scope_code(item.get("scope_code")) or DEFAULT_SCOPE_CODE
    item["state_code"] = derive_state_code(item.get("state_code"), item.get("activity_state"), item.get("contradiction_flag"))
    item["version"] = max(int(item.get("version") or 1), 1)
    item["decay_score"] = float(item.get("decay_score") or 0.0)
    item["emotional_weight"] = float(item.get("emotional_weight") or 0.0)
    item["identity_weight"] = float(item.get("identity_weight") or 0.0)
    return item


class MemoryCreateRequest(BaseModel):
    content: str
    summary_short: Optional[str] = None
    memory_type: str
    source: Optional[str] = None
    importance_score: float = Field(default=0.5, ge=0.0, le=1.0)
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)
    tags: Optional[str] = None
    layer_code: Optional[str] = None
    area_code: Optional[str] = None
    state_code: Optional[str] = None
    scope_code: Optional[str] = None
    parent_memory_id: Optional[int] = Field(default=None, ge=1)
    version: int = Field(default=1, ge=1)
    promoted_from_id: Optional[int] = Field(default=None, ge=1)
    demoted_from_id: Optional[int] = Field(default=None, ge=1)
    supersedes_memory_id: Optional[int] = Field(default=None, ge=1)
    valid_from: Optional[str] = None
    valid_to: Optional[str] = None
    decay_score: float = Field(default=0.0, ge=0.0, le=1.0)
    emotional_weight: float = Field(default=0.0, ge=0.0, le=1.0)
    identity_weight: float = Field(default=0.0, ge=0.0, le=1.0)
    project_key: Optional[str] = None
    conversation_key: Optional[str] = None
    last_validated_at: Optional[str] = None
    validation_source: Optional[str] = None

    @field_validator("content")
    @classmethod
    def validate_content(cls, value: str) -> str:
        return normalize_required_text(value, "content")

    @field_validator("memory_type")
    @classmethod
    def validate_memory_type(cls, value: str) -> str:
        return normalize_required_text(value, "memory_type")

    @field_validator("summary_short", "source", "tags", "valid_from", "valid_to", "project_key", "conversation_key", "last_validated_at", "validation_source", mode="before")
    @classmethod
    def validate_texts(cls, value: str | None) -> str | None:
        return normalize_optional_text(value)

    @field_validator("layer_code", mode="before")
    @classmethod
    def validate_layer_code(cls, value: str | None) -> str | None:
        return normalize_layer_code(value)

    @field_validator("area_code", mode="before")
    @classmethod
    def validate_area_code(cls, value: str | None) -> str | None:
        return normalize_area_code(value)

    @field_validator("state_code", mode="before")
    @classmethod
    def validate_state_code(cls, value: str | None) -> str | None:
        return normalize_state_code(value)

    @field_validator("scope_code", mode="before")
    @classmethod
    def validate_scope_code(cls, value: str | None) -> str | None:
        return normalize_scope_code(value)


class MemoryLinkRequest(BaseModel):
    from_memory_id: int
    to_memory_id: int
    relation_type: str
    weight: float
    origin: Optional[str] = None


class MemoryRecallRequest(BaseModel):
    memory_id: int
    recall_type: str = "direct"
    source: Optional[str] = None
    strength: Optional[float] = 0.1


class MemoryResponse(BaseModel):
    id: int
    content: str
    summary_short: Optional[str] = None
    memory_type: str
    source: Optional[str] = None
    importance_score: float
    confidence_score: float
    tags: Optional[str] = None
    layer_code: str = DEFAULT_LAYER_CODE
    area_code: str = DEFAULT_AREA_CODE
    state_code: str = DEFAULT_STATE_CODE
    scope_code: str = DEFAULT_SCOPE_CODE
    parent_memory_id: Optional[int] = None
    version: int = 1
    promoted_from_id: Optional[int] = None
    demoted_from_id: Optional[int] = None
    supersedes_memory_id: Optional[int] = None
    valid_from: Optional[str] = None
    valid_to: Optional[str] = None
    decay_score: float = 0.0
    emotional_weight: float = 0.0
    identity_weight: float = 0.0
    project_key: Optional[str] = None
    conversation_key: Optional[str] = None
    last_validated_at: Optional[str] = None
    validation_source: Optional[str] = None


class MemoryLinkResponse(BaseModel):
    id: int
    from_memory_id: int
    to_memory_id: int
    relation_type: str
    weight: float
    origin: Optional[str] = None
