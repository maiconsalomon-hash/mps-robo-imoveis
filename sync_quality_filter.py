"""
MINI-ETAPA 8A — filtro de qualidade antes do upsert no Supabase.

O SQLite continua com todos os registros; apenas o payload do sync é filtrado.
"""
from __future__ import annotations

from typing import Any

from config import (
    SYNC_BLOCK_LEGACY_IDENTITY,
    SYNC_FILTER_ENABLED,
    SYNC_MIN_DATA_QUALITY_SCORE,
)


def classify_sync_filter_row(row: dict[str, Any]) -> str | None:
    """
    Retorna ``None`` se o imóvel pode ir ao Supabase; caso contrário o código do bloqueio:
    ``low_data_quality``, ``low_identity_quality``, ``legacy_identity``.
    """
    if not SYNC_FILTER_ENABLED:
        return None

    try:
        sc = int(row.get("data_quality_score")) if row.get("data_quality_score") is not None else 0
    except (TypeError, ValueError):
        sc = 0
    if sc < SYNC_MIN_DATA_QUALITY_SCORE:
        return "low_data_quality"

    iq = str(row.get("identity_quality") or "").strip().upper()
    if iq == "LOW":
        return "low_identity_quality"

    if SYNC_BLOCK_LEGACY_IDENTITY:
        src = str(row.get("identity_source") or "").strip()
        try:
            fb = int(row.get("identity_fallback") or 0)
        except (TypeError, ValueError):
            fb = 0
        if fb == 1 or src == "legacy_fallback":
            return "legacy_identity"

    return None


def empty_reason_counts() -> dict[str, int]:
    return {
        "low_data_quality": 0,
        "low_identity_quality": 0,
        "legacy_identity": 0,
    }
