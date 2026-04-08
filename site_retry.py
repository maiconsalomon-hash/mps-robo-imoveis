"""
MINI-ETAPA 7B — retry conservador por site após falhas transitórias (rede/render).
"""
from __future__ import annotations

import sqlite3
from typing import Any

from site_health_history import PATTERN_CONSISTENTLY_BROKEN, get_site_health_history

RETRY_RESULT_NOT_ATTEMPTED = "não_tentado"


def _normalize_error_code(raw: str) -> str:
    return (raw or "").strip().upper().replace("-", "_")


def pick_retry_candidates(
    round_results: list[dict[str, Any]],
    sites_by_id: dict[int, dict],
    conn: sqlite3.Connection,
    *,
    eligible_statuses: set[str],
    max_sites: int,
) -> list[dict[str, Any]]:
    """
    Escolhe até ``max_sites`` sites para segunda tentativa na mesma rodada.

    Retorna lista de dicts: ``site`` (dict do SITES), ``original_status``, ``reason``.
    """
    if max_sites <= 0 or not eligible_statuses:
        return []

    eligible_norm = {_normalize_error_code(s) for s in eligible_statuses}
    out: list[dict[str, Any]] = []

    for r in round_results:
        if len(out) >= max_sites:
            break
        st = _normalize_error_code(str(r.get("extraction_status") or ""))
        if not st.startswith("ERRO"):
            continue
        if st not in eligible_norm:
            continue
        sid = int(r.get("site_id") or 0)
        if not sid or sid not in sites_by_id:
            continue
        h = get_site_health_history(conn, sid, last_n=10, site_name_hint=r.get("site_name"))
        if h.get("trend_pattern") == PATTERN_CONSISTENTLY_BROKEN:
            continue
        reason = (
            f"falha_transitória({st});histórico_não_CONSISTENTLY_BROKEN;"
            f"retry_automático_7B"
        )
        out.append(
            {
                "site": sites_by_id[sid],
                "original_status": st,
                "reason": reason,
            }
        )
    return out
