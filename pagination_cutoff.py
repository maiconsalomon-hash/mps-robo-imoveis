"""
Corte inteligente de paginação (MINI-ETAPA 4B) — fim útil da listagem / cauda ruim.

Constantes alinhadas a ``PAGINATION_*`` em ``scraper.py`` (manter sincronizado).
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any

FULL_PAGE_MIN = 18
TAIL_RATIO = 0.34
TAIL_MAX_ITEMS = 18


@dataclass
class PaginationCutoffDecision:
    should_stop: bool
    reason: str = ""


@dataclass
class PaginationRunningStats:
    """Estado mutável ao longo do loop de listagem."""

    page_volumes_full: list[int] = field(default_factory=list)
    consecutive_low_yield: int = 0
    pages_low_yield_count: int = 0


def compute_typical_page_volume(
    running: PaginationRunningStats, max_listing_peak: int, n_new: int
) -> float:
    """Mediana das últimas páginas ``cheias``; fallback para pico ou página atual."""
    if running.page_volumes_full:
        return float(statistics.median(running.page_volumes_full))
    if max_listing_peak > 0:
        return float(max_listing_peak)
    return float(n_new)


def register_full_page_volume(running: PaginationRunningStats, n_new: int) -> None:
    if n_new >= FULL_PAGE_MIN:
        running.page_volumes_full.append(n_new)
        if len(running.page_volumes_full) > 6:
            running.page_volumes_full.pop(0)


def should_stop_pagination(
    *,
    n_bruto: int,
    n_new: int,
    dup_n: int,
    page_num: int,
    max_listing_peak: int,
    running: PaginationRunningStats,
    baseline: dict[str, Any] | None = None,
) -> PaginationCutoffDecision:
    """
    Decide se a paginação deve parar após processar uma página com itens novos.

    ``baseline`` reservado para evoluções (ex.: cruzar com histórico); hoje não obrigatório.
    """
    _ = baseline
    y = n_new / max(n_bruto, 1)

    if n_bruto >= 10 and y < 0.2:
        running.consecutive_low_yield += 1
    else:
        running.consecutive_low_yield = 0

    if n_bruto >= 8 and y < 0.35:
        running.pages_low_yield_count += 1

    # 1) Cauda / destaques (equivalente ao tail_cut histórico)
    if (
        max_listing_peak >= FULL_PAGE_MIN
        and 0 < n_new < max_listing_peak * TAIL_RATIO
        and n_new <= TAIL_MAX_ITEMS
    ):
        return PaginationCutoffDecision(True, "tail_volume_low")

    # 2) Quase tudo repetido na página (vitrine / reaproveitamento)
    if (
        n_bruto >= 8
        and max_listing_peak >= FULL_PAGE_MIN
        and dup_n >= int(n_bruto * 0.82)
    ):
        return PaginationCutoffDecision(True, "excessive_duplicate_ratio")

    # 3) Duas páginas seguidas com ganho líquido muito baixo
    if (
        running.consecutive_low_yield >= 2
        and max_listing_peak >= FULL_PAGE_MIN
        and page_num > 2
    ):
        return PaginationCutoffDecision(True, "consecutive_low_yield")

    return PaginationCutoffDecision(False, "")


def should_stop_pagination_from_stats(
    page_stats: dict[str, Any],
    running: PaginationRunningStats,
    baseline: dict[str, Any] | None = None,
) -> PaginationCutoffDecision:
    """API alternativa: ``page_stats`` com chaves ``n_bruto``, ``n_new``, ``dup_n``, ``page_num``, ``max_listing_peak``."""
    return should_stop_pagination(
        n_bruto=int(page_stats["n_bruto"]),
        n_new=int(page_stats["n_new"]),
        dup_n=int(page_stats["dup_n"]),
        page_num=int(page_stats["page_num"]),
        max_listing_peak=int(page_stats["max_listing_peak"]),
        running=running,
        baseline=baseline,
    )


def cutoff_log_message(reason: str) -> str:
    """Texto curto para logs (motivo interno → mensagem estável)."""
    return {
        "tail_volume_low": "volume típico de fim de listagem",
        "excessive_duplicate_ratio": "repetição excessiva",
        "consecutive_low_yield": "páginas seguidas com pouco ganho real",
        "no_new_items_after_full_pages": "0 itens novos após páginas cheias",
    }.get(reason, reason)
