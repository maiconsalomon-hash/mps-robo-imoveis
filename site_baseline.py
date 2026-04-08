"""
Baseline histórico por site (MINI-ETAPA 3A).

Persistência em ``site_volume_baseline`` + fonte de verdade auditável em ``log_execucoes``.
Healthy run: ``extraction_status == OK`` e ``volume_total > 0`` (via ``total_encontrados`` no log).
"""
from __future__ import annotations

import json
import sqlite3
import statistics
from datetime import datetime
from typing import Any

from site_diagnostic import SiteRunSummary

# Quantas execuções saudáveis recentes entram nas médias / medianas
HEALTHY_RUN_WINDOW = 30


def is_healthy_run(extraction_status: str, volume_total: int) -> bool:
    """Conservador: só OK com volume coletado."""
    st = (extraction_status or "").strip().upper()
    return st == "OK" and int(volume_total or 0) > 0


def load_recent_healthy_runs(
    conn: sqlite3.Connection, site_id: int, limit: int = HEALTHY_RUN_WINDOW
) -> list[dict[str, Any]]:
    """
    Lê as últimas ``limit`` execuções saudáveis do site em ``log_execucoes`` (mais recentes primeiro).
    Cada item: log_id, data, volume, pages_succeeded.
    """
    rows = conn.execute(
        """
        SELECT id, data, total_encontrados, summary_json
        FROM log_execucoes
        WHERE site_id = ?
          AND total_encontrados > 0
          AND (
            UPPER(TRIM(COALESCE(extraction_status, ''))) = 'OK'
            OR (
              TRIM(COALESCE(extraction_status, '')) = ''
              AND LOWER(TRIM(COALESCE(status, ''))) = 'ok'
            )
          )
        ORDER BY id DESC
        LIMIT ?
        """,
        (site_id, limit),
    ).fetchall()

    out: list[dict[str, Any]] = []
    for log_id, data, total, summary_json in rows:
        pages = 0
        if summary_json:
            try:
                sj = json.loads(summary_json)
                pages = int(sj.get("pages_succeeded") or 0)
            except (json.JSONDecodeError, TypeError, ValueError):
                pages = 0
        out.append(
            {
                "log_id": log_id,
                "data": data or "",
                "volume": int(total or 0),
                "pages_succeeded": pages,
            }
        )
    return out


def compute_expected_volume_range(volumes: list[int]) -> tuple[int, int]:
    """
    Faixa esperada a partir de volumes saudáveis observados.
    Usa min/max amostral (janela já filtrada); com 1 valor, expande levemente em torno da mediana.
    """
    if not volumes:
        return 0, 0
    vols = sorted(int(v) for v in volumes)
    n = len(vols)
    lo, hi = vols[0], vols[-1]
    if n == 1:
        v = lo
        return max(1, int(v * 0.7)), max(v, int(v * 1.3))
    return max(1, lo), max(lo, hi)


def get_site_baseline(conn: sqlite3.Connection, site_id: int) -> dict[str, Any] | None:
    """Lê a linha atual de ``site_volume_baseline`` ou None."""
    row = conn.execute(
        """
        SELECT site_id, site_name, last_healthy_run_at, last_healthy_volume_total,
               avg_healthy_volume_total, median_healthy_volume_total,
               min_expected_volume, max_expected_volume,
               avg_pages_succeeded, healthy_runs_count, updated_at
        FROM site_volume_baseline
        WHERE site_id = ?
        """,
        (site_id,),
    ).fetchone()
    if not row:
        return None
    keys = (
        "site_id",
        "site_name",
        "last_healthy_run_at",
        "last_healthy_volume_total",
        "avg_healthy_volume_total",
        "median_healthy_volume_total",
        "min_expected_volume",
        "max_expected_volume",
        "avg_pages_succeeded",
        "healthy_runs_count",
        "updated_at",
    )
    return dict(zip(keys, row))


def rebuild_site_baseline_from_logs(
    conn: sqlite3.Connection, site_id: int, site_name: str
) -> None:
    """
    Recalcula baseline a partir de ``log_execucoes`` e faz upsert na tabela.
    Chamado após inserir a linha de log da execução atual.
    """
    runs = load_recent_healthy_runs(conn, site_id, HEALTHY_RUN_WINDOW)
    now = datetime.now().isoformat()

    if not runs:
        conn.execute(
            """
            INSERT INTO site_volume_baseline (
                site_id, site_name, last_healthy_run_at, last_healthy_volume_total,
                avg_healthy_volume_total, median_healthy_volume_total,
                min_expected_volume, max_expected_volume,
                avg_pages_succeeded, healthy_runs_count, updated_at
            ) VALUES (?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, ?)
            ON CONFLICT(site_id) DO UPDATE SET
                site_name = excluded.site_name,
                last_healthy_run_at = NULL,
                last_healthy_volume_total = NULL,
                avg_healthy_volume_total = NULL,
                median_healthy_volume_total = NULL,
                min_expected_volume = NULL,
                max_expected_volume = NULL,
                avg_pages_succeeded = NULL,
                healthy_runs_count = 0,
                updated_at = excluded.updated_at
            """,
            (site_id, site_name, now),
        )
        conn.commit()
        return

    volumes = [r["volume"] for r in runs]
    pages_list = [r["pages_succeeded"] for r in runs]
    last = runs[0]
    avg_v = float(statistics.mean(volumes))
    med_v = float(statistics.median(volumes))
    min_e, max_e = compute_expected_volume_range(volumes)
    avg_pages = float(statistics.mean(pages_list)) if pages_list else 0.0

    conn.execute(
        """
        INSERT INTO site_volume_baseline (
            site_id, site_name, last_healthy_run_at, last_healthy_volume_total,
            avg_healthy_volume_total, median_healthy_volume_total,
            min_expected_volume, max_expected_volume,
            avg_pages_succeeded, healthy_runs_count, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_id) DO UPDATE SET
            site_name = excluded.site_name,
            last_healthy_run_at = excluded.last_healthy_run_at,
            last_healthy_volume_total = excluded.last_healthy_volume_total,
            avg_healthy_volume_total = excluded.avg_healthy_volume_total,
            median_healthy_volume_total = excluded.median_healthy_volume_total,
            min_expected_volume = excluded.min_expected_volume,
            max_expected_volume = excluded.max_expected_volume,
            avg_pages_succeeded = excluded.avg_pages_succeeded,
            healthy_runs_count = excluded.healthy_runs_count,
            updated_at = excluded.updated_at
        """,
        (
            site_id,
            site_name,
            last["data"],
            last["volume"],
            round(avg_v, 4),
            round(med_v, 4),
            min_e,
            max_e,
            round(avg_pages, 4),
            len(runs),
            now,
        ),
    )
    conn.commit()


def update_site_baseline(
    conn: sqlite3.Connection,
    site_id: int,
    site_name: str,
    summary: SiteRunSummary | None = None,
) -> None:
    """
    Atualiza o baseline persistido a partir do histórico em ``log_execucoes``.
    ``summary`` é opcional (reservado para evoluções); a fonte de verdade é o log já gravado.
    """
    rebuild_site_baseline_from_logs(conn, site_id, site_name)


def enrich_summary_with_baseline(
    summary: SiteRunSummary,
    baseline_row: dict[str, Any] | None,
    current_volume: int,
) -> None:
    """Preenche campos de baseline no summary antes de serializar em ``summary_json``."""
    cnt = int((baseline_row or {}).get("healthy_runs_count") or 0)
    if not baseline_row or cnt <= 0:
        summary.baseline_available = False
        summary.baseline_healthy_runs_count = 0
        summary.baseline_last_healthy_volume = None
        summary.baseline_avg_volume = None
        summary.baseline_median_volume = None
        summary.baseline_expected_min = None
        summary.baseline_expected_max = None
        summary.current_vs_baseline_ratio = None
        return

    summary.baseline_available = True
    summary.baseline_healthy_runs_count = cnt
    lh = baseline_row.get("last_healthy_volume_total")
    summary.baseline_last_healthy_volume = int(lh) if lh is not None else None
    avg = baseline_row.get("avg_healthy_volume_total")
    med = baseline_row.get("median_healthy_volume_total")
    summary.baseline_avg_volume = float(avg) if avg is not None else None
    summary.baseline_median_volume = float(med) if med is not None else None
    bmin = baseline_row.get("min_expected_volume")
    bmax = baseline_row.get("max_expected_volume")
    summary.baseline_expected_min = int(bmin) if bmin is not None else None
    summary.baseline_expected_max = int(bmax) if bmax is not None else None

    denom = summary.baseline_avg_volume or summary.baseline_median_volume or 0.0
    cv = int(current_volume or 0)
    if denom > 0 and cv >= 0:
        summary.current_vs_baseline_ratio = round(cv / denom, 4)
    else:
        summary.current_vs_baseline_ratio = None
