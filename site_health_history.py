"""
MINI-ETAPA 6B — histórico de execução por fonte, padrões de tendência e alertas de queda.

Fonte principal: ``log_execucoes`` (``extraction_status``, ``summary_json``, ``total_encontrados``).
Complemento: ``site_volume_baseline`` para queda de volume vs média saudável persistida.

Não bloqueia o pipeline; diagnóstico e persistência opcional de eventos de alerta.
"""
from __future__ import annotations

import json
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from site_baseline import get_site_baseline
from site_diagnostic import SiteRunSummary

DEFAULT_LAST_N = 10

# Padrões nomeados (trend_pattern)
PATTERN_STABLE_OK = "STABLE_OK"
PATTERN_STABLE_PARTIAL = "STABLE_PARTIAL"
PATTERN_DEGRADING = "DEGRADING"
PATTERN_FLAPPING = "FLAPPING"
PATTERN_CONSISTENTLY_BROKEN = "CONSISTENTLY_BROKEN"
PATTERN_RECOVERING = "RECOVERING"
PATTERN_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"

# Códigos de alerta
ALERT_TAXA_SUCESSO_BAIXA = "ALERTA_TAXA_SUCESSO_BAIXA"
ALERT_QUEDA_QUALIDADE_DADOS = "ALERTA_QUEDA_QUALIDADE_DADOS"
ALERT_QUEDA_VOLUME_PROGRESSIVA = "ALERTA_QUEDA_VOLUME_PROGRESSIVA"
ALERT_ERROS_CONSECUTIVOS = "ALERTA_ERROS_CONSECUTIVOS"
ALERT_INSTABILIDADE = "ALERTA_INSTABILIDADE"


def migrate_site_health_alert_events(conn: sqlite3.Connection) -> None:
    """Tabela append-only de alertas históricos detectados por rodada (idempotente)."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS site_health_alert_events (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id        INTEGER NOT NULL,
            site_name      TEXT,
            round_label    TEXT,
            alert_code     TEXT NOT NULL,
            details_json   TEXT,
            created_at     TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_health_alerts_site
            ON site_health_alert_events (site_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_health_alerts_code
            ON site_health_alert_events (alert_code, created_at);
        """
    )
    conn.commit()


def _norm_status(extraction_status: str | None, legacy_status: str | None) -> str:
    st = (extraction_status or "").strip().upper()
    if st:
        return st
    leg = (legacy_status or "").strip().lower()
    if leg == "ok":
        return "OK"
    if leg == "suspeito":
        return "SUSPEITO_LEGACY"
    return "ERRO_EXTRACAO" if leg == "erro" else "ERRO_EXTRACAO"


def _is_ok(st: str) -> bool:
    return st == "OK"


def _is_suspeito(st: str) -> bool:
    return st.startswith("SUSPEITO")


def _is_erro(st: str) -> bool:
    return st.startswith("ERRO")


def _mean_data_score_from_summary_json(summary_json: str | None) -> float | None:
    if not summary_json:
        return None
    try:
        sj = json.loads(summary_json)
    except (json.JSONDecodeError, TypeError):
        return None
    th = int(sj.get("total_data_high") or 0)
    tm = int(sj.get("total_data_medium") or 0)
    tl = int(sj.get("total_data_low") or 0)
    denom = th + tm + tl
    if denom <= 0:
        return None
    ssum = float(sj.get("data_quality_score_sum") or 0)
    return round(ssum / denom, 4)


def _identity_fallback_ratio_from_summary_json(summary_json: str | None) -> float | None:
    if not summary_json:
        return None
    try:
        sj = json.loads(summary_json)
    except (json.JSONDecodeError, TypeError):
        return None
    total = int(sj.get("volume_total") or sj.get("volume_total") or 0) or int(
        sum(
            int(sj.get(k) or 0)
            for k in (
                "total_external_id",
                "total_canonical_url",
                "total_stable_fingerprint",
                "total_legacy_fallback",
            )
        )
    )
    # Prefer volume_total when present
    vt = int(sj.get("volume_total") or 0)
    if vt > 0:
        total = vt
    lf = int(sj.get("total_legacy_fallback") or 0)
    if total <= 0:
        return None
    return round(lf / total, 4)


def load_recent_execution_rows(
    conn: sqlite3.Connection, site_id: int, last_n: int = DEFAULT_LAST_N
) -> list[dict[str, Any]]:
    """Últimas execuções, mais recente primeiro."""
    rows = conn.execute(
        """
        SELECT id, data, extraction_status, status, total_encontrados, summary_json
        FROM log_execucoes
        WHERE site_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (site_id, max(1, int(last_n))),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for log_id, data, ex_st, leg_st, total, sj in rows:
        st = _norm_status(ex_st, leg_st)
        out.append(
            {
                "log_id": log_id,
                "data": data or "",
                "extraction_status": st,
                "volume": int(total or 0),
                "data_quality_mean": _mean_data_score_from_summary_json(sj),
                "identity_fallback_ratio": _identity_fallback_ratio_from_summary_json(sj),
            }
        )
    return out


def _merge_pending_run(
    rows_newest_first: list[dict[str, Any]],
    *,
    pending_status: str,
    pending_volume: int,
    pending_data_mean: float | None,
    pending_fallback_ratio: float | None,
    last_n: int,
) -> list[dict[str, Any]]:
    """Insere execução corrente (ainda não gravada) no topo da série."""
    cap = max(1, int(last_n))
    synthetic = {
        "log_id": None,
        "data": "",
        "extraction_status": pending_status.strip().upper(),
        "volume": int(pending_volume or 0),
        "data_quality_mean": pending_data_mean,
        "identity_fallback_ratio": pending_fallback_ratio,
        "_pending": True,
    }
    merged = [synthetic] + rows_newest_first[: cap - 1]
    return merged[:cap]


def _chronological(series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return list(reversed(series))


def _success_suspicion_error_rates(series_chr: list[dict[str, Any]]) -> tuple[float, float, float]:
    n = len(series_chr)
    if n <= 0:
        return 0.0, 0.0, 0.0
    ok = sum(1 for r in series_chr if _is_ok(r["extraction_status"]))
    sus = sum(1 for r in series_chr if _is_suspeito(r["extraction_status"]))
    err = sum(1 for r in series_chr if _is_erro(r["extraction_status"]))
    return ok / n, sus / n, err / n


def _volume_trend_ok_runs(series_chr: list[dict[str, Any]]) -> str:
    vols = [r["volume"] for r in series_chr if _is_ok(r["extraction_status"]) and r["volume"] > 0]
    if len(vols) < 2:
        return "stable"
    head, tail = vols[: max(1, len(vols) // 2)], vols[max(1, len(vols) // 2) :]
    a = statistics.mean(head)
    b = statistics.mean(tail)
    if a <= 0:
        return "stable"
    ratio = b / a
    if ratio < 0.7:
        return "declining"
    if ratio > 1.3:
        return "growing"
    return "stable"


def _data_quality_trend_ok_runs(series_chr: list[dict[str, Any]]) -> str:
    """Tendência do score médio de dados apenas em execuções OK (ordem cronológica)."""
    scores = [
        float(r["data_quality_mean"])
        for r in series_chr
        if _is_ok(r["extraction_status"]) and r.get("data_quality_mean") is not None
    ]
    if len(scores) < 2:
        return "stable"
    mid = max(1, len(scores) // 2)
    a = statistics.mean(scores[:mid])
    b = statistics.mean(scores[mid:])
    if b < a - 5.0:
        return "declining"
    if b > a + 5.0:
        return "growing"
    return "stable"


def _bad_rate(st: str) -> float:
    return 1.0 if (not _is_ok(st)) else 0.0


def _degrading_trend(series_chr: list[dict[str, Any]]) -> bool:
    n = len(series_chr)
    if n < 4:
        return False
    mid = n // 2
    older = series_chr[:mid]
    newer = series_chr[mid:]
    br_o = statistics.mean(_bad_rate(r["extraction_status"]) for r in older) if older else 0.0
    br_n = statistics.mean(_bad_rate(r["extraction_status"]) for r in newer) if newer else 0.0
    return br_n >= br_o + 0.25 and br_n >= 0.35


def _flapping_ok_erro(series_chr: list[dict[str, Any]]) -> bool:
    flags: list[str] = []
    for r in series_chr:
        st = r["extraction_status"]
        if _is_ok(st):
            flags.append("OK")
        elif _is_erro(st):
            flags.append("ERRO")
        # SUSPEITO não entra na alternância estrita OK/ERRO
    if len(flags) < 4:
        return False
    flips = sum(1 for i in range(len(flags) - 1) if flags[i] != flags[i + 1])
    return flips >= 3 and set(flags) == {"OK", "ERRO"}


def _recovering(series_chr: list[dict[str, Any]]) -> bool:
    n = len(series_chr)
    if n < 4:
        return False
    older = series_chr[: n // 2]
    newer = series_chr[n // 2 :]
    bad_o = sum(1 for r in older if not _is_ok(r["extraction_status"]))
    bad_n = sum(1 for r in newer if not _is_ok(r["extraction_status"]))
    tail_ok = all(_is_ok(r["extraction_status"]) for r in series_chr[-3:])
    return bad_o >= max(2, len(older) // 2) and bad_n == 0 and tail_ok and len(newer) >= 2


def _consistently_broken(sr: float, sus_r: float, err_r: float) -> bool:
    return sr < 0.45 and (sus_r + err_r) >= 0.55


def detect_trend_pattern(series_chr: list[dict[str, Any]]) -> str:
    n = len(series_chr)
    if n < 3:
        return PATTERN_INSUFFICIENT_DATA
    sr, sus_r, err_r = _success_suspicion_error_rates(series_chr)

    if _recovering(series_chr):
        return PATTERN_RECOVERING
    if _consistently_broken(sr, sus_r, err_r):
        return PATTERN_CONSISTENTLY_BROKEN
    if _flapping_ok_erro(series_chr):
        return PATTERN_FLAPPING
    if _degrading_trend(series_chr):
        return PATTERN_DEGRADING

    if sr >= 0.7 and sus_r <= 0.2 and err_r <= 0.1:
        vtr = _volume_trend_ok_runs(series_chr)
        if vtr == "declining" and sus_r + err_r > 0.05:
            return PATTERN_STABLE_PARTIAL
        return PATTERN_STABLE_OK
    if sr >= 0.5 and (sus_r >= 0.15 or err_r > 0):
        return PATTERN_STABLE_PARTIAL
    if sr >= 0.5:
        return PATTERN_STABLE_OK
    return PATTERN_STABLE_PARTIAL


def _consecutive_errors_from_newest(series_newest_first: list[dict[str, Any]]) -> int:
    c = 0
    for r in series_newest_first:
        if _is_erro(r["extraction_status"]):
            c += 1
        else:
            break
    return c


def _last_ok_at(series_chr: list[dict[str, Any]]) -> str | None:
    for r in reversed(series_chr):
        if _is_ok(r["extraction_status"]):
            return r.get("data") or None
    return None


def _avg_data_quality_ok_runs(series_chr: list[dict[str, Any]]) -> float | None:
    scores = [r["data_quality_mean"] for r in series_chr if _is_ok(r["extraction_status"])]
    scores = [s for s in scores if s is not None]
    if not scores:
        return None
    return float(statistics.mean(scores))


def compute_active_alerts(
    *,
    series_chr: list[dict[str, Any]],
    series_newest_first: list[dict[str, Any]],
    trend_pattern: str,
    success_rate: float,
    volume_trend: str,
    baseline_avg_volume: float | None,
    site_name: str,
) -> list[dict[str, Any]]:
    """Lista de alertas com código e detalhes (não bloqueantes)."""
    alerts: list[dict[str, Any]] = []

    n = len(series_chr)
    if n >= 3 and success_rate < 0.5:
        alerts.append(
            {
                "code": ALERT_TAXA_SUCESSO_BAIXA,
                "detail": f"{site_name}: taxa de sucesso {success_rate:.0%} nas últimas {n} execuções (limite 50%).",
                "success_rate": round(success_rate, 4),
            }
        )

    # Qualidade: últimas 3 OK vs restante histórico na janela
    ok_chr = [r for r in series_chr if _is_ok(r["extraction_status"])]
    recent_ok = ok_chr[-3:] if len(ok_chr) >= 3 else ok_chr
    older_ok = ok_chr[:-3] if len(ok_chr) > 3 else []
    scores_recent = [r["data_quality_mean"] for r in recent_ok if r.get("data_quality_mean") is not None]
    scores_old = [r["data_quality_mean"] for r in older_ok if r.get("data_quality_mean") is not None]
    if len(scores_recent) >= 1 and len(scores_old) >= 1:
        mn_r = statistics.mean(scores_recent)
        mn_o = statistics.mean(scores_old)
        if mn_o - mn_r > 15.0:
            alerts.append(
                {
                    "code": ALERT_QUEDA_QUALIDADE_DADOS,
                    "detail": (
                        f"{site_name}: score médio de dados caiu {mn_o - mn_r:.1f} pts "
                        f"(histórico na janela {mn_o:.1f} → recente {mn_r:.1f})."
                    ),
                    "mean_historical": round(mn_o, 2),
                    "mean_recent": round(mn_r, 2),
                }
            )

    vols_ok = [r["volume"] for r in series_chr if _is_ok(r["extraction_status"]) and r["volume"] > 0]
    if baseline_avg_volume and baseline_avg_volume > 0 and len(vols_ok) >= 2:
        mv = statistics.mean(vols_ok[-5:])
        drop = 1.0 - (mv / baseline_avg_volume)
        if drop > 0.30:
            alerts.append(
                {
                    "code": ALERT_QUEDA_VOLUME_PROGRESSIVA,
                    "detail": (
                        f"{site_name}: volume médio recente ({mv:.0f}) ~{drop:.0%} abaixo do baseline ({baseline_avg_volume:.0f})."
                    ),
                    "mean_recent_volume": round(mv, 2),
                    "baseline_avg": round(baseline_avg_volume, 2),
                }
            )

    ce = _consecutive_errors_from_newest(series_newest_first)
    if ce >= 3:
        alerts.append(
            {
                "code": ALERT_ERROS_CONSECUTIVOS,
                "detail": f"{site_name}: {ce} execuções consecutivas com ERRO_*.",
                "consecutive_errors": ce,
            }
        )

    if trend_pattern == PATTERN_FLAPPING:
        alerts.append(
            {
                "code": ALERT_INSTABILIDADE,
                "detail": f"{site_name}: padrão FLAPPING (alternância OK/ERRO) nas execuções recentes.",
            }
        )

    return alerts


@dataclass
class SiteHealthSnapshot:
    """Resultado interno de uma avaliação de histórico."""

    trend_pattern: str
    success_rate: float
    suspicion_rate: float
    error_rate: float
    volume_trend: str
    data_quality_trend: str
    avg_data_quality_score: float | None
    active_alerts: list[dict[str, Any]]
    last_ok_at: str | None
    consecutive_errors: int
    execution_count: int
    instability_note: list[str]


def evaluate_site_health_from_series(
    series_newest_first: list[dict[str, Any]],
    *,
    site_name: str = "",
    baseline_avg_volume: float | None = None,
) -> SiteHealthSnapshot:
    """Avalia métricas e alertas a partir de uma série já truncada (mais recente primeiro)."""
    series_chr = _chronological(series_newest_first)
    n = len(series_chr)
    sr, sus_r, err_r = _success_suspicion_error_rates(series_chr)
    pattern = detect_trend_pattern(series_chr)
    v_tr = _volume_trend_ok_runs(series_chr)
    dq = _avg_data_quality_ok_runs(series_chr)
    dq_tr = _data_quality_trend_ok_runs(series_chr)
    alerts = compute_active_alerts(
        series_chr=series_chr,
        series_newest_first=series_newest_first,
        trend_pattern=pattern,
        success_rate=sr,
        volume_trend=v_tr,
        baseline_avg_volume=baseline_avg_volume,
        site_name=site_name or "?",
    )
    note: list[str] = []
    if n >= 4:
        vrs = [float(r["volume"]) for r in series_chr]
        try:
            if len(vrs) >= 3 and statistics.pstdev(vrs) / (statistics.mean(vrs) + 1e-6) > 0.45:
                note.append("variância_alta_volume_entre_execuções")
        except statistics.StatisticsError:
            pass
    return SiteHealthSnapshot(
        trend_pattern=pattern,
        success_rate=sr,
        suspicion_rate=sus_r,
        error_rate=err_r,
        volume_trend=v_tr,
        data_quality_trend=dq_tr,
        avg_data_quality_score=dq,
        active_alerts=alerts,
        last_ok_at=_last_ok_at(series_chr),
        consecutive_errors=_consecutive_errors_from_newest(series_newest_first),
        execution_count=n,
        instability_note=note,
    )


def get_site_health_history(
    conn: sqlite3.Connection,
    site_id: int,
    last_n: int = DEFAULT_LAST_N,
    *,
    site_name_hint: str | None = None,
) -> dict[str, Any]:
    """
    Diagnóstico consultável por site a partir do log.

    Retorna dicionário com taxas, padrão de tendência, alertas ativos (da avaliação atual)
    e metadados.
    """
    rows = load_recent_execution_rows(conn, site_id, last_n)
    name = site_name_hint or ""
    if rows:
        r0 = conn.execute(
            "SELECT site_name FROM log_execucoes WHERE site_id = ? ORDER BY id DESC LIMIT 1",
            (site_id,),
        ).fetchone()
        if r0 and r0[0]:
            name = r0[0]
    base = get_site_baseline(conn, site_id)
    bavg = float(base["avg_healthy_volume_total"]) if base and base.get("avg_healthy_volume_total") else None

    snap = evaluate_site_health_from_series(rows, site_name=name, baseline_avg_volume=bavg)
    return {
        "site_id": site_id,
        "site_name": name,
        "trend_pattern": snap.trend_pattern,
        "success_rate": round(snap.success_rate, 4),
        "error_rate": round(snap.error_rate, 4),
        "suspicion_rate": round(snap.suspicion_rate, 4),
        "avg_data_quality_score": snap.avg_data_quality_score,
        "volume_trend": snap.volume_trend,
        "data_quality_trend": snap.data_quality_trend,
        "active_alerts": snap.active_alerts,
        "last_ok_at": snap.last_ok_at,
        "consecutive_errors": snap.consecutive_errors,
        "execution_count_in_window": snap.execution_count,
        "instability_notes": snap.instability_note,
        "recent_executions_preview": [
            {
                "extraction_status": r["extraction_status"],
                "volume": r["volume"],
                "data": r.get("data"),
            }
            for r in rows[:5]
        ],
    }


def enrich_summary_with_site_health(
    summary: SiteRunSummary,
    conn: sqlite3.Connection,
    site: dict[str, Any],
    pending_extraction_status: str,
    *,
    last_n: int = DEFAULT_LAST_N,
) -> None:
    """
    Preenche campos MINI-ETAPA 6B no ``SiteRunSummary`` **antes** de serializar no log.
    Usa até last_n-1 linhas do banco + execução pendente sintética.
    """
    rows_db = load_recent_execution_rows(conn, int(site["id"]), last_n)
    dq_denom = summary.total_data_high + summary.total_data_medium + summary.total_data_low
    pending_mean = (
        float(summary.data_quality_score_sum) / dq_denom if dq_denom > 0 else None
    )
    tot_id = (
        int(summary.total_external_id or 0)
        + int(summary.total_canonical_url or 0)
        + int(summary.total_stable_fingerprint or 0)
        + int(summary.total_legacy_fallback or 0)
    )
    pend_fb = (
        float(summary.total_legacy_fallback) / int(summary.volume_total or tot_id or 1)
        if (summary.volume_total or tot_id) > 0
        else None
    )
    merged = _merge_pending_run(
        rows_db,
        pending_status=pending_extraction_status,
        pending_volume=int(summary.volume_total or 0),
        pending_data_mean=pending_mean,
        pending_fallback_ratio=pend_fb,
        last_n=last_n,
    )
    base = get_site_baseline(conn, int(site["id"]))
    bavg = float(base["avg_healthy_volume_total"]) if base and base.get("avg_healthy_volume_total") else None
    snap = evaluate_site_health_from_series(merged, site_name=site.get("name") or "", baseline_avg_volume=bavg)
    summary.site_trend_pattern = snap.trend_pattern
    summary.site_success_rate_last10 = round(snap.success_rate * 100.0, 2)
    summary.site_consecutive_errors = snap.consecutive_errors
    summary.site_active_alerts = [a["code"] for a in snap.active_alerts]


def build_sites_atencao_section(
    conn: sqlite3.Connection,
    site_ids: list[int],
    *,
    last_n: int = DEFAULT_LAST_N,
    site_names: dict[int, str] | None = None,
) -> dict[str, Any]:
    """
    Sites com padrões preocupantes ou alertas ativos para o relatório da rodada.
    Independente dos erros apenas desta rodada.
    """
    degrading: list[dict[str, Any]] = []
    broken: list[dict[str, Any]] = []
    flapping: list[dict[str, Any]] = []
    with_alerts: list[dict[str, Any]] = []

    for sid in site_ids:
        hint = (site_names or {}).get(sid)
        h = get_site_health_history(conn, sid, last_n, site_name_hint=hint)
        entry = {
            "site_id": h["site_id"],
            "site_name": h["site_name"] or f"site_id={sid}",
            "trend_pattern": h["trend_pattern"],
            "success_rate": h["success_rate"],
            "volume_trend": h["volume_trend"],
            "active_alerts": [a["code"] for a in h["active_alerts"]],
            "active_alerts_detail": h["active_alerts"],
            "consecutive_errors": h["consecutive_errors"],
            "execution_count": h["execution_count_in_window"],
        }
        pat = h["trend_pattern"]
        if pat == PATTERN_DEGRADING:
            degrading.append(entry)
        if pat == PATTERN_CONSISTENTLY_BROKEN:
            broken.append(entry)
        if pat == PATTERN_FLAPPING:
            flapping.append(entry)
        if h["active_alerts"]:
            with_alerts.append(entry)

    return {
        "degrading": sorted(degrading, key=lambda x: x["site_name"]),
        "consistently_broken": sorted(broken, key=lambda x: x["site_name"]),
        "flapping": sorted(flapping, key=lambda x: x["site_name"]),
        "with_alerts": sorted(with_alerts, key=lambda x: x["site_name"]),
    }


def persist_health_alerts_for_round(
    conn: sqlite3.Connection,
    sites_atencao: dict[str, Any],
    *,
    round_label: str,
) -> int:
    """Grava eventos de alerta para consulta posterior. Retorna quantidade inserida."""
    now = datetime.now().isoformat()
    n = 0
    seen: set[tuple[int, str]] = set()
    for bucket in ("with_alerts", "degrading", "consistently_broken", "flapping"):
        for entry in sites_atencao.get(bucket) or []:
            sid = int(entry["site_id"])
            sname = entry.get("site_name") or ""
            for al in entry.get("active_alerts_detail") or []:
                code = al.get("code") or ""
                key = (sid, code)
                if not code or key in seen:
                    continue
                seen.add(key)
                conn.execute(
                    """
                    INSERT INTO site_health_alert_events (
                        site_id, site_name, round_label, alert_code, details_json, created_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        sid,
                        sname,
                        round_label,
                        code,
                        json.dumps({**al, "bucket": bucket}, ensure_ascii=False),
                        now,
                    ),
                )
                n += 1
    conn.commit()
    return n
