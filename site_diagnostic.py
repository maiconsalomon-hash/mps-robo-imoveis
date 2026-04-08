"""
Status e resumo estruturado por site (MINI-ETAPA 1B + 2A) e agregado da rodada (1C + 2B).
Classificação por site; ``removals_safe`` (com ``compute_sync_removals_safe``) é regra
operacional para remoções no SQLite: comparação snapshot run anterior vs. atual em
``apply_site_removals_with_guard`` (tabela ``site_listing_snapshots``).
MINI-ETAPA 2B: ``build_round_aggregate`` inclui ``global_sync_safe`` / ``reason``; o bloqueio do RPC
global de remoções no Supabase quando a rodada é RISKY é aplicado em ``scraper.sync_supabase``.
MINI-ETAPA 3A: campos ``baseline_*`` em ``SiteRunSummary`` são preenchidos em ``scraper._persist_site_log``
via ``site_baseline``.
MINI-ETAPA 3B: ``resolve_final_extraction_status`` usa baseline quando disponível; anomalias em ``SiteRunSummary``.
MINI-ETAPA 4A: campos ``next_page_*`` / ``pagination_*`` no summary; guard em ``pagination_guard`` + ``scraper``.
MINI-ETAPA 4B: ``pagination_cutoff_*`` / ``typical_page_volume``; lógica em ``pagination_cutoff``.
MINI-ETAPA 5A: ``identity_stats`` no resumo por site (fontes da chave estável).
MINI-ETAPA 5B: ``total_identity_*``, ``identity_low_ratio``, ``identity_quality_summary`` e alertas.
MINI-ETAPA 6A: ``total_data_*``, ``data_low_ratio``, ``data_quality_summary`` e alertas de campos.
MINI-ETAPA 6B: ``site_trend_pattern``, taxas históricas na janela recente e ``site_active_alerts``.
MINI-ETAPA 7B: campos ``retry_*`` no resumo serializado em ``log_execucoes.summary_json``.
MINI-ETAPA 8A: contadores ``sync_*`` preenchidos no agregado da rodada / ``round_results`` após o sync.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from datetime import datetime
from typing import Any

import requests


class SiteExtractionStatus(str, Enum):
    OK = "OK"
    SUSPEITO_ZERO_RESULTADOS = "SUSPEITO_ZERO_RESULTADOS"
    SUSPEITO_VOLUME_BAIXO = "SUSPEITO_VOLUME_BAIXO"
    SUSPEITO_QUEDA_ABRUPTA = "SUSPEITO_QUEDA_ABRUPTA"
    ERRO_PAGINACAO = "ERRO_PAGINACAO"
    ERRO_REQUISICAO = "ERRO_REQUISICAO"
    ERRO_RENDERIZACAO = "ERRO_RENDERIZACAO"
    ERRO_EXTRACAO = "ERRO_EXTRACAO"
    ERRO_LISTAGEM_INVALIDA = "ERRO_LISTAGEM_INVALIDA"


VOLUME_BAIXO_MAX = 3  # heurística sem baseline histórico; ajustável depois


def format_exception(exc: BaseException) -> tuple[str, str]:
    """Tipo real da exceção + mensagem preservada."""
    name = type(exc).__qualname__
    msg = str(exc).strip() or repr(exc)
    return name, msg


def is_request_layer_error(exc: BaseException) -> bool:
    """Timeout, HTTP, conexão, DNS típico via requests/OS."""
    if isinstance(exc, requests.exceptions.RequestException):
        return True
    if isinstance(exc, OSError):
        m = str(exc).lower()
        if any(x in m for x in ("getaddrinfo", "name or service not known", "gaierror", "errno 11001")):
            return True
    return False


def is_render_layer_error(exc: BaseException) -> bool:
    """Erros típicos de browser / Playwright."""
    name = type(exc).__qualname__
    mod = type(exc).__module__ or ""
    if "playwright" in mod.lower():
        return True
    if name in ("TimeoutError",):
        return True
    if isinstance(exc, OSError) and not is_request_layer_error(exc):
        return False
    return False


def html_sugere_listagem(html: str) -> bool:
    """Sinais de HTML de listagem (cards / imóveis), sem baseline de tamanho do site."""
    if not html or len(html) < 1500:
        return False
    low = html.lower()
    if any(p in low for p in ("nenhum imóvel encontrado", "nenhum imovel encontrado", "sem resultados")):
        return False
    hints = (
        "imovel" in low,
        "imóvel" in html.lower(),
        "property-card" in low,
        "listing" in low and "imovel" in low,
        low.count("r$") >= 2,
        bool(re.search(r"\d+\s*(?:quarto|dorm|m²|m2)", low)),
    )
    return sum(hints) >= 2


def legacy_log_status(extraction: SiteExtractionStatus) -> str:
    """Valor da coluna ``status`` em ``log_execucoes`` (compatível com relatórios)."""
    if extraction == SiteExtractionStatus.OK:
        return "ok"
    if extraction.value.startswith("SUSPEITO_"):
        return "suspeito"
    return "erro"


def compute_sync_removals_safe(extraction: SiteExtractionStatus, volume_total: int) -> tuple[bool, bool]:
    """
    ``sync_safe`` — informativo (sync global ainda não é bloqueado por isso).
    ``removals_safe`` — se False, o robô não deve executar remoções por site nesta rodada
    (status ERRO_*, SUSPEITO_*, ou OK sem volume coletado).
    """
    if extraction.value.startswith("ERRO_"):
        return False, False
    if extraction != SiteExtractionStatus.OK:
        return False, False
    return True, volume_total > 0


@dataclass
class SiteRunSummary:
    site: str
    site_id: int
    started_at: str
    finished_at: str = ""
    duration_seconds: float = 0.0
    pages_attempted: int = 0
    pages_succeeded: int = 0
    extraction_status: str = SiteExtractionStatus.OK.value
    error_type: str | None = None
    error_message: str | None = None
    volume_total: int = 0
    new_count: int = 0
    updated_count: int = 0
    removed_count: int = 0
    listing_url: str = ""
    next_page_failures: int = 0
    # MINI-ETAPA 4A — guard de paginação
    next_page_attempts: int = 0
    next_page_successes: int = 0
    next_page_failure_reason: str = ""
    pagination_loop_detected: bool = False
    pagination_stopped_reason: str = ""
    redirect_loop_detected: bool = False
    # MINI-ETAPA 4B — corte inteligente de paginação
    pagination_cutoff_triggered: bool = False
    pagination_cutoff_reason: str = ""
    pages_with_no_new_items: int = 0
    pages_low_yield_count: int = 0
    typical_page_volume: float = 0.0
    last_page_new_items: int = 0
    last_page_omitted_items: int = 0
    sync_safe: bool = True
    removals_safe: bool = True
    removals_blocked: bool = False
    removals_blocked_reason: str = ""
    removed_count_attempted: int = 0
    removed_count_applied: int = 0
    # MINI-ETAPA 3A — baseline histórico (preenchido antes de gravar summary_json)
    baseline_available: bool = False
    baseline_healthy_runs_count: int = 0
    baseline_last_healthy_volume: int | None = None
    baseline_avg_volume: float | None = None
    baseline_median_volume: float | None = None
    baseline_expected_min: int | None = None
    baseline_expected_max: int | None = None
    current_vs_baseline_ratio: float | None = None
    # MINI-ETAPA 3B — anomalias / origem da decisão de status
    anomaly_detected: bool = False
    anomaly_type: str | None = None
    anomaly_details: dict[str, Any] = field(default_factory=dict)
    status_decision_source: str = "heuristic"
    warnings: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    # MINI-ETAPA 5A — agregado por fonte de identidade (external_id, canonical_url, …)
    identity_stats: dict[str, int] = field(default_factory=dict)
    # MINI-ETAPA 5B — qualidade e auditoria da identidade por site
    total_external_id: int = 0
    total_canonical_url: int = 0
    total_stable_fingerprint: int = 0
    total_legacy_fallback: int = 0
    total_identity_high: int = 0
    total_identity_medium: int = 0
    total_identity_low: int = 0
    identity_low_ratio: float = 0.0
    identity_quality_summary: str = ""
    identity_warning_detected: bool = False
    identity_warning_reason: str = ""
    # MINI-ETAPA 6A — qualidade dos campos extraídos
    total_data_high: int = 0
    total_data_medium: int = 0
    total_data_low: int = 0
    data_low_ratio: float = 0.0
    data_quality_summary: str = ""
    data_warning_detected: bool = False
    data_warning_reason: str = ""
    data_missing_price_count: int = 0
    data_missing_location_count: int = 0
    data_quality_score_sum: float = 0.0
    # MINI-ETAPA 6B — tendência / alertas históricos (preenchido antes de summary_json)
    site_trend_pattern: str = ""
    site_success_rate_last10: float | None = None  # 0–100 %, janela configurável no módulo de histórico
    site_consecutive_errors: int = 0
    site_active_alerts: list[str] = field(default_factory=list)
    # MINI-ETAPA 7B — retry na mesma rodada (segunda execução do site)
    retry_attempted: bool = False
    retry_result: str = ""  # OK / ERRO_* / SUSPEITO_* / «não_tentado» na passagem principal
    retry_reason: str = ""
    original_status: str = ""
    # MINI-ETAPA 8A — filtro Supabase (relatório da rodada; log por site pode ficar 0 até pós-sync)
    sync_sent_count: int = 0
    sync_filtered_count: int = 0
    sync_filter_reasons: dict[str, int] = field(default_factory=dict)
    # Família de plataforma (ex.: API Tecimob / Gerenciar Imóveis CF + front Next.js)
    platform_family_detected: str = ""
    family_specific_extractor_used: bool = False
    family_card_count: int = 0
    family_detail_links_count: int = 0
    family_pagination_pattern_detected: str = ""
    family_listing_validation_reason: str = ""
    # API Gerenciar / Tecimob (campos explícitos para operação e painéis)
    gerenciar_cf_api_probe_used: bool = False
    gerenciar_cf_api_probe_status: str = ""
    gerenciar_cf_api_host_used: str = ""
    gerenciar_cf_api_pages_fetched: int = 0
    gerenciar_cf_api_total_items: int = 0
    gerenciar_cf_api_stop_reason: str = ""

    def seal(self, started_monotonic: float | None = None) -> None:
        self.finished_at = datetime.now().isoformat()
        if started_monotonic is not None:
            self.duration_seconds = round(time.monotonic() - started_monotonic, 3)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


def resolve_final_extraction_status(
    *,
    had_request_error: bool,
    had_render_error: bool,
    pagination_error: bool,
    queda_abrupta: bool,
    page1_bruto_zero: bool,
    page1_dedupe_zero_but_bruto_positive: bool,
    listagem_invalida: bool,
    volume_total: int,
    baseline_available: bool = False,
    baseline_expected_min: int | None = None,
    baseline_healthy_runs_count: int = 0,
) -> tuple[SiteExtractionStatus, str]:
    """
    Ordem de gravidade coerente com as regras da mini-etapa 1B + 3B.

    Retorna ``(status, status_decision_source)`` onde a fonte é ``\"baseline\"`` ou
    ``\"heuristic\"`` para o ramo de volume/OK (erros e suspeitos fixos usam ``\"heuristic\"``).
    """
    _h = "heuristic"

    if had_request_error:
        return SiteExtractionStatus.ERRO_REQUISICAO, _h
    if had_render_error:
        return SiteExtractionStatus.ERRO_RENDERIZACAO, _h
    if pagination_error:
        return SiteExtractionStatus.ERRO_PAGINACAO, _h
    if page1_dedupe_zero_but_bruto_positive:
        return SiteExtractionStatus.ERRO_EXTRACAO, _h
    if listagem_invalida:
        return SiteExtractionStatus.ERRO_LISTAGEM_INVALIDA, _h
    if volume_total == 0 and page1_bruto_zero:
        return SiteExtractionStatus.SUSPEITO_ZERO_RESULTADOS, _h
    if queda_abrupta:
        return SiteExtractionStatus.SUSPEITO_QUEDA_ABRUPTA, _h

    # MINI-ETAPA 3B — volume baixo: baseline tem prioridade sobre faixa fixa 1–3
    if (
        baseline_available
        and baseline_expected_min is not None
        and baseline_healthy_runs_count > 0
        and volume_total > 0
        and volume_total < baseline_expected_min
    ):
        return SiteExtractionStatus.SUSPEITO_VOLUME_BAIXO, "baseline"
    if not baseline_available and 0 < volume_total <= VOLUME_BAIXO_MAX:
        return SiteExtractionStatus.SUSPEITO_VOLUME_BAIXO, _h

    return SiteExtractionStatus.OK, _h


# ── Relatório agregado da rodada (MINI-ETAPA 1C) ───────────────────────────────


class RoundHealth(str, Enum):
    """Classificação heurística da rodada (MINI-ETAPA 2B: RISKY desativa sync destrutivo global)."""
    SAFE = "SAFE"
    PARTIAL = "PARTIAL"
    RISKY = "RISKY"


def classify_round_health(total_sites: int, total_erros: int, total_suspeitos: int) -> RoundHealth:
    """
    Heurística inicial:
    - SAFE — todos os sites OK (sem ERRO_* nem SUSPEITO_*).
    - RISKY — muitos ERRO_*, ou combinação ERRO+SUSPEITO alta, ou rodadas pequenas “todas ruins”.
    - PARTIAL — existe suspeita ou erro pontual, mas a rodada segue utilizável.
    """
    if total_sites <= 0:
        return RoundHealth.SAFE
    if total_erros == 0 and total_suspeitos == 0:
        return RoundHealth.SAFE

    err_r = total_erros / total_sites
    sus_r = total_suspeitos / total_sites
    combined_r = (total_erros + total_suspeitos) / total_sites

    if err_r >= 0.15 or total_erros >= 10:
        return RoundHealth.RISKY
    if combined_r >= 0.40:
        return RoundHealth.RISKY
    if total_sites <= 5 and total_erros >= 2:
        return RoundHealth.RISKY
    if total_sites <= 3 and (total_erros + total_suspeitos) >= total_sites:
        return RoundHealth.RISKY
    if sus_r >= 0.35 and total_erros > 0:
        return RoundHealth.RISKY

    return RoundHealth.PARTIAL


def build_round_observations(agg: dict[str, Any]) -> list[str]:
    """Observações curtas para o rodapé do relatório final."""
    obs: list[str] = []
    n = agg["total_sites"]
    te = agg["total_erros"]
    ts = agg["total_suspeitos"]
    if te:
        obs.append(
            f"{te} site(s) com ERRO_* — rede, renderização, paginação ou extração; checar logs e summary_json."
        )
    if ts:
        obs.append(
            f"{ts} site(s) com SUSPEITO_* — possível listagem vazia, volume atípico ou queda na paginação."
        )
    health = agg.get("round_health", "PARTIAL")
    if health == RoundHealth.RISKY.value:
        obs.append(
            "Classificação RISKY: taxa relevante de falhas/suspeitas; interpretar totais e sync com cautela."
        )
        obs.append(
            "Governança 2B: rodada marcada como unsafe — não executar RPC global de remoções no Supabase após os upserts."
        )
    elif health == RoundHealth.PARTIAL.value and (te or ts):
        obs.append(
            "Classificação PARTIAL: rodada utilizável, mas há sites que precisam de atenção (listas abaixo)."
        )
    ss = agg["total_sites_sync_safe"]
    sr = agg["total_sites_removals_safe"]
    if n >= 8 and ss < max(1, int(n * 0.85)):
        obs.append(f"Só {ss}/{n} sites com sync_safe informativo — parte da coleta não atende critério estrito de OK.")
    if n >= 8 and sr < max(1, int(n * 0.85)):
        obs.append(
            f"Só {sr}/{n} sites com removals_safe — nestes, remoções por site foram bloqueadas ou não aplicadas por política de segurança."
        )
    br = int(agg.get("total_removals_blocked_safety_count") or 0)
    nb = int(agg.get("total_sites_removals_blocked") or 0)
    if nb and br:
        obs.append(
            f"{nb} site(s) com remoções bloqueadas por segurança ({br} imóvel(is) teriam sido marcados inativos se a política não existisse)."
        )
    elif nb:
        obs.append(
            f"{nb} site(s) com remoções bloqueadas por segurança (sem pendências de inativação nesses sites nesta rodada)."
        )
    return obs


def build_round_aggregate(site_results: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Consolida resultados por site (após scrape_site).
    Cada item esperado: site_name, site_id (opcional), extraction_status, sync_safe, removals_safe,
    volume_total, removals_blocked, removed_count_attempted, removed_count_applied (opcionais).
    """
    n = len(site_results)
    sites_suspeitos: list[dict[str, Any]] = []
    sites_erro: list[dict[str, Any]] = []
    n_ok = n_sus = n_err = 0
    total_sync_safe = 0
    total_removals_safe = 0
    total_sites_removals_blocked = 0
    total_sites_removals_permitted = 0
    total_removals_blocked_safety_count = 0
    sites_removals_blocked: list[dict[str, Any]] = []

    for r in site_results:
        name = r.get("site_name") or r.get("site") or "?"
        st = (r.get("extraction_status") or SiteExtractionStatus.OK.value).strip()
        vol = int(r.get("volume_total") or 0)
        sync_ok = bool(r.get("sync_safe"))
        rem_ok = bool(r.get("removals_safe"))
        if sync_ok:
            total_sync_safe += 1
        if rem_ok:
            total_removals_safe += 1

        rem_blocked = bool(r.get("removals_blocked"))
        att = int(r.get("removed_count_attempted") or 0)
        if rem_blocked:
            total_sites_removals_blocked += 1
            total_removals_blocked_safety_count += att
            sites_removals_blocked.append(
                {
                    "site": name,
                    "status": st,
                    "removals_blocked_reason": (r.get("removals_blocked_reason") or st) or "",
                    "removed_count_attempted": att,
                }
            )
        elif rem_ok:
            total_sites_removals_permitted += 1

        entry = {"site": name, "status": st, "volume_total": vol}
        if st.startswith("ERRO"):
            n_err += 1
            sites_erro.append(entry)
        elif st.startswith("SUSPEITO"):
            n_sus += 1
            sites_suspeitos.append(entry)
        else:
            n_ok += 1

    sites_erro.sort(key=lambda x: (x["status"], x["site"]))
    sites_suspeitos.sort(key=lambda x: (x["status"], x["site"]))

    health = classify_round_health(n, n_err, n_sus)
    rh_val = health.value
    agg: dict[str, Any] = {
        "total_sites": n,
        "total_ok": n_ok,
        "total_suspeitos": n_sus,
        "total_erros": n_err,
        "sites_suspeitos": sites_suspeitos,
        "sites_erro": sites_erro,
        "total_sites_sync_safe": total_sync_safe,
        "total_sites_removals_safe": total_removals_safe,
        "total_sites_removals_permitted": total_sites_removals_permitted,
        "total_sites_removals_blocked": total_sites_removals_blocked,
        "total_removals_blocked_safety_count": total_removals_blocked_safety_count,
        "sites_removals_blocked": sorted(
            sites_removals_blocked, key=lambda x: (x.get("removals_blocked_reason") or "", x["site"])
        ),
        "round_health": rh_val,
        # MINI-ETAPA 2B — governança global (sync_mode / sync_decision preenchidos após sync no main)
        "global_sync_safe": rh_val != RoundHealth.RISKY.value,
        "reason": rh_val,
        "round_unsafe": rh_val == RoundHealth.RISKY.value,
        "sync_mode": None,
        "sync_decision": None,
    }
    agg["observations"] = build_round_observations(agg)
    return agg
