"""
Robô Agregador de Imóveis - Jaraguá do Sul / SC
================================================
Varre 53 imobiliárias locais, extrai imóveis, normaliza com IA e
salva em SQLite local. Roda manualmente ou agendado (cron / Task Scheduler).

Uso:
    python scraper.py              # roda tudo
    python scraper.py --site 1     # roda só o site de id 1
    python scraper.py --report     # mostra resumo do banco sem scraping
    python scraper.py --export     # exporta CSV com todos os imóveis
    python scraper.py --profiles   # mapa host → extrator (JSON + SQLite + embutido)
    python scraper.py --scheduler  # loop agendado (veja SCHEDULE_* no .env)
    python scraper.py --scheduler-status

Credenciais: copie .env.example para .env (veja README). Variáveis sensíveis em config.py / .env.

Perfis de extração: ``site_profiles.json`` + tabela ``site_scrape_profiles`` no SQLite
(veja refresh_site_profiles). Novos sites no mesmo “tema” recebem a chave do extrator sem
alterar código, desde que exista em EXTRACTOR_REGISTRY — ex.: ``imonov_webflow`` (Morada /
Webflow), ``apreme`` (Apre.me / links ``/12345`` ou ``/imovel/…``). Itaivan usa ramo próprio
(``extract_imoveis_itaivan`` + Playwright), não o mapa por host.
"""

import sqlite3
import json
import time
import csv
import re
import sys
import argparse
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright as _sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from pagination_cutoff import (  # noqa: E402
    PaginationRunningStats,
    compute_typical_page_volume,
    cutoff_log_message,
    register_full_page_volume,
    should_stop_pagination,
)
from pagination_guard import (  # noqa: E402
    CODE_CROSS_DOMAIN,
    CODE_EMPTY,
    CODE_REPEATED,
    CODE_SAME_PAGE,
    normalize_pagination_url,
    validate_next_page_url,
)
from config import (  # noqa: E402 — .env antes do restante da execução
    ANTHROPIC_API_KEY,
    RETRY_DELAY_SECONDS,
    RETRY_ENABLED,
    RETRY_ERRORS_ELIGIBLE,
    RETRY_MAX_SITES,
    SUPABASE_BATCH_SIZE,
    SUPABASE_CONFIG_INCOMPLETE,
    SUPABASE_KEY,
    SUPABASE_SYNC_ENABLED,
    SUPABASE_TABLE,
    SUPABASE_URL,
    SYNC_BLOCK_LEGACY_IDENTITY,
    SYNC_FILTER_ENABLED,
    SYNC_FULL_RESYNC_INTERVAL_HOURS,
    SYNC_INCREMENTAL_ENABLED,
    SYNC_INCREMENTAL_FULL_THRESHOLD_PCT,
    SYNC_MIN_DATA_QUALITY_SCORE,
)
from round_lock import try_acquire_round_lock  # noqa: E402 — MINI-ETAPA 7A

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

if SUPABASE_CONFIG_INCOMPLETE:
    log.warning(
        "Configuração Supabase incompleta: defina SUPABASE_URL e SUPABASE_KEY (ou "
        "SUPABASE_SERVICE_ROLE_KEY) no arquivo .env ou no ambiente. O sync com Supabase "
        "permanece desativado até os dois estarem definidos."
    )

from site_baseline import (  # noqa: E402
    enrich_summary_with_baseline,
    get_site_baseline,
    update_site_baseline,
)
from field_quality import (  # noqa: E402 — MINI-ETAPA 6A
    DATA_LOW_RATIO_WARN as DATA_Q_LOW_RATIO_WARN,
    DATA_MEAN_SCORE_WARN,
    DATA_MISSING_LOCATION_RATIO_WARN,
    DATA_MISSING_PRICE_RATIO_WARN,
    data_quality_issues_json,
    evaluate_field_quality,
)
from property_identity import (  # noqa: E402 — MINI-ETAPA 5A / 5B
    IDENTITY_LEGACY_RATIO_WARN,
    IDENTITY_LOW_RATIO_WARN,
    IDENTITY_STRONG_SOURCE_MIN_RATIO,
    apply_property_identity,
    canonical_property_url,
    legacy_content_hash,
    stable_hash_for_record,
)
from site_diagnostic import (  # noqa: E402
    SiteExtractionStatus,
    SiteRunSummary,
    VOLUME_BAIXO_MAX,
    build_round_aggregate,
    compute_sync_removals_safe,
    format_exception,
    html_sugere_listagem,
    is_render_layer_error,
    is_request_layer_error,
    legacy_log_status,
    resolve_final_extraction_status,
)
from site_health_history import (  # noqa: E402 — MINI-ETAPA 6B
    build_sites_atencao_section,
    enrich_summary_with_site_health,
    migrate_site_health_alert_events,
    persist_health_alerts_for_round,
)
from site_retry import pick_retry_candidates  # noqa: E402 — MINI-ETAPA 7B
from sync_quality_filter import (  # noqa: E402 — MINI-ETAPA 8A
    classify_sync_filter_row,
    empty_reason_counts,
)

# ── Configurações ──────────────────────────────────────────────────────────────
DB_FILE = "imoveis.db"
ROUND_LOCK_FILE = Path(__file__).resolve().parent / "scrape_round.lock"
# Perfis de extração: host → chave de extrator (JSON + SQLite + embutido — ver refresh_site_profiles)
SITE_PROFILES_JSON = Path(__file__).resolve().parent / "site_profiles.json"
REQUEST_TIMEOUT = 20          # segundos por requisição
DELAY_BETWEEN_SITES = 2       # segundos entre sites (respeita os servidores)
MAX_PAGES_PER_SITE = 50       # máximo de páginas por imobiliária (cobre até ~1000 imóveis)
ITAIVAN_MAX_PAGES = 80        # Itaivan (Playwright): listagem filtrada pode ter 50+ páginas (ex.: 58)
# Fim de listagem: após páginas “cheias”, muitos sites (ex.: Morada Brasil) ainda respondem
# /pag/N+1 com poucos cards (destaques/sidebar), sem a grade real — corta paginação em falso.
# (Regras numéricas espelhadas em ``pagination_cutoff`` — manter alinhado.)
PAGINATION_FULL_PAGE_MIN = 18   # tamanho típico “cheio” (Morada vira ~21 após dedup URL/hash)
PAGINATION_TAIL_RATIO = 0.34    # legado / doc; cauda aplicada via pagination_cutoff.TAIL_RATIO
PAGINATION_TAIL_MAX_ITEMS = 18  # legado / doc; cauda aplicada via pagination_cutoff.TAIL_MAX_ITEMS
# ANTHROPIC_API_KEY, Supabase: ver config.py e .env (.env.example)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Lista de imobiliárias ──────────────────────────────────────────────────────
SITES = [
    {"id": 1,  "name": "mouraimoveis.com.br",          "url": "https://mouraimoveis.com.br/comprar/imoveis?sort=-created_at%2Cid&offset=1&limit=21&typeArea=total_area&floorComparision=equals"},
    {"id": 2,  "name": "imobiliariahabitat.com.br",     "url": "https://www.imobiliariahabitat.com.br/imoveis?pretensao=comprar&bairro="},
    {"id": 3,  "name": "haus.imb.br",                  "url": "https://haus.imb.br/imoveis/venda"},
    {"id": 4,  "name": "imobimobiliariasc.com.br",      "url": "https://imobimobiliariasc.com.br/imoveis/"},
    {"id": 5,  "name": "sollusimobiliaria.com.br",      "url": "https://www.sollusimobiliaria.com.br/imoveis/a-venda"},
    {"id": 6,  "name": "mahnkeimoveis.com.br",          "url": "https://www.mahnkeimoveis.com.br/imoveis/venda/-/-/-/-"},
    {"id": 7,  "name": "d2imoveis.com",                 "url": "https://d2imoveis.com/imoveis/venda"},
    {"id": 8,  "name": "atlantaimoveis.com",            "url": "https://atlantaimoveis.com/imoveis/a-venda"},
    {"id": 9,  "name": "dualimobiliaria.com.br",        "url": "https://dualimobiliaria.com.br/buscar?purpose=&city=&cod="},
    {"id": 10, "name": "interimob.com.br",              "url": "https://interimob.com.br/imovel/venda"},
    {"id": 11, "name": "poffoimoveis.com.br",           "url": "https://poffoimoveis.com.br/imoveis/"},
    {"id": 12, "name": "vivendaimoveis.com",            "url": "https://vivendaimoveis.com/busca"},
    {"id": 13, "name": "chaleimobiliaria.com.br",       "url": "https://chaleimobiliaria.com.br/busca"},
    {"id": 14, "name": "m2jaragua.com.br",              "url": "https://m2jaragua.com.br/imoveis?pretensao=comprar"},
    {"id": 15, "name": "donnajaragua.com.br",           "url": "https://donnajaragua.com.br/imoveis?pretensao=comprar&pagina=1"},
    {"id": 16, "name": "seculus.net",                   "url": "https://seculus.net/imoveis/venda"},
    {"id": 17, "name": "imobiliariaurbana.com.br",      "url": "https://imobiliariaurbana.com.br/buscar?tipo_negocio=Venda&cidade=&codigo=&minprice=&maxprice=&dormitorios=&vagas=&search="},
    {"id": 18, "name": "macroimoveis.com",              "url": "https://macroimoveis.com/imoveis/"},
    {"id": 19, "name": "imobiliariapradi.com.br",       "url": "https://www.imobiliariapradi.com.br/imoveis/a-venda"},
    {"id": 20, "name": "spacoimoveis.net",              "url": "https://spacoimoveis.net/imoveis/"},
    {"id": 21, "name": "poderimoveis.com",              "url": "https://www.poderimoveis.com/imoveis/venda"},
    {"id": 22, "name": "imobiliariaachave.com.br",      "url": "https://imobiliariaachave.com.br/a_vendas.php?estadodiv=close"},
    {"id": 23, "name": "lotusimoveissc.com.br",         "url": "https://lotusimoveissc.com.br/comprar-alugar/imoveis?sort=-created_at%2Cid&offset=1&limit=21&typeArea=total_area&floorComparision=equals"},
    {"id": 24, "name": "imoveiscidade.com.br",          "url": "https://imoveiscidade.com.br/busca?finalidade=Venda"},
    {"id": 25, "name": "imobiliariabarrasul.com",       "url": "https://www.imobiliariabarrasul.com/venda/imoveis/todas-as-cidades/todos-os-bairros/0-quartos/0-suite-ou-mais/0-vaga/0-banheiro-ou-mais/todos-os-condominios?valorminimo=0&valormaximo=0"},
    {"id": 26, "name": "dalcasta.com.br",               "url": "https://www.dalcasta.com.br/imoveis/a-venda/todos/"},
    {"id": 27, "name": "splendoreimoveis.com",          "url": "https://www.splendoreimoveis.com/imoveis?pretensao=comprar"},
    {"id": 28, "name": "imobiliariajaragua.com.br",     "url": "https://www.imobiliariajaragua.com.br/venda/?&pagina=1"},
    {"id": 29, "name": "luciannerodrigues.com.br",      "url": "https://luciannerodrigues.com.br/imoveis/a-venda"},
    {"id": 30, "name": "eccorretoresdeimoveis.com.br",  "url": "https://www.eccorretoresdeimoveis.com.br/imovel/venda"},
    {"id": 31, "name": "josititzcorretora.com.br",      "url": "https://www.josititzcorretora.com.br/imoveis/?disponibilidade=a-venda&categoria=&cidade=&bairro=&codigo="},
    {"id": 32, "name": "michaelsalomon.com.br",         "url": "https://michaelsalomon.com.br/comprar/imoveis?sort=-created_at%2Cid&offset=1&limit=21&typeArea=total_area&floorComparision=equals"},
    {"id": 33, "name": "imobiliariabeta.com.br",        "url": "https://www.imobiliariabeta.com.br/imoveis-para-venda.php?cidade=&tipo%5B%5D=&qtd_quartos%5B%5D=&codigo_imovel="},
    {"id": 34, "name": "itaivan.com",                   "url": "https://www.itaivan.com/venda/imovel/jaragua-do-sul-e-regiao/todos-os-bairros/todos-os-condominios/todas-as-opcoes/"},
    {"id": 35, "name": "piermann.com.br",               "url": "https://piermann.com.br/imoveis/venda"},
    {"id": 36, "name": "moradabrasil.com",              "url": "https://www.moradabrasil.com/filtro/venda/todos/todas----todos/todos/todos----todos/todos----todos/0-10000000/todos/1"},
    {"id": 37, "name": "megaempreendimentos.com",       "url": "https://megaempreendimentos.com/imoveis/"},
    {"id": 38, "name": "deocarimoveis.com.br",          "url": "https://deocarimoveis.com.br/comprar/"},
    {"id": 39, "name": "rzimobi.com.br",                "url": "https://www.rzimobi.com.br/imoveis/filtragem/?tipo="},
    {"id": 40, "name": "paulastringari.com",            "url": "https://www.paulastringari.com/imoveis/filtragem/?tipo=&busca="},
    {"id": 41, "name": "divinacasaimobiliaria.com.br",  "url": "https://www.divinacasaimobiliaria.com.br/imoveis/a-venda"},
    {"id": 42, "name": "mavicimoveis.com.br",           "url": "https://mavicimoveis.com.br/imoveis/venda"},
    {"id": 43, "name": "suzanaimoveis.com.br",          "url": "https://www.suzanaimoveis.com.br/index.php?pagina=busca&busca=all&negociacao=Venda"},
    {"id": 44, "name": "itatimoveis.com.br",            "url": "https://www.itatimoveis.com.br/venda/imoveis/todas-as-cidades/todos-os-bairros/0-quartos/0-suite-ou-mais/0-vaga/0-banheiro-ou-mais/todos-os-condominios?valorminimo=0&valormaximo=0"},
    {"id": 45, "name": "yatil.com.br",                  "url": "https://yatil.com.br/comprar/imoveis?sort=is_price_shown%2Ccalculated_price%2Cid&offset=1&limit=21&typeArea=total_area&floorComparision=equals"},
    {"id": 46, "name": "brisaimoveis.com",              "url": "https://brisaimoveis.com/imoveis/venda"},
    {"id": 47, "name": "engetecimoveis.com.br",         "url": "https://www.engetecimoveis.com.br/imoveis-venda"},
    {"id": 48, "name": "sartorimobiliaria.com.br",      "url": "https://sartorimobiliaria.com.br/imoveis/"},
    {"id": 49, "name": "adrivanimoveis.com",            "url": "https://adrivanimoveis.com/comprar/imoveis?sort=-created_at%2Cid&offset=1&limit=21&typeArea=total_area&floorComparision=equals"},
    {"id": 50, "name": "mg-imoveis.com",                "url": "https://mg-imoveis.com/imoveis/venda"},
    {"id": 51, "name": "schroederimoveis.com.br",       "url": "https://www.schroederimoveis.com.br/imovel/venda"},
    {"id": 52, "name": "girolla.com.br",                "url": "https://girolla.com.br/busca?finalidade=Venda"},
    {"id": 53, "name": "imoveisplaneta.com.br",         "url": "https://www.imoveisplaneta.com.br/imoveis?pretensao=comprar&pagina=1"},
]


# ══════════════════════════════════════════════════════════════════════════════
# BANCO DE DADOS
# ══════════════════════════════════════════════════════════════════════════════

def init_db(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS imoveis (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        hash          TEXT UNIQUE,          -- chave estável do imóvel (MINI-ETAPA 5A; ver property_identity)
        site_id       INTEGER,
        site_name     TEXT,
        titulo        TEXT,
        tipo          TEXT,                 -- apartamento, casa, terreno...
        finalidade    TEXT DEFAULT 'venda',
        preco         REAL,
        preco_texto   TEXT,
        area_m2       REAL,
        quartos       INTEGER,
        banheiros     INTEGER,
        vagas         INTEGER,
        bairro        TEXT,
        cidade        TEXT,
        endereco      TEXT,
        descricao     TEXT,
        url_anuncio   TEXT,
        url_foto      TEXT,
        codigo        TEXT,
        identity_source         TEXT,
        legacy_hash             TEXT,
        canonical_url_anuncio   TEXT,
        identity_fallback       INTEGER DEFAULT 0,
        identity_quality          TEXT,
        identity_quality_reason   TEXT,
        data_quality_score        INTEGER,
        data_quality_level        TEXT,
        data_quality_issues       TEXT,
        ativo         INTEGER DEFAULT 1,    -- 1=ativo, 0=saiu do mercado
        primeira_vez  TEXT,                 -- data que apareceu pela 1ª vez
        ultima_vez    TEXT,                 -- data da última atualização
        raw_json      TEXT                  -- dados brutos extraídos
    );

    CREATE TABLE IF NOT EXISTS historico_precos (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        imovel_id   INTEGER,
        preco       REAL,
        preco_texto TEXT,
        data        TEXT,
        FOREIGN KEY (imovel_id) REFERENCES imoveis(id)
    );

    CREATE TABLE IF NOT EXISTS log_execucoes (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        data         TEXT,
        site_id      INTEGER,
        site_name    TEXT,
        status       TEXT,                  -- ok, erro, bloqueado
        total_encontrados INTEGER DEFAULT 0,
        novos         INTEGER DEFAULT 0,
        atualizados   INTEGER DEFAULT 0,
        removidos     INTEGER DEFAULT 0,
        erro_msg      TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_hash     ON imoveis(hash);
    CREATE INDEX IF NOT EXISTS idx_ativo    ON imoveis(ativo);
    CREATE INDEX IF NOT EXISTS idx_site     ON imoveis(site_id);
    CREATE INDEX IF NOT EXISTS idx_preco    ON imoveis(preco);
    CREATE INDEX IF NOT EXISTS idx_quartos  ON imoveis(quartos);

    CREATE TABLE IF NOT EXISTS site_scrape_profiles (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        host_substring  TEXT NOT NULL UNIQUE,
        extractor_key   TEXT NOT NULL,
        notes           TEXT,
        updated_at      TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_site_scrape_profiles_host ON site_scrape_profiles(host_substring);
    """)
    conn.commit()
    migrate_log_execucoes(conn)
    migrate_site_volume_baseline(conn)
    migrate_site_health_alert_events(conn)
    migrate_imoveis_identity(conn)
    migrate_imoveis_field_quality(conn)
    migrate_scheduled_runs(conn)
    migrate_scheduled_runs_retry_columns(conn)
    migrate_supabase_sync_state(conn)


def migrate_scheduled_runs(conn):
    """MINI-ETAPA 7A — histórico de rodadas disparadas pelo agendador."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS scheduled_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scheduled_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            round_health TEXT,
            sites_ok INTEGER,
            sites_suspeitos INTEGER,
            sites_erros INTEGER,
            triggered_by TEXT NOT NULL DEFAULT 'scheduled',
            notes TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_scheduled_runs_started
            ON scheduled_runs (started_at DESC);
        """
    )
    conn.commit()


def migrate_scheduled_runs_retry_columns(conn):
    """MINI-ETAPA 7B — contadores de retry na rodada agendada."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(scheduled_runs)")}
    if "retries_attempted" not in cols:
        conn.execute(
            "ALTER TABLE scheduled_runs ADD COLUMN retries_attempted INTEGER NOT NULL DEFAULT 0"
        )
    if "retries_succeeded" not in cols:
        conn.execute(
            "ALTER TABLE scheduled_runs ADD COLUMN retries_succeeded INTEGER NOT NULL DEFAULT 0"
        )
    conn.commit()


def migrate_supabase_sync_state(conn):
    """MINI-ETAPA 8B — checkpoint do último sync bem-sucedido com Supabase (uma linha)."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS supabase_sync_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_successful_sync_at TEXT,
            last_full_sync_at TEXT
        )
        """
    )
    conn.execute("INSERT OR IGNORE INTO supabase_sync_state (id) VALUES (1)")
    conn.commit()


def _read_supabase_sync_state(conn) -> tuple[str | None, str | None]:
    row = conn.execute(
        "SELECT last_successful_sync_at, last_full_sync_at FROM supabase_sync_state WHERE id = 1"
    ).fetchone()
    if not row:
        return None, None
    return (row[0] if row[0] else None), (row[1] if row[1] else None)


def _parse_iso_to_utc(s: str | None) -> datetime | None:
    if not s or not str(s).strip():
        return None
    raw = str(s).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _row_dirty_for_incremental(rowd: dict, last_sync_iso: str) -> bool:
    """True se o imóvel deve ser reenviado no modo incremental (conservador se timestamp inválido)."""
    pv, uv = rowd.get("primeira_vez"), rowd.get("ultima_vez")
    ps, us = (str(pv).strip() if pv else ""), (str(uv).strip() if uv else "")
    if not ps and not us:
        return True
    ativo = int(rowd.get("ativo") or 0)
    if ativo == 0:
        if not us:
            return True
        return us >= last_sync_iso
    if ps and ps >= last_sync_iso:
        return True
    if us and us >= last_sync_iso:
        return True
    return False


def _classify_sent_row_for_report(rowd: dict, prev_success_iso: str | None) -> str:
    """Retorna 'new' | 'updated' | 'removed' para contagem pós-filtro 8A."""
    ativo = int(rowd.get("ativo") or 0)
    if ativo == 0:
        return "removed"
    if not prev_success_iso:
        return "new"
    ps = str(rowd.get("primeira_vez") or "").strip()
    if ps and ps >= prev_success_iso:
        return "new"
    return "updated"


def _persist_supabase_sync_checkpoint(conn, *, now_iso: str, wrote_full_sync: bool) -> None:
    if wrote_full_sync:
        conn.execute(
            """
            UPDATE supabase_sync_state
            SET last_successful_sync_at = ?, last_full_sync_at = ?
            WHERE id = 1
            """,
            (now_iso, now_iso),
        )
    else:
        conn.execute(
            """
            UPDATE supabase_sync_state SET last_successful_sync_at = ? WHERE id = 1
            """,
            (now_iso,),
        )
    conn.commit()


def migrate_log_execucoes(conn):
    """Acrescenta colunas de diagnóstico em log_execucoes (idempotente)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(log_execucoes)")}
    if "extraction_status" not in cols:
        conn.execute("ALTER TABLE log_execucoes ADD COLUMN extraction_status TEXT")
    if "summary_json" not in cols:
        conn.execute("ALTER TABLE log_execucoes ADD COLUMN summary_json TEXT")
    conn.commit()


def migrate_site_volume_baseline(conn):
    """Tabela de baseline por site (MINI-ETAPA 3A); idempotente."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS site_volume_baseline (
            site_id                    INTEGER PRIMARY KEY,
            site_name                  TEXT NOT NULL,
            last_healthy_run_at        TEXT,
            last_healthy_volume_total  INTEGER,
            avg_healthy_volume_total   REAL,
            median_healthy_volume_total REAL,
            min_expected_volume        INTEGER,
            max_expected_volume        INTEGER,
            avg_pages_succeeded        REAL,
            healthy_runs_count         INTEGER NOT NULL DEFAULT 0,
            updated_at                 TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_log_exec_site_status
            ON log_execucoes (site_id, extraction_status);
        """
    )
    conn.commit()


def migrate_imoveis_identity(conn):
    """MINI-ETAPA 5A: colunas de identidade estável em ``imoveis`` (idempotente)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(imoveis)")}
    alters = [
        ("identity_source", "TEXT"),
        ("legacy_hash", "TEXT"),
        ("canonical_url_anuncio", "TEXT"),
        ("identity_fallback", "INTEGER DEFAULT 0"),
        ("identity_quality", "TEXT"),
        ("identity_quality_reason", "TEXT"),
    ]
    for name, decl in alters:
        if name not in cols:
            conn.execute(f"ALTER TABLE imoveis ADD COLUMN {name} {decl}")
    conn.commit()


def migrate_imoveis_field_quality(conn):
    """MINI-ETAPA 6A: colunas de qualidade de dados em ``imoveis`` (idempotente)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(imoveis)")}
    for name, decl in (
        ("data_quality_score", "INTEGER"),
        ("data_quality_level", "TEXT"),
        ("data_quality_issues", "TEXT"),
    ):
        if name not in cols:
            conn.execute(f"ALTER TABLE imoveis ADD COLUMN {name} {decl}")
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# EXTRATORES — detecta padrões comuns de sistemas imobiliários
# ══════════════════════════════════════════════════════════════════════════════

def fetch_page(url, session) -> tuple[str, str]:
    """Faz a requisição HTTP com retry simples. Retorna (html, url_final após redirects)."""
    for attempt in range(3):
        try:
            r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text, r.url
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def make_hash(data: dict) -> str:
    """Hash legado (URL + título + preço textual). Mantido para compatibilidade; preferir ``apply_property_identity``."""
    return legacy_content_hash(data)


def parse_preco(texto: str) -> float | None:
    """Extrai valor numérico de strings como 'R$ 450.000,00'."""
    if not texto:
        return None
    nums = re.sub(r"[^\d,]", "", texto).replace(",", ".")
    # remove pontos de milhar deixando só a vírgula decimal
    parts = nums.split(".")
    if len(parts) > 2:
        nums = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(nums)
    except ValueError:
        return None


def parse_area(texto: str) -> float | None:
    if not texto:
        return None
    m = re.search(r"(\d+[\.,]?\d*)\s*m", texto, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", "."))
    return None


def parse_int(texto: str) -> int | None:
    if not texto:
        return None
    m = re.search(r"\d+", texto)
    return int(m.group()) if m else None



def extract_imoveis_apreme(html: str, base_url: str, site: dict) -> list[dict]:
    """
    Extrator para HTML no padrão Apre.me (e afins): /<codigo>, /imovel/<codigo>/slug,
    /imovel/slug/<codigo>. Chave estável no registro: ``apreme`` (host → extrator via JSON/SQLite).
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen_codes = set()

    for tag in soup.select("nav, footer, header, script, style"):
        tag.decompose()

    domain = urlparse(base_url).netloc
    scheme = urlparse(base_url).scheme or "https"

    for a in soup.find_all("a", href=True):
        href_raw = (a.get("href") or "").strip()
        if not href_raw or href_raw.startswith("#"):
            continue
        href = href_raw.split("#")[0]
        full_joined = urljoin(base_url, href)
        pu = urlparse(full_joined)
        path = pu.path.rstrip("/")

        codigo = None
        full_url = full_joined.split("#")[0]

        # 1) Apre.me clássico: /12345 ou https://domínio/12345
        if re.fullmatch(r"/\d+", path):
            codigo = path[1:]
            full_url = f"{scheme}://{domain}{path}"
        elif re.fullmatch(r"https?://[^/]+/\d+", full_url.split("?")[0]):
            codigo = path.split("/")[-1]
        # 2) Itaivan e white-label Apre: /imovel/75766/slug-… ou /imovel/slug-…/18393
        elif path.startswith("/imovel/"):
            m = re.match(r"^/imovel/(\d+)/.+", path)
            if m:
                codigo = m.group(1)
            else:
                m = re.match(r"^/imovel/[^/]+/(\d+)$", path)
                if m:
                    codigo = m.group(1)

        if not codigo:
            continue

        if codigo in seen_codes:
            continue
        seen_codes.add(codigo)

        # Pega o bloco pai com tamanho de card individual (50-700 chars)
        # Sobe devagar e para assim que o texto fica "grande o suficiente"
        card = a.parent
        prev = card
        for _ in range(10):
            if card is None:
                card = prev
                break
            t = card.get_text(" ", strip=True)
            if len(t) >= 60:
                break  # achou um bloco com conteúdo suficiente
            prev = card
            card = card.parent

        # Se o card ficou gigante (container geral), volta ao anterior
        if card and len(card.get_text(" ", strip=True)) > 1500:
            card = prev if prev else a.parent

        text = (card.get_text(" ", strip=True) if card else "") or ""

        # Título — vem do texto do link <a> que aponta para o imóvel
        titulo = a.get_text(strip=True)
        titulo = re.sub(r"\s*(LANÇAMENTO|NOVO|Comprar|Ver mais|WhatsApp|Conversar).*", "", titulo, flags=re.I).strip()
        if not titulo or len(titulo) < 4:
            for sel in ["h5", "h4", "h3", "h2"]:
                el = card.select_one(sel) if card else None
                if el:
                    titulo = el.get_text(strip=True)[:200]
                    break
        # Link só com foto (comum em grades Apre/Itaivan)
        if not titulo or len(titulo) < 4:
            segs = [s for s in path.split("/") if s]
            if segs:
                leaf = segs[-1]
                if leaf.isdigit():
                    titulo = f"Imóvel código {leaf}"
                elif len(leaf) > 3 and re.match(r"^[\w-]+$", leaf, re.UNICODE):
                    titulo = re.sub(r"-+", " ", leaf).strip().title()[:200]
        if not titulo or len(titulo) < 4:
            continue

        titulo = titulo[:200]

        # Preço
        preco_texto, preco = "", None
        m = re.search(r"R\$\s*[\d.,]+", text)
        if m:
            preco_texto = m.group()
            preco = parse_preco(preco_texto)

        # Quartos
        quartos = None
        m = re.search(r"(\d+)\s*(?:Dormitório|dormitório|Quarto|quarto|Suíte|Suite)", text)
        if m:
            quartos = int(m.group(1))

        # Banheiros
        banheiros = None
        m = re.search(r"(\d+)\s*(?:~\s*\d+\s*)?Banheiro", text, re.I)
        if m:
            banheiros = int(m.group(1))

        # Vagas
        vagas = None
        m = re.search(r"(\d+)\s*Vaga", text, re.I)
        if m:
            vagas = int(m.group(1))

        # Área
        area_m2 = None
        m = re.search(r"Privativo:\s*([\d.,]+)\s*m", text, re.I)
        if m:
            area_m2 = float(m.group(1).replace(",", "."))
        else:
            m = re.search(r"(\d+[\.,]?\d*)\s*m[²2]", text)
            if m:
                area_m2 = parse_area(m.group())

        # Bairro / cidade
        bairro, cidade = "", "Jaraguá do Sul"
        m = re.search(
            r"([A-ZÀ-Ú][^\d,]{2,30}),\s*(Jaraguá do Sul|Guaramirim|Schroeder|Penha|Joinville|Pomerode)",
            text
        )
        if m:
            bairro = m.group(1).strip()
            cidade = m.group(2).strip()

        # Foto
        url_foto = ""
        img = card.find("img") if card else None
        if img:
            src = img.get("src") or img.get("data-src") or ""
            if src and not src.startswith("data:"):
                url_foto = src if src.startswith("http") else f"https://{domain}{src}"

        tipo = detect_tipo(titulo + " " + text[:300])

        raw = {
            "site_id":     site["id"],
            "site_name":   site["name"],
            "titulo":      titulo,
            "tipo":        tipo,
            "preco_texto": preco_texto,
            "preco":       preco,
            "area_m2":     area_m2,
            "quartos":     quartos,
            "banheiros":   banheiros,
            "vagas":       vagas,
            "bairro":      bairro,
            "cidade":      cidade,
            "codigo":      codigo,
            "url_anuncio": full_url,
            "url_foto":    url_foto,
        }
        results.append(raw)

    return results


def _itaivan_minimal_record(site: dict, base_url: str, codigo: str, slug: str, slug_first: bool) -> dict:
    """Monta registro mínimo a partir de URL /imovel/COD/slug ou /imovel/slug/COD."""
    titulo = slug.replace("-", " ").title()[:200]
    if slug_first:
        url_anuncio = urljoin(base_url, f"/imovel/{slug}/{codigo}")
    else:
        url_anuncio = urljoin(base_url, f"/imovel/{codigo}/{slug}")
    raw = {
        "site_id": site["id"],
        "site_name": site["name"],
        "titulo": titulo,
        "tipo": detect_tipo(titulo),
        "preco_texto": "",
        "preco": None,
        "area_m2": None,
        "quartos": None,
        "banheiros": None,
        "vagas": None,
        "bairro": "",
        "cidade": "Jaraguá do Sul",
        "url_anuncio": url_anuncio,
        "url_foto": "",
        "codigo": codigo,
    }
    return raw


def extract_imoveis_itaivan_embedded_urls(html: str, base_url: str, site: dict) -> list[dict]:
    """
    A listagem Itaivan é montada no browser, mas o HTML inicial costuma incluir
    JSON em <script> com URLs /imovel/COD/slug — extraímos por regex.
    """
    bad_slug = re.compile(
        r"pagina|ordenacao|widget|trabalhe|condominio|favoritos|terreno\+|comercial\+",
        re.I,
    )
    seen: set[tuple[str, str, int]] = set()
    out: list[dict] = []
    # JSON em script escapa barras como \/
    blob = html.replace("\\/", "/")

    for m in re.finditer(
        r'/imovel/(\d+)/([^\d/"\'?&<>\s]{3,})(?:/|\"|\'|\?|&|\s|>|\\u002f|$)',
        blob,
        re.I,
    ):
        codigo, slug = m.group(1), m.group(2)
        slug_l = slug.lower()
        if bad_slug.search(slug_l):
            continue
        key = (codigo, slug_l, 0)
        if key in seen:
            continue
        seen.add(key)
        out.append(_itaivan_minimal_record(site, base_url, codigo, slug, slug_first=False))

    for m in re.finditer(
        r'/imovel/([^\d/"\'?&<>\s]{8,})/(\d{4,})(?:/|\"|\'|\?|&|\s|>|\\u002f|$)',
        blob,
        re.I,
    ):
        slug, codigo = m.group(1), m.group(2)
        slug_l = slug.lower()
        if bad_slug.search(slug_l):
            continue
        key = (codigo, slug_l, 1)
        if key in seen:
            continue
        seen.add(key)
        out.append(_itaivan_minimal_record(site, base_url, codigo, slug, slug_first=True))

    return out


def _next_data_collect_imoveis(tree) -> list[dict]:
    records: list[dict] = []
    seen: set[str] = set()

    def is_row(d: dict) -> bool:
        if len(d) < 5:
            return False
        lk = {str(k).lower() for k in d}
        if "buildid" in lk or "gssp" in lk or "locales" in lk:
            return False
        code = d.get("codigo") or d.get("Codigo") or d.get("id")
        if code is None:
            return False
        score = sum(
            1
            for h in (
                "valorvenda", "valor_venda", "precovenda", "bairro", "cidade",
                "quartos", "areaprivativa", "urlamigavel", "fotos", "descricao",
                "titulo",
            )
            if h in lk
        )
        return score >= 2

    def visit(node):
        if isinstance(node, dict):
            if is_row(node):
                cid = str(node.get("codigo") or node.get("Codigo") or node.get("id") or "").strip()
                if cid.isdigit() and cid not in seen:
                    seen.add(cid)
                    records.append(node)
            for v in node.values():
                visit(v)
        elif isinstance(node, list):
            for v in node:
                visit(v)

    visit(tree)
    return records


def extract_imoveis_next_data(html: str, base_url: str, site: dict) -> list[dict]:
    m = re.search(
        r'<script id="__NEXT_DATA__"\s*[^>]*>([\s\S]*?)</script>',
        html,
        re.I,
    )
    if not m:
        return []
    try:
        tree = json.loads(m.group(1))
    except json.JSONDecodeError:
        return []
    rows = _next_data_collect_imoveis(tree)
    out: list[dict] = []
    for d in rows:
        codigo = str(d.get("codigo") or d.get("Codigo") or d.get("id") or "").strip()
        if not codigo.isdigit():
            continue
        titulo = (d.get("titulo") or d.get("Titulo") or "").strip()
        slug = (d.get("urlAmigavel") or d.get("url_amigavel") or d.get("slug") or "").strip()
        if not titulo and slug:
            titulo = re.sub(r"-+", " ", slug).strip().title()[:200]
        if not titulo:
            titulo = f"Imóvel código {codigo}"

        url_anuncio = (d.get("url") or d.get("urlCompleta") or d.get("link") or "").strip()
        if url_anuncio and not url_anuncio.startswith("http"):
            url_anuncio = urljoin(base_url, url_anuncio)
        elif slug:
            url_anuncio = urljoin(base_url, f"/imovel/{codigo}/{slug.strip('/')}")
        else:
            url_anuncio = urljoin(base_url, f"/imovel/{codigo}")

        preco_txt, preco = "", None
        for key in ("valorVenda", "valor_venda", "precoVenda", "preco", "Preco", "valor"):
            v = d.get(key)
            if v is None:
                continue
            if isinstance(v, (int, float)):
                preco = float(v)
                preco_txt = f"R$ {preco:,.0f}".replace(",", ".")
            elif isinstance(v, str) and v.strip():
                preco_txt = v.strip()
                preco = parse_preco(preco_txt)
            break

        bairro = str(d.get("bairro") or d.get("Bairro") or "")[:100]
        cidade = str(d.get("cidade") or d.get("Cidade") or "Jaraguá do Sul")[:100]
        q = d.get("quartos") or d.get("Quartos") or d.get("numeroQuartos")
        quartos = int(q) if isinstance(q, int) else parse_int(str(q)) if q is not None else None
        ap = d.get("areaPrivativa") or d.get("area_privativa") or d.get("metragem")
        area_m2 = float(ap) if isinstance(ap, (int, float)) else parse_area(str(ap)) if ap else None

        foto = ""
        fotos = d.get("fotos") or d.get("Fotos") or []
        if isinstance(fotos, list) and fotos:
            first = fotos[0]
            if isinstance(first, dict):
                foto = str(first.get("url") or first.get("src") or "")
            elif isinstance(first, str):
                foto = first
        if foto and not foto.startswith("http"):
            foto = urljoin(base_url, foto)

        desc = (d.get("descricao") or d.get("Descricao") or "")[:500]
        raw = {
            "site_id": site["id"],
            "site_name": site["name"],
            "titulo": titulo[:200],
            "tipo": detect_tipo(titulo + " " + desc),
            "preco_texto": preco_txt,
            "preco": preco,
            "area_m2": area_m2,
            "quartos": quartos,
            "banheiros": None,
            "vagas": None,
            "bairro": bairro,
            "cidade": cidade,
            "descricao": desc,
            "url_anuncio": url_anuncio,
            "url_foto": foto,
            "codigo": codigo,
        }
        out.append(raw)
    return out


_ITAIVAN_LISTAGEM_URL = "https://www.itaivan.com/venda/?pagina=1&ordenacao=dataatualizacaodesc"
_ITAIVAN_DETAIL_RE = re.compile(r"^/imovel/[^/]+/\d+/?(?:\?.*)?$", re.I)
_ITAIVAN_BAD_SLUG = re.compile(r"pagina|ordenacao|widget|trabalhe|condominio|favoritos", re.I)


def _itaivan_url_with_page(list_base: str, pagina: int) -> str:
    """Preserva path e query da listagem; força ``pagina`` na query (Apresenta.me)."""
    from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

    p = urlparse(list_base)
    qs = parse_qs(p.query, keep_blank_values=True)
    qs["pagina"] = [str(pagina)]
    return urlunparse(p._replace(query=urlencode(qs, doseq=True)))


def _is_itaivan_detail_href(href: str) -> bool:
    if not href or "/imovel/" not in href:
        return False
    if "/venda/imovel/" in href or "/aluguel/imovel/" in href:
        return False
    try:
        from urllib.parse import urlparse as _up
        path = _up(href).path
    except Exception:
        return False
    return bool(_ITAIVAN_DETAIL_RE.match(path))


def scrape_itaivan_playwright(site: dict, max_pages: int | None = None) -> tuple[list[dict], dict]:
    """
    Scraper dedicado para Itaivan usando Playwright (site renderizado por JS).
    Retorna ``(imóveis, meta)`` com contagens de páginas e erro estruturado, se houver.
    """
    meta = {
        "pages_attempted": 0,
        "pages_succeeded": 0,
        "playwright_error_type": None,
        "playwright_error_message": None,
        "render_fallback": False,
    }

    if max_pages is None:
        max_pages = max(MAX_PAGES_PER_SITE, ITAIVAN_MAX_PAGES)

    if not PLAYWRIGHT_AVAILABLE:
        log.warning("    Itaivan: Playwright não instalado — usando extrator estático (resultados limitados).")
        log.warning("    Para resultados completos: pip install playwright && playwright install chromium")
        meta["render_fallback"] = True
        return [], meta

    all_imoveis: list[dict] = []
    seen_urls: set[str] = set()

    raw_u = (site.get("url") or "").strip()
    if "itaivan.com" in raw_u and "/venda/" in raw_u.lower():
        list_seed = raw_u
    else:
        list_seed = _ITAIVAN_LISTAGEM_URL
    first_url = _itaivan_url_with_page(list_seed, 1)

    log.info("    Itaivan: Playwright — listagem: %s", first_url[:88] + ("…" if len(first_url) > 88 else ""))

    try:
        with _sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(locale="pt-BR")
            page.set_default_timeout(30000)

            page.goto(first_url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(2000)

            # Dispensa banner de cookies se aparecer
            for loc in (
                page.get_by_role("button", name=re.compile(r"entendi|aceito|ok|concordo|aceitar", re.I)),
                page.locator(".cc-btn"),
            ):
                if loc.count():
                    try:
                        loc.first.click(timeout=3000)
                        page.wait_for_timeout(500)
                    except Exception:
                        pass
                    break

            list_base_url = page.url

            for pagina in range(1, max_pages + 1):
                meta["pages_attempted"] += 1
                if pagina > 1:
                    next_url = _itaivan_url_with_page(list_base_url, pagina)
                    page.goto(next_url, wait_until="networkidle", timeout=60000)
                    page.wait_for_timeout(1800)

                links = page.locator('a[href*="/imovel/"]')
                count = links.count()
                novos_pagina = 0

                for i in range(count):
                    href = (links.nth(i).get_attribute("href") or "").strip()
                    if not href or not _is_itaivan_detail_href(href):
                        continue
                    # Monta URL absoluta
                    if href.startswith("/"):
                        abs_url = "https://www.itaivan.com" + href
                    elif not href.startswith("http"):
                        abs_url = "https://www.itaivan.com/" + href
                    else:
                        abs_url = href
                    if abs_url in seen_urls:
                        continue

                    # Extrai código e slug da URL
                    try:
                        from urllib.parse import urlparse as _up2
                        parts = _up2(abs_url).path.rstrip("/").split("/")
                        # /imovel/<slug>/<id> ou /imovel/<id>/<slug>
                        codigo = next((p for p in reversed(parts) if p.isdigit()), None)
                        slug = next((p for p in reversed(parts) if not p.isdigit() and p not in ("", "imovel")), "")
                    except Exception:
                        codigo, slug = None, ""

                    if not codigo:
                        continue
                    if _ITAIVAN_BAD_SLUG.search(slug):
                        continue

                    seen_urls.add(abs_url)
                    texto = links.nth(i).inner_text()
                    titulo = texto.strip()[:200] if texto.strip() else slug.replace("-", " ").title()[:200]
                    preco = None
                    preco_texto = ""
                    # Tenta extrair preço do texto do card
                    m_preco = re.search(r"R\$\s*([\d.,]+)", texto)
                    if m_preco:
                        preco_texto = "R$ " + m_preco.group(1)
                        try:
                            preco = float(m_preco.group(1).replace(".", "").replace(",", "."))
                        except Exception:
                            pass

                    im = {
                        "site_id": site["id"],
                        "site_name": site["name"],
                        "titulo": titulo,
                        "tipo": detect_tipo(titulo),
                        "finalidade": "venda",
                        "preco": preco,
                        "preco_texto": preco_texto,
                        "area_m2": None,
                        "quartos": None,
                        "banheiros": None,
                        "vagas": None,
                        "bairro": "",
                        "cidade": "Jaraguá do Sul",
                        "url_anuncio": abs_url,
                        "url_foto": "",
                        "codigo": codigo,
                    }
                    all_imoveis.append(im)
                    novos_pagina += 1

                log.info(f"    Itaivan Playwright: página {pagina} — {novos_pagina} novos imóveis")

                if novos_pagina > 0:
                    meta["pages_succeeded"] += 1

                if novos_pagina == 0:
                    log.info(f"    Itaivan Playwright: sem novos na página {pagina}, encerrando.")
                    break

                time.sleep(0.8)

            browser.close()

    except Exception as e:
        et, em = format_exception(e)
        meta["playwright_error_type"] = et
        meta["playwright_error_message"] = em
        log.error("    Itaivan Playwright: erro [%s] — %s", et, em)
        return all_imoveis, meta

    log.info(f"    Itaivan Playwright: total = {len(all_imoveis)} imóveis")
    return all_imoveis, meta


def extract_imoveis_itaivan(html: str, base_url: str, site: dict) -> list[dict]:
    """
    Itaivan (Apresenta.me): listagem híbrida — tenta dados embutidos, depois Apre, depois HTML genérico.
    """
    found = extract_imoveis_next_data(html, base_url, site)
    if found:
        return found
    found = extract_imoveis_itaivan_embedded_urls(html, base_url, site)
    if found:
        return found
    found = extract_imoveis_apreme(html, base_url, site)
    if found:
        return found
    return _extract_imoveis_generic_main(html, base_url, site)


def _extract_imoveis_generic_main(html: str, base_url: str, site: dict) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for tag in soup.select("nav, footer, header, script, style, .menu, #menu"):
        tag.decompose()

    sistema = detect_sistema(html, base_url)

    if sistema == "apreme":
        return extract_imoveis_apreme(html, base_url, site)

    card_selectors = {
        "jetimob":    [".property-card", ".imovel-card", "[class*='property']"],
        "kenlo":      [".result-item", ".listing-item", "[class*='listing']"],
        "superlogica":["[class*='imovel']", ".imovel", ".card-imovel"],
        "generic":    [
            "[class*='imovel']", "[class*='property']", "[class*='listing']",
            "[class*='card']", "article", ".item", "[class*='result']",
            "[class*='produto']", "[class*='imo']",
        ],
    }

    selectors = card_selectors.get(sistema, card_selectors["generic"])
    cards = []
    for sel in selectors:
        found = soup.select(sel)
        found = [c for c in found if len(c.get_text(strip=True)) > 30]
        if len(found) >= 3:
            cards = found
            break

    if not cards:
        cards = find_imovel_links(soup, base_url)

    for card in cards[:50]:
        item = extract_from_card(card, base_url, site)
        if item and item.get("titulo"):
            results.append(item)

    return results


def extract_imoveis_generic(html: str, base_url: str, site: dict) -> list[dict]:
    """
    Extrator genérico: tenta múltiplas heurísticas para detectar cards de imóveis
    nos padrões mais comuns de sistemas imobiliários brasileiros
    (oimóvel, superlogica, jetimob, kenlo, etc.).
    """
    netloc = (urlparse(base_url).netloc or "").lower()
    if netloc.endswith("itaivan.com"):
        return extract_imoveis_itaivan(html, base_url, site)

    ext_key = resolve_extractor_key_for_host(netloc)
    if ext_key:
        fn = EXTRACTOR_REGISTRY.get(ext_key)
        if fn:
            log.debug("Extrator %s para host %s", ext_key, netloc)
            return fn(html, base_url, site)
        log.warning("Chave de extrator desconhecida %r para %s — usando genérico", ext_key, netloc)

    return _extract_imoveis_generic_main(html, base_url, site)


def detect_sistema(html: str, url: str) -> str:
    """Detecta qual sistema imobiliário o site usa."""
    signals = {
        "apreme":      ["apre.me", "img.apre.me", "apreme"],
        "jetimob":     ["jetimob", "jet-imovel", "data-jet"],
        "kenlo":       ["kenlo", "jetimob-ng", "kenlo-"],
        "superlogica": ["superlogica", "superlógica"],
        "vistasoft":   ["vistasoft", "vista-"],
    }
    html_lower = html[:5000].lower()
    for sistema, keys in signals.items():
        if any(k in html_lower for k in keys):
            return sistema
    return "generic"


def find_imovel_links(soup, base_url):
    """Último recurso: encontra links que parecem imóveis pelo padrão de URL."""
    patterns = [
        r"/imovel/", r"/imoveis/", r"/property/",
        r"/venda/", r"/comprar/", r"cod=", r"codigo=",
    ]
    links = soup.find_all("a", href=True)
    cards = []
    seen = set()
    for a in links:
        href = a["href"]
        full = urljoin(base_url, href)
        if full in seen:
            continue
        if any(re.search(p, full, re.I) for p in patterns):
            # pega o bloco pai como "card"
            parent = a.find_parent(["div", "li", "article", "section"])
            if parent and len(parent.get_text(strip=True)) > 40:
                cards.append(parent)
                seen.add(full)
    return cards[:50]


def extract_from_card(card, base_url: str, site: dict) -> dict:
    """Extrai campos estruturados de um card HTML."""
    text = card.get_text(" ", strip=True)

    # Título
    titulo = ""
    for sel in ["h2", "h3", "h1", "[class*='title']", "[class*='titulo']", "[class*='nome']"]:
        el = card.select_one(sel)
        if el and el.get_text(strip=True):
            titulo = el.get_text(strip=True)[:200]
            break
    if not titulo:
        titulo = text[:80]

    # Preço
    preco_texto = ""
    for sel in ["[class*='price']", "[class*='preco']", "[class*='valor']"]:
        el = card.select_one(sel)
        if el:
            preco_texto = el.get_text(strip=True)
            break
    if not preco_texto:
        m = re.search(r"R\$\s*[\d.,]+", text)
        if m:
            preco_texto = m.group()

    # URL do anúncio
    url_anuncio = ""
    a_tag = card.find("a", href=True)
    if a_tag:
        url_anuncio = urljoin(base_url, a_tag["href"])

    # Foto
    url_foto = ""
    img = card.find("img")
    if img:
        src = img.get("src") or img.get("data-src") or img.get("data-lazy") or ""
        if src and not src.startswith("data:"):
            url_foto = urljoin(base_url, src)

    # Área
    area_texto = ""
    m = re.search(r"(\d+[\.,]?\d*)\s*m[²2]?", text, re.IGNORECASE)
    if m:
        area_texto = m.group()

    # Quartos
    quartos_texto = ""
    for pattern in [r"(\d+)\s*(?:quarto|dorm|suíte|suite)", r"(\d+)\s*(?:qto|dmt)"]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            quartos_texto = m.group(1)
            break

    # Banheiros
    ban_texto = ""
    m = re.search(r"(\d+)\s*(?:banheiro|wc|ban)", text, re.IGNORECASE)
    if m:
        ban_texto = m.group(1)

    # Vagas
    vagas_texto = ""
    m = re.search(r"(\d+)\s*(?:vaga|garagem|garage)", text, re.IGNORECASE)
    if m:
        vagas_texto = m.group(1)

    # Bairro / endereço
    bairro = ""
    for sel in ["[class*='bairro']", "[class*='location']", "[class*='endereco']", "[class*='address']"]:
        el = card.select_one(sel)
        if el:
            bairro = el.get_text(strip=True)[:100]
            break

    # Tipo
    tipo = detect_tipo(titulo + " " + text[:200])

    raw = {
        "site_id": site["id"],
        "site_name": site["name"],
        "titulo": titulo,
        "tipo": tipo,
        "preco_texto": preco_texto,
        "preco": parse_preco(preco_texto),
        "area_m2": parse_area(area_texto),
        "quartos": parse_int(quartos_texto),
        "banheiros": parse_int(ban_texto),
        "vagas": parse_int(vagas_texto),
        "bairro": bairro,
        "cidade": "Jaraguá do Sul",
        "url_anuncio": url_anuncio,
        "url_foto": url_foto,
    }
    return raw


def extract_imoveis_imonov_webflow(html: str, base_url: str, site: dict) -> list[dict]:
    """Imonov + Webflow: grade ``.resultado`` com cards ``.box-destaque-imovel`` (links ``/imovel/venda/``).

    Usado por Morada Brasil e outros sites no mesmo tema. O extrator genérico casaria centenas de
    nós ``[class*='imovel']``; aqui só a listagem real (~18 itens/página quando o site usa esse layout).
    """
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one(".div-block-57.resultado") or soup.select_one(".resultado")
    if not root:
        log.warning("Imonov/Webflow: bloco .resultado não encontrado — fallback genérico")
        return _extract_imoveis_generic_main(html, base_url, site)

    results: list[dict] = []
    for card in root.select(".box-destaque-imovel"):
        url_anuncio = ""
        for a in card.select("a[href]"):
            href = (a.get("href") or "").strip()
            if "/imovel/venda/" in href:
                url_anuncio = urljoin(base_url, href)
                break
        if not url_anuncio:
            continue

        titulo = ""
        h3 = card.select_one("h3.t-imoveis, h3")
        if h3:
            titulo = h3.get_text(strip=True)[:200]
        if not titulo:
            img = card.select_one("img[alt]")
            alt = (img.get("alt") if img else "") or ""
            if alt.strip():
                titulo = alt.strip()[:200]

        preco_texto = ""
        vel = card.select_one(".valor-imovel")
        if vel:
            preco_texto = re.sub(r"\s+", " ", vel.get_text(" ", strip=True))

        codigo = ""
        for p in card.select("p.endereco-imovel, .endereco-imovel"):
            t = p.get_text(strip=True)
            mref = re.search(r"Refer[êe]ncia:\s*(\d+)", t, re.I)
            if mref:
                codigo = mref.group(1)
                break
        if not codigo:
            mtail = re.search(r"/(\d+)\s*$", url_anuncio.rstrip("/"))
            if mtail:
                codigo = mtail.group(1)

        bairro = ""
        for p in card.select("p.endereco-imovel, .endereco-imovel"):
            t = p.get_text(strip=True)
            if re.search(r"Refer[êe]ncia:", t, re.I):
                continue
            if "/" in t:
                bairro = t.split("/")[0].strip()[:100]
            break

        url_foto = ""
        imgi = card.select_one("img.image-57[src], .carrossel-galeria-imoveis img[src]")
        if imgi:
            src = imgi.get("src") or ""
            if src and not src.startswith("data:"):
                url_foto = urljoin(base_url, src)

        area_m2 = None
        for box in card.select(".box-detalhes-imoveis"):
            if not box.find("img", src=re.compile(r"ico-metros", re.I)):
                continue
            vit = box.select_one(".valor-iten")
            if not vit:
                continue
            vt = vit.get_text(strip=True)
            if re.match(r"^[\d.,]+$", vt):
                try:
                    area_m2 = float(vt.replace(".", "").replace(",", "."))
                except ValueError:
                    pass
            break

        tipo_txt = titulo or "imóvel"
        raw = {
            "site_id": site["id"],
            "site_name": site["name"],
            "titulo": titulo or "Imóvel",
            "tipo": detect_tipo(tipo_txt),
            "finalidade": "venda",
            "preco_texto": preco_texto,
            "preco": parse_preco(preco_texto),
            "area_m2": area_m2,
            "quartos": None,
            "banheiros": None,
            "vagas": None,
            "bairro": bairro,
            "cidade": "Jaraguá do Sul",
            "endereco": "",
            "descricao": "",
            "url_anuncio": url_anuncio,
            "url_foto": url_foto,
            "codigo": codigo,
        }
        results.append(raw)

    return results


# ── Registro de extratores por tipo de site (memória: JSON + SQLite + embutido) ───────────

BUILTIN_HOST_EXTRACTOR: dict[str, str] = {
    "moradabrasil.com": "imonov_webflow",
}

_HOST_EXTRACTOR_MAP: dict[str, str] = {}

EXTRACTOR_REGISTRY = {
    "imonov_webflow": extract_imoveis_imonov_webflow,
    "apreme": extract_imoveis_apreme,
}


def _load_site_profiles_json() -> dict[str, str]:
    out: dict[str, str] = {}
    if not SITE_PROFILES_JSON.is_file():
        return out
    try:
        data = json.loads(SITE_PROFILES_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("site_profiles.json inválido: %s", e)
        return out
    for prof in data.get("profiles", []):
        key = (prof.get("extractor") or "").strip()
        if not key:
            continue
        for h in prof.get("host_match") or []:
            if isinstance(h, str) and h.strip():
                out[h.strip().lower()] = key
    return out


def refresh_site_profiles(conn: sqlite3.Connection | None = None) -> None:
    """Monta o mapa host_substring → extractor_key: embutido, depois JSON, depois SQLite (prioridade)."""
    global _HOST_EXTRACTOR_MAP
    m = dict(BUILTIN_HOST_EXTRACTOR)
    m.update(_load_site_profiles_json())
    if conn is not None:
        try:
            for row in conn.execute(
                "SELECT host_substring, extractor_key FROM site_scrape_profiles ORDER BY id"
            ):
                hs = (row[0] or "").strip().lower()
                ek = (row[1] or "").strip()
                if hs and ek:
                    m[hs] = ek
        except sqlite3.Error as e:
            log.warning("site_scrape_profiles: %s", e)
    _HOST_EXTRACTOR_MAP = m


def resolve_extractor_key_for_host(netloc: str) -> str | None:
    """Maior substring de host vence (ex.: ``foo.moradabrasil.com`` casa ``moradabrasil.com``)."""
    if not _HOST_EXTRACTOR_MAP:
        refresh_site_profiles(None)
    if not netloc:
        return None
    nl = netloc.lower()
    best_key, best_len = None, -1
    for host_sub, key in _HOST_EXTRACTOR_MAP.items():
        if host_sub in nl and len(host_sub) > best_len:
            best_key, best_len = key, len(host_sub)
    return best_key


def print_site_profiles() -> None:
    """Imprime o mapa atual (embutido + JSON + último refresh; rode após refresh no main com DB)."""
    if not _HOST_EXTRACTOR_MAP:
        refresh_site_profiles(None)
    print("\n  Mapa host → extrator (site_scrape_profiles / site_profiles.json / embutido)\n")
    for host in sorted(_HOST_EXTRACTOR_MAP.keys(), key=len, reverse=True):
        k = _HOST_EXTRACTOR_MAP[host]
        ok = "✓" if k in EXTRACTOR_REGISTRY else "✗ desconhecido"
        print(f"    {host!r:<40} → {k!r} {ok}")
    print(f"\n  Extratores registrados no código: {', '.join(sorted(EXTRACTOR_REGISTRY.keys()))}\n")


def detect_tipo(text: str) -> str:
    text = text.lower()
    tipos = {
        "apartamento": ["apartamento", "apto", "ap "],
        "casa":        ["casa", "residência", "sobrado"],
        "terreno":     ["terreno", "lote", "área"],
        "comercial":   ["comercial", "sala", "loja", "galpão", "escritório"],
        "cobertura":   ["cobertura", "penthouse"],
        "chácara":     ["chácara", "sítio", "fazenda"],
    }
    for tipo, keywords in tipos.items():
        if any(k in text for k in keywords):
            return tipo
    return "outros"


def get_next_page_url(html: str, current_url: str, page_num: int) -> str | None:
    """
    Detecta a URL da próxima página.
    Suporta múltiplos padrões de paginação usados em sistemas imobiliários brasileiros:
      - Apre.me:      /imoveis/venda/pagina-2  (hífen, sem barra final)
      - Jetimob:      /imoveis/venda/2
      - Query string: ?page=2, ?pagina=2, ?pg=2
      - Offset:       ?offset=21&limit=21
      - Path barra:   /page/2/, /pagina/2/
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1) Link rel="next" explícito — mais confiável
    for sel in ["a[rel='next']", "link[rel='next']"]:
        el = soup.select_one(sel)
        if el and el.get("href"):
            next_url = urljoin(current_url, el["href"])
            if next_url != current_url:
                return next_url

    # 2) Botão/link de próxima página por texto ou classe
    for sel in [
        ".next a", "[class*='next'] a", "[class*='proxim'] a",
        "a[class*='next']", "a[aria-label*='próxima']", "a[aria-label*='next']",
        "[class*='pagination'] a[class*='active'] + a",
        "[class*='paginacao'] a[class*='ativo'] + a",
    ]:
        el = soup.select_one(sel)
        if el and el.get("href"):
            href = el["href"].strip()
            if href and href != "#":
                next_url = urljoin(current_url, href)
                if next_url != current_url:
                    return next_url

    # 3) Query string primeiro (mais específico que path): ?page=N, ?pagina=N, ?pg=N
    for pattern in [r"([?&])(page|pagina|pg|p)=(\d+)"]:
        m = re.search(pattern, current_url, re.I)
        if m:
            old_val = int(m.group(3))
            new_val = old_val + 1
            return current_url[:m.start(1)] + m.group(1) + m.group(2) + "=" + str(new_val) + current_url[m.end():]

    # 4) Offset: ?offset=N&limit=M  (Kenlo, mouraimoveis, etc.)
    m = re.search(r"([?&])(offset)=(\d+)", current_url, re.I)
    if m:
        old_val = int(m.group(3))
        step_m = re.search(r"limit=(\d+)", current_url, re.I)
        step = int(step_m.group(1)) if step_m else 21
        new_val = old_val + step
        return current_url[:m.start(1)] + m.group(1) + "offset=" + str(new_val) + current_url[m.end():]

    # 5) Padrão Apre.me: /pagina-N (hífen, sem barra)
    #    ex: /imoveis/venda/pagina-2  →  /imoveis/venda/pagina-3
    m = re.search(r"/pagina-(\d+)(/|$)", current_url, re.I)
    if m:
        new_num = int(m.group(1)) + 1
        return current_url[:m.start()] + f"/pagina-{new_num}" + m.group(2)

    # 6) Padrão path com barra: /page/N/ ou /pagina/N/
    m = re.search(r"/(page|pagina)/(\d+)(/|$)", current_url, re.I)
    if m:
        new_num = int(m.group(2)) + 1
        return current_url[:m.start()] + f"/{m.group(1)}/{new_num}" + m.group(3)

    # 7) Padrão path numérico simples no final: /imoveis/venda/2 ou /todos/1
    m = re.search(r"/(\d+)(/?)$", current_url)
    if m:
        cur_num = int(m.group(1))
        # aceita: o número no path é a página atual (page_num-1) ou é 1 (primeira página)
        if cur_num == page_num - 1 or cur_num == 1:
            new_num = cur_num + 1
            return current_url[:m.start()] + f"/{new_num}" + m.group(2)

    # 8) Última tentativa: URL sem paginação ainda → adiciona /pagina-2
    #    Só aplica se o HTML contiver referência a "pagina-2" (confirma que o site usa esse padrão)
    #    Isso evita gerar URLs inválidas em sites que usam query string ou outro padrão
    if page_num == 2 and not re.search(r"/pagina[-/]\d+", current_url, re.I):
        if re.search(r'href=["\'][^"\']*pagina[-/]2|pagina=2|page=2', html, re.I):
            base = current_url.rstrip("/").split("?")[0]
            qs = "?" + current_url.split("?")[1] if "?" in current_url else ""
            return f"{base}/pagina-2{qs}"

    return None


def _norm_listing_url(url: str | None) -> str:
    """Chave de deduplicação por URL: canônica (tracking removido) + host/path estáveis."""
    if not url:
        return ""
    return canonical_property_url(url).lower()


def dedupe_imoveis_novos_na_sessao(
    imoveis: list[dict], urls_vistas: set[str], hashes_sessao: set[str]
) -> list[dict]:
    """Primeira ocorrência na sessão: URL canônica; sem URL, hash estável (sem depender de preço/título).

    Sem isso, cards sem ``url_anuncio`` duplicam dezenas de vezes por página (ex.: Morada).
    """
    seen_u = set(urls_vistas)
    seen_h = set(hashes_sessao)
    out = []
    for im in imoveis:
        nu = _norm_listing_url(im.get("url_anuncio"))
        h = stable_hash_for_record(im)
        if nu:
            if nu in seen_u:
                continue
            seen_u.add(nu)
            seen_h.add(h)
            out.append(im)
            continue
        if h in seen_h:
            continue
        seen_h.add(h)
        out.append(im)
    return out


def has_more_results(html: str, imoveis_count: int) -> bool:
    """
    Verifica se há mais páginas detectando sinais de fim de resultados.
    Retorna False quando claramente não há mais imóveis.
    """
    if imoveis_count == 0:
        return False

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True).lower()

    # Sinais de lista vazia / erro de página. Cuidado: "404" solto casa com preços (ex.: R$ 404.832);
    # "0 imóveis" solto casa com "10 imóveis", "20 imóveis"...
    fim_phrases = (
        "nenhum imóvel encontrado",
        "nenhum imovel encontrado",
        "nenhum resultado",
        "sem resultados",
        "no results",
        "página não encontrada",
        "pagina nao encontrada",
        "não encontramos",
        "nao encontramos",
    )
    if any(p in text for p in fim_phrases):
        return False
    if re.search(r"\bnão encontrado\b", text) or re.search(r"\bnao encontrado\b", text):
        return False
    for rx in (
        r"\b0\s+imóveis\b",
        r"\b0\s+imoveis\b",
        r"erro\s*404",
        r"\b404\s+not\s+found\b",
        r"http\s+404",
    ):
        if re.search(rx, text):
            return False

    # Verifica se link "próximo" existe
    for sel in ["a[rel='next']", ".next a", "[class*='next'] a"]:
        if soup.select_one(sel):
            return True

    # Se chegou aqui e tem imóveis, assume que pode ter mais
    return imoveis_count > 0


# ══════════════════════════════════════════════════════════════════════════════
# NORMALIZAÇÃO COM IA (opcional)
# ══════════════════════════════════════════════════════════════════════════════

def normalizar_com_ia(imoveis: list[dict]) -> list[dict]:
    """
    Envia lote de imóveis para a Claude API e recebe dados normalizados.
    Só é chamada se ANTHROPIC_API_KEY estiver preenchida.
    """
    if not ANTHROPIC_API_KEY:
        return imoveis

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Processa em lotes de 10
    normalized = []
    for i in range(0, len(imoveis), 10):
        batch = imoveis[i:i+10]
        prompt = f"""Você recebe uma lista de imóveis extraídos de sites. 
Normalize e complete os dados faltantes quando possível.
Retorne APENAS JSON com o mesmo array, corrigindo:
- tipo: normalize para: apartamento|casa|terreno|comercial|cobertura|chácara|outros
- quartos, banheiros, vagas: extraia do título/descrição se não preenchido
- bairro: extraia do título/descrição se não preenchido
- preco: se preco_texto tiver valor mas preco for null, calcule

Dados:
{json.dumps(batch, ensure_ascii=False, indent=2)}

Retorne APENAS o JSON do array, sem texto adicional."""

        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}]
            )
            text = msg.content[0].text.strip()
            text = re.sub(r"```json|```", "", text).strip()
            batch_norm = json.loads(text)
            normalized.extend(batch_norm)
        except Exception as e:
            log.warning(f"IA normalização falhou: {e}. Usando dados brutos.")
            normalized.extend(batch)

    return normalized


# ══════════════════════════════════════════════════════════════════════════════
# PERSISTÊNCIA (upsert com diff)
# ══════════════════════════════════════════════════════════════════════════════

def _merge_identity_stats(summary: SiteRunSummary, page_stats: dict) -> None:
    """Agrega contadores de fonte (5A) e de qualidade HIGH/MEDIUM/LOW (5B) no resumo do site."""
    for k, v in page_stats.get("identity_stats", {}).items():
        summary.identity_stats[k] = summary.identity_stats.get(k, 0) + int(v)
    qu = page_stats.get("identity_quality_counts") or {}
    summary.total_identity_high += int(qu.get("HIGH", 0))
    summary.total_identity_medium += int(qu.get("MEDIUM", 0))
    summary.total_identity_low += int(qu.get("LOW", 0))
    mig = int(page_stats.get("identity_migrated") or 0)
    if mig > 0:
        summary.notes.append(f"identity_migrated:{mig}")


def finalize_identity_audit_summary(summary: SiteRunSummary, site_name: str) -> None:
    """
    MINI-ETAPA 5B: preenche totais por fonte, ``identity_low_ratio``, resumo textual e alertas
    (não bloqueiam o pipeline). Deve ser chamado antes de serializar ``summary_json``.
    """
    s = summary.identity_stats
    summary.total_external_id = int(s.get("external_id", 0))
    summary.total_canonical_url = int(s.get("canonical_url", 0))
    summary.total_stable_fingerprint = int(s.get("stable_fingerprint", 0))
    summary.total_legacy_fallback = int(s.get("legacy_fallback", 0))

    denom = summary.total_identity_high + summary.total_identity_medium + summary.total_identity_low
    if denom <= 0:
        summary.identity_low_ratio = 0.0
        summary.identity_quality_summary = "sem_amostra_identidade"
        summary.identity_warning_detected = False
        summary.identity_warning_reason = ""
        return

    hi = summary.total_identity_high / denom
    summary.identity_low_ratio = round(summary.total_identity_low / denom, 4)
    med = summary.total_identity_medium / denom
    lo = summary.identity_low_ratio

    summary.identity_quality_summary = (
        f"HIGH={summary.total_identity_high} ({hi:.0%}) | "
        f"MEDIUM={summary.total_identity_medium} ({med:.0%}) | "
        f"LOW={summary.total_identity_low} ({lo:.0%}) | "
        f"fontes: ext={summary.total_external_id} url={summary.total_canonical_url} "
        f"fp={summary.total_stable_fingerprint} leg={summary.total_legacy_fallback}"
    )

    warn_bits: list[str] = []
    if summary.identity_low_ratio >= IDENTITY_LOW_RATIO_WARN:
        warn_bits.append("identity_low_ratio_alto")
        summary.warnings.append(
            f"Identidade fraca (LOW) em {summary.identity_low_ratio:.0%} dos imóveis de {site_name}"
        )
    legacy_r = summary.total_legacy_fallback / denom
    if legacy_r >= IDENTITY_LEGACY_RATIO_WARN:
        warn_bits.append("legacy_fallback_elevado")
        if legacy_r >= 0.5:
            summary.warnings.append(
                f"Site {site_name} depende majoritariamente de legacy_fallback "
                f"({legacy_r:.0%} das identidades por fonte)"
            )
        else:
            summary.warnings.append(
                f"legacy_fallback elevado em {site_name} ({legacy_r:.0%} por fonte)"
            )
    strong_share = (summary.total_external_id + summary.total_canonical_url) / denom
    if strong_share < IDENTITY_STRONG_SOURCE_MIN_RATIO:
        warn_bits.append("poucos_external_id_ou_canonical_url")
        summary.warnings.append(
            f"Poucos imóveis com external_id ou canonical_url em {site_name} "
            f"({strong_share:.0%} do total avaliado; meta mínima {IDENTITY_STRONG_SOURCE_MIN_RATIO:.0%})"
        )

    summary.identity_warning_detected = len(warn_bits) > 0
    summary.identity_warning_reason = ";".join(warn_bits)

    if hi >= 0.8 and summary.total_identity_low / denom <= 0.15:
        log.info(
            "[auditoria identidade] %s com boa qualidade de identidade (%s%% HIGH, LOW=%s%%)",
            site_name,
            int(round(hi * 100)),
            int(round(lo * 100)),
        )
    elif summary.identity_warning_detected:
        log.info(
            "[auditoria identidade] %s — %s",
            site_name,
            summary.identity_quality_summary,
        )


def _merge_data_quality_stats(summary: SiteRunSummary, page_stats: dict) -> None:
    """MINI-ETAPA 6A: agrega qualidade de campos por página no resumo do site."""
    dq = page_stats.get("data_quality_counts") or {}
    summary.total_data_high += int(dq.get("HIGH", 0))
    summary.total_data_medium += int(dq.get("MEDIUM", 0))
    summary.total_data_low += int(dq.get("LOW", 0))
    summary.data_quality_score_sum += float(page_stats.get("data_quality_score_sum") or 0)
    fl = page_stats.get("data_quality_flags") or {}
    summary.data_missing_price_count += int(fl.get("missing_price", 0))
    summary.data_missing_location_count += int(fl.get("missing_location", 0))


def finalize_data_quality_summary(summary: SiteRunSummary, site_name: str) -> None:
    """
    MINI-ETAPA 6A: ``data_low_ratio``, resumo textual e alertas (não bloqueiam o pipeline).
    """
    denom = summary.total_data_high + summary.total_data_medium + summary.total_data_low
    if denom <= 0:
        summary.data_low_ratio = 0.0
        summary.data_quality_summary = "sem_amostra_dados"
        summary.data_warning_detected = False
        summary.data_warning_reason = ""
        return

    mean = summary.data_quality_score_sum / denom
    hi_share = summary.total_data_high / denom
    summary.data_low_ratio = round(summary.total_data_low / denom, 4)
    missing_p = summary.data_missing_price_count / denom
    missing_l = summary.data_missing_location_count / denom

    summary.data_quality_summary = (
        f"HIGH={summary.total_data_high} ({hi_share:.0%}) | "
        f"MEDIUM={summary.total_data_medium} | "
        f"LOW={summary.total_data_low} ({summary.data_low_ratio:.0%}) | "
        f"score_médio={mean:.1f} | sem_preço={missing_p:.0%} | sem_local={missing_l:.0%}"
    )

    warn_bits: list[str] = []
    if summary.data_low_ratio >= DATA_Q_LOW_RATIO_WARN:
        warn_bits.append("data_low_ratio_alto")
        summary.warnings.append(
            f"Baixa qualidade de dados: {summary.data_low_ratio:.0%} dos imóveis em LOW em {site_name}"
        )
    if missing_p >= DATA_MISSING_PRICE_RATIO_WARN:
        warn_bits.append("muitos_sem_preco")
        summary.warnings.append(
            f"Site {site_name}: {missing_p:.0%} dos imóveis sem preço válido"
        )
    if missing_l >= DATA_MISSING_LOCATION_RATIO_WARN:
        warn_bits.append("muitos_sem_localizacao")
        summary.warnings.append(
            f"Site {site_name}: {missing_l:.0%} dos imóveis sem localização (bairro/endereço)"
        )
    if mean < DATA_MEAN_SCORE_WARN:
        warn_bits.append("score_medio_baixo")
        summary.warnings.append(
            f"Site {site_name} com baixa qualidade de dados (score médio {mean:.0f}/100)"
        )

    summary.data_warning_detected = len(warn_bits) > 0
    summary.data_warning_reason = ";".join(warn_bits)

    log.info("[auditoria dados] %s — %s", site_name, summary.data_quality_summary)


def upsert_imoveis(conn, imoveis: list[dict], site_id: int) -> dict:
    """Salva imóveis no banco com lógica de novo / atualizado / sem mudança.

    MINI-ETAPA 5A: coluna ``hash`` recebe a chave estável; migração automática se o registro
    antigo ainda usa o hash legado (url+título+preço).
    """
    agora = datetime.now().isoformat()
    stats = {
        "novos": 0,
        "atualizados": 0,
        "sem_mudanca": 0,
        "identity_stats": {},
        "identity_quality_counts": {"HIGH": 0, "MEDIUM": 0, "LOW": 0},
        "identity_migrated": 0,
        "data_quality_counts": {"HIGH": 0, "MEDIUM": 0, "LOW": 0},
        "data_quality_flags": {"missing_price": 0, "missing_location": 0},
        "data_quality_score_sum": 0.0,
    }

    for im in imoveis:
        if not im.get("titulo"):
            continue
        id_res = apply_property_identity(im)
        evaluate_field_quality(im)
        h = im["hash"]
        src = id_res.identity_source
        stats["identity_stats"][src] = stats["identity_stats"].get(src, 0) + 1
        iq = im.get("identity_quality") or "LOW"
        stats["identity_quality_counts"][iq] = stats["identity_quality_counts"].get(iq, 0) + 1
        dq_lv = im.get("data_quality_level") or "LOW"
        stats["data_quality_counts"][dq_lv] = stats["data_quality_counts"].get(dq_lv, 0) + 1
        stats["data_quality_score_sum"] += float(im.get("data_quality_score") or 0)
        for iss in im.get("data_quality_issues") or []:
            if iss == "missing_price":
                stats["data_quality_flags"]["missing_price"] = (
                    stats["data_quality_flags"].get("missing_price", 0) + 1
                )
            elif iss == "missing_location":
                stats["data_quality_flags"]["missing_location"] = (
                    stats["data_quality_flags"].get("missing_location", 0) + 1
                )

        if id_res.identity_fallback:
            log.debug(
                "Identidade legacy_fallback: titulo=%r debug=%s",
                (im.get("titulo") or "")[:80],
                id_res.to_debug_dict(),
            )

        leg = legacy_content_hash(im)
        candidates = [h]
        if leg != h:
            candidates.append(leg)
        ph = ",".join("?" * len(candidates))
        existente = conn.execute(
            f"SELECT id, preco, preco_texto, hash FROM imoveis WHERE site_id=? AND hash IN ({ph})",
            (site_id, *candidates),
        ).fetchone()

        canon = id_res.canonical_url_anuncio or None
        ifb = 1 if id_res.identity_fallback else 0
        leg_col = im.get("legacy_hash")
        iqual = im.get("identity_quality")
        iq_reason = im.get("identity_quality_reason")
        dqs = im.get("data_quality_score")
        dql = im.get("data_quality_level")
        dqij = data_quality_issues_json(im)

        if existente is None:
            cur = conn.execute(
                """
                INSERT INTO imoveis
                (hash, site_id, site_name, titulo, tipo, finalidade, preco, preco_texto,
                 area_m2, quartos, banheiros, vagas, bairro, cidade, endereco, descricao,
                 url_anuncio, url_foto, codigo, identity_source, legacy_hash, canonical_url_anuncio,
                 identity_fallback, identity_quality, identity_quality_reason,
                 data_quality_score, data_quality_level, data_quality_issues,
                 ativo, primeira_vez, ultima_vez, raw_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?)
                """,
                (
                    h,
                    im.get("site_id"),
                    im.get("site_name"),
                    im.get("titulo"),
                    im.get("tipo"),
                    im.get("finalidade", "venda"),
                    im.get("preco"),
                    im.get("preco_texto"),
                    im.get("area_m2"),
                    im.get("quartos"),
                    im.get("banheiros"),
                    im.get("vagas"),
                    im.get("bairro"),
                    im.get("cidade", "Jaraguá do Sul"),
                    im.get("endereco"),
                    im.get("descricao"),
                    im.get("url_anuncio"),
                    im.get("url_foto"),
                    im.get("codigo"),
                    src,
                    leg_col,
                    canon,
                    ifb,
                    iqual,
                    iq_reason,
                    dqs,
                    dql,
                    dqij,
                    agora,
                    agora,
                    json.dumps(im, ensure_ascii=False),
                ),
            )
            imovel_id = cur.lastrowid
            if im.get("preco"):
                conn.execute(
                    "INSERT INTO historico_precos (imovel_id, preco, preco_texto, data) VALUES (?,?,?,?)",
                    (imovel_id, im["preco"], im.get("preco_texto"), agora),
                )
            stats["novos"] += 1
        else:
            imovel_id, preco_ant, _, hash_antigo = existente
            if hash_antigo != h:
                conn.execute(
                    """
                    UPDATE imoveis SET hash=?, identity_source=?, legacy_hash=?, canonical_url_anuncio=?,
                    identity_fallback=?, identity_quality=?, identity_quality_reason=?,
                    data_quality_score=?, data_quality_level=?, data_quality_issues=? WHERE id=?
                    """,
                    (h, src, hash_antigo, canon, ifb, iqual, iq_reason, dqs, dql, dqij, imovel_id),
                )
                stats["identity_migrated"] += 1
                log.debug(
                    "Identidade migrada: imovel_id=%s hash_antigo=%s… -> hash_novo=%s… fonte=%s",
                    imovel_id,
                    hash_antigo[:12],
                    h[:12],
                    src,
                )

            conn.execute(
                """
                UPDATE imoveis SET ultima_vez=?, ativo=1, identity_source=?, canonical_url_anuncio=?,
                identity_fallback=?, identity_quality=?, identity_quality_reason=?,
                data_quality_score=?, data_quality_level=?, data_quality_issues=? WHERE id=?
                """,
                (agora, src, canon, ifb, iqual, iq_reason, dqs, dql, dqij, imovel_id),
            )
            if im.get("preco") and im["preco"] != preco_ant:
                conn.execute(
                    "INSERT INTO historico_precos (imovel_id, preco, preco_texto, data) VALUES (?,?,?,?)",
                    (imovel_id, im["preco"], im.get("preco_texto"), agora),
                )
                conn.execute(
                    "UPDATE imoveis SET preco=?, preco_texto=? WHERE id=?",
                    (im["preco"], im.get("preco_texto"), imovel_id),
                )
                stats["atualizados"] += 1
                log.debug(
                    "Preço alterado; identidade mantida: imovel_id=%s fonte=%s hash=%s…",
                    imovel_id,
                    src,
                    h[:12],
                )
            else:
                stats["sem_mudanca"] += 1

    conn.commit()
    return stats


def marcar_removidos(conn, site_id: int, hashes_vistos: set) -> int:
    """Marca como inativos imóveis do site que não apareceram nessa rodada."""
    ativos = conn.execute(
        "SELECT id, hash FROM imoveis WHERE site_id=? AND ativo=1", (site_id,)
    ).fetchall()
    removidos = 0
    agora = datetime.now().isoformat()
    for row in ativos:
        if row[1] not in hashes_vistos:
            conn.execute(
                "UPDATE imoveis SET ativo=0, ultima_vez=? WHERE id=?",
                (agora, row[0])
            )
            removidos += 1
    conn.commit()
    return removidos


def contar_removidos_pendentes(conn, site_id: int, hashes_vistos: set) -> int:
    """Quantos ativos do site não estão em ``hashes_vistos`` (seriam inativados por ``marcar_removidos``)."""
    rows = conn.execute(
        "SELECT hash FROM imoveis WHERE site_id=? AND ativo=1", (site_id,)
    ).fetchall()
    return sum(1 for row in rows if row[0] not in hashes_vistos)


def apply_site_removals_with_guard(
    conn,
    site: dict,
    hashes_vistos: set,
    extraction: SiteExtractionStatus,
    volume_total: int,
) -> dict[str, int | bool | str]:
    """
    Aplica ``marcar_removidos`` só quando ``removals_safe`` (derivado de status + volume).
    Retorna campos para ``stats``/``SiteRunSummary``.
    """
    _, removals_safe = compute_sync_removals_safe(extraction, volume_total)
    site_name = site["name"]

    if removals_safe:
        applied = marcar_removidos(conn, site["id"], hashes_vistos)
        log.info("Remoções permitidas para %s", site_name)
        return {
            "removidos": applied,
            "removals_blocked": False,
            "removals_blocked_reason": "",
            "removed_count_attempted": applied,
            "removed_count_applied": applied,
        }

    reason = extraction.value
    attempted = contar_removidos_pendentes(conn, site["id"], hashes_vistos)
    log.info(
        "Remoções bloqueadas para %s por status %s",
        site_name,
        reason,
    )
    return {
        "removidos": 0,
        "removals_blocked": True,
        "removals_blocked_reason": reason,
        "removed_count_attempted": attempted,
        "removed_count_applied": 0,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SCRAPING DE UM SITE
# ══════════════════════════════════════════════════════════════════════════════

def _unpack_baseline_for_resolve(
    baseline_row: dict | None,
) -> tuple[bool, int | None, int | None, int]:
    """
    baseline_available para classificação 3B: há histórico saudável e ``min_expected`` válido.
    """
    if not baseline_row:
        return False, None, None, 0
    cnt = int(baseline_row.get("healthy_runs_count") or 0)
    rmin = baseline_row.get("min_expected_volume")
    rmax = baseline_row.get("max_expected_volume")
    bmin = int(rmin) if rmin is not None else None
    bmax = int(rmax) if rmax is not None else None
    b_avail = cnt > 0 and bmin is not None
    return b_avail, bmin, bmax, cnt


def _apply_extraction_anomalies_and_logs(
    site: dict,
    summary: SiteRunSummary,
    stats: dict,
    extraction: SiteExtractionStatus,
    status_source: str,
    baseline_row: dict | None,
    queda_abrupta: bool,
) -> None:
    """MINI-ETAPA 3B: preenche anomaly_* / status_decision_source e logs."""
    vol = int(stats.get("total") or 0)
    br = baseline_row or {}
    b_avail, bmin, bmax, _cnt = _unpack_baseline_for_resolve(baseline_row)

    summary.status_decision_source = status_source
    summary.anomaly_detected = False
    summary.anomaly_type = None
    summary.anomaly_details = {}

    if extraction.value.startswith("ERRO"):
        summary.status_decision_source = "heuristic"
        return

    if extraction == SiteExtractionStatus.SUSPEITO_ZERO_RESULTADOS:
        summary.status_decision_source = "heuristic"
        return

    avg_v = (
        float(br["avg_healthy_volume_total"])
        if br.get("avg_healthy_volume_total") is not None
        else None
    )
    ratio = round(vol / avg_v, 4) if avg_v and avg_v > 0 else None

    if extraction == SiteExtractionStatus.SUSPEITO_VOLUME_BAIXO:
        summary.status_decision_source = status_source
        if status_source == "baseline":
            summary.anomaly_detected = True
            summary.anomaly_type = "VOLUME_BELOW_BASELINE"
            summary.anomaly_details = {
                "current_volume": vol,
                "expected_min": bmin,
                "expected_max": bmax,
                "ratio": ratio,
            }
            log.warning(
                "Anomalia detectada para %s: volume %s abaixo do mínimo esperado %s",
                site["name"],
                vol,
                bmin,
            )
            log.info(
                "Status definido por baseline para %s: SUSPEITO_VOLUME_BAIXO",
                site["name"],
            )
        else:
            summary.anomaly_detected = True
            summary.anomaly_type = "VOLUME_LOW_HEURISTIC"
            summary.anomaly_details = {
                "current_volume": vol,
                "heuristic_band_max": VOLUME_BAIXO_MAX,
            }
            if not b_avail:
                log.info(
                    "Sem baseline disponível para %s; usando heurística",
                    site["name"],
                )
        return

    if extraction == SiteExtractionStatus.SUSPEITO_QUEDA_ABRUPTA:
        summary.status_decision_source = "heuristic"
        summary.anomaly_detected = True
        summary.anomaly_type = "ABRUPT_PAGINATION_DROP"
        summary.anomaly_details = {"queda_abrupta_flag": True}
        if b_avail:
            summary.anomaly_details["baseline_context"] = {
                "expected_min": bmin,
                "expected_max": bmax,
                "current_vs_baseline_ratio": ratio,
            }
        return

    if extraction == SiteExtractionStatus.OK:
        summary.status_decision_source = status_source
        if b_avail and bmax is not None and vol > bmax:
            summary.anomaly_detected = True
            summary.anomaly_type = "VOLUME_ABOVE_BASELINE"
            summary.status_decision_source = "baseline"
            summary.anomaly_details = {
                "current_volume": vol,
                "expected_min": bmin,
                "expected_max": bmax,
                "ratio": ratio,
                "note": "observação; status OK",
            }
        return


def _apply_retry_summary_fields(
    summary: SiteRunSummary,
    extraction: SiteExtractionStatus,
    retry_context: dict | None,
) -> None:
    """MINI-ETAPA 7B — metadados de retry no JSON do site."""
    from site_retry import RETRY_RESULT_NOT_ATTEMPTED

    if retry_context and retry_context.get("is_retry"):
        summary.retry_attempted = True
        summary.original_status = str(retry_context.get("original_status") or "")
        summary.retry_reason = str(retry_context.get("reason") or "")
        summary.retry_result = extraction.value
    else:
        summary.retry_attempted = False
        summary.original_status = ""
        summary.retry_reason = ""
        summary.retry_result = RETRY_RESULT_NOT_ATTEMPTED


def _persist_site_log(
    conn: sqlite3.Connection,
    agora: str,
    site: dict,
    stats: dict,
    summary: SiteRunSummary,
    extraction: SiteExtractionStatus,
    erro_msg: str | None = None,
    retry_context: dict | None = None,
) -> None:
    """Grava log_execucoes com status legado + diagnóstico estruturado."""
    summary.extraction_status = extraction.value
    summary.volume_total = stats.get("total", 0)
    summary.new_count = stats.get("novos", 0)
    summary.updated_count = stats.get("atualizados", 0)
    summary.removed_count = stats.get("removidos", 0)
    sync_safe, removals_safe = compute_sync_removals_safe(extraction, summary.volume_total)
    summary.sync_safe = sync_safe
    summary.removals_safe = removals_safe
    summary.removals_blocked = bool(stats.get("removals_blocked", False))
    summary.removals_blocked_reason = str(stats.get("removals_blocked_reason") or "")
    summary.removed_count_applied = int(
        stats.get("removed_count_applied", summary.removed_count)
    )
    summary.removed_count_attempted = int(
        stats.get("removed_count_attempted", summary.removed_count)
    )
    baseline_row = get_site_baseline(conn, site["id"])
    enrich_summary_with_baseline(summary, baseline_row, stats.get("total", 0))
    finalize_identity_audit_summary(summary, site["name"])
    finalize_data_quality_summary(summary, site["name"])
    enrich_summary_with_site_health(summary, conn, site, extraction.value)
    _apply_retry_summary_fields(summary, extraction, retry_context)
    legacy = legacy_log_status(extraction)
    payload = summary.to_json()
    conn.execute(
        """
        INSERT INTO log_execucoes (
            data, site_id, site_name, status, total_encontrados, novos, atualizados, removidos,
            erro_msg, extraction_status, summary_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            agora,
            site["id"],
            site["name"],
            legacy,
            stats["total"],
            stats["novos"],
            stats["atualizados"],
            stats["removidos"],
            erro_msg,
            extraction.value,
            payload,
        ),
    )
    conn.commit()
    update_site_baseline(conn, site["id"], site["name"], summary)


def _log_pagination_guard_failure(site_name: str, guard, target_page: int) -> None:
    """MINI-ETAPA 4A — mensagens alinhadas aos códigos de ``pagination_guard``."""
    code = guard.code
    if code == CODE_CROSS_DOMAIN:
        log.warning("Próxima página inválida para %s: host incompatível", site_name)
    elif code == CODE_REPEATED:
        log.warning("Paginação interrompida para %s: próxima URL já visitada", site_name)
    elif code == CODE_SAME_PAGE:
        log.warning("Paginação interrompida para %s: próxima URL igual à atual", site_name)
    else:
        log.warning(
            "Paginação interrompida para %s (destino pág. %s): [%s] %s",
            site_name,
            target_page,
            code,
            guard.detail,
        )


def scrape_site(site: dict, conn, session, *, retry_context: dict | None = None) -> dict:
    """Scrapa todas as páginas de um site e salva no banco. Retorna stats + extraction_status + site_summary."""
    log.info(f"[{site['id']:2d}/53] {site['name']}")
    t_mono = time.monotonic()
    started = datetime.now().isoformat()
    agora = started
    stats = {"novos": 0, "atualizados": 0, "sem_mudanca": 0, "removidos": 0, "total": 0}
    hashes_vistos = set()

    summary = SiteRunSummary(
        site=site["name"],
        site_id=site["id"],
        started_at=started,
        listing_url=site.get("url") or "",
    )

    had_request_error = False
    had_render_error = False
    pagination_error = False
    queda_abrupta = False
    page1_bruto_zero = False
    page1_dedupe_zero_but_bruto_positive = False
    listagem_invalida = False
    prev_deduped_count = 0
    prev_had_more = False

    extraction = SiteExtractionStatus.OK
    erro_msg_full: str | None = None

    try:
        # ── Itaivan: scraper dedicado Playwright ──────────────────────────────
        if "itaivan.com" in site["url"]:
            imoveis_todos, pw_meta = scrape_itaivan_playwright(site)
            summary.pages_attempted = int(pw_meta.get("pages_attempted") or 0)
            summary.pages_succeeded = int(pw_meta.get("pages_succeeded") or 0)
            summary.notes.append(json.dumps({"itaivan_playwright": pw_meta}, ensure_ascii=False))

            if pw_meta.get("playwright_error_type"):
                summary.error_type = pw_meta["playwright_error_type"]
                summary.error_message = pw_meta.get("playwright_error_message")
                summary.warnings.append(
                    f"Playwright: {summary.error_type}: {summary.error_message or ''}"
                )
                if not imoveis_todos:
                    had_render_error = True
                else:
                    summary.warnings.append("Coleta parcial antes do erro no Playwright.")
            elif pw_meta.get("render_fallback"):
                had_render_error = True
                summary.error_type = "PlaywrightUnavailable"
                summary.error_message = "Playwright não disponível para renderização."

            if imoveis_todos:
                imoveis_todos = normalizar_com_ia(imoveis_todos)
                page_stats = upsert_imoveis(conn, imoveis_todos, site["id"])
                _merge_identity_stats(summary, page_stats)
                _merge_data_quality_stats(summary, page_stats)
                for k in ["novos", "atualizados", "sem_mudanca"]:
                    stats[k] += page_stats[k]
                stats["total"] += len(imoveis_todos)
                hashes_vistos.update(im["hash"] for im in imoveis_todos if im.get("hash"))

            vol = stats["total"]
            baseline_row_it = get_site_baseline(conn, site["id"])
            b_avail_it, bmin_it, bmax_it, hcnt_it = _unpack_baseline_for_resolve(
                baseline_row_it
            )
            if had_render_error and vol == 0:
                extraction = SiteExtractionStatus.ERRO_RENDERIZACAO
                _apply_extraction_anomalies_and_logs(
                    site, summary, stats, extraction, "heuristic", baseline_row_it, False
                )
            elif vol == 0:
                extraction = SiteExtractionStatus.SUSPEITO_ZERO_RESULTADOS
                _apply_extraction_anomalies_and_logs(
                    site, summary, stats, extraction, "heuristic", baseline_row_it, False
                )
            else:
                extraction, status_src_it = resolve_final_extraction_status(
                    had_request_error=False,
                    had_render_error=False,
                    pagination_error=False,
                    queda_abrupta=False,
                    page1_bruto_zero=False,
                    page1_dedupe_zero_but_bruto_positive=False,
                    listagem_invalida=False,
                    volume_total=vol,
                    baseline_available=b_avail_it,
                    baseline_expected_min=bmin_it,
                    baseline_healthy_runs_count=hcnt_it,
                )
                _apply_extraction_anomalies_and_logs(
                    site,
                    summary,
                    stats,
                    extraction,
                    status_src_it,
                    baseline_row_it,
                    False,
                )

            rm = apply_site_removals_with_guard(conn, site, hashes_vistos, extraction, vol)
            stats["removidos"] = rm["removidos"]
            stats["removals_blocked"] = rm["removals_blocked"]
            stats["removals_blocked_reason"] = rm["removals_blocked_reason"]
            stats["removed_count_attempted"] = rm["removed_count_attempted"]
            stats["removed_count_applied"] = rm["removed_count_applied"]

            summary.seal(t_mono)
            _persist_site_log(
                conn, agora, site, stats, summary, extraction, erro_msg_full, retry_context
            )
            log.info(
                "    ✓ %s total | +%s novos | ~%s atualizados | -%s removidos  | status=%s | id=%s",
                stats["total"],
                stats["novos"],
                stats["atualizados"],
                stats["removidos"],
                extraction.value,
                summary.identity_stats or {},
            )
            stats["extraction_status"] = extraction.value
            stats["site_summary"] = summary.to_dict()
            return stats

        # ── Fluxo normal (requests + BeautifulSoup) ───────────────────────────
        url = site["url"]
        urls_vistas: set[str] = set()
        hashes_sessao: set[str] = set()
        max_listing_peak = 0
        pagination_seen: set[str] = set()
        cutoff_rs = PaginationRunningStats()
        for page_num in range(1, MAX_PAGES_PER_SITE + 1):
            summary.pages_attempted += 1
            try:
                html, url_final = fetch_page(url, session)
            except Exception as fetch_exc:
                et, em = format_exception(fetch_exc)
                summary.error_type = et
                summary.error_message = em
                erro_msg_full = f"{et}: {em}"
                had_request_error = is_request_layer_error(fetch_exc)
                if not had_request_error:
                    had_render_error = is_render_layer_error(fetch_exc)
                    if not had_render_error:
                        summary.warnings.append(f"Exceção não classificada no fetch: {et}: {em}")
                log.error("    ✗ Falha ao obter página %s [%s]: %s", page_num, et, em)
                # Não marca removidos em falha de rede (evita inativar tudo com hashes_vistos vazio).
                extraction = (
                    SiteExtractionStatus.ERRO_REQUISICAO
                    if had_request_error
                    else (
                        SiteExtractionStatus.ERRO_RENDERIZACAO
                        if had_render_error
                        else SiteExtractionStatus.ERRO_EXTRACAO
                    )
                )
                summary.seal(t_mono)
                _persist_site_log(
                    conn, agora, site, stats, summary, extraction, erro_msg_full, retry_context
                )
                stats["extraction_status"] = extraction.value
                stats["site_summary"] = summary.to_dict()
                return stats

            norm_listing = normalize_pagination_url(url_final)
            if norm_listing and norm_listing in pagination_seen:
                summary.pagination_loop_detected = True
                summary.redirect_loop_detected = True
                summary.pagination_stopped_reason = "listing_url_repeated"
                summary.next_page_failure_reason = "listing_url_repeated"
                log.warning(
                    "Paginação interrompida para %s: URL de listagem já visitada (loop/redirect)",
                    site["name"],
                )
                break
            if norm_listing:
                pagination_seen.add(norm_listing)

            if url_final != url and page_num > 1:
                summary.warnings.append(
                    f"Redirect na paginação (página {page_num}): {url[:70]}… → {url_final[:70]}…"
                )
                log.info(
                    f"    Redirect detectado ({url} → {url_final}) — fim da paginação"
                )
                break

            imoveis_pagina = extract_imoveis_generic(html, url_final, site)

            if not imoveis_pagina:
                if page_num > 1 and prev_deduped_count >= PAGINATION_FULL_PAGE_MIN and prev_had_more:
                    queda_abrupta = True
                    summary.warnings.append(
                        "Página sem itens após listagem cheia com indicação de mais resultados."
                    )
                elif page_num == 1:
                    page1_bruto_zero = True
                    if html_sugere_listagem(html):
                        listagem_invalida = True
                log.info(f"    Página {page_num}: 0 imóveis — parando paginação")
                break

            n_bruto = len(imoveis_pagina)
            imoveis_pagina = dedupe_imoveis_novos_na_sessao(
                imoveis_pagina, urls_vistas, hashes_sessao
            )
            if not imoveis_pagina:
                if page_num == 1 and n_bruto > 0:
                    page1_dedupe_zero_but_bruto_positive = True
                    summary.warnings.append(
                        "Página 1: itens brutos perdidos na deduplicação (URLs/hashes repetidos?)."
                    )
                    log.info(
                        f"    Página {page_num}: {n_bruto} extraídos, nenhum item novo (URL/hash) — parando paginação"
                    )
                elif page_num > 1 and prev_deduped_count >= PAGINATION_FULL_PAGE_MIN:
                    summary.pages_with_no_new_items += 1
                    summary.pagination_cutoff_triggered = True
                    summary.pagination_cutoff_reason = "no_new_items_after_full_pages"
                    log.info(
                        "Paginação encerrada para %s: 0 itens novos após páginas cheias",
                        site["name"],
                    )
                    log.info(
                        f"    Página {page_num}: {n_bruto} extraídos, nenhum item novo (URL/hash) — parando paginação"
                    )
                else:
                    log.info(
                        f"    Página {page_num}: {n_bruto} extraídos, nenhum item novo (URL/hash) — parando paginação"
                    )
                break
            dup_n = n_bruto - len(imoveis_pagina)
            dup_suffix = f" — {dup_n} omitido(s) (URL/hash já visto)" if dup_n else ""
            log.info(f"    Página {page_num}: {len(imoveis_pagina)} imóveis extraídos{dup_suffix}")

            summary.pages_succeeded += 1

            n_page = len(imoveis_pagina)
            if n_page >= PAGINATION_FULL_PAGE_MIN:
                max_listing_peak = max(max_listing_peak, n_page)

            register_full_page_volume(cutoff_rs, n_page)
            typical_vol = compute_typical_page_volume(
                cutoff_rs, max_listing_peak, n_page
            )

            imoveis_pagina = normalizar_com_ia(imoveis_pagina)

            page_stats = upsert_imoveis(conn, imoveis_pagina, site["id"])
            _merge_identity_stats(summary, page_stats)
            _merge_data_quality_stats(summary, page_stats)
            for k in ["novos", "atualizados", "sem_mudanca"]:
                stats[k] += page_stats[k]
            stats["total"] += len(imoveis_pagina)
            hashes_vistos.update(im["hash"] for im in imoveis_pagina if im.get("hash"))
            urls_vistas.update(
                _norm_listing_url(im.get("url_anuncio"))
                for im in imoveis_pagina
                if im.get("url_anuncio")
            )
            hashes_sessao.update(im["hash"] for im in imoveis_pagina if im.get("hash"))

            summary.last_page_new_items = n_page
            summary.last_page_omitted_items = dup_n
            summary.typical_page_volume = round(typical_vol, 2)
            summary.pages_low_yield_count = cutoff_rs.pages_low_yield_count

            cutoff_decision = should_stop_pagination(
                n_bruto=n_bruto,
                n_new=n_page,
                dup_n=dup_n,
                page_num=page_num,
                max_listing_peak=max_listing_peak,
                running=cutoff_rs,
                baseline=None,
            )
            if cutoff_decision.should_stop:
                summary.pagination_cutoff_triggered = True
                summary.pagination_cutoff_reason = cutoff_decision.reason
                log.info(
                    "Paginação encerrada para %s: %s (pág. %s, novos=%s, pico=%s)",
                    site["name"],
                    cutoff_log_message(cutoff_decision.reason),
                    page_num,
                    n_page,
                    max_listing_peak,
                )
                break

            prev_deduped_count = n_page
            prev_had_more = has_more_results(html, len(imoveis_pagina))

            if not prev_had_more:
                log.info(f"    Sem mais resultados — encerrando paginação")
                break
            summary.next_page_attempts += 1
            next_raw = get_next_page_url(html, url_final, page_num + 1)
            guard = validate_next_page_url(
                url_final, next_raw, site["url"], pagination_seen
            )
            if not guard.ok:
                pagination_error = True
                summary.next_page_failures += 1
                summary.next_page_failure_reason = guard.code
                summary.pagination_stopped_reason = guard.code
                if guard.code in (CODE_REPEATED, CODE_SAME_PAGE):
                    summary.pagination_loop_detected = True
                if guard.code == CODE_EMPTY:
                    log.info(f"    Não encontrou URL de próxima página — encerrando")
                else:
                    _log_pagination_guard_failure(
                        site["name"], guard, page_num + 1
                    )
                break
            summary.next_page_successes += 1
            follow = guard.normalized_url or next_raw
            log.info(
                "Paginação segura: seguindo para página %s (%s)",
                page_num + 1,
                site["name"],
            )
            log.info(f"    Próxima página: {follow}")
            url = follow
            time.sleep(0.8)

        baseline_row = get_site_baseline(conn, site["id"])
        b_avail, bmin, bmax, hcnt = _unpack_baseline_for_resolve(baseline_row)
        extraction, status_source = resolve_final_extraction_status(
            had_request_error=had_request_error,
            had_render_error=had_render_error,
            pagination_error=pagination_error,
            queda_abrupta=queda_abrupta,
            page1_bruto_zero=page1_bruto_zero,
            page1_dedupe_zero_but_bruto_positive=page1_dedupe_zero_but_bruto_positive,
            listagem_invalida=listagem_invalida,
            volume_total=stats["total"],
            baseline_available=b_avail,
            baseline_expected_min=bmin,
            baseline_healthy_runs_count=hcnt,
        )
        _apply_extraction_anomalies_and_logs(
            site,
            summary,
            stats,
            extraction,
            status_source,
            baseline_row,
            queda_abrupta,
        )

        rm = apply_site_removals_with_guard(
            conn, site, hashes_vistos, extraction, stats["total"]
        )
        stats["removidos"] = rm["removidos"]
        stats["removals_blocked"] = rm["removals_blocked"]
        stats["removals_blocked_reason"] = rm["removals_blocked_reason"]
        stats["removed_count_attempted"] = rm["removed_count_attempted"]
        stats["removed_count_applied"] = rm["removed_count_applied"]

        summary.seal(t_mono)
        _persist_site_log(
            conn, agora, site, stats, summary, extraction, erro_msg_full, retry_context
        )
        log.info(
            "    ✓ %s total | +%s novos | ~%s atualizados | -%s removidos  | status=%s | id=%s",
            stats["total"],
            stats["novos"],
            stats["atualizados"],
            stats["removidos"],
            extraction.value,
            summary.identity_stats or {},
        )

    except Exception as e:
        et, em = format_exception(e)
        summary.error_type = et
        summary.error_message = em
        erro_msg_full = f"{et}: {em}"
        log.error("    ✗ ERRO [%s]: %s", et, em)
        extraction = (
            SiteExtractionStatus.ERRO_REQUISICAO
            if is_request_layer_error(e)
            else SiteExtractionStatus.ERRO_EXTRACAO
        )
        summary.seal(t_mono)
        _persist_site_log(
            conn, agora, site, stats, summary, extraction, erro_msg_full, retry_context
        )

    stats["extraction_status"] = extraction.value
    stats["site_summary"] = summary.to_dict()
    return stats


# ══════════════════════════════════════════════════════════════════════════════
# SYNC SUPABASE
# ══════════════════════════════════════════════════════════════════════════════

def _supabase_headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",  # upsert: atualiza se já existe
    }


def _sqlite_row_to_supabase(row, columns: list) -> dict:
    """Converte uma linha do SQLite para o formato esperado pelo Supabase."""
    d = dict(zip(columns, row))
    # Garante que booleanos/ints ficam corretos
    d["ativo"] = bool(d.get("ativo", 1))
    # Remove raw_json para não estourar o payload (opcional: comente se quiser manter)
    d.pop("raw_json", None)
    # Remove id local — o Supabase usa o próprio id ou o hash como chave
    d.pop("id", None)
    # Colunas só locais / filtro 8A — não assumimos que existam na tabela remota
    for _k in (
        "data_quality_score",
        "identity_quality",
        "identity_source",
        "identity_fallback",
    ):
        d.pop(_k, None)
    return d


def supabase_upsert_batch(rows: list[dict]) -> bool:
    """Envia um lote de imóveis para o Supabase via upsert REST.
    Retorna True se bem-sucedido."""
    if not rows:
        return True
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{SUPABASE_TABLE}?on_conflict=hash"
    try:
        resp = requests.post(
            url,
            headers=_supabase_headers(),
            json=rows,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True
        log.error(f"    Supabase erro {resp.status_code}: {resp.text[:300]}")
        return False
    except Exception as e:
        log.error(f"    Supabase exceção: {e}")
        return False


def supabase_marcar_removidos(hashes_ativos: list[str]) -> bool:
    """Marca como ativo=false no Supabase os imóveis que saíram do mercado.
    Estratégia: envia apenas os hashes ativos; o Supabase não tem EXCEPT nativo
    via REST, então fazemos um UPDATE com filtro de hash NOT IN via RPC, ou
    simplesmente pulamos — os upserts já atualizam ativo=True nos que existem.
    Para marcar removidos com precisão, use a função SQL abaixo no Supabase:

        UPDATE properties SET ativo = false
        WHERE ativo = true
          AND hash NOT IN (SELECT unnest($1::text[]));

    Chamada via RPC:
    """
    if not hashes_ativos:
        return True
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/rpc/marcar_imoveis_removidos"
    try:
        resp = requests.post(
            url,
            headers=_supabase_headers(),
            json={"hashes_ativos": hashes_ativos},
            timeout=30,
        )
        if resp.status_code in (200, 204):
            return True
        # RPC não existe ainda — apenas avisa, não quebra o fluxo
        if resp.status_code == 404:
            log.warning("    Supabase: RPC marcar_imoveis_removidos não encontrada (opcional — veja README)")
            return True
        log.warning(f"    Supabase RPC removidos: {resp.status_code} {resp.text[:200]}")
        return False
    except Exception as e:
        log.warning(f"    Supabase RPC removidos exceção: {e}")
        return False


def _empty_sync_filter_meta(
    *,
    active: bool | None = None,
    sync_scope: str | None = None,
) -> dict:
    """Metadados padrão do filtro 8A + 8B para retornos de ``sync_supabase``."""
    return {
        "sync_filter_active": bool(SYNC_FILTER_ENABLED if active is None else active),
        "sync_rows_total": 0,
        "sync_rows_sent": 0,
        "sync_rows_filtered": 0,
        "sync_filter_reasons": empty_reason_counts(),
        "sync_filter_by_site": {},
        "sync_scope": sync_scope,
        "sync_incremental_forced_reasons": [],
        "sync_new_count": 0,
        "sync_updated_count": 0,
        "sync_skipped_count": 0,
        "sync_removed_count": 0,
        "last_sync_at": None,
        "prev_last_successful_sync_at": None,
    }


def sync_supabase(
    conn,
    site_id: int | None = None,
    *,
    global_sync_safe: bool = True,
    governance_round_label: str = "SAFE",
    force_full_sync: bool = False,
) -> dict:
    """Lê imóveis do SQLite e sincroniza com o Supabase em lotes.

    Se ``site_id`` for informado, só essa imobiliária; caso contrário, todas.

    MINI-ETAPA 2B: com ``global_sync_safe=False`` (rodada RISKY), não chama o RPC global
    ``marcar_imoveis_removidos``; upserts seguem (modo seguro / parcial).

    MINI-ETAPA 8B: com ``SYNC_INCREMENTAL_ENABLED`` e checkpoint em ``supabase_sync_state``,
    envia só imóveis alterados desde o último sync bem-sucedido, salvo fallback (intervalo,
    RISKY, limiar de mudança, CLI ``force_full_sync``).

    Retorna stats incluindo ``sync_mode`` (FULL | INCREMENTAL | PARTIAL | BLOCKED) e
    ``sync_decision`` (full | incremental | partial | blocked).
    """
    round_unsafe = not global_sync_safe
    base_meta = {
        "sync_mode": "BLOCKED",
        "sync_decision": "blocked",
        "destructive_global_skipped": True,
        "round_unsafe": round_unsafe,
        "governance_round_label": governance_round_label,
    }

    if not SUPABASE_SYNC_ENABLED:
        log.warning(
            "SYNC BLOQUEADO: Supabase não configurado (SUPABASE_URL/SUPABASE_KEY ausentes)"
        )
        log.info("  Supabase sync desabilitado (SUPABASE_URL/SUPABASE_KEY não configurados)")
        return {
            **base_meta,
            "enviados": 0,
            "erros": 0,
            "lotes": 0,
            **_empty_sync_filter_meta(active=False, sync_scope=None),
        }

    migrate_supabase_sync_state(conn)
    prev_ok_iso, prev_full_iso = _read_supabase_sync_state(conn)

    log.info(f"\n{'─'*50}")
    if round_unsafe:
        log.warning(
            "GOVERNANÇA: rodada classificada como unsafe (RISKY) — sync destrutivo global será omitido se aplicável"
        )

    if site_id is not None:
        log.info(f"  Iniciando sync com Supabase (site_id={site_id})...")
    else:
        log.info("  Iniciando sync com Supabase (todos os sites)...")

    sql = """
        SELECT hash, site_id, site_name, titulo, tipo, finalidade,
               preco, preco_texto, area_m2, quartos, banheiros, vagas,
               bairro, cidade, endereco, descricao, url_anuncio, url_foto,
               codigo, ativo, primeira_vez, ultima_vez,
               data_quality_score, identity_quality, identity_source, identity_fallback
        FROM imoveis
    """
    if site_id:
        cursor = conn.execute(sql + " WHERE site_id = ? ORDER BY ultima_vez DESC", (site_id,))
    else:
        cursor = conn.execute(sql + " ORDER BY ultima_vez DESC")
    columns = [d[0] for d in cursor.description]
    rows = cursor.fetchall()

    if not rows:
        log.info("  Nenhum imóvel para sincronizar.")
        return {
            **base_meta,
            "enviados": 0,
            "erros": 0,
            "lotes": 0,
            "sync_mode": "FULL",
            "sync_decision": "full",
            "destructive_global_skipped": site_id is not None or round_unsafe,
            **_empty_sync_filter_meta(sync_scope="full"),
        }

    total = len(rows)
    if SYNC_FILTER_ENABLED:
        log.info(
            "  Sync filter ativo: score mínimo=%s, block_legacy=%s",
            SYNC_MIN_DATA_QUALITY_SCORE,
            SYNC_BLOCK_LEGACY_IDENTITY,
        )
    else:
        log.info("  Sync filter desligado (SYNC_FILTER_ENABLED=false) — enviando todos os registros.")

    forced_reasons: list[str] = []
    use_full = True
    incremental_allowed = bool(SYNC_INCREMENTAL_ENABLED) and not force_full_sync and not round_unsafe

    if not SYNC_INCREMENTAL_ENABLED:
        forced_reasons.append("SYNC_INCREMENTAL_ENABLED=false (sync completo)")
    elif force_full_sync:
        forced_reasons.append("--full-sync")
        use_full = True
    elif round_unsafe:
        forced_reasons.append("rodada RISKY — sync completo forçado (governança 8B)")
        use_full = True
    elif prev_ok_iso is None:
        forced_reasons.append("primeiro sync (sem checkpoint anterior)")
        use_full = True
    else:
        use_full = False

    if incremental_allowed and not use_full:
        anchor_iso = prev_full_iso or prev_ok_iso
        anchor_dt = _parse_iso_to_utc(anchor_iso)
        if anchor_dt is None:
            forced_reasons.append("checkpoint inválido — sync completo")
            use_full = True
        elif datetime.now(timezone.utc) - anchor_dt >= timedelta(
            hours=SYNC_FULL_RESYNC_INTERVAL_HOURS
        ):
            forced_reasons.append(
                f"intervalo de {SYNC_FULL_RESYNC_INTERVAL_HOURS}h desde último sync completo"
            )
            use_full = True

    eligible_dirty = 0
    eligible_total = 0
    if incremental_allowed and not use_full and prev_ok_iso:
        for r in rows:
            rowd = dict(zip(columns, r))
            why = classify_sync_filter_row(rowd)
            if why is not None:
                continue
            eligible_total += 1
            if _row_dirty_for_incremental(rowd, prev_ok_iso):
                eligible_dirty += 1
        if eligible_total > 0:
            pct = eligible_dirty * 100.0 / eligible_total
            if pct > float(SYNC_INCREMENTAL_FULL_THRESHOLD_PCT):
                forced_reasons.append(
                    f"mudança ampla: {eligible_dirty}/{eligible_total} ({pct:.1f}%) "
                    f"> {SYNC_INCREMENTAL_FULL_THRESHOLD_PCT}% → sync completo"
                )
                use_full = True

    sync_scope = "full" if use_full else "incremental"
    if forced_reasons and use_full and SYNC_INCREMENTAL_ENABLED and not force_full_sync:
        log.info("  Sync completo forçado: %s", "; ".join(forced_reasons))

    if sync_scope == "incremental":
        log.info(
            "  Modo incremental 8B ativo (checkpoint anterior %s) — só linhas com "
            "primeira_vez/ultima_vez ≥ checkpoint ou inativação recente",
            prev_ok_iso,
        )
    else:
        if SYNC_INCREMENTAL_ENABLED and not forced_reasons and not force_full_sync:
            pass
        elif not SYNC_INCREMENTAL_ENABLED:
            log.info("  Modo sync completo (incremental desligado por configuração).")
        elif forced_reasons:
            log.info("  Modo sync completo — %s", "; ".join(forced_reasons))

    log.info(
        "  %s imóveis no SQLite; filtrando e enviando em lotes de %s...",
        total,
        SUPABASE_BATCH_SIZE,
    )

    id_to_sname: dict[int, str] = {}
    for _r in rows:
        _d = dict(zip(columns, _r))
        _sid = int(_d.get("site_id") or 0)
        if _sid not in id_to_sname:
            id_to_sname[_sid] = str(_d.get("site_name") or "")

    hashes_ativos: list[str] = []
    for r in rows:
        rowd = dict(zip(columns, r))
        if int(rowd.get("ativo") or 0) != 1:
            continue
        if classify_sync_filter_row(rowd) is not None:
            continue
        h = rowd.get("hash")
        if h:
            hashes_ativos.append(str(h))

    enviados = 0
    erros = 0
    lotes = 0
    sync_filtered_total = 0
    sync_skipped = 0
    sync_new_count = 0
    sync_updated_count = 0
    sync_removed_count = 0
    reason_counts: dict[str, int] = empty_reason_counts()
    by_site: dict[int, dict] = {}

    def _bump_site(sid: int, field: str, reason: str | None = None) -> None:
        st = by_site.setdefault(
            sid,
            {"sent": 0, "filtered": 0, "skipped": 0, "reasons": empty_reason_counts()},
        )
        st[field] += 1
        if reason:
            st["reasons"][reason] = st["reasons"].get(reason, 0) + 1

    work_queue: list = []
    for r in rows:
        rowd = dict(zip(columns, r))
        sid = int(rowd.get("site_id") or 0)
        why = classify_sync_filter_row(rowd)
        if why is not None:
            sync_filtered_total += 1
            reason_counts[why] = reason_counts.get(why, 0) + 1
            _bump_site(sid, "filtered", why)
            continue
        send_row = use_full or (
            sync_scope == "incremental"
            and prev_ok_iso
            and _row_dirty_for_incremental(rowd, prev_ok_iso)
        )
        if not send_row:
            sync_skipped += 1
            _bump_site(sid, "skipped")
            continue
        work_queue.append(r)
        cls = _classify_sent_row_for_report(rowd, prev_ok_iso)
        if cls == "new":
            sync_new_count += 1
        elif cls == "removed":
            sync_removed_count += 1
        else:
            sync_updated_count += 1

    n_work = len(work_queue)
    if sync_scope == "incremental" and n_work == 0:
        log.info(
            "  Sync incremental: 0 mudanças detectadas — nada a enviar (ignorados por checkpoint: %s)",
            sync_skipped,
        )
    elif sync_scope == "incremental":
        log.info(
            "  Sync incremental: %s novos, %s atualizados, %s inativos a enviar, %s ignorados (sem mudança)",
            sync_new_count,
            sync_updated_count,
            sync_removed_count,
            sync_skipped,
        )

    for i in range(0, n_work, SUPABASE_BATCH_SIZE):
        batch_rows = work_queue[i : i + SUPABASE_BATCH_SIZE]
        batch_pass: list = []
        for r in batch_rows:
            rowd = dict(zip(columns, r))
            sid = int(rowd.get("site_id") or 0)
            batch_pass.append(r)
            _bump_site(sid, "sent")
        payload = [_sqlite_row_to_supabase(r, columns) for r in batch_pass]
        ok = supabase_upsert_batch(payload)
        lotes += 1
        if ok:
            enviados += len(payload)
            log.info(
                f"  Lote {lotes}: ✓ {len(payload)} imóveis enviados (acumulado enviados={enviados}, "
                f"filtrados={sync_filtered_total})"
            )
        else:
            erros += len(payload)
            log.error(f"  Lote {lotes}: ✗ erro ao enviar {len(payload)} imóveis")
        time.sleep(0.3)  # respeita rate limit do Supabase

    if SYNC_FILTER_ENABLED and sync_filtered_total:
        ld = reason_counts.get("low_data_quality", 0)
        li = reason_counts.get("low_identity_quality", 0)
        lg = reason_counts.get("legacy_identity", 0)
        log.info(
            "  Filtro de sync: %s enviados, %s bloqueados (%s low_data_quality, %s low_identity_quality, %s legacy_identity)",
            enviados,
            sync_filtered_total,
            ld,
            li,
            lg,
        )
        for sid, st in sorted(by_site.items()):
            fu = int(st.get("filtered") or 0)
            if fu <= 0:
                continue
            sname = id_to_sname.get(sid) or f"site_id={sid}"
            log.info(
                "  Site %s: %s imóvel(is) bloqueado(s) do sync por qualidade insuficiente",
                sname,
                fu,
            )

    sync_mode = "FULL"
    sync_decision = "full"
    destructive_skipped = site_id is not None
    now_checkpoint = datetime.now(timezone.utc).isoformat()

    if sync_scope == "incremental":
        sync_mode = "INCREMENTAL"
        sync_decision = "incremental"
    if site_id is None:
        if global_sync_safe:
            supabase_marcar_removidos(hashes_ativos)
            if sync_scope == "incremental":
                log.info(
                    "SYNC INCREMENTAL executado — upserts delta + RPC global de removidos (lista completa de ativos 8A)"
                )
            else:
                log.info(
                    "SYNC COMPLETO executado (rodada SAFE/PARTIAL) — upserts + RPC global de removidos"
                )
            destructive_skipped = False
        else:
            log.warning("SYNC BLOQUEADO: rodada classificada como RISKY")
            log.info("SYNC PARCIAL executado (modo seguro) — apenas upserts, sem RPC de removidos")
            sync_mode = "PARTIAL"
            sync_decision = "partial"
            destructive_skipped = True
    else:
        if global_sync_safe:
            log.info(
                "SYNC executado (rodada SAFE/PARTIAL, escopo: um site — RPC global não se aplica)"
            )
        else:
            log.info(
                "SYNC PARCIAL executado (modo seguro, escopo: um site — RPC global não se aplica neste modo)"
            )
            sync_mode = "PARTIAL"
            sync_decision = "partial"

    if erros == 0:
        _persist_supabase_sync_checkpoint(
            conn, now_iso=now_checkpoint, wrote_full_sync=(sync_scope == "full")
        )
        log.info("  Checkpoint 8B: last_successful_sync_at atualizado (%s)", now_checkpoint)
        if sync_scope == "full":
            log.info("  Checkpoint 8B: last_full_sync_at atualizado (sync completo)")
    else:
        log.warning(
            "  Checkpoint 8B NÃO atualizado (%s erro(s) em lotes — próxima rodada reenviará de forma conservadora)",
            erros,
        )

    log.info(
        "  Sync concluído: %s enviados | %s filtrados (8A) | %s ignorados (8B incremental) | %s erros | %s lotes",
        enviados,
        sync_filtered_total,
        sync_skipped,
        erros,
        lotes,
    )
    log.info(f"{'─'*50}\n")
    return {
        "enviados": enviados,
        "erros": erros,
        "lotes": lotes,
        "sync_mode": sync_mode,
        "sync_decision": sync_decision,
        "destructive_global_skipped": destructive_skipped,
        "round_unsafe": round_unsafe,
        "governance_round_label": governance_round_label,
        "sync_filter_active": bool(SYNC_FILTER_ENABLED),
        "sync_rows_total": total,
        "sync_rows_sent": enviados,
        "sync_rows_filtered": sync_filtered_total,
        "sync_filter_reasons": dict(reason_counts),
        "sync_filter_by_site": {k: dict(v) for k, v in by_site.items()},
        "sync_scope": sync_scope,
        "sync_incremental_forced_reasons": list(forced_reasons),
        "sync_new_count": sync_new_count,
        "sync_updated_count": sync_updated_count,
        "sync_skipped_count": sync_skipped,
        "sync_removed_count": sync_removed_count,
        "last_sync_at": now_checkpoint if erros == 0 else prev_ok_iso,
        "prev_last_successful_sync_at": prev_ok_iso,
    }


# ══════════════════════════════════════════════════════════════════════════════
# RELATÓRIO
# ══════════════════════════════════════════════════════════════════════════════

def print_report(conn):
    print("\n" + "═" * 60)
    print("  RELATÓRIO DO BANCO DE IMÓVEIS")
    print("═" * 60)

    total, ativos = conn.execute("SELECT COUNT(*), SUM(ativo) FROM imoveis").fetchone()
    print(f"  Total de imóveis cadastrados : {total}")
    print(f"  Ativos (no mercado)          : {ativos or 0}")
    print(f"  Saíram do mercado            : {(total or 0) - (ativos or 0)}")

    print("\n  Por tipo:")
    for row in conn.execute("SELECT tipo, COUNT(*) FROM imoveis WHERE ativo=1 GROUP BY tipo ORDER BY 2 DESC"):
        print(f"    {row[0]:<20} {row[1]}")

    print("\n  Por imobiliária (top 10):")
    for row in conn.execute("SELECT site_name, COUNT(*) FROM imoveis WHERE ativo=1 GROUP BY site_name ORDER BY 2 DESC LIMIT 10"):
        print(f"    {row[0]:<40} {row[1]}")

    print("\n  Última execução por site:")
    for row in conn.execute(
        """
        SELECT e.site_name, COALESCE(e.extraction_status, e.status), e.data, e.total_encontrados
        FROM log_execucoes e
        JOIN (SELECT site_id, MAX(id) AS mid FROM log_execucoes GROUP BY site_id) last
          ON e.id = last.mid
        ORDER BY e.data DESC
        LIMIT 10
        """
    ):
        print(f"    [{row[1]}] {row[0]:<35} {row[3]:>4} imóveis  {row[2][:16]}")

    print("═" * 60 + "\n")


def print_round_run_report(agg: dict) -> None:
    """Resumo agregado da rodada (status por site + saúde SAFE/PARTIAL/RISKY)."""
    w = 62
    print("\n" + "═" * w)
    print("  RELATÓRIO DA RODADA — diagnóstico agregado")
    print("═" * w)
    print(f"  Sites processados              : {agg['total_sites']}")
    print(f"  OK                             : {agg['total_ok']}")
    print(f"  Suspeitos (SUSPEITO_*)         : {agg['total_suspeitos']}")
    print(f"  Com erro (ERRO_*)              : {agg['total_erros']}")
    print(
        f"  Sync seguro (informativo)      : {agg['total_sites_sync_safe']} sites"
    )
    print(
        f"  Remoções — sites com política OK (removals_safe): {agg['total_sites_removals_safe']} sites"
    )
    print(
        f"  Remoções — aplicadas no SQLite (permitidas)      : {agg.get('total_sites_removals_permitted', 0)} sites"
    )
    print(
        f"  Remoções — bloqueadas por segurança              : {agg.get('total_sites_removals_blocked', 0)} sites"
    )
    tbc = int(agg.get("total_removals_blocked_safety_count") or 0)
    if tbc:
        print(
            f"  Total de inativações evitadas (estim.)         : {tbc} imóvel(is)"
        )
    srb = agg.get("sites_removals_blocked") or []
    if srb:
        print(f"\n  ── Sites com remoções bloqueadas ({len(srb)}) ──")
        for it in srb:
            print(
                f"     • {it['site']:<36} {it.get('removals_blocked_reason', it.get('status', '')):<28} "
                f"evitados≈{it.get('removed_count_attempted', 0)}"
            )
    print()
    rh = agg["round_health"]
    rh_desc = {
        "SAFE": "coleta consistente em todos os sites",
        "PARTIAL": "utilizável, com pontos de atenção",
        "RISKY": "muitas falhas ou suspeitas — revisar antes de confiar nos totais",
    }.get(rh, rh)
    print(f"  Classificação geral            : {rh} — {rh_desc}")

    rb = agg.get("retry_batch") or {}
    ra = int(rb.get("attempted") or 0)
    if ra > 0:
        rs = int(rb.get("succeeded") or 0)
        rbefore = rb.get("round_health_before_retry") or "—"
        print("\n  ── Retry automático (7B) ──")
        print(f"  Tentativas / recuperados (OK)  : {rs} / {ra}")
        print(f"  Saúde antes do retry           : {rbefore}")
        rec = rb.get("recovered_sites") or []
        if rec:
            print("  Sites que voltaram a OK        :")
            for it in rec:
                print(
                    f"     • {it.get('site', '?')}: {it.get('was')} → {it.get('now')} "
                    f"(vol={it.get('volume', 0)})"
                )
        if rb.get("round_health_before_retry") and rb.get("round_health_before_retry") != rh:
            print(f"  Saúde após retry               : {rh} (mudou)")

    gss = bool(agg.get("global_sync_safe", True))
    sm = agg.get("sync_mode")
    sd = agg.get("sync_decision")
    print("\n  ── Governança sync (Supabase) ──")
    print(f"  global_sync_safe               : {gss}")
    print(f"  round_unsafe (RISKY)           : {bool(agg.get('round_unsafe', False))}")
    print(f"  motivo (saúde da rodada)       : {agg.get('reason', rh)}")
    print(
        f"  sync_mode                      : {sm if sm is not None else '— (sync não executado ainda)'}"
    )
    print(
        f"  decisão                        : {sd if sd is not None else '— (sync não executado ainda)'}"
    )

    ss = agg.get("sync_scope")
    if ss:
        print("\n  ── Sync incremental (8B) ──")
        print(f"  sync_scope                     : {ss}")
        print(f"  novos / atualiz. / inativos    : {agg.get('sync_new_count', 0)} / "
              f"{agg.get('sync_updated_count', 0)} / {agg.get('sync_removed_count', 0)}")
        print(f"  ignorados (sem mudança)        : {agg.get('sync_skipped_count', 0)}")
        print(f"  last_sync_at (checkpoint)      : {agg.get('last_sync_at') or '—'}")
        fr = agg.get("sync_incremental_forced_reasons") or []
        if fr:
            print(f"  sync completo forçado (motivos): {'; '.join(fr)}")

    if "total_sync_sent" in agg:
        tsf = int(agg.get("total_sync_filtered") or 0)
        sfa = bool(agg.get("sync_filter_active"))
        print("\n  ── Filtro de qualidade no Supabase (8A) ──")
        print(f"  Filtro ativo                    : {sfa}")
        print(f"  Enviados ao Supabase           : {agg.get('total_sync_sent', 0)}")
        print(f"  Bloqueados (só SQLite)         : {tsf}")
        sfr = agg.get("sync_filter_reasons") or {}
        print(
            f"  Motivos                        : low_data_quality={sfr.get('low_data_quality', 0)}, "
            f"low_identity_quality={sfr.get('low_identity_quality', 0)}, "
            f"legacy_identity={sfr.get('legacy_identity', 0)}"
        )

    if agg["sites_erro"]:
        print(f"\n  ── Sites com erro ({len(agg['sites_erro'])}) ──")
        for it in agg["sites_erro"]:
            print(f"     • {it['site']:<38} {it['status']:<26} vol={it['volume_total']}")
    else:
        print("\n  ── Sites com erro: nenhum nesta rodada ──")

    if agg["sites_suspeitos"]:
        print(f"\n  ── Sites suspeitos ({len(agg['sites_suspeitos'])}) ──")
        for it in agg["sites_suspeitos"]:
            print(f"     • {it['site']:<38} {it['status']:<26} vol={it['volume_total']}")
    else:
        print("\n  ── Sites suspeitos: nenhum nesta rodada ──")

    sa = agg.get("sites_atencao") or {}
    if any(sa.get(k) for k in ("degrading", "consistently_broken", "flapping", "with_alerts")):
        print("\n  ── Sites em atenção (histórico recente no log) ──")
        print("      (tendência nas últimas execuções — não é o mesmo que erro só desta rodada)")
        sub = sa.get("degrading") or []
        if sub:
            print(f"\n      Padrão DEGRADING ({len(sub)}):")
            for it in sub:
                print(
                    f"         • {it['site_name']:<36} sucesso≈{it['success_rate']:.0%} "
                    f"vol_trend={it['volume_trend']}"
                )
        sub = sa.get("consistently_broken") or []
        if sub:
            print(f"\n      Padrão CONSISTENTLY_BROKEN ({len(sub)}):")
            for it in sub:
                print(
                    f"         • {it['site_name']:<36} sucesso≈{it['success_rate']:.0%} "
                    f"exec_consecutivas_erro={it.get('consecutive_errors', 0)}"
                )
        sub = sa.get("flapping") or []
        if sub:
            print(f"\n      Padrão FLAPPING — instabilidade OK/ERRO ({len(sub)}):")
            for it in sub:
                print(f"         • {it['site_name']:<36} sucesso≈{it['success_rate']:.0%}")
        sub = sa.get("with_alerts") or []
        if sub:
            print(f"\n      Com alertas históricos ativos ({len(sub)}):")
            for it in sub:
                codes = ", ".join(it.get("active_alerts") or [])
                print(f"         • {it['site_name']:<36} [{codes}]")
    else:
        print("\n  ── Sites em atenção (histórico): nenhum padrão preocupante na janela avaliada ──")

    obs = agg.get("observations") or []
    if obs:
        print("\n  ── Observações ──")
        for line in obs:
            print(f"     • {line}")
    print("═" * w + "\n")


def export_csv(conn, filename="imoveis_export.csv"):
    rows = conn.execute("""
        SELECT id, site_name, titulo, tipo, preco, preco_texto,
               area_m2, quartos, banheiros, vagas, bairro, cidade,
               url_anuncio, url_foto, ativo, primeira_vez, ultima_vez
        FROM imoveis ORDER BY primeira_vez DESC
    """).fetchall()

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id","imobiliaria","titulo","tipo","preco","preco_texto",
                         "area_m2","quartos","banheiros","vagas","bairro","cidade",
                         "url","foto","ativo","primeira_vez","ultima_vez"])
        writer.writerows(rows)
    print(f"✓ Exportado: {filename} ({len(rows)} imóveis)")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run_full_scrape_round(
    conn: sqlite3.Connection,
    *,
    sites: list[dict],
    site_id_filter: int | None = None,
    round_label: str | None = None,
    verbose: bool = True,
    force_supabase_full_sync: bool = False,
) -> dict[str, Any]:
    """
    Uma rodada completa (equivalente a ``py scraper.py`` sem flags extras).
    Usa lock em ``ROUND_LOCK_FILE`` para não executar em paralelo com outra rodada.
    ``force_supabase_full_sync``: quando True, ignora o modo incremental 8B e reenvia todos
    os registros elegíveis pelo filtro 8A.
    """
    lock = try_acquire_round_lock(ROUND_LOCK_FILE)
    if lock is None:
        log.warning(
            "Outra rodada parece estar em andamento (lock em %s). Não iniciando esta execução.",
            ROUND_LOCK_FILE,
        )
        return {
            "ok": False,
            "error_message": "round_lock_busy",
            "round_agg": None,
            "totais": {},
            "erros": 0,
            "elapsed_seconds": 0,
            "sync_stats": None,
            "retries_attempted": 0,
            "retries_succeeded": 0,
        }

    inicio = time.time()
    round_label = round_label or datetime.now().isoformat()
    totais: dict[str, int] = {"novos": 0, "atualizados": 0, "removidos": 0, "total": 0}
    erros = 0
    round_results: list[dict] = []
    round_agg: dict | None = None
    sync_stats: dict | None = None
    retries_attempted = 0
    retries_succeeded = 0

    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        if verbose:
            print(f"\n🏠 Iniciando varredura de {len(sites)} imobiliárias...")
            print(f"   {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")

        _contrib_keys = ("novos", "atualizados", "removidos", "total")
        round_health_before_retry: str | None = None
        retry_recovered: list[dict] = []

        for site in sites:
            stats = scrape_site(site, conn, session)
            for k in totais:
                totais[k] += int(stats.get(k, 0) or 0)
            if str(stats.get("extraction_status", "")).startswith("ERRO"):
                erros += 1
            summ = stats.get("site_summary") or {}
            round_results.append(
                {
                    "site_name": site["name"],
                    "site_id": site["id"],
                    "extraction_status": stats.get("extraction_status", "OK"),
                    "sync_safe": summ.get("sync_safe", False),
                    "removals_safe": summ.get("removals_safe", False),
                    "volume_total": stats.get("total", 0),
                    "removals_blocked": summ.get("removals_blocked", False),
                    "removals_blocked_reason": summ.get("removals_blocked_reason", ""),
                    "removed_count_attempted": summ.get("removed_count_attempted", 0),
                    "removed_count_applied": summ.get("removed_count_applied", 0),
                    "_stats_contrib": {
                        k: int(stats.get(k, 0) or 0) for k in _contrib_keys
                    },
                }
            )
            time.sleep(DELAY_BETWEEN_SITES)

        if RETRY_ENABLED and RETRY_MAX_SITES > 0 and sites:
            sites_by_id = {s["id"]: s for s in sites}
            candidates = pick_retry_candidates(
                round_results,
                sites_by_id,
                conn,
                eligible_statuses=set(RETRY_ERRORS_ELIGIBLE),
                max_sites=RETRY_MAX_SITES,
            )
            if candidates:
                round_health_before_retry = build_round_aggregate(round_results)[
                    "round_health"
                ]
                log.info(
                    "Retry: %s sites elegíveis identificados após rodada principal",
                    len(candidates),
                )
                if RETRY_DELAY_SECONDS > 0:
                    log.info(
                        "Aguardando %ss antes de iniciar retries...",
                        RETRY_DELAY_SECONDS,
                    )
                    time.sleep(RETRY_DELAY_SECONDS)
                for ri, c in enumerate(candidates, start=1):
                    site_d = c["site"]
                    orig_st = c["original_status"]
                    reason = c["reason"]
                    log.info(
                        "Retry [%s/%s]: %s (%s)",
                        ri,
                        len(candidates),
                        site_d["name"],
                        orig_st,
                    )
                    retries_attempted += 1
                    idx = next(
                        i
                        for i, r in enumerate(round_results)
                        if int(r.get("site_id") or 0) == site_d["id"]
                    )
                    old_row = round_results[idx]
                    old_c = old_row.get("_stats_contrib") or {}
                    for k in _contrib_keys:
                        totais[k] -= int(old_c.get(k, 0) or 0)
                    rctx = {
                        "is_retry": True,
                        "original_status": orig_st,
                        "reason": reason,
                    }
                    nstats = scrape_site(site_d, conn, session, retry_context=rctx)
                    for k in _contrib_keys:
                        totais[k] += int(nstats.get(k, 0) or 0)
                    new_st = str(nstats.get("extraction_status") or "OK")
                    nsumm = nstats.get("site_summary") or {}
                    round_results[idx] = {
                        "site_name": site_d["name"],
                        "site_id": site_d["id"],
                        "extraction_status": new_st,
                        "sync_safe": nsumm.get("sync_safe", False),
                        "removals_safe": nsumm.get("removals_safe", False),
                        "volume_total": nstats.get("total", 0),
                        "removals_blocked": nstats.get("removals_blocked", False),
                        "removals_blocked_reason": nstats.get(
                            "removals_blocked_reason", ""
                        ),
                        "removed_count_attempted": nstats.get(
                            "removed_count_attempted", 0
                        ),
                        "removed_count_applied": nstats.get("removed_count_applied", 0),
                        "_stats_contrib": {
                            k: int(nstats.get(k, 0) or 0) for k in _contrib_keys
                        },
                    }
                    if orig_st.startswith("ERRO") and new_st == "OK":
                        retries_succeeded += 1
                        retry_recovered.append(
                            {
                                "site": site_d["name"],
                                "was": orig_st,
                                "now": new_st,
                                "volume": int(nstats.get("total", 0) or 0),
                            }
                        )
                        log.info(
                            "Retry OK: %s recuperou com %s imóveis",
                            site_d["name"],
                            nstats.get("total", 0),
                        )
                    elif new_st.startswith("ERRO"):
                        log.info(
                            "Retry FALHOU: %s — mantendo %s",
                            site_d["name"],
                            new_st,
                        )
                    else:
                        log.info(
                            "Retry concluído: %s agora %s (vol=%s)",
                            site_d["name"],
                            new_st,
                            nstats.get("total", 0),
                        )
                    time.sleep(DELAY_BETWEEN_SITES)
                erros = sum(
                    1
                    for r in round_results
                    if str(r.get("extraction_status", "")).startswith("ERRO")
                )

        round_agg = build_round_aggregate(round_results)
        round_agg["retry_batch"] = {
            "attempted": retries_attempted,
            "succeeded": retries_succeeded,
            "recovered_sites": retry_recovered,
            "round_health_before_retry": round_health_before_retry,
        }
        if retries_attempted:
            obs = list(round_agg.get("observations") or [])
            obs.append(
                f"Retry 7B: {retries_succeeded}/{retries_attempted} site(s) passaram a OK "
                f"após falha transitória elegível."
            )
            round_agg["observations"] = obs
        if (
            round_health_before_retry
            and retries_attempted
            and round_agg.get("round_health") != round_health_before_retry
        ):
            log.info(
                "Resultado pós-retry: %s (era %s antes dos retries)",
                round_agg["round_health"],
                round_health_before_retry,
            )

        id_to_name = {s["id"]: s["name"] for s in sites}
        round_agg["sites_atencao"] = build_sites_atencao_section(
            conn, [s["id"] for s in sites], site_names=id_to_name
        )
        try:
            na = persist_health_alerts_for_round(
                conn, round_agg["sites_atencao"], round_label=round_label
            )
            if na:
                log.info("MINI-ETAPA 6B: gravados %s evento(s) em site_health_alert_events", na)
        except Exception as e:
            log.warning("MINI-ETAPA 6B: não foi possível persistir alertas históricos: %s", e)

        sync_stats = sync_supabase(
            conn,
            site_id=site_id_filter if site_id_filter else None,
            global_sync_safe=bool(round_agg.get("global_sync_safe", True)),
            governance_round_label=str(
                round_agg.get("reason") or round_agg.get("round_health", "SAFE")
            ),
            force_full_sync=bool(force_supabase_full_sync),
        )
        round_agg["sync_mode"] = sync_stats.get("sync_mode")
        round_agg["sync_decision"] = sync_stats.get("sync_decision")
        round_agg["sync_scope"] = sync_stats.get("sync_scope")
        round_agg["sync_incremental_forced_reasons"] = list(
            sync_stats.get("sync_incremental_forced_reasons") or []
        )
        round_agg["sync_new_count"] = int(sync_stats.get("sync_new_count") or 0)
        round_agg["sync_updated_count"] = int(sync_stats.get("sync_updated_count") or 0)
        round_agg["sync_skipped_count"] = int(sync_stats.get("sync_skipped_count") or 0)
        round_agg["sync_removed_count"] = int(sync_stats.get("sync_removed_count") or 0)
        round_agg["last_sync_at"] = sync_stats.get("last_sync_at")
        round_agg["prev_last_successful_sync_at"] = sync_stats.get(
            "prev_last_successful_sync_at"
        )
        round_agg["total_sync_sent"] = int(
            sync_stats.get("sync_rows_sent") or sync_stats.get("enviados") or 0
        )
        round_agg["total_sync_filtered"] = int(sync_stats.get("sync_rows_filtered") or 0)
        round_agg["sync_filter_active"] = bool(sync_stats.get("sync_filter_active"))
        round_agg["sync_filter_reasons"] = dict(
            sync_stats.get("sync_filter_reasons") or empty_reason_counts()
        )
        by_pf_raw = sync_stats.get("sync_filter_by_site") or {}
        by_pf = {int(k): v for k, v in by_pf_raw.items()}
        for r in round_results:
            sid = int(r.get("site_id") or 0)
            st = by_pf.get(sid) or {}
            r["sync_sent_count"] = int(st.get("sent") or 0)
            r["sync_filtered_count"] = int(st.get("filtered") or 0)
            r["sync_filter_reasons"] = dict(st.get("reasons") or empty_reason_counts())
        if int(round_agg["total_sync_filtered"]) > 0:
            obs = list(round_agg.get("observations") or [])
            obs.append(
                f"Sync 8A: {round_agg['total_sync_filtered']} imóvel(is) mantidos só no SQLite "
                f"(filtro de qualidade no Supabase)."
            )
            round_agg["observations"] = obs
        if str(round_agg.get("sync_scope") or "") == "incremental":
            obs = list(round_agg.get("observations") or [])
            obs.append(
                f"Sync 8B incremental: {round_agg['sync_new_count']} novos, "
                f"{round_agg['sync_updated_count']} atualizados, "
                f"{round_agg['sync_removed_count']} inativos enviados, "
                f"{round_agg['sync_skipped_count']} ignorados (sem mudança)."
            )
            round_agg["observations"] = obs

        elapsed = int(time.time() - inicio)
        if verbose:
            print(f"\n{'═'*50}")
            print(f"✅ Concluído em {elapsed//60}min {elapsed%60}s")
            print(
                f"   +{totais['novos']} novos  |  ~{totais['atualizados']} atualizados  |  "
                f"-{totais['removidos']} removidos (aplicados no SQLite)"
            )
            print(
                f"   Resumo por status: OK {round_agg['total_ok']}  |  "
                f"SUSPEITO {round_agg['total_suspeitos']}  |  ERRO {round_agg['total_erros']}  "
                f"(de {round_agg['total_sites']} sites)"
            )
            print(f"   Sites com erro (ERRO_*): {erros}/{len(sites)}")
            nbl = int(round_agg.get("total_sites_removals_blocked") or 0)
            nperm = int(round_agg.get("total_sites_removals_permitted") or 0)
            tev = int(round_agg.get("total_removals_blocked_safety_count") or 0)
            if nbl or tev:
                print(
                    f"   Remoções por site: {nperm} permitida(s)  |  {nbl} bloqueada(s) por segurança"
                    + (f"  (~{tev} inativações evitadas)" if tev else "")
                )
            print(
                f"   Governança sync: global_sync_safe={round_agg.get('global_sync_safe')}  |  "
                f"sync_mode={round_agg.get('sync_mode')}  |  escopo={round_agg.get('sync_scope')}  |  "
                f"decisão={round_agg.get('sync_decision')}"
            )
            print(f"{'═'*50}")

            print_round_run_report(round_agg)

            print_report(conn)

        return {
            "ok": True,
            "error_message": None,
            "round_agg": round_agg,
            "totais": totais,
            "erros": erros,
            "elapsed_seconds": elapsed,
            "sync_stats": sync_stats,
            "retries_attempted": retries_attempted,
            "retries_succeeded": retries_succeeded,
        }
    except Exception as e:
        log.exception("Rodada interrompida por exceção: %s", e)
        elapsed = int(time.time() - inicio)
        return {
            "ok": False,
            "error_message": f"{type(e).__qualname__}: {e}",
            "round_agg": round_agg,
            "totais": totais,
            "erros": erros,
            "elapsed_seconds": elapsed,
            "sync_stats": sync_stats,
            "retries_attempted": retries_attempted,
            "retries_succeeded": retries_succeeded,
        }
    finally:
        lock.release()


def main():
    # Força UTF-8 no console Windows (evita erro com acentos/emojis no cp1252)
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Robô Agregador de Imóveis")
    parser.add_argument("--site", type=int, help="Scrapa apenas o site com este ID")
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Força sync completo com Supabase (ignora modo incremental 8B nesta rodada)",
    )
    parser.add_argument("--report", action="store_true", help="Mostra relatório do banco")
    parser.add_argument("--export", action="store_true", help="Exporta CSV")
    parser.add_argument(
        "--profiles",
        action="store_true",
        help="Lista mapa host → extrator (JSON + SQLite + embutido) e encerra",
    )
    parser.add_argument(
        "--scheduler",
        action="store_true",
        help="Modo agendador: loop com horários em SCHEDULE_* (.env); requer SCHEDULE_ENABLED=true",
    )
    parser.add_argument(
        "--scheduler-status",
        action="store_true",
        help="Mostra estado do agendador e últimas rodadas registradas em scheduled_runs",
    )
    args = parser.parse_args()

    if args.scheduler:
        from scraper_scheduler import run_scheduler_main

        run_scheduler_main()
        return

    if args.scheduler_status:
        from scraper_scheduler import print_scheduler_status

        conn = sqlite3.connect(DB_FILE)
        init_db(conn)
        print_scheduler_status(Path(__file__).resolve().parent, conn)
        conn.close()
        return

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)
    refresh_site_profiles(conn)

    if args.profiles:
        print_site_profiles()
        conn.close()
        return

    if args.report:
        print_report(conn)
        conn.close()
        return

    if args.export:
        export_csv(conn)
        conn.close()
        return

    sites = SITES
    if args.site:
        sites = [s for s in SITES if s["id"] == args.site]
        if not sites:
            print(f"Site ID {args.site} não encontrado.")
            conn.close()
            return

    round_label = datetime.now().isoformat()
    result = run_full_scrape_round(
        conn,
        sites=sites,
        site_id_filter=args.site if args.site else None,
        round_label=round_label,
        verbose=True,
        force_supabase_full_sync=bool(getattr(args, "full_sync", False)),
    )
    if not result.get("ok") and result.get("error_message") == "round_lock_busy":
        print(
            "\n⚠ Outra rodada já está em execução (lock ativo). "
            "Aguarde término ou remova o lock só se tiver certeza de que não há processo rodando.\n"
        )
        sys.exit(2)
    conn.close()


if __name__ == "__main__":
    main()
