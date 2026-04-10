"""
Configuração sensível e overrides via variáveis de ambiente + arquivo .env na raiz do projeto.
Carregue este módulo cedo na execução (scraper.py importa antes do sync / IA).
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")


def _env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip()


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def parse_schedule_times(raw: str) -> list[tuple[int, int]]:
    """Lista ordenada de (hora, minuto) a partir de ``06:00,12:00,18:00``."""
    out: list[tuple[int, int]] = []
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            continue
        a, b = part.split(":", 1)
        try:
            h, m = int(a.strip()), int(b.strip())
        except ValueError:
            continue
        if 0 <= h <= 23 and 0 <= m <= 59:
            out.append((h, m))
    return sorted(set(out))


ANTHROPIC_API_KEY = _env_str("ANTHROPIC_API_KEY", "")

# MINI-ETAPA 7A — agendador (opcional)
SCHEDULE_ENABLED = _env_bool("SCHEDULE_ENABLED", False)
SCHEDULE_TIMES = parse_schedule_times(_env_str("SCHEDULE_TIMES", "06:00,12:00,18:00"))
SCHEDULE_TIMEZONE = _env_str("SCHEDULE_TIMEZONE", "America/Sao_Paulo") or "America/Sao_Paulo"
SCHEDULE_MAX_CONSECUTIVE_FAILURES = max(1, _env_int("SCHEDULE_MAX_CONSECUTIVE_FAILURES", 3))


def parse_retry_eligible_errors(raw: str) -> frozenset[str]:
    """Códigos ``ERRO_*`` permitidos para retry (MINI-ETAPA 7B)."""
    s: set[str] = set()
    for part in (raw or "").split(","):
        p = part.strip().upper().replace("-", "_")
        if p.startswith("ERRO_"):
            s.add(p)
    return frozenset(s)


# MINI-ETAPA 7B — retry por site na mesma rodada
RETRY_ENABLED = _env_bool("RETRY_ENABLED", True)
RETRY_MAX_SITES = max(0, _env_int("RETRY_MAX_SITES", 5))
RETRY_DELAY_SECONDS = max(0, _env_int("RETRY_DELAY_SECONDS", 60))
RETRY_ERRORS_ELIGIBLE = parse_retry_eligible_errors(
    _env_str("RETRY_ERRORS_ELIGIBLE", "ERRO_REQUISICAO,ERRO_RENDERIZACAO")
)

SUPABASE_URL = _env_str("SUPABASE_URL", "")
# service_role no painel; alias comum em tutoriais Supabase
SUPABASE_KEY = _env_str("SUPABASE_KEY", "") or _env_str("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_TABLE = _env_str("SUPABASE_TABLE", "properties") or "properties"
SUPABASE_BATCH_SIZE = _env_int("SUPABASE_BATCH_SIZE", 100)

SUPABASE_SYNC_ENABLED = bool(SUPABASE_URL and SUPABASE_KEY)
SUPABASE_CONFIG_INCOMPLETE = bool(SUPABASE_URL or SUPABASE_KEY) and not SUPABASE_SYNC_ENABLED

# MINI-ETAPA 8A — filtro de qualidade no sync (Supabase apenas; SQLite intacto)
SYNC_FILTER_ENABLED = _env_bool("SYNC_FILTER_ENABLED", True)
SYNC_MIN_DATA_QUALITY_SCORE = _env_int("SYNC_MIN_DATA_QUALITY_SCORE", 40)
SYNC_BLOCK_LEGACY_IDENTITY = _env_bool("SYNC_BLOCK_LEGACY_IDENTITY", True)

# MINI-ETAPA 8B — sync incremental (checkpoint em SQLite; fallback sync completo)
SYNC_INCREMENTAL_ENABLED = _env_bool("SYNC_INCREMENTAL_ENABLED", True)
SYNC_FULL_RESYNC_INTERVAL_HOURS = max(1, _env_int("SYNC_FULL_RESYNC_INTERVAL_HOURS", 24))
# Se a parcela de imóveis “sujos” (passíveis de sync) ≥ este %, forçar sync completo.
SYNC_INCREMENTAL_FULL_THRESHOLD_PCT = max(1, min(100, _env_int("SYNC_INCREMENTAL_FULL_THRESHOLD_PCT", 35)))

# API REST (Domus / consumo mobile) — api.py + uvicorn (obrigatória em produção; vazia = 401 nas rotas protegidas)
ROBOT_API_KEY = _env_str("ROBOT_API_KEY", "")
API_PORT = _env_int("API_PORT", 8000)
API_HOST = _env_str("API_HOST", "0.0.0.0") or "0.0.0.0"

# Playwright (sites com listagem via JS — scraper.py)
PLAYWRIGHT_ENABLED = _env_bool("PLAYWRIGHT_ENABLED", True)
PLAYWRIGHT_MAX_CONCURRENT = max(1, _env_int("PLAYWRIGHT_MAX_CONCURRENT", 1))
PLAYWRIGHT_TIMEOUT = max(5000, _env_int("PLAYWRIGHT_TIMEOUT", 30000))
