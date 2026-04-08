"""
API REST FastAPI para o app mobile Domus.

Inicia o uvicorn e, em thread em segundo plano, o scheduler de rodadas (mesma lógica que
``scraper.py --scheduler``), desde que ``SCHEDULE_ENABLED=true`` e ``SCHEDULE_TIMES`` válido.

Leitura de imóveis: ``GET /api/v1/properties`` usa Supabase (tabela ``properties``) como fonte
de verdade. SQLite permanece para coleta, logs e metadados operacionais do scraper.

Uso local: ``python api.py`` ou ``uvicorn api:app --host 0.0.0.0 --port 8000``
Railway: defina ``PORT`` (a API usa ``PORT`` se existir, senão ``API_PORT``).
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import requests
import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from config import (
    API_HOST,
    API_PORT,
    ROBOT_API_KEY,
    SCHEDULE_ENABLED,
    SUPABASE_KEY,
    SUPABASE_SYNC_ENABLED,
    SUPABASE_TABLE,
    SUPABASE_URL,
)
from scraper import DB_FILE, SITES, init_db, refresh_site_profiles, run_full_scrape_round
from scraper_scheduler import read_scheduler_state, run_scheduler_worker

ROOT = Path(__file__).resolve().parent

log = logging.getLogger("domus_api")
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

_scheduler_thread: threading.Thread | None = None

# Colunas usadas na listagem (evita SELECT * em ~15k linhas). identity_source não é enviada pelo sync atual.
_PROPERTIES_SELECT = (
    "id,hash,titulo,preco,preco_texto,tipo,bairro,cidade,area_m2,quartos,banheiros,vagas,"
    "url_anuncio,url_foto,site_name,site_id,primeira_vez,ultima_vez,data_quality_level"
)


def _pg_pattern_safe(fragment: str) -> str:
    """Evita quebrar a gramática ``and=(...)`` do PostgREST (wildcards ``*`` e vírgulas)."""
    return (
        fragment.replace("*", "")
        .replace(",", " ")
        .replace("(", " ")
        .replace(")", " ")
        .strip()
    )


def _ilike_pattern(user_text: str) -> str:
    """Monta trecho seguro para ``ilike.*…*``; espaços viram ``*`` (evita token quebrado no ``and=``)."""
    frag = _pg_pattern_safe(user_text)
    if not frag:
        return ""
    return "*".join(p for p in frag.split() if p)


def _parse_content_range_total(header_val: str | None) -> int | None:
    """Extrai o total a partir de ``Content-Range`` (ex.: ``0-9/15000`` ou ``*/15000``)."""
    if not header_val:
        return None
    h = header_val.strip()
    if "/" not in h:
        return None
    right = h.split("/")[-1].strip()
    if right in ("*", ""):
        return None
    try:
        return int(right)
    except ValueError:
        return None


def _format_price_filter(n: float) -> str:
    """Evita notação científica e ambiguidade de pontos em ``preco.gte.*``."""
    if n == int(n):
        return str(int(n))
    return format(n, "f").rstrip("0").rstrip(".")


class SupabasePropertiesRest:
    """Cliente PostgREST para a tabela ``properties`` (mesma URL/chave que o sync no scraper)."""

    def __init__(self, url: str, key: str, table: str) -> None:
        self._url = f"{url.rstrip('/')}/rest/v1/{table}"
        self._session = requests.Session()
        self._session.headers.update(
            {
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self._timeout = (15, 90)

    def _and_clause(
        self,
        *,
        neighborhood: str | None,
        property_type: str | None,
        min_price: float | None,
        max_price: float | None,
    ) -> str:
        parts = ["or(ativo.eq.true,ativo.eq.1)"]
        if neighborhood and neighborhood.strip():
            pat = _ilike_pattern(neighborhood.strip())
            if pat:
                parts.append(f"bairro.ilike.*{pat}*")
        if property_type and property_type.strip():
            pat = _ilike_pattern(property_type.strip())
            if pat:
                parts.append(f"tipo.ilike.*{pat}*")
        if min_price is not None:
            parts.append(f"preco.gte.{_format_price_filter(min_price)}")
        if max_price is not None:
            parts.append(f"preco.lte.{_format_price_filter(max_price)}")
        return f"({','.join(parts)})"

    def count_matching(
        self,
        *,
        neighborhood: str | None,
        property_type: str | None,
        min_price: float | None,
        max_price: float | None,
    ) -> int:
        params: dict[str, str] = {
            "select": "hash",
            "and": self._and_clause(
                neighborhood=neighborhood,
                property_type=property_type,
                min_price=min_price,
                max_price=max_price,
            ),
        }
        headers = {**self._session.headers, "Prefer": "count=exact"}
        r = self._session.head(self._url, params=params, headers=headers, timeout=self._timeout)
        r.raise_for_status()
        total = _parse_content_range_total(r.headers.get("Content-Range"))
        if total is not None:
            return total
        r2 = self._session.get(
            self._url,
            params={**params, "limit": "1"},
            headers=headers,
            timeout=self._timeout,
        )
        r2.raise_for_status()
        total2 = _parse_content_range_total(r2.headers.get("Content-Range"))
        return int(total2) if total2 is not None else 0

    def fetch_page(
        self,
        *,
        limit: int,
        offset: int,
        neighborhood: str | None,
        property_type: str | None,
        min_price: float | None,
        max_price: float | None,
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {
            "select": _PROPERTIES_SELECT,
            "and": self._and_clause(
                neighborhood=neighborhood,
                property_type=property_type,
                min_price=min_price,
                max_price=max_price,
            ),
            "order": "ultima_vez.desc",
            "limit": str(limit),
            "offset": str(offset),
        }
        r = self._session.get(self._url, params=params, timeout=self._timeout)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []


def _listening_port() -> int:
    raw = os.environ.get("PORT")
    if raw and str(raw).strip().isdigit():
        return int(str(raw).strip())
    return int(API_PORT)


def _init_supabase_properties_client() -> SupabasePropertiesRest | None:
    if not SUPABASE_SYNC_ENABLED:
        log.warning(
            "SUPABASE_URL/SUPABASE_KEY ausentes — GET /api/v1/properties responderá 503 até configurar."
        )
        return None
    try:
        cli = SupabasePropertiesRest(SUPABASE_URL.strip(), SUPABASE_KEY, SUPABASE_TABLE)
        log.info("Cliente PostgREST (Supabase) inicializado para a API (tabela %s).", SUPABASE_TABLE)
        return cli
    except Exception:
        log.exception("Falha ao preparar cliente Supabase/PostgREST para a API")
        return None


def _count_active_properties(client: SupabasePropertiesRest | None) -> int | None:
    if client is None:
        return None
    try:
        return client.count_matching(
            neighborhood=None,
            property_type=None,
            min_price=None,
            max_price=None,
        )
    except Exception:
        log.exception("Falha ao contar imóveis ativos no Supabase")
        return None


def _map_supabase_row_to_api_item(row: dict[str, Any]) -> dict[str, Any]:
    """Mesmo formato que o endpoint SQLite antigo + ultima_coleta (alias de ultima_vez)."""
    preco = row.get("preco")
    ultima = row.get("ultima_vez")
    out: dict[str, Any] = {
        "id": row.get("id"),
        "hash": row.get("hash"),
        "titulo": row.get("titulo"),
        "preco": preco,
        "preco_numeric": preco,
        "preco_texto": row.get("preco_texto"),
        "tipo": row.get("tipo"),
        "bairro": row.get("bairro"),
        "cidade": row.get("cidade"),
        "quartos": row.get("quartos"),
        "banheiros": row.get("banheiros"),
        "vagas": row.get("vagas"),
        "url_anuncio": row.get("url_anuncio"),
        "url_foto": row.get("url_foto"),
        "site_name": row.get("site_name"),
        "site_id": row.get("site_id"),
        "primeira_vez": row.get("primeira_vez"),
        "ultima_vez": ultima,
        "ultima_coleta": ultima,
        "data_quality_level": row.get("data_quality_level"),
        "identity_source": row.get("identity_source"),
        "area": row.get("area_m2"),
    }
    return out


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler_thread
    app.state.supabase_properties = _init_supabase_properties_client()

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)
    refresh_site_profiles(conn)
    conn.close()

    def _scheduler_target() -> None:
        try:
            run_scheduler_worker(install_signals=False)
        except Exception:
            log.exception("Thread do scheduler encerrou com erro")

    if SCHEDULE_ENABLED:
        _scheduler_thread = threading.Thread(
            target=_scheduler_target,
            name="domus-scheduler",
            daemon=True,
        )
        _scheduler_thread.start()
        log.info("Scheduler iniciado em thread em segundo plano.")
    else:
        log.warning("SCHEDULE_ENABLED=false — apenas a API HTTP está ativa (sem agendador).")

    yield


app = FastAPI(
    title="Robô Imobiliário API",
    description="Listagem de imóveis via Supabase; SQLite para coleta, logs e disparo de rodadas.",
    version="1.0.0",
    lifespan=lifespan,
)


def require_api_key(
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    if not x_api_key or x_api_key != ROBOT_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key",
        )


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/health")
def health(request: Request) -> dict[str, Any]:
    st = read_scheduler_state(ROOT) or {}
    alive = bool(_scheduler_thread and _scheduler_thread.is_alive())
    client: SupabasePropertiesRest | None = getattr(request.app.state, "supabase_properties", None)
    supabase_properties_count = _count_active_properties(client)
    return {
        "status": "ok",
        "scheduler_active": alive and SCHEDULE_ENABLED,
        "last_round_health": st.get("last_round_health") or "UNKNOWN",
        "supabase_properties_count": supabase_properties_count,
    }


@app.get("/api/v1/properties", dependencies=[Depends(require_api_key)])
def list_properties(
    request: Request,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    neighborhood: str | None = Query(None, description="Filtra bairro (contém, case-insensitive)"),
    property_type: str | None = Query(None, alias="type", description="Filtra tipo de imóvel (contém)"),
    min_price: float | None = Query(None, ge=0),
    max_price: float | None = Query(None, ge=0),
) -> dict[str, Any]:
    client: SupabasePropertiesRest | None = getattr(request.app.state, "supabase_properties", None)
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Supabase não configurado ou indisponível. Defina SUPABASE_URL e SUPABASE_KEY "
                "no ambiente e reinicie a API."
            ),
        )

    try:
        total = client.count_matching(
            neighborhood=neighborhood,
            property_type=property_type,
            min_price=min_price,
            max_price=max_price,
        )
        rows = client.fetch_page(
            limit=limit,
            offset=offset,
            neighborhood=neighborhood,
            property_type=property_type,
            min_price=min_price,
            max_price=max_price,
        )
    except requests.RequestException as e:
        log.exception("Erro HTTP ao consultar imóveis no Supabase")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Falha ao consultar Supabase (rede ou serviço indisponível).",
        ) from e
    except Exception as e:
        log.exception("Erro ao consultar imóveis no Supabase")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Falha ao consultar Supabase: {type(e).__name__}",
        ) from e

    items = [_map_supabase_row_to_api_item(dict(r)) for r in rows]
    return {"total": total, "limit": limit, "offset": offset, "items": items}


def _sources_last_runs_map(conn: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT e.site_id, e.site_name, COALESCE(e.extraction_status, e.status) AS st,
               e.data, e.total_encontrados
        FROM log_execucoes e
        INNER JOIN (
            SELECT site_id, MAX(id) AS mid FROM log_execucoes GROUP BY site_id
        ) last ON e.id = last.mid
        """
    )
    out: dict[int, dict[str, Any]] = {}
    for row in cur.fetchall():
        sid = int(row[0] or 0)
        out[sid] = {
            "site_name": row[1],
            "status": row[2],
            "last_run_at": row[3],
            "volume_total": int(row[4] or 0),
        }
    return out


@app.get("/api/v1/sources", dependencies=[Depends(require_api_key)])
def list_sources() -> dict[str, Any]:
    conn = get_db()
    try:
        last_by_site = _sources_last_runs_map(conn)
        items = []
        for site in SITES:
            sid = int(site["id"])
            meta = last_by_site.get(sid, {})
            cur = conn.execute(
                "SELECT COUNT(*) FROM imoveis WHERE site_id = ? AND ativo = 1",
                (sid,),
            )
            active_count = int(cur.fetchone()[0])
            extraction_status = str(meta.get("status") or "never")
            items.append(
                {
                    "id": sid,
                    "nome": site["name"],
                    "url": site.get("url") or "",
                    "status": extraction_status,
                    "active_properties_count": active_count,
                    "extraction_status": extraction_status,
                    "last_run_at": meta.get("last_run_at"),
                    "volume_total": meta.get("volume_total", 0),
                }
            )
    finally:
        conn.close()
    return {"items": items, "count": len(items)}


def _site_by_id(site_id: int) -> dict | None:
    for s in SITES:
        if int(s["id"]) == site_id:
            return s
    return None


def _run_scrape_background(*, site_id: int | None, enrich_details: bool = False) -> None:
    """Dispara ``run_full_scrape_round`` numa thread; respeita o lock global da rodada."""

    def job() -> None:
        conn = None
        try:
            conn = sqlite3.connect(DB_FILE, check_same_thread=False)
            init_db(conn)
            refresh_site_profiles(conn)
            if enrich_details:
                log.info("API: enrich_details=true (reservado; coleta padrão sem enrichment extra)")
            sites = SITES
            filt: int | None = None
            if site_id is not None:
                sites = [s for s in SITES if int(s["id"]) == site_id]
                filt = site_id
                if not sites:
                    log.warning("API: site_id %s não encontrado na lista SITES", site_id)
                    return
            run_full_scrape_round(
                conn,
                sites=sites,
                site_id_filter=filt,
                round_label=datetime.now().isoformat(),
                verbose=False,
            )
        except Exception:
            log.exception("Coleta em background (API) falhou")
        finally:
            if conn:
                conn.close()

    threading.Thread(target=job, name="api-scrape", daemon=True).start()


@app.post("/api/v1/sources/run-all", dependencies=[Depends(require_api_key)])
def trigger_run_all() -> JSONResponse:
    _run_scrape_background(site_id=None, enrich_details=False)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"message": "Coleta de todas as fontes aceita; execução em background.", "scope": "all"},
    )


class RunSourceBody(BaseModel):
    enrich_details: bool = Field(False, description="Reservado para evolução futura da coleta")


@app.post(
    "/api/v1/sources/{site_id}/run",
    dependencies=[Depends(require_api_key)],
    status_code=status.HTTP_202_ACCEPTED,
)
def trigger_run_one(site_id: int, body: RunSourceBody = RunSourceBody()) -> dict[str, Any]:
    if _site_by_id(site_id) is None:
        raise HTTPException(status_code=404, detail=f"Fonte id={site_id} não encontrada")
    enrich = body.enrich_details
    _run_scrape_background(site_id=site_id, enrich_details=enrich)
    return {
        "message": "Coleta aceita; execução em background.",
        "site_id": site_id,
        "enrich_details": enrich,
    }


@app.get("/api/v1/sources/{site_id}/run-status", dependencies=[Depends(require_api_key)])
def get_run_status(site_id: int) -> dict[str, Any]:
    site = _site_by_id(site_id)
    if site is None:
        raise HTTPException(status_code=404, detail=f"Fonte id={site_id} não encontrada")
    conn = get_db()
    try:
        cur = conn.execute(
            """
            SELECT COALESCE(e.extraction_status, e.status), e.data, e.total_encontrados
            FROM log_execucoes e
            WHERE e.site_id = ?
            ORDER BY e.id DESC
            LIMIT 1
            """,
            (site_id,),
        )
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return {
            "site_id": site_id,
            "site_name": site["name"],
            "extraction_status": None,
            "volume_total": 0,
            "last_run_at": None,
        }
    return {
        "site_id": site_id,
        "site_name": site["name"],
        "extraction_status": row[0],
        "volume_total": int(row[2] or 0),
        "last_run_at": row[1],
    }


if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=_listening_port(), log_level="info")
