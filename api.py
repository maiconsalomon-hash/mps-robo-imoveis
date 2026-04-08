"""
API REST FastAPI para o app mobile Domus.

Inicia o uvicorn e, em thread em segundo plano, o scheduler de rodadas (mesma lógica que
``scraper.py --scheduler``), desde que ``SCHEDULE_ENABLED=true`` e ``SCHEDULE_TIMES`` válido.

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

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from config import API_HOST, API_PORT, ROBOT_API_KEY, SCHEDULE_ENABLED
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


def _listening_port() -> int:
    raw = os.environ.get("PORT")
    if raw and str(raw).strip().isdigit():
        return int(str(raw).strip())
    return int(API_PORT)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler_thread
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
    description="Leitura do SQLite local e disparo de coletas para o app Domus.",
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


def _row_as_dict(r: sqlite3.Row) -> dict[str, Any]:
    return {k: r[k] for k in r.keys()}


@app.get("/health")
def health() -> dict[str, Any]:
    st = read_scheduler_state(ROOT) or {}
    alive = bool(_scheduler_thread and _scheduler_thread.is_alive())
    return {
        "status": "ok",
        "scheduler_active": alive and SCHEDULE_ENABLED,
        "last_round_health": st.get("last_round_health") or "UNKNOWN",
    }


@app.get("/api/v1/properties", dependencies=[Depends(require_api_key)])
def list_properties(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    neighborhood: str | None = Query(None, description="Filtra bairro (contém, case-insensitive)"),
    property_type: str | None = Query(None, alias="type", description="Filtra tipo de imóvel (contém)"),
    min_price: float | None = Query(None, ge=0),
    max_price: float | None = Query(None, ge=0),
) -> dict[str, Any]:
    where = ["ativo = 1"]
    args: list[Any] = []

    if neighborhood and neighborhood.strip():
        where.append("LOWER(bairro) LIKE LOWER(?)")
        args.append(f"%{neighborhood.strip()}%")
    if property_type and property_type.strip():
        where.append("LOWER(tipo) LIKE LOWER(?)")
        args.append(f"%{property_type.strip()}%")
    if min_price is not None:
        where.append("(preco IS NOT NULL AND preco >= ?)")
        args.append(min_price)
    if max_price is not None:
        where.append("(preco IS NOT NULL AND preco <= ?)")
        args.append(max_price)

    sql_where = " AND ".join(where)
    conn = get_db()
    try:
        cur = conn.execute(
            f"SELECT COUNT(*) FROM imoveis WHERE {sql_where}",
            args,
        )
        total = int(cur.fetchone()[0])
        cur = conn.execute(
            f"""
            SELECT id, hash, titulo, preco, preco_texto, tipo, bairro, cidade, area_m2,
                   quartos, banheiros, vagas, url_anuncio, url_foto, site_name, site_id,
                   primeira_vez, ultima_vez, data_quality_level, identity_source
            FROM imoveis
            WHERE {sql_where}
            ORDER BY ultima_vez DESC
            LIMIT ? OFFSET ?
            """,
            [*args, limit, offset],
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    items = []
    for r in rows:
        d = _row_as_dict(r)
        d["area"] = d.pop("area_m2", None)
        items.append(d)

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
