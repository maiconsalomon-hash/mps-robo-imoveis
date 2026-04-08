"""
MINI-ETAPA 7A — agendador de rodadas (sem Celery/Redis).

Uso: ``py scraper.py --scheduler`` com variáveis SCHEDULE_* no .env.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from datetime import tzinfo as DtTzInfo
from pathlib import Path
from typing import Any

from config import (
    SCHEDULE_ENABLED,
    SCHEDULE_MAX_CONSECUTIVE_FAILURES,
    SCHEDULE_TIMES,
    SCHEDULE_TIMEZONE,
)
from round_lock import try_acquire_round_lock

log = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore[misc, assignment]


def _resolve_tz() -> DtTzInfo:
    if ZoneInfo is None:
        log.warning("zoneinfo indisponível; usando UTC.")
        return timezone.utc
    try:
        return ZoneInfo(SCHEDULE_TIMEZONE)
    except Exception:
        log.warning(
            "Timezone %r inválido ou sem base IANA (instale tzdata). Usando UTC.",
            SCHEDULE_TIMEZONE,
        )
        return timezone.utc


def _next_fire_after(times: list[tuple[int, int]], tz: DtTzInfo) -> datetime:
    now = datetime.now(tz)
    today = now.date()
    for h, m in times:
        cand = datetime(today.year, today.month, today.day, h, m, 0, 0, tzinfo=tz)
        if cand > now:
            return cand
    h0, m0 = times[0]
    nxt = today + timedelta(days=1)
    return datetime(nxt.year, nxt.month, nxt.day, h0, m0, 0, 0, tzinfo=tz)


def _fmt_hm_local(dt: datetime) -> str:
    return dt.strftime("%H:%M")


def _fmt_countdown(sec: float) -> str:
    if sec < 90:
        return f"{int(sec)}s"
    if sec < 3600:
        return f"{int(sec // 60)}min"
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    return f"{h}h {m}min"


def _fmt_duration(sec: int) -> str:
    m, s = sec // 60, sec % 60
    return f"{m}m{s:02d}s"


def _next_fire_label(next_dt: datetime, tz: DtTzInfo) -> str:
    now = datetime.now(tz)
    hm = _fmt_hm_local(next_dt)
    if next_dt.date() > now.date():
        return f"{hm} amanhã"
    return hm


def insert_scheduled_run(
    conn: sqlite3.Connection,
    *,
    scheduled_at: str,
    started_at: str,
    finished_at: str,
    round_health: str,
    sites_ok: int,
    sites_suspeitos: int,
    sites_erros: int,
    triggered_by: str,
    notes: str | None = None,
    retries_attempted: int = 0,
    retries_succeeded: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO scheduled_runs (
            scheduled_at, started_at, finished_at, round_health,
            sites_ok, sites_suspeitos, sites_erros, triggered_by, notes,
            retries_attempted, retries_succeeded
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            scheduled_at,
            started_at,
            finished_at,
            round_health,
            sites_ok,
            sites_suspeitos,
            sites_erros,
            triggered_by,
            notes or "",
            int(retries_attempted),
            int(retries_succeeded),
        ),
    )
    conn.commit()


def write_scheduler_state(root: Path, data: dict[str, Any]) -> None:
    p = root / "scheduler_state.json"
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def read_scheduler_state(root: Path) -> dict[str, Any] | None:
    p = root / "scheduler_state.json"
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def process_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, ValueError):
        return False
    return True


def print_scheduler_status(root: Path, conn: sqlite3.Connection) -> None:
    """Saída do comando ``--scheduler-status``."""
    tz = _resolve_tz()
    state = read_scheduler_state(root) or {}
    pid = int(state.get("pid") or 0)
    alive = process_exists(pid)
    print("\n" + "═" * 58)
    print("  STATUS DO SCHEDULER (MINI-ETAPA 7A)")
    print("═" * 58)
    print(f"  Processo agendador (PID gravado) : {pid or '—'}")
    print(f"  PID ativo no SO                   : {'sim' if alive else 'não'}")
    if state.get("started_at"):
        print(f"  Último registro de início         : {state.get('started_at')}")
    if state.get("next_round_at"):
        print(f"  Próxima rodada (último cálculo)   : {state.get('next_round_at')}")
    if state.get("last_round_finished_at"):
        print(f"  Última rodada terminou em         : {state.get('last_round_finished_at')}")
    if state.get("last_round_health"):
        print(f"  Última saúde (SAFE/…)             : {state.get('last_round_health')}")
    cf = int(state.get("consecutive_failures") or 0)
    if cf:
        print(f"  Falhas consecutivas registradas   : {cf}")
    if state.get("paused"):
        print(f"  PAUSADO                           : {state.get('pause_reason', '')}")

    rows = conn.execute(
        """
        SELECT scheduled_at, started_at, finished_at, round_health,
               sites_ok, sites_suspeitos, sites_erros, triggered_by, notes,
               COALESCE(retries_attempted, 0), COALESCE(retries_succeeded, 0)
        FROM scheduled_runs
        ORDER BY id DESC
        LIMIT 5
        """
    ).fetchall()

    print(f"\n  Horário local do status           : {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("\n  ── Últimas 5 rodadas em scheduled_runs ──")
    if not rows:
        print("     (nenhum registro)")
    else:
        for r in rows:
            ra, rs = int(r[9] or 0), int(r[10] or 0)
            rex = f" | retry {rs}/{ra}" if ra else ""
            print(
                f"     • {r[1] or '?'} → {r[2] or '?'} | {r[3] or '?'} | "
                f"OK={r[4]} sus={r[5]} err={r[6]} | {r[7]}{rex}"
            )
            if r[8]:
                note = str(r[8])
                print(f"       nota: {note[:120]}{'…' if len(note) > 120 else ''}")
    if SCHEDULE_ENABLED and SCHEDULE_TIMES:
        nf = _next_fire_after(SCHEDULE_TIMES, tz)
        print(f"\n  Próximo horário configurado       : {_fmt_hm_local(nf)} ({SCHEDULE_TIMEZONE})")
    print("═" * 58 + "\n")


def _setup_stdio_utf8() -> None:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def _scheduler_forever_loop(
    conn: sqlite3.Connection,
    root: Path,
    round_state: dict[str, Any],
    tz: DtTzInfo,
    *,
    run_full_scrape_round: Any,
    sites: list[dict],
) -> None:
    """Loop de agendamento até ``immediate_exit``, ``graceful_stop`` ou pausa por falhas."""
    while True:
        if round_state["immediate_exit"]:
            log.info("Shutdown imediato (sem rodada).")
            break

        if round_state["failures"] >= SCHEDULE_MAX_CONSECUTIVE_FAILURES:
            log.error(
                "SCHEDULER PAUSADO: %s falhas consecutivas. Verificação manual necessária.",
                round_state["failures"],
            )
            cur = read_scheduler_state(root) or {}
            cur.update(
                {
                    "pid": os.getpid(),
                    "paused": True,
                    "pause_reason": "max_consecutive_failures",
                    "consecutive_failures": round_state["failures"],
                }
            )
            write_scheduler_state(root, cur)
            break

        next_dt = _next_fire_after(SCHEDULE_TIMES, tz)
        cur = read_scheduler_state(root) or {}
        cur.update(
            {
                "pid": os.getpid(),
                "scheduler_started_at": cur.get("scheduler_started_at")
                or datetime.now(timezone.utc).isoformat(),
                "next_round_at": next_dt.isoformat(),
                "consecutive_failures": round_state["failures"],
                "paused": False,
                "timezone": SCHEDULE_TIMEZONE,
            }
        )
        write_scheduler_state(root, cur)

        while True:
            if round_state["immediate_exit"]:
                break
            if round_state["graceful_stop"] and not round_state["round_running"]:
                break
            now = datetime.now(tz)
            remaining = (next_dt - now).total_seconds()
            if remaining <= 0:
                break
            time.sleep(min(1.0, remaining))

        if round_state["immediate_exit"]:
            break
        if round_state["graceful_stop"] and not round_state["round_running"]:
            log.info("Encerramento após espera: scheduler finalizado.")
            break

        slot_label = next_dt.isoformat()
        log.info(
            "Rodada agendada iniciando às %s [%s]",
            datetime.now(tz).strftime("%H:%M:%S"),
            _fmt_hm_local(next_dt),
        )
        started_mono = time.monotonic()
        started_wall = datetime.now(timezone.utc).isoformat()
        round_state["round_running"] = True
        try:
            result = run_full_scrape_round(
                conn,
                sites=sites,
                site_id_filter=None,
                round_label=started_wall,
                verbose=True,
            )
        finally:
            round_state["round_running"] = False

        finished_wall = datetime.now(timezone.utc).isoformat()
        elapsed = int(time.monotonic() - started_mono)

        if result.get("error_message") == "round_lock_busy":
            log.warning(
                "Rodada anterior ainda em execução (lock ativo). Pulando horário %s.",
                _fmt_hm_local(next_dt),
            )
            if round_state["graceful_stop"]:
                log.info("Encerramento após rodada: scheduler finalizado.")
                break
            continue

        if not result.get("ok"):
            round_state["failures"] += 1
            err = result.get("error_message") or "erro"
            insert_scheduled_run(
                conn,
                scheduled_at=slot_label,
                started_at=started_wall,
                finished_at=finished_wall,
                round_health="ERROR",
                sites_ok=0,
                sites_suspeitos=0,
                sites_erros=0,
                triggered_by="scheduled",
                notes=err[:2000],
                retries_attempted=int(result.get("retries_attempted") or 0),
                retries_succeeded=int(result.get("retries_succeeded") or 0),
            )
            log.error("Rodada falhou (%s). Falhas seguidas: %s", err, round_state["failures"])
            if round_state["failures"] < SCHEDULE_MAX_CONSECUTIVE_FAILURES:
                nxt = _next_fire_after(SCHEDULE_TIMES, tz)
                log.warning(
                    "ATENÇÃO: %s falha(s) consecutiva(s). Próxima tentativa: %s",
                    round_state["failures"],
                    _next_fire_label(nxt, tz),
                )
            if round_state["graceful_stop"]:
                log.info("Encerramento após rodada com falha: scheduler finalizado.")
                break
            continue

        round_state["failures"] = 0
        agg = result.get("round_agg") or {}
        rh = str(agg.get("round_health") or "UNKNOWN")
        insert_scheduled_run(
            conn,
            scheduled_at=slot_label,
            started_at=started_wall,
            finished_at=finished_wall,
            round_health=rh,
            sites_ok=int(agg.get("total_ok") or 0),
            sites_suspeitos=int(agg.get("total_suspeitos") or 0),
            sites_erros=int(agg.get("total_erros") or 0),
            triggered_by="scheduled",
            notes="",
            retries_attempted=int(result.get("retries_attempted") or 0),
            retries_succeeded=int(result.get("retries_succeeded") or 0),
        )

        nxt = _next_fire_after(SCHEDULE_TIMES, tz)
        log.info(
            "Rodada concluída: %s | duração %s | próxima: %s",
            rh,
            _fmt_duration(elapsed),
            _next_fire_label(nxt, tz),
        )
        cur = read_scheduler_state(root) or {}
        cur.update(
            {
                "pid": os.getpid(),
                "last_round_finished_at": finished_wall,
                "last_round_health": rh,
                "next_round_at": nxt.isoformat(),
                "consecutive_failures": 0,
            }
        )
        write_scheduler_state(root, cur)

        if round_state["graceful_stop"]:
            log.info("Encerramento após rodada: scheduler finalizado.")
            break


def run_scheduler_worker(*, install_signals: bool = True) -> None:
    """Executa o loop do agendador até encerrar. Sem ``sys.exit`` (adequado a thread / API).

    Se ``SCHEDULE_ENABLED`` ou ``SCHEDULE_TIMES`` forem inválidos, registra aviso e retorna.
    Se não conseguir o lock de instância, registra erro e retorna.
    """
    if not SCHEDULE_ENABLED:
        log.warning("Scheduler não iniciado: SCHEDULE_ENABLED=false.")
        return
    if not SCHEDULE_TIMES:
        log.warning("Scheduler não iniciado: SCHEDULE_TIMES vazio.")
        return

    from scraper import (  # noqa: WPS433 — import local intencional
        DB_FILE,
        SITES,
        init_db,
        refresh_site_profiles,
        run_full_scrape_round,
    )

    root = Path(__file__).resolve().parent
    tz = _resolve_tz()
    instance_lock_path = root / "scheduler_instance.lock"
    instance = try_acquire_round_lock(instance_lock_path)
    if instance is None:
        log.error(
            "Scheduler não iniciado: lock de instância em uso (%s).",
            instance_lock_path,
        )
        return

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)
    refresh_site_profiles(conn)

    round_state: dict[str, Any] = {
        "round_running": False,
        "graceful_stop": False,
        "immediate_exit": False,
        "failures": 0,
    }

    if install_signals:

        def on_signal(signum: int, _frame: Any) -> None:
            name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
            if not round_state["round_running"]:
                log.info("%s recebido: encerramento imediato (sem rodada em andamento).", name)
                round_state["immediate_exit"] = True
                return
            round_state["graceful_stop"] = True
            log.info(
                "%s recebido: aguardando conclusão da rodada atual antes de encerrar…",
                name,
            )

        signal.signal(signal.SIGINT, on_signal)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, on_signal)

    try:
        first_next = _next_fire_after(SCHEDULE_TIMES, tz)
        log.info(
            "Scheduler iniciado. Próxima rodada: %s (%s) (em %s)",
            _fmt_hm_local(first_next),
            _next_fire_label(first_next, tz),
            _fmt_countdown(max(0, (first_next - datetime.now(tz)).total_seconds())),
        )
        _scheduler_forever_loop(
            conn,
            root,
            round_state,
            tz,
            run_full_scrape_round=run_full_scrape_round,
            sites=SITES,
        )
    finally:
        instance.release()
        try:
            conn.close()
        except Exception:
            pass
        log.info("Scheduler encerrado: lock de instância liberado.")


def run_scheduler_main() -> None:
    _setup_stdio_utf8()

    if not SCHEDULE_ENABLED:
        log.error(
            "SCHEDULE_ENABLED=false. Defina SCHEDULE_ENABLED=true no .env para usar --scheduler."
        )
        sys.exit(1)
    if not SCHEDULE_TIMES:
        log.error("SCHEDULE_TIMES está vazio. Configure horários (ex.: 06:00,12:00,18:00).")
        sys.exit(1)

    from scraper import DB_FILE, SITES, init_db, refresh_site_profiles, run_full_scrape_round  # noqa: WPS433

    root = Path(__file__).resolve().parent
    instance_lock_path = root / "scheduler_instance.lock"
    instance = try_acquire_round_lock(instance_lock_path)
    if instance is None:
        log.error(
            "Outro scheduler já está em execução (lock %s). Encerrando.",
            instance_lock_path,
        )
        sys.exit(1)

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)
    refresh_site_profiles(conn)

    round_state: dict[str, Any] = {
        "round_running": False,
        "graceful_stop": False,
        "immediate_exit": False,
        "failures": 0,
    }

    def on_signal(signum: int, _frame: Any) -> None:
        name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
        if not round_state["round_running"]:
            log.info("%s recebido: encerramento imediato (sem rodada em andamento).", name)
            round_state["immediate_exit"] = True
            return
        round_state["graceful_stop"] = True
        log.info(
            "%s recebido: aguardando conclusão da rodada atual antes de encerrar…",
            name,
        )

    signal.signal(signal.SIGINT, on_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, on_signal)
    tz = _resolve_tz()

    try:
        first_next = _next_fire_after(SCHEDULE_TIMES, tz)
        log.info(
            "Scheduler iniciado. Próxima rodada: %s (%s) (em %s)",
            _fmt_hm_local(first_next),
            _next_fire_label(first_next, tz),
            _fmt_countdown(max(0, (first_next - datetime.now(tz)).total_seconds())),
        )
        _scheduler_forever_loop(
            conn,
            root,
            round_state,
            tz,
            run_full_scrape_round=run_full_scrape_round,
            sites=SITES,
        )
    finally:
        instance.release()
        try:
            conn.close()
        except Exception:
            pass
        log.info("Scheduler encerrado: lock de instância liberado.")
