#!/usr/bin/env python3
"""
Validação do ciclo de vida do imóvel (SQLite): NEW, UNCHANGED, PRICE_CHANGE, CONTENT_CHANGE,
REMOVED, REAPPEARED — banco, imovel_historico, sem duplicata de imóvel.

Uso:
  py -3 teste_ciclo_vida_imoveis.py
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path

# Raiz do projeto no path
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from site_diagnostic import SiteExtractionStatus  # noqa: E402

from scraper import (  # noqa: E402
    LS_CONTENT_CHANGE,
    LS_NEW,
    LS_PRICE_CHANGE,
    LS_REAPPEARED,
    LS_REMOVED,
    LS_UNCHANGED,
    _save_site_listing_keys_snapshot,
    apply_site_removals_with_guard,
    init_db,
    upsert_imoveis,
)


SITE_ID = 9901
SITE = {"id": SITE_ID, "name": "test.ciclo.vida", "url": "https://test-ciclo.example/lista"}


def _im_base(suf: str, **extra) -> dict:
    return {
        "site_id": SITE_ID,
        "site_name": SITE["name"],
        "titulo": f"Apartamento teste {suf}",
        "tipo": "apartamento",
        "finalidade": "venda",
        "url_anuncio": f"https://test-ciclo.example/imovel/{suf}",
        "codigo": suf,
        "preco_texto": "R$ 300.000,00",
        "bairro": "Centro",
        "cidade": "Jaraguá do Sul",
        "area_m2": 80.0,
        "quartos": 2,
        **extra,
    }


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _count_imoveis(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM imoveis WHERE site_id=?", (SITE_ID,)
        ).fetchone()[0]
    )


def _no_duplicate_hash(conn: sqlite3.Connection) -> None:
    bad = conn.execute(
        """
        SELECT hash, COUNT(*) FROM imoveis WHERE site_id=? GROUP BY hash HAVING COUNT(*)>1
        """,
        (SITE_ID,),
    ).fetchall()
    _assert(not bad, f"hash duplicado: {bad}")


def _last_historico_status(conn: sqlite3.Connection, imovel_id: int) -> str | None:
    row = conn.execute(
        """
        SELECT status FROM imovel_historico
        WHERE imovel_id=? ORDER BY id DESC LIMIT 1
        """,
        (imovel_id,),
    ).fetchone()
    return row[0] if row else None


def _historico_por_run(conn: sqlite3.Connection, imovel_id: int, run_id: int) -> str | None:
    row = conn.execute(
        """
        SELECT status FROM imovel_historico
        WHERE imovel_id=? AND run_id=? ORDER BY id DESC LIMIT 1
        """,
        (imovel_id, run_id),
    ).fetchone()
    return row[0] if row else None


def _dump_imovel(conn: sqlite3.Connection, imovel_id: int) -> dict:
    cur = conn.execute(
        """
        SELECT id, hash, titulo, ativo, total_coletas, preco, preco_numeric, preco_texto,
               bairro, cidade, area_m2, quartos, removed_at, reappeared_at
        FROM imoveis WHERE id=?
        """,
        (imovel_id,),
    )
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else {}


def _dump_historico(conn: sqlite3.Connection, imovel_id: int) -> str:
    rows = conn.execute(
        """
        SELECT id, run_id, status, preco, substr(content_hash,1,12) AS ch
        FROM imovel_historico WHERE imovel_id=? ORDER BY id
        """,
        (imovel_id,),
    ).fetchall()
    parts = [f"id={r[0]} run={r[1]} st={r[2]} preco={r[3]} ch={r[4]}..." for r in rows]
    return "; ".join(parts) if parts else "(vazio)"


def run() -> int:
    report: list[str] = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report.append("=" * 72)
    report.append(f"RELATORIO - validacao ciclo de vida (imoveis) - {ts}")
    report.append("=" * 72)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        init_db(conn)

        # --- 1. NEW (imóvel A) ---
        report.append("\n[1] NOVO imovel -> esperado: NEW")
        im_a = _im_base("LCA")
        upsert_imoveis(conn, [im_a], SITE_ID, run_id=101, historico_dedupe={})
        row_a = conn.execute(
            "SELECT id, hash FROM imoveis WHERE site_id=? AND codigo=?",
            (SITE_ID, "LCA"),
        ).fetchone()
        _assert(row_a is not None, "imóvel A não inserido")
        id_a, hash_a = int(row_a[0]), row_a[1]
        st = _historico_por_run(conn, id_a, 101)
        _assert(st == LS_NEW, f"histórico run 101: esperado {LS_NEW!r}, obteve {st!r}")
        ta = conn.execute(
            "SELECT ativo, total_coletas FROM imoveis WHERE id=?", (id_a,)
        ).fetchone()
        _assert(int(ta[0]) == 1 and int(ta[1]) == 1, "ativo/total_coletas após NEW")
        report.append(f"    OK - imovel_id={id_a}, status={st}, total_coletas=1")
        report.append(f"    Exemplo linha imoveis: {_dump_imovel(conn, id_a)}")

        # --- 2. UNCHANGED (mesmo dado, outro run) ---
        report.append("\n[2] Sem mudanca -> esperado: UNCHANGED")
        im_a2 = _im_base("LCA")
        upsert_imoveis(conn, [im_a2], SITE_ID, run_id=102, historico_dedupe={})
        st = _historico_por_run(conn, id_a, 102)
        _assert(st == LS_UNCHANGED, f"esperado {LS_UNCHANGED!r}, obteve {st!r}")
        tc = conn.execute(
            "SELECT total_coletas FROM imoveis WHERE id=?", (id_a,)
        ).fetchone()[0]
        _assert(int(tc) == 2, f"total_coletas deveria ser 2, é {tc}")
        report.append(f"    OK - status={st}, total_coletas={tc}")
        report.append(f"    Historico A (amostra): {_dump_historico(conn, id_a)}")

        # --- 3. PRICE_CHANGE ---
        report.append("\n[3] Preco mudou -> esperado: PRICE_CHANGE")
        im_a3 = _im_base("LCA", preco_texto="R$ 310.000,00")
        upsert_imoveis(conn, [im_a3], SITE_ID, run_id=103, historico_dedupe={})
        st = _historico_por_run(conn, id_a, 103)
        _assert(st == LS_PRICE_CHANGE, f"esperado {LS_PRICE_CHANGE!r}, obteve {st!r}")
        pn = conn.execute(
            "SELECT preco_numeric FROM imoveis WHERE id=?", (id_a,)
        ).fetchone()[0]
        _assert(pn is not None and abs(float(pn) - 310000.0) < 1, f"preco_numeric={pn}")
        report.append(f"    OK - status={st}, preco_numeric={pn}")

        # --- 4. CONTENT_CHANGE (preço igual, bairro muda) ---
        report.append("\n[4] Conteudo mudou (bairro) -> esperado: CONTENT_CHANGE")
        im_a4 = _im_base("LCA", preco_texto="R$ 310.000,00", bairro="Vila Nova")
        upsert_imoveis(conn, [im_a4], SITE_ID, run_id=104, historico_dedupe={})
        st = _historico_por_run(conn, id_a, 104)
        _assert(st == LS_CONTENT_CHANGE, f"esperado {LS_CONTENT_CHANGE!r}, obteve {st!r}")
        br = conn.execute("SELECT bairro FROM imoveis WHERE id=?", (id_a,)).fetchone()[0]
        _assert(br == "Vila Nova", f"bairro={br}")
        report.append(f"    OK - status={st}, bairro persistido={br!r}")

        # --- Inserir B para remoção entre runs ---
        im_b = _im_base("LCB", titulo="Casa teste LCB", preco_texto="R$ 500.000,00")
        upsert_imoveis(conn, [im_b], SITE_ID, run_id=105, historico_dedupe={})
        row_b = conn.execute(
            "SELECT id, hash FROM imoveis WHERE site_id=? AND codigo=?",
            (SITE_ID, "LCB"),
        ).fetchone()
        _assert(row_b is not None, "imóvel B não inserido")
        id_b, hash_b = int(row_b[0]), row_b[1]
        _assert(_count_imoveis(conn) == 2, "deveria haver 2 imóveis")
        _no_duplicate_hash(conn)

        # Snapshot: ambos na listagem anterior; atual só A (B sumiu)
        report.append("\n[5] Sumiu da listagem -> esperado: REMOVED em B")
        agora_snap = datetime.now().isoformat()
        _save_site_listing_keys_snapshot(conn, SITE_ID, {hash_a, hash_b}, agora_snap)
        conn.commit()
        rm = apply_site_removals_with_guard(
            conn,
            SITE,
            {hash_a},
            SiteExtractionStatus.OK,
            1,
            run_id=105,
            historico_dedupe={},
        )
        _assert(rm["removidos"] >= 1, f"removidos={rm}")
        tb = conn.execute(
            "SELECT ativo, removed_at FROM imoveis WHERE id=?", (id_b,)
        ).fetchone()
        _assert(int(tb[0]) == 0 and tb[1] is not None, "B deveria estar inativo com removed_at")
        st_b = _historico_por_run(conn, id_b, 105)
        _assert(st_b == LS_REMOVED, f"B histórico: esperado {LS_REMOVED!r}, obteve {st_b!r}")
        report.append(
            f"    OK - B ativo=0, REMOVED no historico, removidos_aplicados={rm['removidos']}"
        )
        report.append(f"    Exemplo imovel B: {_dump_imovel(conn, id_b)}")

        # --- 6. REAPPEARED ---
        report.append("\n[6] Voltou a listagem -> esperado: REAPPEARED em B")
        im_b2 = _im_base("LCB", titulo="Casa teste LCB", preco_texto="R$ 500.000,00")
        upsert_imoveis(conn, [im_b2], SITE_ID, run_id=106, historico_dedupe={})
        tb2 = conn.execute(
            "SELECT ativo, removed_at, reappeared_at FROM imoveis WHERE id=?",
            (id_b,),
        ).fetchone()
        _assert(int(tb2[0]) == 1, "B deveria estar ativo")
        _assert(tb2[1] is None, "removed_at deveria ser NULL após reaparecer")
        _assert(tb2[2] is not None, "reappeared_at deveria estar preenchido")
        st_b2 = _historico_por_run(conn, id_b, 106)
        _assert(st_b2 == LS_REAPPEARED, f"esperado {LS_REAPPEARED!r}, obteve {st_b2!r}")
        report.append(f"    OK - status={st_b2}, ativo=1, removed_at=NULL")
        report.append(f"    Historico B completo: {_dump_historico(conn, id_b)}")

        # Integridade final
        _assert(_count_imoveis(conn) == 2, "nenhum imóvel extra deve existir")
        _no_duplicate_hash(conn)
        tit_a = conn.execute(
            "SELECT titulo FROM imoveis WHERE id=?", (id_a,)
        ).fetchone()[0]
        _assert("LCA" in (tit_a or ""), "título A não deveria ser perdido")

        report.append("\n" + "=" * 72)
        report.append("RESUMO FINAL")
        report.append("=" * 72)
        report.append(f"  Imoveis no site {SITE_ID}: {_count_imoveis(conn)} (esperado 2)")
        report.append("  Linhas imovel_historico (site via imoveis): ")
        n_hist = conn.execute(
            """
            SELECT COUNT(*) FROM imovel_historico h
            JOIN imoveis i ON i.id = h.imovel_id WHERE i.site_id=?
            """,
            (SITE_ID,),
        ).fetchone()[0]
        report.append(f"    total eventos gravados: {n_hist}")
        report.append("  Duplicata por hash: nenhuma (verificada)")
        report.append("  Dados essenciais (titulo/codigos): preservados")
        report.append("\nCONFIRMACAO: todos os cenarios de ciclo de vida passaram.")
        report.append("=" * 72)

        text = "\n".join(report)
        print(text)
        out = ROOT / "relatorio_ciclo_vida_imoveis.txt"
        out.write_text(text, encoding="utf-8")
        print(f"\n(Arquivo tambem salvo em: {out})")
        return 0

    except AssertionError as e:
        err = f"\nFALHA: {e}\n"
        print(err, file=sys.stderr)
        for line in report:
            print(line)
        return 1
    finally:
        if conn is not None:
            conn.close()
        Path(db_path).unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(run())
