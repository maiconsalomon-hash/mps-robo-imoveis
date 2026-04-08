#!/usr/bin/env python3
"""
Migração idempotente: colunas de ciclo de vida em ``imoveis`` + tabela ``imovel_historico``.

Uso:
  python migrations/apply_imovel_lifecycle_historico.py
  python migrations/apply_imovel_lifecycle_historico.py C:\\caminho\\imoveis.db

Não importa o scraper (apenas sqlite3) para rodar em ambientes mínimos.
A mesma lógica é aplicada automaticamente em scraper.init_db().
"""

from __future__ import annotations

import sqlite3
import sys
from collections.abc import Callable
from pathlib import Path

# Nome do índice parcial usado por ``upsert_imoveis`` (ON CONFLICT / anti-duplicata por URL canônica).
IDX_IMOVEIS_SITE_CANONICAL = "idx_imoveis_site_canonical_url"


def migrate_imoveis_lifecycle(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(imoveis)")}
    alters: list[tuple[str, str]] = [
        ("ativo", "INTEGER NOT NULL DEFAULT 1"),
        ("primeira_coleta", "DATETIME"),
        ("ultima_coleta", "DATETIME"),
        ("removed_at", "DATETIME"),
        ("reappeared_at", "DATETIME"),
        ("total_coletas", "INTEGER NOT NULL DEFAULT 0"),
    ]
    for name, decl in alters:
        if name not in cols:
            conn.execute(f"ALTER TABLE imoveis ADD COLUMN {name} {decl}")
    conn.commit()


def migrate_imoveis_preco_numeric(conn: sqlite3.Connection) -> None:
    """Coluna ``preco_numeric`` — valor float canônico para comparações (BR: R$, milhar, vírgula decimal)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(imoveis)")}
    if "preco_numeric" not in cols:
        conn.execute("ALTER TABLE imoveis ADD COLUMN preco_numeric REAL")
    conn.commit()


def migrate_site_listing_snapshots(conn: sqlite3.Connection) -> None:
    """Snapshot das chaves (hash) vistas no último run válido por site — remoções entre runs."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS site_listing_snapshots (
            site_id             INTEGER PRIMARY KEY,
            listing_keys_json   TEXT NOT NULL DEFAULT '[]',
            updated_at          TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.commit()


def migrate_imovel_historico(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS imovel_historico (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            imovel_id     INTEGER NOT NULL,
            run_id        INTEGER,
            preco         REAL,
            content_hash  TEXT,
            status        TEXT,
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (imovel_id) REFERENCES imoveis(id)
        );
        CREATE INDEX IF NOT EXISTS idx_imovel_historico_imovel
            ON imovel_historico (imovel_id);
        CREATE INDEX IF NOT EXISTS idx_imovel_historico_run
            ON imovel_historico (run_id);
        CREATE INDEX IF NOT EXISTS idx_imovel_historico_created
            ON imovel_historico (created_at);
        """
    )
    conn.commit()


def migrate_imoveis_site_canonical_unique(
    conn: sqlite3.Connection,
    warn: Callable[[str], None] | None = None,
) -> None:
    """
    Índice único (site_id, canonical_url_anuncio) quando a URL canônica está preenchida.
    Necessário para garantir um único registro por anúncio e para UPSERT por conflito nesse par.
    Se já existirem duplicatas, o índice não é criado (banco continua funcionando com resolução por hash).
    """
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
        (IDX_IMOVEIS_SITE_CANONICAL,),
    ).fetchone():
        return
    dups = conn.execute(
        """
        SELECT site_id, canonical_url_anuncio, COUNT(*) AS n
        FROM imoveis
        WHERE canonical_url_anuncio IS NOT NULL AND canonical_url_anuncio != ''
        GROUP BY site_id, canonical_url_anuncio
        HAVING COUNT(*) > 1
        """,
    ).fetchall()
    if dups:
        msg = (
            f"Índice {IDX_IMOVEIS_SITE_CANONICAL} não criado: há {len(dups)} URL(s) canônicas "
            f"duplicadas no mesmo site (ex.: site_id={dups[0][0]!r}). Limpe duplicatas e rode a migração."
        )
        if warn:
            warn(msg)
        else:
            print(msg, file=sys.stderr)
        return
    conn.execute(
        f"""
        CREATE UNIQUE INDEX {IDX_IMOVEIS_SITE_CANONICAL}
        ON imoveis (site_id, canonical_url_anuncio)
        WHERE canonical_url_anuncio IS NOT NULL AND canonical_url_anuncio != ''
        """
    )
    conn.commit()


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    db_path = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else (root / "imoveis.db")
    if not db_path.is_file():
        print(f"Arquivo não encontrado: {db_path}", file=sys.stderr)
        return 1
    conn = sqlite3.connect(str(db_path))
    try:
        migrate_imoveis_lifecycle(conn)
        migrate_imoveis_preco_numeric(conn)
        migrate_imovel_historico(conn)
        migrate_imoveis_site_canonical_unique(conn)
        migrate_site_listing_snapshots(conn)
    finally:
        conn.close()
    print(f"OK: migração aplicada em {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
