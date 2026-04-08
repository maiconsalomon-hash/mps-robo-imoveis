import sqlite3
from datetime import datetime

from field_quality import data_quality_issues_json, evaluate_field_quality
from property_identity import apply_property_identity
from scraper import migrate_imoveis_field_quality, migrate_imoveis_identity

DB = "imoveis.db"
ARQUIVO = "urls_d2.txt"
SITE_ID = 7
SITE_NAME = "d2imoveis.com"

with open(ARQUIVO, "r", encoding="utf-8") as f:
    urls = [u.strip() for u in f.readlines() if u.strip()]

print(f"URLs carregadas: {len(urls)}")

conn = sqlite3.connect(DB)
migrate_imoveis_identity(conn)
migrate_imoveis_field_quality(conn)
agora = datetime.now().isoformat()
novos = 0

for url in urls:
    codigo = url.rsplit("/", 1)[-1]
    im = {
        "site_id": SITE_ID,
        "site_name": SITE_NAME,
        "titulo": f"Imóvel {codigo}",
        "tipo": "outros",
        "preco_texto": "",
        "preco": None,
        "url_anuncio": url,
        "codigo": codigo,
    }
    apply_property_identity(im)
    evaluate_field_quality(im)
    h = im["hash"]
    existe = conn.execute("SELECT id FROM imoveis WHERE hash=?", (h,)).fetchone()
    if not existe:
        conn.execute("""
            INSERT INTO imoveis (hash, site_id, site_name, titulo, tipo, url_anuncio, codigo,
                identity_source, legacy_hash, canonical_url_anuncio, identity_fallback,
                identity_quality, identity_quality_reason,
                data_quality_score, data_quality_level, data_quality_issues,
                ativo, primeira_vez, ultima_vez)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)
        """, (
            h,
            SITE_ID,
            SITE_NAME,
            im["titulo"],
            "outros",
            url,
            codigo,
            im.get("identity_source"),
            im.get("legacy_hash"),
            im.get("canonical_url_anuncio"),
            im.get("identity_fallback", 0),
            im.get("identity_quality"),
            im.get("identity_quality_reason"),
            im.get("data_quality_score"),
            im.get("data_quality_level"),
            data_quality_issues_json(im),
            agora,
            agora,
        ))
        novos += 1

conn.commit()
conn.close()
print(f"Importados: {novos} novos imóveis")

conn2 = sqlite3.connect(DB)
total = conn2.execute("SELECT COUNT(*) FROM imoveis WHERE site_id=7").fetchone()[0]
print(f"d2imoveis.com: {total} imóveis no banco")