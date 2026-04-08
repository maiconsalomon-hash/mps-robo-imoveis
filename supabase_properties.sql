-- Supabase: tabela public.properties alinhada ao sync em scraper.py (POST + on_conflict=hash).
-- Service role: Settings → API → Project API keys → service_role (nunca commite no git).

-- ── 1) Ver colunas atuais (cole o resultado na conversa se quiser validação) ─
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'properties'
ORDER BY ordinal_position;

-- ── 2) Projeto novo: criar tabela (pule se properties já existe com outro layout) ─
CREATE TABLE IF NOT EXISTS properties (
    hash          TEXT NOT NULL,
    site_id       INTEGER,
    site_name     TEXT,
    titulo        TEXT,
    tipo          TEXT,
    finalidade    TEXT DEFAULT 'venda',
    preco         DOUBLE PRECISION,
    preco_texto   TEXT,
    area_m2       DOUBLE PRECISION,
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
    ativo         INTEGER DEFAULT 1,
    primeira_vez  TEXT,
    ultima_vez    TEXT,
    raw_json      TEXT,
    CONSTRAINT properties_hash_unique UNIQUE (hash)
);

-- ── 3) Tabela já existente: adicionar hash + unicidade (ajuste se hash já existir) ─
-- ALTER TABLE properties ADD COLUMN IF NOT EXISTS hash TEXT;
-- CREATE UNIQUE INDEX IF NOT EXISTS properties_hash_unique ON properties (hash);
