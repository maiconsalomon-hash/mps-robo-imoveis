"""
MINI-ETAPA 5A — identidade estável do imóvel (SQLite / pipeline).
MINI-ETAPA 5B — qualidade da identidade (HIGH / MEDIUM / LOW) e auditoria.

Prioridade da chave primária lógica (valor gravado na coluna ``hash``):
1. ``external_id`` — ``codigo`` / código do anúncio normalizado, quando existir
2. ``canonical_url`` — URL canônica do anúncio (sem fragmento, sem query de tracking)
3. ``stable_fingerprint`` — host + tipo + cidade/bairro + área + quartos + banheiros (+ último segmento de path)
4. ``legacy_fallback`` — MD5 legado ``url_anuncio|titulo|preco_texto`` (compatível com registros antigos)

Qualidade (5B): HIGH = external_id ou canonical **forte**; MEDIUM = fingerprint;
LOW = legacy_fallback ou canonical **fraca**.

Preço e título completo não são pivô da identidade estável (exceto no fallback legado).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

IDENTITY_LOW_RATIO_WARN = 0.35
IDENTITY_LEGACY_RATIO_WARN = 0.25
IDENTITY_STRONG_SOURCE_MIN_RATIO = 0.20  # fração mínima desejada (external_id + canonical_url)

# Segmentos de path típicos de listagem (um único segmento = canônica fraca para identidade).
_GENERIC_SINGLE_SEGMENTS = frozenset(
    s.lower()
    for s in (
        "imoveis",
        "imovel",
        "imóveis",
        "busca",
        "buscar",
        "venda",
        "comprar",
        "alugar",
        "lista",
        "filtro",
        "index",
        "home",
        "properties",
        "property",
        "search",
        "resultados",
    )
)

# Query params comuns de tracking / analytics — removidos na canônica.
_TRACKING_QUERY_KEYS = frozenset(
    k.lower()
    for k in (
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_content",
        "utm_term",
        "utm_id",
        "fbclid",
        "gclid",
        "gbraid",
        "wbraid",
        "mc_cid",
        "mc_eid",
        "igshid",
        "_ga",
        "_gl",
        "ref",
        "referrer",
        "source",
        "campaign",
        "yclid",
        "msclkid",
    )
)


def legacy_content_hash(data: dict) -> str:
    """Hash legado usado pelo extrator genérico: inclui URL, título e preço textual."""
    key = f"{data.get('url_anuncio', '')}-{data.get('titulo', '')}-{data.get('preco_texto', '')}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def _norm_segment(s: str | None, max_len: int = 80) -> str:
    if not s:
        return ""
    t = re.sub(r"\s+", " ", str(s).strip().lower())
    return t[:max_len]


def canonical_property_url(url: str | None) -> str:
    """
    Normaliza URL do anúncio para comparação e identidade:
    - remove fragmento;
    - host em minúsculas;
    - esquema http -> https quando seguro;
    - remove parâmetros de query conhecidos de tracking;
    - path sem barra final redundante (exceto raiz).
    """
    if not url or not isinstance(url, str):
        return ""
    u = url.strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
    except Exception:
        return u.split("#")[0].strip()

    scheme = (p.scheme or "https").lower()
    if scheme == "http":
        scheme = "https"
    netloc = (p.netloc or "").lower()
    if not netloc:
        return ""

    # query filtrada
    pairs = [
        (k, v)
        for k, v in parse_qsl(p.query, keep_blank_values=True)
        if k.lower() not in _TRACKING_QUERY_KEYS
    ]
    pairs.sort(key=lambda x: (x[0].lower(), x[1]))
    query = urlencode(pairs, doseq=True)

    path = p.path or ""
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")

    out = urlunparse((scheme, netloc, path, "", query, ""))
    return out


def canonical_identity_strength(canonical: str) -> tuple[str, str]:
    """
    Classifica se a URL canônica é **forte** (detalhe provável) ou **fraca** (listagem/genérica).
    Retorna (``strong``|``weak``, código_curto_para_auditoria).
    """
    if not canonical:
        return "weak", "empty"
    try:
        path = (urlparse(canonical).path or "").strip("/")
    except Exception:
        return "weak", "parse_error"
    if not path:
        return "weak", "path_empty"

    segments = [x for x in path.split("/") if x]
    lowpath = path.lower()

    if re.search(r"\d{4,}", path):
        return "strong", "path_numeric_id"
    if "imovel" in lowpath or "imóvel" in lowpath or "property" in lowpath:
        return "strong", "path_imovel_segment"
    if len(segments) >= 3:
        return "strong", "path_depth_3plus"

    last = segments[-1] if segments else ""
    if last and re.search(r"\d", last) and len(last) >= 4:
        return "strong", "path_segment_with_digits"

    if len(segments) == 1:
        seg = segments[0].lower()
        if seg in _GENERIC_SINGLE_SEGMENTS or len(seg) < 4:
            return "weak", "generic_single_segment"

    if len(path) < 10:
        return "weak", "path_short"

    if len(segments) == 2 and all(len(s) < 5 for s in segments):
        return "weak", "short_two_segments"

    return "strong", "path_default_acceptable"


def resolve_identity_quality(resolution: "IdentityResolution") -> tuple[str, str]:
    """
    HIGH | MEDIUM | LOW e motivo curto (ex.: ``external_id``, ``canonical_url_weak:generic_single_segment``).
    """
    src = resolution.identity_source
    if src == "external_id":
        return "HIGH", "external_id"
    if src == "canonical_url":
        strength, why = canonical_identity_strength(resolution.canonical_url_anuncio)
        if strength == "strong":
            return "HIGH", f"canonical_url_strong:{why}"
        return "LOW", f"canonical_url_weak:{why}"
    if src == "stable_fingerprint":
        return "MEDIUM", "stable_fingerprint"
    return "LOW", "legacy_fallback"


def _path_meaningful(canonical: str) -> bool:
    if not canonical:
        return False
    try:
        path = urlparse(canonical).path or ""
    except Exception:
        return False
    return len(path.strip("/")) >= 1


def stable_fingerprint_payload(data: dict) -> str:
    """Ingredientes estáveis (sem preço, sem título). Versão explícita para auditoria."""
    raw_url = data.get("url_anuncio") or ""
    canon = canonical_property_url(raw_url)
    try:
        host = (urlparse(canon or raw_url).netloc or "").lower()
    except Exception:
        host = ""

    path_tail = ""
    try:
        path = (urlparse(canon or raw_url).path or "").strip("/")
        if path:
            path_tail = path.split("/")[-1][:120]
    except Exception:
        pass

    sid = data.get("site_id", "")
    tipo = _norm_segment(data.get("tipo"), 40)
    cidade = _norm_segment(data.get("cidade"), 60)
    bairro = _norm_segment(data.get("bairro"), 60)
    area = data.get("area_m2")
    area_s = f"{float(area):.4g}" if isinstance(area, (int, float)) else ""
    q = data.get("quartos")
    b = data.get("banheiros")
    q_s = str(int(q)) if isinstance(q, int) else (str(q) if q is not None else "")
    b_s = str(int(b)) if isinstance(b, int) else (str(b) if b is not None else "")

    return (
        f"v1|site={sid}|host={host}|tipo={tipo}|cidade={cidade}|bairro={bairro}"
        f"|area={area_s}|q={q_s}|ban={b_s}|tail={path_tail}"
    )


def _md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class IdentityResolution:
    """Resultado da resolução da identidade estável."""

    stable_hash: str
    identity_source: str  # external_id | canonical_url | stable_fingerprint | legacy_fallback
    legacy_hash: str
    canonical_url_anuncio: str
    identity_fallback: bool
    fingerprint_payload: str

    def to_debug_dict(self) -> dict:
        return {
            "identity_key": self.stable_hash,
            "identity_source": self.identity_source,
            "legacy_hash": self.legacy_hash,
            "canonical_url_anuncio": self.canonical_url_anuncio,
            "identity_fallback": bool(self.identity_fallback),
            "fingerprint_payload": self.fingerprint_payload,
        }


def resolve_property_identity(data: dict) -> IdentityResolution:
    """
    Calcula a chave estável e metadados. ``legacy_hash`` é sempre o hash legado (url+título+preço)
    para compatibilidade e migração de linhas antigas.
    """
    site_id = data.get("site_id", "")
    leg = legacy_content_hash(data)

    codigo = (data.get("codigo") or data.get("external_id") or "").strip()
    if isinstance(codigo, str) and codigo:
        codigo = re.sub(r"\s+", "", codigo)
    if codigo:
        h = _md5_hex(f"{site_id}-codigo-{codigo}")
        canon = canonical_property_url(data.get("url_anuncio"))
        return IdentityResolution(
            stable_hash=h,
            identity_source="external_id",
            legacy_hash=leg,
            canonical_url_anuncio=canon,
            identity_fallback=False,
            fingerprint_payload=stable_fingerprint_payload(data),
        )

    canon = canonical_property_url(data.get("url_anuncio"))
    if canon and _path_meaningful(canon):
        h = _md5_hex(f"{site_id}-url-{canon}")
        return IdentityResolution(
            stable_hash=h,
            identity_source="canonical_url",
            legacy_hash=leg,
            canonical_url_anuncio=canon,
            identity_fallback=False,
            fingerprint_payload=stable_fingerprint_payload(data),
        )

    fp = stable_fingerprint_payload(data)
    # Só usa fingerprint se houver algum sinal mínimo (evita colisão em massa em cards vazios)
    if host_has_signal(data, fp):
        h = _md5_hex(f"{site_id}-fp-{fp}")
        return IdentityResolution(
            stable_hash=h,
            identity_source="stable_fingerprint",
            legacy_hash=leg,
            canonical_url_anuncio=canon,
            identity_fallback=False,
            fingerprint_payload=fp,
        )

    h = leg
    return IdentityResolution(
        stable_hash=h,
        identity_source="legacy_fallback",
        legacy_hash=leg,
        canonical_url_anuncio=canon,
        identity_fallback=True,
        fingerprint_payload=fp,
    )


def host_has_signal(data: dict, _fp_payload: str) -> bool:
    """Evita fingerprint genérico vazio que colidiria cards sem nenhum sinal estável."""
    u = data.get("url_anuncio") or ""
    canon = canonical_property_url(u)
    try:
        netloc = (urlparse(canon or u).netloc or "").strip().lower()
        if netloc:
            return True
    except Exception:
        pass
    for k in ("area_m2", "quartos", "banheiros", "bairro"):
        v = data.get(k)
        if v is not None and v != "":
            return True
    return False


def stable_hash_for_record(data: dict) -> str:
    """Atalho para deduplicação na sessão (sem mutar o dict)."""
    return resolve_property_identity(data).stable_hash


def apply_property_identity(data: dict) -> IdentityResolution:
    """
    Preenche ``hash``, qualidade da identidade (5B) e ``identity_debug`` no dict do imóvel.
    """
    r = resolve_property_identity(data)
    data["hash"] = r.stable_hash
    data["identity_source"] = r.identity_source
    data["legacy_hash"] = r.legacy_hash if r.legacy_hash != r.stable_hash else None
    data["canonical_url_anuncio"] = r.canonical_url_anuncio or None
    data["identity_fallback"] = 1 if r.identity_fallback else 0
    iq, iqr = resolve_identity_quality(r)
    data["identity_quality"] = iq
    data["identity_quality_reason"] = iqr
    dbg = r.to_debug_dict()
    dbg["identity_quality"] = iq
    dbg["identity_quality_reason"] = iqr
    if r.identity_source == "canonical_url":
        st, st_why = canonical_identity_strength(r.canonical_url_anuncio)
        dbg["canonical_strength"] = st
        dbg["canonical_strength_detail"] = st_why
    data["identity_debug"] = dbg
    return r


def finalize_imoveis_identity(imoveis: list[dict]) -> dict[str, int]:
    """Aplica identidade estável a cada item; retorna contagem por ``identity_source``."""
    counts: dict[str, int] = {}
    for im in imoveis:
        r = apply_property_identity(im)
        counts[r.identity_source] = counts.get(r.identity_source, 0) + 1
    return counts
