"""
MINI-ETAPA 6A — auditoria da qualidade dos campos extraídos do imóvel (diagnóstico apenas).

Não bloqueia scraping nem persistência; preenche ``data_quality_*`` no dict do imóvel e colunas opcionais no SQLite.
"""

from __future__ import annotations

import json
import re
from urllib.parse import urlparse

# Níveis derivados do score 0–100
DATA_QUALITY_HIGH_MIN = 75
DATA_QUALITY_MEDIUM_MIN = 50

# Alertas agregados (só observabilidade)
DATA_LOW_RATIO_WARN = 0.35
DATA_MISSING_PRICE_RATIO_WARN = 0.40
DATA_MISSING_LOCATION_RATIO_WARN = 0.45
DATA_MEAN_SCORE_WARN = 55.0


def _strip(s: str | None) -> str:
    return (s or "").strip()


def _url_looks_valid(url: str | None) -> bool:
    if not url or not isinstance(url, str):
        return False
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        return False
    try:
        p = urlparse(u)
        if not p.netloc:
            return False
        path = (p.path or "").strip("/")
        return len(path) >= 1
    except Exception:
        return False


def _price_suspicious_text(preco_texto: str, preco: float | None) -> bool:
    """Texto parece preço mas ``preco`` não foi parseado."""
    if preco is not None:
        return False
    t = preco_texto.strip()
    if not t:
        return False
    if "R$" in t or re.search(r"\d[\d.,]*", t):
        return True
    return False


def _price_invalid(preco: float | None) -> bool:
    if preco is None:
        return False
    if preco <= 0:
        return True
    if preco > 5e9:
        return True
    return False


def evaluate_field_quality(im: dict) -> None:
    """
    Preenche ``data_quality_score`` (0–100), ``data_quality_level`` (HIGH|MEDIUM|LOW),
    ``data_quality_issues`` (lista de strings) e ``data_quality_debug`` (dict resumido).
    """
    issues: list[str] = []
    components: dict[str, float] = {}

    preco = im.get("preco")
    preco_texto = _strip(im.get("preco_texto"))

    if _price_invalid(preco):
        issues.append("invalid_price")
        components["price"] = 0.0
    elif preco is not None and preco > 0:
        components["price"] = 25.0
    else:
        issues.append("missing_price")
        components["price"] = 0.0
        if _price_suspicious_text(preco_texto, preco):
            issues.append("suspicious_price_format")

    if _url_looks_valid(im.get("url_anuncio")):
        components["url"] = 15.0
    else:
        issues.append("missing_or_weak_url")
        components["url"] = 0.0

    bairro = _strip(im.get("bairro"))
    cidade = _strip(im.get("cidade"))
    endereco = _strip(im.get("endereco"))

    if bairro or endereco:
        components["location"] = 20.0
    elif cidade:
        components["location"] = 12.0
        issues.append("incomplete_location")
    else:
        issues.append("missing_location")
        components["location"] = 0.0

    area = im.get("area_m2")
    if isinstance(area, (int, float)) and area > 0:
        components["area"] = 15.0
    else:
        issues.append("missing_area")
        components["area"] = 0.0

    q = im.get("quartos")
    b = im.get("banheiros")
    if isinstance(q, int) or isinstance(b, int):
        components["rooms"] = 10.0
    else:
        issues.append("missing_rooms")
        components["rooms"] = 0.0

    desc = _strip(im.get("descricao"))
    desc_len = len(desc)
    if desc_len >= 30:
        components["description"] = 15.0
    elif desc_len >= 12:
        components["description"] = 8.0
        issues.append("thin_description")
    else:
        issues.append("missing_details")
        components["description"] = 0.0

    score = int(round(min(100.0, sum(components.values()))))
    if score >= DATA_QUALITY_HIGH_MIN:
        level = "HIGH"
    elif score >= DATA_QUALITY_MEDIUM_MIN:
        level = "MEDIUM"
    else:
        level = "LOW"

    im["data_quality_score"] = score
    im["data_quality_level"] = level
    im["data_quality_issues"] = list(dict.fromkeys(issues))
    im["data_quality_debug"] = {"components": components, "issues": im["data_quality_issues"]}


def data_quality_issues_json(im: dict) -> str | None:
    """Serializa issues para coluna TEXT (JSON array)."""
    iss = im.get("data_quality_issues")
    if not iss:
        return None
    return json.dumps(iss, ensure_ascii=False)
