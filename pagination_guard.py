"""
Guard de paginação (MINI-ETAPA 4A): validação central da próxima URL de listagem.

Usado após ``get_next_page_url`` no fluxo multi-site; não altera a detecção de href,
apenas filtra URLs inseguras, repetidas ou incompatíveis.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse, urldefrag, urlunparse


# Códigos estáveis para summary / logs (não é enum para manter JSON simples)
CODE_OK = "ok"
CODE_EMPTY = "next_url_empty"
CODE_MALFORMED = "next_url_malformed"
CODE_SCHEME_FORBIDDEN = "next_url_javascript_or_special"
CODE_ANCHOR_ONLY = "next_url_anchor_only"
CODE_CROSS_DOMAIN = "next_url_cross_domain"
CODE_SAME_PAGE = "next_url_same_page"
CODE_REPEATED = "next_url_repeated"
CODE_SUSPICIOUS = "next_url_suspicious_truncation"


@dataclass
class NextPageGuardResult:
    ok: bool
    code: str
    detail: str
    normalized_url: str | None = None


def _strip_www(host: str) -> str:
    h = host.lower().split("@")[-1].split(":")[0]
    if h.startswith("www."):
        return h[4:]
    return h


def hosts_compatible(site_base_url: str, candidate_url: str) -> bool:
    """Aceita mesmo host, subdomínio do mesmo registrable básico (heurística simples)."""
    bh = _strip_www(urlparse(site_base_url).netloc)
    ch = _strip_www(urlparse(candidate_url).netloc)
    if not bh or not ch:
        return False
    if ch == bh:
        return True
    if ch.endswith("." + bh) or bh.endswith("." + ch):
        return True
    return False


def normalize_pagination_url(url: str) -> str:
    """
    Chave estável para comparação / anti-loop: scheme + netloc + path + query, sem fragmento.
    """
    if not url or not str(url).strip():
        return ""
    raw = str(url).strip()
    base, _frag = urldefrag(raw)
    p = urlparse(base)
    scheme = (p.scheme or "").lower()
    netloc = (p.netloc or "").lower()
    path = p.path if p.path else "/"
    query = p.query if p.query else ""
    if not scheme and not netloc:
        return ""
    if not scheme and netloc:
        scheme = "http"
    return urlunparse((scheme, netloc, path, "", query, ""))


def validate_next_page_url(
    current_url: str,
    next_url: str | None,
    site_base_url: str,
    visited_normalized: set[str],
) -> NextPageGuardResult:
    """
    Valida a URL sugerida para a próxima página de listagem.

    ``visited_normalized`` deve usar as mesmas chaves que ``normalize_pagination_url``.
    """
    if next_url is None or not str(next_url).strip():
        return NextPageGuardResult(
            ok=False,
            code=CODE_EMPTY,
            detail="Próxima URL vazia ou ausente",
        )

    nu = str(next_url).strip()
    low = nu.lower()

    if "\n" in nu or "\r" in nu or "\t" in nu:
        return NextPageGuardResult(
            ok=False,
            code=CODE_MALFORMED,
            detail="URL contém caracteres de controle",
        )

    if low.startswith("javascript:") or low.startswith("data:") or low.startswith("mailto:"):
        return NextPageGuardResult(
            ok=False,
            code=CODE_SCHEME_FORBIDDEN,
            detail="Esquema não HTTP(S)",
        )

    if nu.strip() == "#" or re.match(r"^#[^#]*$", nu.strip()):
        return NextPageGuardResult(
            ok=False,
            code=CODE_ANCHOR_ONLY,
            detail="URL é apenas âncora",
        )

    suspicious_double = len(re.findall(r"https?://", low, re.I)) > 1
    if suspicious_double or "http://http" in low or "https://http" in low:
        return NextPageGuardResult(
            ok=False,
            code=CODE_SUSPICIOUS,
            detail="URL parece concatenada ou truncada (múltiplos esquemas)",
        )

    parsed = urlparse(nu)
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        return NextPageGuardResult(
            ok=False,
            code=CODE_MALFORMED,
            detail=f"Esquema inválido: {scheme or '(vazio)'}",
        )

    if not parsed.netloc:
        return NextPageGuardResult(
            ok=False,
            code=CODE_MALFORMED,
            detail="Host ausente na URL",
        )

    if not hosts_compatible(site_base_url, nu):
        return NextPageGuardResult(
            ok=False,
            code=CODE_CROSS_DOMAIN,
            detail="Host incompatível com o site base",
        )

    norm_next = normalize_pagination_url(nu)
    if not norm_next:
        return NextPageGuardResult(
            ok=False,
            code=CODE_MALFORMED,
            detail="Não foi possível normalizar a URL",
        )

    norm_cur = normalize_pagination_url(current_url)
    if norm_cur and norm_next == norm_cur:
        return NextPageGuardResult(
            ok=False,
            code=CODE_SAME_PAGE,
            detail="Próxima URL é equivalente à página atual",
        )

    if norm_next in visited_normalized:
        return NextPageGuardResult(
            ok=False,
            code=CODE_REPEATED,
            detail="Próxima URL já foi visitada nesta execução",
        )

    if len(norm_next) > 8000:
        return NextPageGuardResult(
            ok=False,
            code=CODE_SUSPICIOUS,
            detail="URL excessivamente longa",
        )

    return NextPageGuardResult(
        ok=True,
        code=CODE_OK,
        detail="ok",
        normalized_url=nu,
    )
