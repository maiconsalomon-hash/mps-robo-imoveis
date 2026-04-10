"""Diagnóstico ERRO_REQUISICAO — rodar: py -3 _diagnose_sites_fetch.py"""
from __future__ import annotations

import sys
import time
from urllib.parse import urlparse

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import requests

URLS = [
    (5, "sollusimobiliaria", "https://www.sollusimobiliaria.com.br/imoveis/a-venda"),
    (8, "atlantaimoveis", "https://atlantaimoveis.com/imoveis/a-venda"),
    (19, "imobiliariapradi", "https://www.imobiliariapradi.com.br/imoveis/a-venda"),
    (29, "luciannerodrigues", "https://luciannerodrigues.com.br/imoveis/a-venda"),
    (41, "divinacasaimobiliaria", "https://www.divinacasaimobiliaria.com.br/imoveis/a-venda"),
    (47, "engetecimoveis", "https://www.engetecimoveis.com.br/imoveis-venda"),
]

HEADERS_A = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

HEADERS_B = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


def try_get(label: str, fn) -> tuple[str, str]:
    try:
        r = fn()
        le = len(r.text or "")
        raw = (r.text or "")[:200].replace("\n", " ")
        snip = raw.encode("ascii", "replace").decode("ascii")
        return f"{label}: status={r.status_code} len={le}", snip
    except requests.exceptions.SSLError as e:
        return f"{label}: SSL_ERROR {e!r}", ""
    except requests.exceptions.Timeout as e:
        return f"{label}: TIMEOUT {e!r}", ""
    except requests.exceptions.ConnectionError as e:
        return f"{label}: CONN_ERR {e!r}", ""
    except requests.exceptions.HTTPError as e:
        r = e.response
        st = r.status_code if r is not None else "?"
        return f"{label}: HTTPError status={st}", ""
    except Exception as e:
        return f"{label}: {type(e).__name__} {e!r}", ""


def main() -> None:
    try:
        import cloudscraper

        has_cs = True
    except ImportError:
        has_cs = False
        print("(cloudscraper não instalado — Teste D ignorado)\n")

    for sid, name, listing in URLS:
        print(f"=== id={sid} {name} ===")
        pu = urlparse(listing)
        origin = f"{pu.scheme}://{pu.netloc}"

        def a():
            return requests.get(listing, headers=HEADERS_A, timeout=25)

        def b():
            return requests.get(listing, headers=HEADERS_B, timeout=25)

        for label, fn in [("A_basic", a), ("B_browser", b)]:
            msg, snip = try_get(label, fn)
            print(msg)
            if "status=200" in msg and "len=" in msg:
                print("  snip:", snip[:120])

        def c():
            s = requests.Session()
            s.headers.update(HEADERS_B)
            h = s.get(origin + "/", timeout=25)
            h.raise_for_status()
            time.sleep(0.3)
            r = s.get(listing, headers={**HEADERS_B, "Referer": origin + "/"}, timeout=25)
            r.raise_for_status()
            return r

        msg, snip = try_get("C_session_home_referer", c)
        print(msg)
        if "status=200" in msg:
            print("  snip:", snip[:120])

        if has_cs:
            def d():
                scraper = cloudscraper.create_scraper()
                r = scraper.get(listing, timeout=25)
                r.raise_for_status()
                return r

            msg, snip = try_get("D_cloudscraper", d)
            print(msg)
            if "status=200" in msg:
                print("  snip:", snip[:120])

        print()


if __name__ == "__main__":
    main()
