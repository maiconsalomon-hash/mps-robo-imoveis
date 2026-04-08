import re

import requests

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
r = requests.get("https://d2imoveis.com/imoveis/venda", headers=headers)
codigos = set(re.findall(r'href=["\']/(\d+)["\'»]', r.text))
print("Links de imoveis encontrados:", len(codigos))
print("Primeiros 10:", sorted(codigos)[:10])
print("Tamanho do HTML:", len(r.text), "chars")
with open("debug_d2.html", "w", encoding="utf-8") as f:
    f.write(r.text)
print("HTML salvo em debug_d2.html")
