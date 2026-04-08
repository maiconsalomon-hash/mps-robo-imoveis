from playwright.sync_api import sync_playwright
import re

with sync_playwright() as p:
    # Usa chromium com perfil real, não headless
    browser = p.chromium.launch(
        headless=False,  # janela visível
        args=['--disable-blink-features=AutomationControlled']
    )
    context = browser.new_context(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        viewport={'width': 1280, 'height': 800},
    )
    page = context.new_page()
    
    # Remove sinal de automação
    page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    page.goto('https://d2imoveis.com/imoveis/venda', wait_until='networkidle')
    page.wait_for_selector('.LI_Imovel', timeout=15000)

    todos = set()
    sem_mudanca = 0

    for i in range(100):
        atual = page.evaluate("document.querySelectorAll('.LI_Imovel').length")
        urls = set(re.findall(r'href="(https://d2imoveis\.com/\d+)"', page.content()))
        novos = urls - todos
        todos.update(urls)
        print(f"Scroll {i+1}: {atual} cards | {len(novos)} novos | total={len(todos)}")
        
        if not novos:
            sem_mudanca += 1
            if sem_mudanca >= 3:
                break
        else:
            sem_mudanca = 0

        # Scroll até o último card
        page.evaluate("""
            const cards = document.querySelectorAll('.LI_Imovel');
            cards[cards.length-1].scrollIntoView({behavior: 'smooth'});
        """)
        page.wait_for_timeout(2500)

    print(f"\nTotal: {len(todos)}")
    browser.close()