# 🏠 Robô Agregador de Imóveis — Jaraguá do Sul / SC

Varre 53 imobiliárias locais, extrai todos os imóveis à venda,
salva em banco SQLite local e detecta novos / atualizados / removidos.

---

## ⚡ Instalação rápida

### 1. Instale o Python (se não tiver)
Acesse https://python.org e baixe a versão 3.10 ou superior.

### 2. Instale as dependências
Abra o terminal (ou Prompt de Comando) na pasta do projeto e rode:

```bash
pip install -r requirements.txt
```

### 3. Variáveis de ambiente (credenciais)
Credenciais não ficam mais no código. Copie o modelo e ajuste:

```bash
copy .env.example .env
```

Edite o `.env` na pasta do projeto com sua URL e chave do Supabase (e, se usar, `ANTHROPIC_API_KEY`). O programa carrega automaticamente o `.env` ao iniciar. Também é possível definir as mesmas variáveis no sistema ou no agendador de tarefas, sem arquivo `.env`.

---

## 🚀 Como usar

### Rodar o robô completo (todos os 53 sites)
```bash
python scraper.py
```

### Testar um site específico (pelo ID da lista)
```bash
python scraper.py --site 7       # só o d2imoveis.com
python scraper.py --site 1       # só o mouraimoveis.com.br
```

### Ver relatório do banco sem fazer scraping
```bash
python scraper.py --report
```

### Exportar todos os imóveis para CSV
```bash
python scraper.py --export
# gera: imoveis_export.csv
```

---

## 📁 Arquivos gerados

| Arquivo            | O que é                                      |
|--------------------|----------------------------------------------|
| `imoveis.db`       | Banco SQLite com todos os imóveis             |
| `scraper.log`      | Log detalhado de cada execução               |
| `imoveis_export.csv` | Export CSV (gerado com --export)           |

---

## 🤖 Ativar normalização com IA (opcional mas recomendado)

A IA melhora muito a qualidade dos dados: padroniza tipos, 
extrai quartos/bairro de títulos mal formatados, etc.

1. Crie uma conta em https://console.anthropic.com
2. Gere uma API Key
3. No arquivo `.env`, defina:
   ```
   ANTHROPIC_API_KEY=sk-ant-api03-...
   ```

---

## ☁️ Sync com Supabase

O robô pode enviar os imóveis do SQLite para o Supabase após o scraping. Sem `SUPABASE_URL` e `SUPABASE_KEY` (ou `SUPABASE_SERVICE_ROLE_KEY`) no `.env` ou no ambiente, o sync fica desligado e o restante do fluxo segue normal.

Opções no `.env`: `SUPABASE_TABLE` (padrão `properties`), `SUPABASE_BATCH_SIZE` (padrão `100`).

---

## ⏰ Agendar execução semanal

### Windows (Task Scheduler)
1. Abra o Agendador de Tarefas
2. Criar Tarefa Básica → nome "Robô Imóveis"
3. Disparador: Semanalmente, toda segunda às 07:00
4. Ação: Iniciar programa
   - Programa: `python`
   - Argumentos: `scraper.py`
   - Iniciar em: `C:\caminho\para\a\pasta`

### Mac / Linux (cron)
```bash
crontab -e
# Adicione esta linha (toda segunda às 7h):
0 7 * * 1 cd /caminho/para/pasta && python scraper.py >> scraper.log 2>&1
```

---

## 🗄️ Consultar o banco diretamente (SQL)

Use o DB Browser for SQLite (gratuito): https://sqlitebrowser.org

Exemplos de consultas úteis:

```sql
-- Imóveis novos da última semana
SELECT titulo, preco_texto, quartos, bairro, site_name, url_anuncio
FROM imoveis
WHERE ativo = 1 AND primeira_vez > date('now', '-7 days')
ORDER BY primeira_vez DESC;

-- Apartamentos com 3+ quartos até R$600k
SELECT titulo, preco, quartos, bairro, site_name, url_anuncio
FROM imoveis
WHERE ativo = 1 AND tipo = 'apartamento'
  AND quartos >= 3 AND preco <= 600000
ORDER BY preco;

-- Imóveis que baixaram de preço
SELECT i.titulo, i.preco_texto, h1.preco AS preco_anterior, h2.preco AS preco_atual
FROM historico_precos h1
JOIN historico_precos h2 ON h1.imovel_id = h2.imovel_id
JOIN imoveis i ON i.id = h1.imovel_id
WHERE h2.preco < h1.preco AND i.ativo = 1;

-- Sites com mais imóveis
SELECT site_name, COUNT(*) as total
FROM imoveis WHERE ativo = 1
GROUP BY site_name ORDER BY total DESC;
```

---

## 🔧 Configurações

### Via ambiente / `.env` (sensível)

| Variável | Obrigatório | O que faz |
|----------|-------------|-----------|
| `SUPABASE_URL` | Para sync nuvem | URL do projeto Supabase |
| `SUPABASE_KEY` ou `SUPABASE_SERVICE_ROLE_KEY` | Para sync nuvem | Chave service_role |
| `SUPABASE_TABLE` | Não | Nome da tabela (padrão `properties`) |
| `SUPABASE_BATCH_SIZE` | Não | Tamanho do lote REST (padrão `100`) |
| `ANTHROPIC_API_KEY` | Não | Normalização com IA |

### No código (`scraper.py`, não sensível)

| Variável              | Padrão | O que faz                          |
|-----------------------|--------|------------------------------------|
| `REQUEST_TIMEOUT`     | 20s    | Tempo máximo por requisição        |
| `DELAY_BETWEEN_SITES` | 2s     | Pausa entre sites (não sobrecarrega)|
| `MAX_PAGES_PER_SITE`  | 50     | Máximo de páginas por imobiliária  |

---

## ❓ Problemas comuns

**"ModuleNotFoundError: No module named 'requests'"**
→ Rode: `pip install -r requirements.txt`

**Muitos sites com erro**
→ Alguns sites podem bloquear scrapers. Solução: aumente o `DELAY_BETWEEN_SITES` para 5.

**Site retorna 0 imóveis mas tem imóveis no ar**
→ O site pode usar JavaScript para carregar os imóveis (SPA/React).
→ Próximo passo: usar Playwright (veja abaixo).

---

## 🚀 Próximos passos sugeridos

1. **App para clientes** — interface web para buscar nos imóveis do banco
2. **Playwright** — para sites que carregam com JavaScript
3. **Supabase** — já suportado via `.env`; veja seção acima
4. **WhatsApp/Email alerts** — notificar clientes quando aparecer imóvel compatível
