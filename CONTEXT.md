## Estado atual do robô (v2 — em produção no Railway)

- [x] Robô v2 completo — FastAPI + scheduler automático em thread paralela
- [x] Ciclo de vida completo: NEW / UNCHANGED / PRICE_CHANGE / CONTENT_CHANGE / REMOVED / REAPPEARED
- [x] Histórico por run (imovel_historico) com dedupe de status
- [x] Snapshot por site + detecção de removidos com proteção contra falso positivo
- [x] Normalização de preço (preco_numeric, formato BR)
- [x] Hash inteligente baseado em 5 campos (preço, área, quartos, cidade, bairro)
- [x] Filtro de qualidade no sync (score mínimo 40, sem identidade legacy)
- [x] Sync incremental com checkpoint (só envia o que mudou)
- [x] Família gerenciarimoveis_cf / Tecimob — 5 sites via API (mouraimoveis, lotus, yatil, adrivani, michaelsalomon)
- [x] Diagnóstico por site (OK / SUSPEITO_* / ERRO_*)
- [x] Baseline histórico + anomalia por volume
- [x] Paginação robusta com guard, anti-loop, corte de cauda
- [x] Identidade estável (canonical_url > fingerprint > legacy)
- [x] Score de qualidade por imóvel (HIGH / MEDIUM / LOW)
- [x] Histórico de saúde por site (DEGRADING, FLAPPING, CONSISTENTLY_BROKEN)
- [x] Retry inteligente por site (ERRO_REQUISICAO / ERRO_RENDERIZACAO)
- [x] Modo --only-errors / --status / --sites para reprocessamento seletivo
- [x] API REST: GET /api/v1/properties, GET /api/v1/sources, POST /sources/run-all, GET /health
- [x] ROBOT_API_KEY via variável de ambiente (hoje: 123 — trocar antes de distribuir)

## Pendente — robô (Fase 1)

- [ ] Corrigir ERRO_PAGINACAO — 12 sites (imobimobiliaria, d2, interimob, vivenda, chale, m2jaragua, seculus, macroimoveis, poderimoveis, imoveiscidade, eccorretores, deocari, schroeder, girolla)
- [ ] Corrigir ERRO_LISTAGEM_INVALIDA restantes — 13 sites sem família detectada
- [ ] Corrigir ERRO_REQUISICAO — 6 sites (sollus, atlantaimoveis, pradi, luciannerodrigues, engetecimoveis, divinacasa)
- [ ] Corrigir Itaivan (ERRO_RENDERIZACAO — Playwright no Railway precisa Dockerfile)
- [ ] Enrich details em todas as fontes (quartos, vagas, m²)
- [ ] Ajuste: snapshot usar canonical_url em vez de hash
- [ ] Ajuste: ordem de identidade canonical_url > hash > legacy
- [ ] Ajuste: COALESCE(total_coletas, 0) + 1
- [ ] Ajuste: normalize_price suportar formato US (1234.56)
- [ ] Ajuste: filtro de sanidade de preço (< 10k ou > 100M → None)
- [ ] Ajuste: first_status_in_run no histórico
- [ ] Ajuste: run_id nunca None — gerar automático
- [ ] Ajuste: PRAGMA foreign_keys = ON na conexão
- [ ] Trocar ROBOT_API_KEY=123 por chave forte

## Pendente — app mobile (Fase 2)

- [ ] Foto michael.jpg em assets/images/ (tirada do onboarding temporariamente)
- [ ] Mostrar quartos/vagas/m² nos cards do feed (após enrich)
- [ ] Tags "Novo hoje" e "Preço caiu" nos cards
- [ ] Filtros avançados no feed (além do perfil)
- [ ] Mensagem WhatsApp pré-formatada com dados do imóvel
- [ ] Skeleton loading nos cards
- [ ] Tratamento de erro offline
- [ ] Edição de perfil sem refazer onboarding
- [ ] Notificações push de novos imóveis
- [ ] Deploy EAS para distribuição

## Pendente — segurança / LGPD (Fase 3 — antes de clientes reais)

- [ ] Trocar ROBOT_API_KEY=123 por chave forte
- [ ] Rate limiting na API do robô
- [ ] Ativar RLS no Supabase
- [ ] Migrar dados sensíveis do perfil para expo-secure-store
- [ ] Tela de consentimento LGPD no onboarding
- [ ] Política de privacidade publicada
- [ ] Botão "Excluir minha conta e dados"
- [ ] Deploy EAS para distribuição (TestFlight + Google Play interno)
