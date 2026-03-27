# 📊 Invest Certo (RAG + Data Engineering)

Projeto para análise de portfólio de ações com foco em engenharia de dados e evolução para GenAI.

O objetivo é identificar, de forma orientada a dados, qual ativo está mais atrativo para aporte em um determinado período, deixando o LLM apenas como camada de explicação.

---

## 🧱 Arquitetura (estado atual)

* Ingestão de dados via scripts Python
* Camada Bronze salva localmente em parquet
* Coleta incremental por ativo com base na última `date` persistida
* Particionamento Bronze por `asset/year/month`
* Silver implementada em DuckDB com duas visões físicas
* Gold implementada com features, ranking e queries para dashboard
* Evolução futura para S3, Databricks e camada GenAI

---

## 📂 Estrutura do Projeto

```text
invest-certo/

├── config/
│   └── assets.txt                 # Lista de ativos monitorados
│
├── data_contracts/
│   ├── bronze_prices.md           # Contrato da camada Bronze
│   ├── silver_prices_clean.md     # Contrato da camada Silver
│   ├── gold_asset_features.md     # Contrato de features da Gold
│   └── gold_ranking_snapshot.md   # Contrato do ranking da Gold
│
├── pipelines/
│   ├── bronze/
│   │   ├── collect_prices.py      # Pipeline de ingestão de dados
│   │   └── query_prices.py        # Consulta SQL na Bronze
│   ├── silver/
│   │   └── transform_prices.py    # Gera prices_clean e asset_daily_status
│   └── gold/
│       └── build_features.py      # Gera asset_features e ranking_snapshot
│
├── src/
│   ├── collect/                   # Leitura, coleta e escrita da Bronze
│   ├── transform/                 # Regras de transformação
│   └── utils/                     # Funções auxiliares
│
├── queries/
│   ├── bronze/                    # Queries SQL reutilizáveis da Bronze
│   ├── silver/                    # Queries SQL reutilizáveis da Silver
│   └── gold/                      # Queries SQL para o dashboard da Gold
├── data/                          # Dados gerados localmente
├── tests/                         # Validações e testes do pipeline
├── AGENTS.md                      # Memória operacional do projeto
├── pyproject.toml
└── README.md
```

---

## 🚀 Bronze Hoje

O Bronze já está funcional e faz:

* leitura dos ativos a partir de `config/assets.txt`
* validação de linhas inválidas, campos vazios e duplicidade de `asset` e `ticker`
* coleta com `yfinance`
* normalização do schema para `date, open, high, low, close, adj_close, volume, asset, ticker`
* fallback de `adj_close = close` quando `Adj Close` não vier do provider
* gravação incremental por ativo
* persistência em parquet com particionamento por empresa, ano e mês
* consulta local via DuckDB sobre todos os parquets da Bronze

Layout atual:

```text
data/bronze/prices/
  asset=<asset>/
    year=<yyyy>/
      month=<mm>/
        prices_<ingestion_timestamp>.parquet
```

Regra do incremental:

* se o ativo ainda não tem histórico local, a coleta começa em `2015-01-01`
* se já existe histórico, a coleta começa em `max(date) + 1 dia`
* se não houver novos dados, o ativo é reportado como sem atualização

Comando para rodar:

```bash
PYTHONPATH=. ./.venv/bin/python pipelines/bronze/collect_prices.py
```

Comando para consultar:

```bash
PYTHONPATH=. ./.venv/bin/python pipelines/bronze/query_prices.py --file queries/bronze/summary_by_asset.sql
```

A view temporária disponível nas queries é `bronze_prices`, apontando para todos os arquivos em `data/bronze/prices/**/*.parquet`.

Exemplos:

```bash
PYTHONPATH=. ./.venv/bin/python pipelines/bronze/query_prices.py --file queries/bronze/summary_by_asset.sql
PYTHONPATH=. ./.venv/bin/python pipelines/bronze/query_prices.py --file queries/bronze/date_range_by_asset.sql
PYTHONPATH=. ./.venv/bin/python pipelines/bronze/query_prices.py --file queries/bronze/latest_rows_for_asset.sql --limit 10
```

---

## 🧪 Validação

Existe uma validação da Bronze em `tests/test_bronze_data.py`, pensada para conferir:

* presença das colunas esperadas
* datas válidas
* ausência de duplicidade por `asset`, `ticker` e `date`
* consistência de último preço por ativo
* execução de consultas SQL na Bronze e tratamento de erros do utilitário
* transformação da Silver e regras de elegibilidade diária
* construção da Gold para dashboard e ranking de aporte

Se o ambiente tiver `pytest` instalado, o teste pode ser executado com:

```bash
PYTHONPATH=. ./.venv/bin/python -m pytest tests/test_bronze_data.py tests/test_bronze_query.py tests/test_silver_transform.py tests/test_gold_build_features.py
```

---

## 🥈 Silver Hoje

A Silver já está implementada em DuckDB e gera duas visões físicas:

* `data/silver/prices_clean`: base canônica, tipada, deduplicada e enriquecida com cadastro
* `data/silver/asset_daily_status`: visão diária de qualidade e prontidão analítica por ativo

O pipeline faz:

* leitura integral da Bronze
* join com `config/assets.txt`
* cast de `date` para `DATE`
* remoção de duplicados exatos por `asset + date`
* falha quando houver duplicidade conflitante
* exclusão de snapshots anômalos do provider com `open/high/low = 0`, `close > 0` e `volume = 0`
* flags de histórico mínimo em 30, 90 e 252 observações
* flags de gap de calendário e elegibilidade para futura Gold

Comando para rodar:

```bash
PYTHONPATH=. ./.venv/bin/python pipelines/silver/transform_prices.py
```

---

## 🥇 Gold Hoje

A Gold já está implementada e gera duas visões físicas:

* `data/gold/asset_features`: base diária de sinais quantitativos para gráficos, análise e ranking
* `data/gold/ranking_snapshot`: fotografia diária da recomendação relativa de aporte

O pipeline faz:

* leitura integral da Silver
* cálculo de retorno, volatilidade, drawdown, médias móveis e sharpe simplificado
* buckets discretos de momentum e risco
* score `v1` auditável para ordenação dos ativos
* deltas de score e ranking em horizontes comparáveis de 7 e 30 dias
* materialização de queries em `queries/gold/` para:
  * recomendações mais recentes
  * histórico de preço e tendência
  * histórico de ranking
  * overview agregado de mercado

Comando para rodar:

```bash
PYTHONPATH=. ./.venv/bin/python pipelines/gold/build_features.py
```

---

## 🚀 Roadmap

### Fase 1 - Base de Dados

* [x] Coleta de preços históricos
* [x] Persistência local em parquet na Bronze
* [x] Consulta SQL local da Bronze com DuckDB
* [x] Implementação inicial da Silver com duas visões
* [ ] Armazenamento em S3

### Fase 2 - Transformação

* [x] Criação da camada Silver com DuckDB
* [x] Criação da camada Gold com métricas e ranking
* [x] Formalização inicial dos contratos de dados

### Fase 3 - GenAI

* [ ] Integração com LLM
* [ ] Implementação de RAG com notícias e contexto
* [ ] Explicação automatizada das decisões

---

## 🎯 Objetivo Final

Dado um conjunto de ativos, responder:

* qual ativo está mais atrativo para aporte
* qual o ranking entre os ativos
* qual a explicação baseada em dados e contexto

---

## ⚠️ Observações

* Projeto orientado a arquitetura de dados, não apenas IA
* O LLM será utilizado como camada de explicação, não como motor de decisão
* A Bronze hoje salva localmente; S3 ainda não foi implementado
* A Bronze pode ser explorada localmente via DuckDB usando o script de consulta e arquivos `.sql`
* A Silver já materializa `prices_clean` e `asset_daily_status`
* A Gold já materializa `asset_features` e `ranking_snapshot`
* O dashboard pode consumir `queries/gold/` sem replicar regra de negócio
* A estrutura foi pensada para futura migração para Databricks
