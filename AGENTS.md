# INVEST CERTO – PROJECT_CONTEXT

> Este arquivo e a memoria operacional do projeto. Antes de mudar codigo, leia este contexto e confirme se `README.md` e `data_contracts/` continuam coerentes com a implementacao.

## 1) Objetivo

Construir uma plataforma de apoio a aporte mensal em acoes e FIIs, baseada em dados historicos de preco. O pipeline gera datasets analiticos e ranking; o LLM e apenas camada futura de explicacao, nao motor de decisao.

## 2) Arquitetura atual

- Bronze: coleta via `yfinance`, validacao e persistencia local em parquet.
- Silver: limpeza, deduplicacao, enriquecimento de cadastro e status diario de elegibilidade.
- Gold: features quantitativas e ranking diario para consumo do dashboard.
- App: dashboard local em Streamlit lendo somente a Gold.
- Futuro: S3, Databricks e camada GenAI/RAG.

## 3) Estado implementado

### Bronze

- Entrada: `config/assets.txt`, formato `asset|type|source|ticker`.
- Leitura e validacao de catalogo em `src/collect/reader.py`.
- Coleta em `src/collect/fetcher.py` usando `yfinance`.
- Persistencia em `src/collect/writer.py`.
- Pipeline principal: `pipelines/bronze/collect_prices.py`.
- Consulta local somente leitura: `pipelines/bronze/query_prices.py`.
- Layout fisico:
  ```text
  data/bronze/prices/
    asset=<asset>/
      year=<yyyy>/
        month=<mm>/
          prices_<ingestion_timestamp>.parquet
  ```
- Incremental por ativo:
  - usa `max(date) + 1 dia` quando ja existe historico
  - faz carga completa desde `2015-01-01` quando nao existe historico local
- `date` e normalizada para timestamp naive.
- Se `Adj Close` nao vier do provider, o pipeline preenche `adj_close = close`.
- A validacao Bronze fica em `src/validators/bronze_prices_validator.py`.
  - schema minimo obrigatorio
  - colunas criticas sem nulos
  - ausencia de duplicidade por `asset + date`
  - precos e volume nao negativos
  - consistencia de preco com tolerancia compartilhada `PRICE_TOLERANCE = 1e-3` definida em `src/validators/price_rules.py`
  - a regra aceita ruido de ponto flutuante do provider sem arredondar ou reescrever o dado bruto

### Silver

- Pipeline: `pipelines/silver/transform_prices.py`.
- Saidas:
  - `data/silver/prices_clean`
  - `data/silver/asset_daily_status`
- Implementacao em DuckDB com full refresh.
- `prices_clean`:
  - le a Bronze inteira
  - enriquece com `config/assets.txt`
  - converte `date` para `DATE`
  - remove duplicados exatos por `asset + date`
  - falha em duplicidade conflitante
  - remove anomalia do provider com `open/high/low = 0`, `close > 0`, `volume = 0`
- `asset_daily_status`:
  - calcula gaps de calendario
  - calcula historico minimo em 30, 90 e 252 observacoes
  - deriva `eligibility_status` e `is_feature_eligible`

### Gold

- Pipeline: `pipelines/gold/build_features.py`.
- Saidas:
  - `data/gold/asset_features`
  - `data/gold/ranking_snapshot`
- Full refresh a partir da Silver.
- Materializa score, ranking, buckets de risco/momentum e deltas temporais.
- Queries para dashboard ficam em `queries/gold/`.

### Dashboard

- App principal: `app/streamlit_app.py`.
- Camada de acesso: `app/data_access.py`.
- Consome apenas `data/gold/...` e queries versionadas em `queries/gold/`.

### Contratos e testes

- Contratos versionados em `data_contracts/`.
- Testes existentes cobrem Bronze, query utility, Silver, Gold e acesso do dashboard.
- Ha teste especifico da tolerancia Bronze em `tests/test_bronze_prices_validator.py`.
- A suite usa `pytest`; se o ambiente nao tiver `pytest` instalado, os testes nao executam.

## 4) Regras para continuar o projeto

- Nao mudar o schema Bronze sem atualizar:
  - `data_contracts/bronze_prices.md`
  - `README.md`
  - este arquivo
- Nao remover a estrategia incremental da Bronze.
- Nao introduzir arredondamento destrutivo nos precos de origem.
- Reutilizar `src/validators/price_rules.py` para qualquer regra nova de tolerancia de preco; nao duplicar constantes entre Bronze e Silver.
- Manter Silver e Gold como full refresh ate uma decisao explicita de incremental.
- Priorizar DuckDB para leitura e transformacao local.
- Tratar `date` como `DATE` nas camadas analiticas, salvo necessidade real de `TIMESTAMP`.
- Qualquer nova regra de elegibilidade da Gold deve passar por `asset_daily_status` primeiro.

## 5) Fluxo operacional recomendado

- Sincronizar dependencias:
  ```bash
  uv sync
  ```
- Rodar Bronze:
  ```bash
  PYTHONPATH=. uv run python pipelines/bronze/collect_prices.py
  ```
- Consultar Bronze:
  ```bash
  PYTHONPATH=. uv run python pipelines/bronze/query_prices.py --file queries/bronze/summary_by_asset.sql
  ```
- Rodar Silver:
  ```bash
  uv run python pipelines/silver/transform_prices.py
  ```
- Rodar Gold:
  ```bash
  uv run python pipelines/gold/build_features.py
  ```
- Rodar dashboard:
  ```bash
  uv run streamlit run app/streamlit_app.py
  ```
- Rodar pipeline completo automatizado:
  ```bash
  ./run_pipeline.sh
  ```

## 6) Notas operacionais

- `run_pipeline.sh` executa `uv sync`, Bronze, Silver e Gold em sequencia, grava logs em `logs/` e envia alertas por Telegram via `.env`.
- Se `data/bronze/prices` for removido, a proxima Bronze volta a fazer backfill completo desde `2015-01-01`.
- O utilitario `query_prices.py` e somente leitura e registra a view temporaria `bronze_prices` sobre `data/bronze/prices/**/*.parquet`.
- O projeto pode precisar de `PYTHONPATH=.` em alguns comandos para resolver imports de `src`.
- O README deve refletir a estrutura real:
  - `src/collect`
  - `src/validators`
  - `tests/test_bronze_prices_validator.py`
  - `run_pipeline.sh`

## 7) Backlog real

- Instalar e fixar `pytest` no ambiente do projeto para execucao consistente da suite.
- Expandir testes unitarios de `reader`, `fetcher` e `writer`.
- Evoluir operacionalizacao para S3 e versionamento de datasets.
- Avaliar integracao futura com GenAI/RAG sem acoplar decisoes de investimento ao modelo.

## Como usar este arquivo

Sempre que houver mudanca de estrutura, contrato, regra de qualidade ou fluxo operacional, atualize este arquivo no mesmo conjunto de alteracoes.
