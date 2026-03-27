# INVEST CERTO – PROJECT_CONTEXT

> Este arquivo é a “memória” do projeto. Antes de mexer em qualquer coisa, **leia este contexto** para alinhar como o pipeline foi pensado, o que já existe e o que falta.

## 1) Objetivo do projeto
Construir uma plataforma simples de **recomendação de aportes mensais** (renda variável e FIIs), com base em dados históricos de preços, gerando **ranking/métricas** e usando **LLM apenas como camada de explicação** (RAG/GenAI **não** decide nada sozinho).

## 2) Arquitetura planejada (Bronze → Silver → Gold + GenAI)
- **Bronze**: coleta (raw) -> histórico de preços
- **Silver**: limpeza/normalização -> tabelas prontas para feature engineering
- **Gold**: features + ranking -> “opções de aportes” para o usuário
- **GenAI**: camada textual em cima do **Gold** (explicar e justificar escolhas)
- Futuro: apoiar infra em **DuckDB/S3/Databricks** (README fala isso)

## 3) O que já está implementado (estado atual)

### 3.1 Bronze (coleta)
- Script: `pipelines/bronze/collect_prices.py`
- Consulta local: `pipelines/bronze/query_prices.py`
- Entrada: `config/assets.txt`
  - Formato esperado das linhas: `asset|type|source|ticker`
  - `src/collect/reader.py` valida:
    - todos os campos existem
    - `asset` e `ticker` são únicos (sem duplicação)
- Fonte: `yfinance` (`src/collect/fetcher.py`)
  - Schema exigido: `date, open, high, low, close, adj_close, volume, asset, ticker`
  - Falta de colunas relevantes estoura erro (isso é bom, evita drift silencioso)
- Persistência: `src/collect/writer.py`
  - Output local (por enquanto): `data/bronze/prices`
  - Particionamento:
    ```
    data/bronze/prices/
      asset=<asset>/
        year=<yyyy>/
          month=<mm>/
            prices_<ingestion_timestamp>.parquet
    ```
  - O incremental funciona assim:
    - pega a **maior `date`** já armazenada por `asset`
    - começa a coleta no **dia seguinte**
    - se não existe histórico, pega desde `2015-01-01`
  - Arquivos parquet são salvos com colunas `year` e `month` removidas.

**Decisões implícitas até aqui:**
- **`date` sem timezone** (naive) dentro dos dataframes/parquets (converter para UTC e depois remover tz).
- O “S3” do README ainda **não foi aplicado**: hoje o Bronze salva localmente.
- A forma recomendada de explorar a Bronze localmente e via DuckDB, usando a view temporaria `bronze_prices`.

### 3.2 Silver / Gold
- Existem scripts placeholders:
  - `pipelines/silver/transform_prices.py`
  - `pipelines/gold/build_features.py`
- Contratos já definidos:
  - `data_contracts/bronze_prices.md`
  - `data_contracts/silver_prices_clean.md`
  - `data_contracts/gold_asset_features.md`
  - `data_contracts/gold_ranking_snapshot.md`
- Ou seja: os contratos-base já foram formalizados, mas a implementação de Silver e Gold ainda falta.

## 4) Checklist / backlog (ordem sugerida)
1. Implementar **Silver** usando DuckDB:
   - ler parquet do Bronze (particionado)
   - filtrar colunas e tipos corretos
   - remover duplicados (por `asset`,`date`)
   - lidar com dados faltantes (volume etc.) de forma consciente
   - output em `data/silver/...` (a definir no contrato)
2. Implementar **Gold**:
   - saída `asset_features` por ativo/data
   - saída `ranking_snapshot` por data de referência
   - features iniciais: retorno, volatilidade, drawdown, tendência, métricas de “qualidade” (p.ex. sharpe simplificado)
3. **Alinhar com README**:
   - introduzir S3/Boto3 (upload/download)
   - versionamento de datasets
4. Testes:
   - unit tests para reader/fetcher/writer
   - validação de schema (colunas, tipos) no Silver/Gold.

## 5) Regras para continuar o projeto (Codex)
Quando o Codex for “continuar”:
- **Não reimplementar** o Bronze; respeitar:
  - schema exigido
  - incrementais via max(date)+1
  - particionamento por `asset/year/month`
- Para Silver e Gold:
  - priorizar DuckDB para consultas (já está como dependência)
  - tratar `date` como `DATE` para operações diárias e `TIMESTAMP` apenas onde fizer sentido
  - evitar “sobra” de timezone (padronizar UTC→naive como no Bronze)
  - usar a Silver como camada canônica para cálculo da Gold
  - separar a Gold em pelo menos dois datasets: `asset_features` e `ranking_snapshot`
  - Sempre alinhar o código com os contratos que você for criar (esse arquivo + `data_contracts/`).

## 6) Comandos rápidos (para rodar no desenvolvimento)
- Rodar Bronze:
  ```
  PYTHONPATH=. ./.venv/bin/python pipelines/bronze/collect_prices.py
  ```
- Consultar Bronze:
  ```
  PYTHONPATH=. ./.venv/bin/python pipelines/bronze/query_prices.py --file queries/bronze/summary_by_asset.sql
  ```
- (Futuro) Silver e Gold: mantenha o padrão:
  ```
  PYTHONPATH=. ./.venv/bin/python pipelines/silver/transform_prices.py
  PYTHONPATH=. ./.venv/bin/python pipelines/gold/build_features.py
  ```

## 7) Notas operacionais importantes
- O `fetcher` pode não receber `Adj Close` do `yfinance` em alguns ativos/execuções.
  - Nesses casos, o Bronze preenche `adj_close` com o valor de `close` para manter o schema estável.
- A coleta incremental depende do histórico já salvo localmente em `data/bronze/prices`.
  - Se a pasta for removida, a próxima execução volta a fazer carga completa desde `2015-01-01`.
- O layout vigente do Bronze é:
  ```
  data/bronze/prices/
    asset=<asset>/
      year=<yyyy>/
        month=<mm>/
          prices_<ingestion_timestamp>.parquet
  ```
- O projeto hoje usa `.venv` local para execução.
  - Se o import de `src` falhar, o `PYTHONPATH=.` precisa estar presente no comando.
- O utilitario de consulta da Bronze e somente leitura.
  - Ele registra a view temporaria `bronze_prices` apontando para `data/bronze/prices/**/*.parquet`.
  - As consultas sao fornecidas por arquivos `.sql` em `queries/bronze/`.

---

## Como usar este arquivo
Sempre que você fizer mudanças relevantes (estrutura, schema, regras de negócio), atualize este arquivo e faça o Codex “começar por aqui”.
