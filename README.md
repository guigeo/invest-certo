# 📊 Invest Certo (RAG + Data Engineering)

Projeto para análise de portfólio de ações com foco em engenharia de dados e evolução para GenAI.

O objetivo é identificar, de forma orientada a dados, qual ativo está mais atrativo para aporte em um determinado período, deixando o LLM apenas como camada de explicação.

---

## 🧱 Arquitetura (estado atual)

* Ingestão de dados via scripts Python
* Camada Bronze salva localmente em parquet
* Coleta incremental por ativo com base na última `date` persistida
* Particionamento Bronze por `asset/year/month`
* Evolução futura para DuckDB (Silver/Gold), S3 e Databricks + GenAI

---

## 📂 Estrutura do Projeto

```text
invest-certo/

├── config/
│   └── assets.txt                 # Lista de ativos monitorados
│
├── data_contracts/
│   ├── bronze.md                  # Definição da camada Bronze
│   ├── silver.md                  # Definição da camada Silver
│   └── gold.md                    # Definição da camada Gold
│
├── pipelines/
│   ├── bronze/
│   │   └── collect_prices.py      # Pipeline de ingestão de dados
│   ├── silver/
│   │   └── transform_prices.py    # Limpeza e transformação
│   └── gold/
│       └── build_features.py      # Criação de métricas e ranking
│
├── src/
│   ├── collect/                   # Leitura, coleta e escrita da Bronze
│   ├── transform/                 # Regras de transformação
│   └── utils/                     # Funções auxiliares
│
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

---

## 🧪 Validação

Existe uma validação da Bronze em `tests/test_bronze_data.py`, pensada para conferir:

* presença das colunas esperadas
* datas válidas
* ausência de duplicidade por `asset`, `ticker` e `date`
* consistência de último preço por ativo

Se o ambiente tiver `pytest` instalado, o teste pode ser executado com:

```bash
PYTHONPATH=. ./.venv/bin/python -m pytest tests/test_bronze_data.py
```

---

## 🚀 Roadmap

### Fase 1 - Base de Dados

* [x] Coleta de preços históricos
* [x] Persistência local em parquet na Bronze
* [ ] Armazenamento em S3

### Fase 2 - Transformação

* [ ] Criação da camada Silver com DuckDB
* [ ] Criação da camada Gold com métricas e ranking
* [ ] Formalização dos contratos de dados

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
* Silver e Gold ainda estão como placeholders
* A estrutura foi pensada para futura migração para Databricks
