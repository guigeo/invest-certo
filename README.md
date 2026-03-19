# 📊 Invest Certo (RAG + Data Engineering)

Projeto para análise de portfólio de ações com foco em engenharia de dados e evolução para GenAI (RAG + LLM).

O objetivo é identificar, de forma orientada a dados, qual ativo está mais atrativo para aporte em um determinado período.

---

## 🧱 Arquitetura (inicial)

* Ingestão de dados via scripts Python (rodando em servidor local 24/7)
* Armazenamento de dados brutos em S3 (camada Bronze)
* Processamento local utilizando DuckDB (Silver e Gold)
* Evolução futura para Databricks + GenAI (RAG + LLM)

---

## 📂 Estrutura do Projeto

```
portfolio-advisor/

├── config/
│   └── assets.txt                # Lista de ativos monitorados
│
├── data_contracts/
│   ├── bronze.md                # Definição da camada Bronze
│   ├── silver.md                # Definição da camada Silver
│   └── gold.md                  # Definição da camada Gold
│
├── pipelines/
│   ├── bronze/
│   │   └── collect_prices.py    # Pipeline de ingestão de dados
│   │
│   ├── silver/
│   │   └── transform_prices.py  # Limpeza e transformação
│   │
│   └── gold/
│       └── build_features.py    # Criação de métricas e ranking
│
├── src/
│   ├── collect/                 # Lógica de coleta
│   ├── transform/               # Regras de transformação
│   └── utils/                   # Funções auxiliares
│
├── tests/                       # Testes (futuro)
│
├── .env                         # Variáveis de ambiente
├── README.md
```

---

## 🚀 Roadmap (alto nível)

### Fase 1 - Base de Dados

* [ ] Coleta de preços históricos
* [ ] Armazenamento em S3 (Bronze)

### Fase 2 - Transformação

* [ ] Criação da camada Silver (métricas básicas)
* [ ] Criação da camada Gold (ranking de ativos)

### Fase 3 - GenAI

* [ ] Integração com LLM
* [ ] Implementação de RAG com notícias e contexto
* [ ] Explicação automatizada das decisões

---

## 🎯 Objetivo final

Dado um conjunto de ativos, responder:

* Qual ativo está mais atrativo para aporte?
* Qual o ranking entre os ativos?
* Qual a explicação baseada em dados e contexto?

---

## ⚠️ Observações

* Projeto orientado a arquitetura de dados (não apenas IA)
* LLM será utilizado como camada de explicação, não como motor de decisão
* Estrutura pensada para futura migração para Databricks

---
