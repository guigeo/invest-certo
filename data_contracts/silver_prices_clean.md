# Data Contract: Silver `prices_clean`

## 1. Descricao do dataset

O dataset `prices_clean` da camada Silver representa a versao normalizada e consolidada do historico de precos vindo da Bronze.

O objetivo desta camada e servir como base canonica para calculo de features e ranking na Gold, removendo ambiguidades operacionais da Bronze sem descaracterizar o historico de mercado.

Fonte de entrada:

* origem: Bronze `prices`
* granularidade: diaria
* formato de processamento esperado: DuckDB
* local de armazenamento proposto: `data/silver/prices_clean`

## 2. Estrutura fisica do dataset

Layout proposto:

```text
data/silver/prices_clean/
  year=<yyyy>/
    month=<mm>/
      prices_clean_<run_date>.parquet
```

Observacoes:

* `run_date` representa a data de execucao da pipeline Silver no formato `YYYYMMDD`.
* o dataset Silver pode ser reprocessado integralmente a partir da Bronze.
* diferentemente da Bronze, o objetivo aqui nao e refletir o lote de ingestao, e sim entregar uma visao consolidada e confiavel.

## 3. Schema persistido no Parquet

| Coluna | Tipo | Obrigatoria | Descricao |
|---|---|---|---|
| `date` | `date` | Sim | Data de negociacao do ativo, sem timezone. |
| `asset` | `string` | Sim | Identificador logico do ativo no projeto. |
| `ticker` | `string` | Sim | Ticker consultado no provider. |
| `asset_type` | `string` | Sim | Tipo do ativo vindo do cadastro. Ex.: `stock`, `fii`. |
| `source` | `string` | Sim | Fonte declarada no cadastro. Hoje o valor esperado e `yahoo`. |
| `open` | `double` | Sim | Preco de abertura do dia. |
| `high` | `double` | Sim | Maior preco negociado no dia. |
| `low` | `double` | Sim | Menor preco negociado no dia. |
| `close` | `double` | Sim | Preco de fechamento do dia. |
| `adj_close` | `double` | Sim | Preco ajustado do dia. |
| `volume` | `bigint` | Sim | Volume negociado do dia. |
| `is_volume_missing` | `boolean` | Sim | Flag para indicar volume originalmente ausente ou nao confiavel antes do tratamento da Silver. |

## 4. Regras de transformacao

Regras esperadas da Bronze para a Silver:

* leitura de todos os parquets da Bronze `prices`
* cast explicito de `date` para `DATE`
* normalizacao de tipos numericos
* enriquecimento com `asset_type` e `source` a partir de `config/assets.txt`
* remocao de duplicados por chave `asset + date`
* ordenacao logica por `asset, date`

## 5. Regras de qualidade de dados

### 5.1 Integridade de schema

O dataset deve conter exatamente as colunas previstas neste contrato.

### 5.2 Chave unica

A chave unica logica da Silver e:

* `asset + date`

Nao deve existir mais de um registro por ativo por dia no dataset consolidado.

### 5.3 Valores nulos

Nao sao permitidos valores nulos em:

* `date`
* `asset`
* `ticker`
* `asset_type`
* `source`
* `open`
* `high`
* `low`
* `close`
* `adj_close`
* `volume`
* `is_volume_missing`

### 5.4 Regras numericas

* `open`, `high`, `low`, `close`, `adj_close` devem ser maiores ou iguais a zero
* `volume` deve ser maior ou igual a zero
* `low <= close <= high`
* `low <= open <= high`

### 5.5 Cobertura cadastral

Todo `asset` presente na Silver deve existir em `config/assets.txt`.

## 6. Politica para dados faltantes

Diretriz inicial:

* valores de preco nao devem ser imputados
* registros com preco invalido devem quebrar a pipeline
* `volume` ausente pode ser convertido para `0` desde que `is_volume_missing = true`

Essa regra permite manter o dataset utilizavel sem mascarar perda de qualidade.

## 7. Diferencas em relacao a Bronze

Principais diferencas esperadas:

* `date` passa de timestamp naive para `DATE`
* `asset_type` e `source` passam a existir como colunas persistidas
* duplicidades consolidadas deixam de ser aceitaveis
* a Silver vira a fonte canonica para features da Gold

## 8. O que deve quebrar a pipeline

Falhas que devem interromper a execucao:

* ausencia de colunas obrigatorias na Bronze
* duplicidade remanescente por `asset + date` apos consolidacao
* preco nulo ou negativo
* ativo fora do cadastro
* valores de data invalidos

