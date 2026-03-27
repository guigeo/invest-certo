# Data Contract: Gold `ranking_snapshot`

## 1. Descricao do dataset

O dataset `ranking_snapshot` da camada Gold representa o ranking de ativos elegiveis para aporte em uma determinada data de referencia.

O objetivo deste dataset e responder, de forma objetiva e auditavel, quais ativos estao relativamente mais atrativos segundo as features calculadas e as regras de score definidas no pipeline.

Fonte de entrada:

* origem principal: Gold `asset_features`
* granularidade: ranking por data de referencia
* local de armazenamento proposto: `data/gold/ranking_snapshot`

## 2. Estrutura fisica do dataset

Layout proposto:

```text
data/gold/ranking_snapshot/
  reference_year=<yyyy>/
    reference_month=<mm>/
      ranking_snapshot_<run_date>.parquet
```

## 3. Schema persistido no Parquet

| Coluna | Tipo | Obrigatoria | Descricao |
|---|---|---|---|
| `reference_date` | `date` | Sim | Data usada para geracao do ranking. |
| `asset` | `string` | Sim | Identificador logico do ativo. |
| `ticker` | `string` | Sim | Ticker do ativo. |
| `asset_type` | `string` | Sim | Tipo do ativo. |
| `score` | `double` | Sim | Score numerico final usado para ordenacao. |
| `rank_position` | `integer` | Sim | Posicao do ativo no ranking da data. |
| `ranking_bucket` | `string` | Nao | Classificacao opcional derivada do rank. Ex.: `top_3`, `middle`, `tail`. |
| `eligibility_status` | `string` | Sim | Estado de elegibilidade. Ex.: `eligible`, `insufficient_history`, `excluded_rule`. |
| `score_version` | `string` | Sim | Versao da metodologia de score aplicada. |
| `has_complete_features` | `boolean` | Sim | Indica se o ativo tinha todas as features necessarias para o score principal. |

## 4. Regras de negocio

Diretrizes iniciais:

* o ranking deve ser gerado separadamente para cada `reference_date`
* `rank_position = 1` representa o melhor ativo elegivel naquela data
* ativos inelegiveis podem aparecer no snapshot, mas devem carregar `eligibility_status` apropriado
* a metodologia de score deve ser versionada em `score_version`

## 5. Regras de qualidade de dados

### 5.1 Chave unica

A chave unica logica do dataset e:

* `reference_date + asset`

### 5.2 Integridade minima

Nao sao permitidos valores nulos em:

* `reference_date`
* `asset`
* `ticker`
* `asset_type`
* `score`
* `rank_position`
* `eligibility_status`
* `score_version`
* `has_complete_features`

### 5.3 Regras semanticas

* `rank_position` deve ser inteiro positivo
* nao pode haver repeticao de `rank_position` entre ativos elegiveis da mesma data sem regra explicita de empate
* `score_version` deve ser obrigatoriamente preenchido
* `eligibility_status` deve vir de um conjunto controlado

## 6. O que deve quebrar a pipeline

Falhas que devem interromper a execucao:

* duplicidade por `reference_date + asset`
* ranking sem ordenacao consistente por score
* posicoes faltantes ou invalidas
* score nulo, infinito ou nao numerico

