# Data Contract: Gold `ranking_snapshot`

## 1. Descricao do dataset

O dataset `ranking_snapshot` da camada Gold representa o ranking diario de ativos para apoio ao aporte mensal, mantendo transparencia sobre score, sinais resumidos e situacao de elegibilidade.

Fonte de entrada:

* origem principal: Gold `asset_features`
* gate de elegibilidade: Silver `asset_daily_status`
* granularidade: ativo por data de referencia
* local de armazenamento: `data/gold/ranking_snapshot`

## 2. Estrutura fisica do dataset

Layout esperado:

```text
data/gold/ranking_snapshot/
  reference_year=<yyyy>/
    reference_month=<mm>/
      ranking_snapshot_<run_date>.parquet
```

Observacoes:

* `run_date` representa a data de execucao da pipeline Gold no formato `YYYYMMDD`
* o score atual usa a metodologia versionada `v1`

## 3. Schema persistido no Parquet

| Coluna | Tipo | Obrigatoria | Descricao |
|---|---|---|---|
| `reference_date` | `date` | Sim | Data usada para geracao do ranking. |
| `asset` | `string` | Sim | Identificador logico do ativo. |
| `ticker` | `string` | Sim | Ticker do ativo. |
| `asset_type` | `string` | Sim | Tipo do ativo. |
| `score` | `double` | Sim | Score final do ativo na data. |
| `rank_position` | `integer` | Sim | Posicao sequencial no snapshot da data. |
| `ranking_bucket` | `string` | Sim | Bucket discreto do ranking: `top_3`, `top_5`, `middle`, `tail`. |
| `eligibility_status` | `string` | Sim | Estado de elegibilidade herdado da Silver. |
| `score_version` | `string` | Sim | Versao da metodologia de score. |
| `has_complete_features` | `boolean` | Sim | Indica se o ativo tinha todas as features para score v1. |
| `rank_delta_7d` | `double` | Nao | Variacao do ranking contra o snapshot comparavel de pelo menos 7 dias antes. |
| `rank_delta_30d` | `double` | Nao | Variacao do ranking contra o snapshot comparavel de pelo menos 30 dias antes. |
| `score_delta_7d` | `double` | Nao | Variacao do score em relacao ao snapshot comparavel de pelo menos 7 dias antes. |
| `score_delta_30d` | `double` | Nao | Variacao do score em relacao ao snapshot comparavel de pelo menos 30 dias antes. |
| `primary_signal` | `string` | Sim | Sinal quantitativo principal resumido para o dashboard. |
| `secondary_signal` | `string` | Sim | Sinal quantitativo secundario resumido para o dashboard. |
| `is_top_pick` | `boolean` | Sim | Marca o melhor ativo elegivel do snapshot. |

## 4. Regras de negocio

Diretrizes da versao atual:

* gerar o ranking separadamente por `reference_date`
* usar score `v1` simples e auditavel, combinando tendencia, momentum, qualidade de risco e penalidades de volatilidade e drawdown
* permitir a presenca de ativos inelegiveis no snapshot para transparencia operacional
* permitir deltas nulos quando nao houver historico comparavel para 7 ou 30 dias
* `is_top_pick` deve ser verdadeiro apenas para o melhor ativo elegivel com features completas

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
* `ranking_bucket`
* `eligibility_status`
* `score_version`
* `has_complete_features`
* `primary_signal`
* `secondary_signal`
* `is_top_pick`

### 5.3 Regras semanticas

* `rank_position` deve ser inteiro positivo e sequencial por data
* entre ativos elegiveis, o `score` deve estar ordenado de forma decrescente
* `ranking_bucket` deve estar em `top_3`, `top_5`, `middle`, `tail`
* `eligibility_status` deve vir do dominio controlado da Silver
* `score_version` deve ser obrigatoriamente preenchido

## 6. O que deve quebrar a pipeline

Falhas que devem interromper a execucao:

* duplicidade por `reference_date + asset`
* score nulo, infinito ou nao numerico
* lacuna ou repeticao invalida em `rank_position`
* ordenacao inconsistente de score entre ativos elegiveis
* bucket ou status fora do dominio controlado
