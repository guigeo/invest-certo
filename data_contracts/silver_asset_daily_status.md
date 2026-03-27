# Data Contract: Silver `asset_daily_status`

## 1. Descricao do dataset

O dataset `asset_daily_status` da camada Silver representa a situacao diaria de qualidade e prontidao analitica de cada ativo, derivada diretamente da `prices_clean`.

O objetivo desta visao e concentrar regras operacionais e flags de elegibilidade para que a Gold nao precise reimplementar validacoes de calendario, historico minimo e consistencia basica de entrada.

Fonte de entrada:

* origem: Silver `prices_clean`
* granularidade: diaria por ativo
* formato de processamento esperado: DuckDB
* local de armazenamento: `data/silver/asset_daily_status`

## 2. Estrutura fisica do dataset

Layout esperado:

```text
data/silver/asset_daily_status/
  year=<yyyy>/
    month=<mm>/
      asset_daily_status_<run_date>.parquet
```

Observacoes:

* `run_date` representa a data de execucao da pipeline Silver no formato `YYYYMMDD`
* a visao e reprocessada integralmente a partir da Bronze e da `prices_clean`

## 3. Schema persistido no Parquet

| Coluna | Tipo | Obrigatoria | Descricao |
|---|---|---|---|
| `date` | `date` | Sim | Data de negociacao do ativo. |
| `asset` | `string` | Sim | Identificador logico do ativo. |
| `ticker` | `string` | Sim | Ticker do ativo. |
| `asset_type` | `string` | Sim | Tipo do ativo. |
| `is_price_valid` | `boolean` | Sim | Indica se os precos da linha respeitam as regras basicas de consistencia. |
| `is_volume_missing` | `boolean` | Sim | Replica a flag da `prices_clean` para volume originalmente ausente. |
| `is_zero_volume` | `boolean` | Sim | Indica se o volume persistido na linha e zero. |
| `prev_date` | `date` | Nao | Data anterior disponivel para o mesmo ativo. |
| `days_since_prev_trade` | `integer` | Nao | Diferenca em dias entre a data atual e `prev_date`. |
| `has_calendar_gap_anomaly` | `boolean` | Sim | Marca gaps acima do limite esperado de calendario para a versao atual. |
| `history_length_days` | `integer` | Sim | Quantidade acumulada de observacoes do ativo ate a data corrente. |
| `has_min_history_30d` | `boolean` | Sim | Indica se o ativo ja possui pelo menos 30 observacoes. |
| `has_min_history_90d` | `boolean` | Sim | Indica se o ativo ja possui pelo menos 90 observacoes. |
| `has_min_history_252d` | `boolean` | Sim | Indica se o ativo ja possui pelo menos 252 observacoes. |
| `is_feature_eligible` | `boolean` | Sim | Indica se a linha esta apta para alimentar a Gold v1. |
| `eligibility_status` | `string` | Sim | Motivo controlado da situacao da linha. |

## 4. Regras de transformacao

Regras esperadas:

* derivar a visao exclusivamente a partir de `prices_clean`
* calcular `prev_date` com `LAG(date)` por `asset`
* calcular `days_since_prev_trade` como a diferenca entre `date` e `prev_date`
* considerar anomalia de calendario apenas quando o gap exceder 5 dias
* calcular `history_length_days` como contagem acumulada de observacoes por ativo
* expor flags de historico minimo para 30, 90 e 252 observacoes
* consolidar uma regra unica de elegibilidade em `is_feature_eligible` e `eligibility_status`

## 5. Regras de qualidade de dados

### 5.1 Chave unica

A chave unica logica do dataset e:

* `asset + date`

### 5.2 Integridade minima

Nao sao permitidos valores nulos em:

* `date`
* `asset`
* `ticker`
* `asset_type`
* `is_price_valid`
* `is_volume_missing`
* `is_zero_volume`
* `has_calendar_gap_anomaly`
* `history_length_days`
* `has_min_history_30d`
* `has_min_history_90d`
* `has_min_history_252d`
* `is_feature_eligible`
* `eligibility_status`

### 5.3 Dominio controlado

Os valores aceitos em `eligibility_status` sao:

* `eligible`
* `insufficient_history`
* `volume_missing`
* `calendar_gap_anomaly`
* `invalid_price`

## 6. Regras de elegibilidade

Diretriz inicial da Gold v1:

* linhas com `eligibility_status = eligible` podem seguir para o calculo normal de features
* linhas com historico insuficiente devem permanecer no dataset, mas nao devem alimentar calculos que exijam janela maior
* `volume = 0` nao quebra a pipeline por si so; o sinal fica exposto em `is_zero_volume`
* `volume_missing = true` torna a linha inelegivel para a Gold v1

## 7. O que deve quebrar a pipeline

Falhas que devem interromper a execucao:

* duplicidade por `asset + date`
* ausencia de colunas obrigatorias em `prices_clean`
* `history_length_days` menor ou igual a zero
* `eligibility_status` fora do conjunto controlado
