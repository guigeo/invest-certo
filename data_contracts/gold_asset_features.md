# Data Contract: Gold `asset_features`

## 1. Descricao do dataset

O dataset `asset_features` da camada Gold armazena metricas diarias por ativo para analise de preco, risco, tendencia e momentum, servindo como base para o ranking de aportes e para os graficos do dashboard.

Fonte de entrada:

* origem principal: Silver `prices_clean`
* gate de prontidao: Silver `asset_daily_status`
* granularidade: ativo por dia
* local de armazenamento: `data/gold/asset_features`

## 2. Estrutura fisica do dataset

Layout esperado:

```text
data/gold/asset_features/
  reference_year=<yyyy>/
    reference_month=<mm>/
      asset_features_<run_date>.parquet
```

Observacoes:

* `run_date` representa a data de execucao da pipeline Gold no formato `YYYYMMDD`
* a Gold atual e full refresh a partir da Silver

## 3. Schema persistido no Parquet

| Coluna | Tipo | Obrigatoria | Descricao |
|---|---|---|---|
| `reference_date` | `date` | Sim | Data de referencia do calculo das features. |
| `asset` | `string` | Sim | Identificador logico do ativo. |
| `ticker` | `string` | Sim | Ticker do ativo. |
| `asset_type` | `string` | Sim | Tipo do ativo. |
| `close` | `double` | Sim | Preco de fechamento da data. |
| `adj_close` | `double` | Sim | Preco ajustado da data. |
| `daily_return` | `double` | Nao | Retorno simples em relacao ao dia util anterior. |
| `return_30d` | `double` | Nao | Retorno acumulado em 30 observacoes. |
| `return_90d` | `double` | Nao | Retorno acumulado em 90 observacoes. |
| `return_252d` | `double` | Nao | Retorno acumulado em 252 observacoes. |
| `volatility_30d` | `double` | Nao | Volatilidade rolling anualizada em 30 observacoes. |
| `drawdown_252d` | `double` | Nao | Distancia em relacao ao pico recente de 252 observacoes. |
| `ma_20` | `double` | Nao | Media movel de 20 observacoes. |
| `ma_90` | `double` | Nao | Media movel de 90 observacoes. |
| `trend_ratio` | `double` | Nao | Razao entre `ma_20` e `ma_90`. |
| `sharpe_like_90d` | `double` | Nao | Retorno medio sobre volatilidade em 90 observacoes, anualizado. |
| `data_points_252d` | `integer` | Sim | Quantidade de observacoes validas disponiveis na janela principal. |
| `feature_status` | `string` | Sim | Situacao do calculo. Ex.: `complete`, `insufficient_history`. |
| `distance_to_ma20` | `double` | Nao | Distancia percentual do preco ajustado em relacao a `ma_20`. |
| `distance_to_ma90` | `double` | Nao | Distancia percentual do preco ajustado em relacao a `ma_90`. |
| `price_vs_52w_high` | `double` | Nao | Distancia percentual em relacao ao topo da janela de 252 observacoes. |
| `price_vs_52w_low` | `double` | Nao | Distancia percentual em relacao ao fundo da janela de 252 observacoes. |
| `momentum_bucket` | `string` | Sim | Classificacao discreta de momentum: `strong`, `neutral`, `weak`. |
| `risk_bucket` | `string` | Sim | Classificacao discreta de risco: `low`, `medium`, `high`. |

## 4. Regras de calculo

Diretrizes da versao atual:

* calcular as features por `asset`, ordenadas por `reference_date`
* usar `adj_close` como base para retornos, medias moveis e drawdown
* manter linhas com historico insuficiente, desde que `feature_status` sinalize a situacao
* herdar o gate de elegibilidade da Silver para que a Gold nao replique regras de calendario e volume
* usar buckets discretos para facilitar leitura no dashboard

## 5. Regras de qualidade de dados

### 5.1 Chave unica

A chave unica logica do dataset e:

* `asset + reference_date`

### 5.2 Integridade minima

Nao sao permitidos valores nulos em:

* `reference_date`
* `asset`
* `ticker`
* `asset_type`
* `close`
* `adj_close`
* `data_points_252d`
* `feature_status`
* `momentum_bucket`
* `risk_bucket`

### 5.3 Regras semanticas

* `close` e `adj_close` devem ser positivos
* `data_points_252d` deve ser maior ou igual a zero
* `volatility_30d` nao pode ser negativa
* `drawdown_252d` deve estar no intervalo `[-1, 0]`
* `feature_status` deve pertencer ao dominio controlado
* `momentum_bucket` deve estar em `strong`, `neutral`, `weak`
* `risk_bucket` deve estar em `low`, `medium`, `high`

## 6. Dominio controlado

Valores aceitos em `feature_status`:

* `complete`
* `insufficient_history`
* `volume_missing`
* `calendar_gap_anomaly`
* `invalid_price`

## 7. O que deve quebrar a pipeline

Falhas que devem interromper a execucao:

* duplicidade por `asset + reference_date`
* fechamento nulo, zero ou negativo
* infinito ou NaN em campos numericos obrigatorios
* `drawdown_252d` fora do intervalo previsto
* bucket fora dos dominios controlados
