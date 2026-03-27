# Data Contract: Gold `asset_features`

## 1. Descricao do dataset

O dataset `asset_features` da camada Gold armazena metricas calculadas por ativo e por data de referencia, a partir da Silver `prices_clean`.

O objetivo deste dataset e concentrar os sinais quantitativos que alimentam o ranking de aportes e, no futuro, a camada de explicacao via LLM.

Fonte de entrada:

* origem: Silver `prices_clean`
* granularidade: ativo por dia
* local de armazenamento proposto: `data/gold/asset_features`

## 2. Estrutura fisica do dataset

Layout proposto:

```text
data/gold/asset_features/
  reference_year=<yyyy>/
    reference_month=<mm>/
      asset_features_<run_date>.parquet
```

## 3. Schema persistido no Parquet

| Coluna | Tipo | Obrigatoria | Descricao |
|---|---|---|---|
| `reference_date` | `date` | Sim | Data de referencia do calculo das features. |
| `asset` | `string` | Sim | Identificador logico do ativo. |
| `ticker` | `string` | Sim | Ticker do ativo. |
| `asset_type` | `string` | Sim | Tipo do ativo. |
| `close` | `double` | Sim | Preco de fechamento na data de referencia. |
| `adj_close` | `double` | Sim | Preco ajustado na data de referencia. |
| `daily_return` | `double` | Nao | Retorno diario simples. |
| `return_30d` | `double` | Nao | Retorno acumulado em 30 dias uteis de negociacao. |
| `return_90d` | `double` | Nao | Retorno acumulado em 90 dias uteis de negociacao. |
| `return_252d` | `double` | Nao | Retorno acumulado em aproximadamente 12 meses uteis. |
| `volatility_30d` | `double` | Nao | Volatilidade rolling de 30 observacoes. |
| `drawdown_252d` | `double` | Nao | Drawdown em relacao ao pico recente de 252 observacoes. |
| `ma_20` | `double` | Nao | Media movel simples curta. |
| `ma_90` | `double` | Nao | Media movel simples longa. |
| `trend_ratio` | `double` | Nao | Relacao entre media curta e longa para capturar tendencia. |
| `sharpe_like_90d` | `double` | Nao | Medida simplificada de retorno medio sobre volatilidade. |
| `data_points_252d` | `integer` | Sim | Quantidade de observacoes validas consideradas na janela principal. |
| `feature_status` | `string` | Sim | Estado do calculo. Ex.: `complete`, `insufficient_history`. |

## 4. Regras de calculo

Diretrizes iniciais:

* as features devem ser calculadas por `asset`, ordenadas por `date`
* `reference_date` deve refletir a data da linha de Silver usada no calculo
* janelas devem usar historico anterior disponivel do proprio ativo
* quando nao houver historico suficiente, a feature pode ficar nula, desde que `feature_status` explicite a situacao

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

### 5.3 Regras semanticas

* `data_points_252d` deve ser maior ou igual a zero
* `feature_status` deve vir de um conjunto controlado
* `volatility_30d` nao pode ser negativa
* `drawdown_252d` deve estar no intervalo `[-1, 0]`, salvo ajuste futuro de metodologia

## 6. O que deve quebrar a pipeline

Falhas que devem interromper a execucao:

* ausencia de chave unica por `asset + reference_date`
* ativo sem correspondencia com a Silver
* fechamento nulo ou invalido
* calculo numerico resultando em infinito ou NaN fora dos campos explicitamente opcionais

