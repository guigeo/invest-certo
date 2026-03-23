# Data Contract: Bronze `prices`

## 1. Descrição do dataset

O dataset `prices` da camada Bronze armazena o histórico bruto de preços de mercado coletado via `yfinance` para ações e FIIs monitorados pelo projeto.

O objetivo desta camada e preservar o dado de origem com o minimo de transformacao possivel, mantendo schema estavel, estrategia append-only e particionamento simples para leitura posterior pelas camadas Silver e Gold.

Fonte atual:

* provider: `yfinance`
* granularidade: diaria
* formato fisico: Parquet
* local de armazenamento: `data/bronze/prices`

## 2. Estrutura fisica do dataset

Layout atual:

```text
data/bronze/prices/
  asset=<asset>/
    year=<yyyy>/
      month=<mm>/
        prices_<ingestion_timestamp>.parquet
```

Observacoes:

* `asset`, `year` e `month` sao colunas de particionamento fisico.
* `ingestion_timestamp` nao e persistido como coluna dentro do parquet; ele aparece no nome do arquivo.
* O parquet armazena apenas as colunas de negocio do dataset.

## 3. Schema persistido no Parquet

| Coluna | Tipo | Obrigatoria | Descricao |
|---|---|---|---|
| `date` | `timestamp[ns]` sem timezone | Sim | Data de negociacao do ativo. No Bronze e normalizada para timestamp naive. |
| `open` | `float64` | Sim | Preco de abertura do dia. |
| `high` | `float64` | Sim | Maior preco negociado no dia. |
| `low` | `float64` | Sim | Menor preco negociado no dia. |
| `close` | `float64` | Sim | Preco de fechamento do dia. |
| `adj_close` | `float64` | Sim | Preco ajustado. Quando o provider nao envia `Adj Close`, o pipeline usa o valor de `close`. |
| `volume` | `int64` ou `float64` | Sim | Volume negociado no dia. |
| `asset` | `string` | Sim | Identificador logico do ativo no projeto, vindo de `config/assets.txt`. Ex.: `BBAS3`. |
| `ticker` | `string` | Sim | Ticker enviado ao provider. Ex.: `BBAS3.SA`. |

## 4. Colunas obrigatorias

As colunas obrigatorias do dataset sao:

* `date`
* `open`
* `high`
* `low`
* `close`
* `adj_close`
* `volume`
* `asset`
* `ticker`

## 5. Colunas opcionais

No estado atual do projeto, o dataset `prices` nao possui colunas opcionais persistidas no parquet.

Qualquer nova coluna deve ser explicitamente adicionada ao contrato antes de entrar em producao.

## 6. Metadata operacional

Metadados relevantes para rastreabilidade:

| Campo | Tipo | Persistido como coluna | Origem | Descricao |
|---|---|---|---|---|
| `source` | `string` | Nao | Configuracao do ativo | Fonte do dado. Hoje o valor esperado e `yahoo`, consumido via `yfinance`. |
| `ingestion_timestamp` | `string` UTC no formato `YYYYMMDDTHHMMSSZ` | Nao | Nome do arquivo | Momento da escrita do parquet. Ex.: `prices_20260322T210000Z.parquet`. |
| `asset` | `string` | Sim e tambem no path | Coluna + particionamento | Chave logica do ativo. |
| `year` | `string` | Nao | Particionamento | Ano derivado de `date` para organizacao fisica. |
| `month` | `string` com zero a esquerda | Nao | Particionamento | Mes derivado de `date` para organizacao fisica. |

## 7. Regras de qualidade de dados

### 7.1 Integridade de schema

O dataset deve conter todas as colunas obrigatorias com nomes exatamente iguais aos definidos neste contrato.

### 7.2 Valores nulos

Nao sao permitidos valores nulos nas colunas criticas:

* `date`
* `open`
* `high`
* `low`
* `close`
* `adj_close`
* `volume`
* `asset`
* `ticker`

### 7.3 Precos invalidos

Nao sao permitidos valores negativos em:

* `open`
* `high`
* `low`
* `close`
* `adj_close`

Para `volume`, o valor esperado e maior ou igual a zero.

### 7.4 Consistencia de preco

A seguinte relacao deve ser verdadeira para cada linha:

* `low <= close <= high`

### 7.5 Chave unica

A chave unica logica do dataset e:

* `asset + date`

Isso significa que nao deve existir mais de um registro para o mesmo ativo na mesma data dentro do conjunto Bronze consolidado.

## 8. Estrategia de particionamento

O dataset e particionado por:

* `asset`
* `year`
* `month`

Justificativa:

* `asset` facilita leitura seletiva por ativo
* `year` e `month` reduzem o volume de arquivos lidos em cargas e consultas historicas
* a estrategia funciona bem com o incremental atual sem introduzir complexidade desnecessaria

## 9. Politica de atualizacao

Politica atual: append-only.

Regras:

* o pipeline nunca reescreve parquet anterior
* cada execucao gera novos arquivos `prices_<ingestion_timestamp>.parquet`
* a coleta e incremental por ativo
* a data inicial da coleta de um ativo e calculada como `max(date) + 1 dia`
* se o ativo nao tiver historico salvo, a coleta comeca em `2015-01-01`

## 10. Limitacoes conhecidas

* feriados e fins de semana geram lacunas naturais no calendario
* o provider pode nao devolver `Adj Close`; nesse caso o pipeline usa `close`
* podem existir datas ausentes por indisponibilidade temporaria do provider ou por suspensao de negociacao do ativo
* o dataset depende do calendario do mercado e da disponibilidade do Yahoo Finance
* `source` e `ingestion_timestamp` ainda nao sao colunas persistidas dentro do parquet

## 11. Validacao esperada no pipeline

Antes de salvar o parquet, o pipeline Bronze deve validar:

* schema minimo
* colunas criticas sem nulos
* ausencia de duplicidade por `asset + date`
* consistencia de preco

Falhas de validacao devem interromper a execucao imediatamente.
