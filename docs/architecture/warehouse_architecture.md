# Arquitetura — bcb-warehouse

> Projeto 2 de 3 do portfólio de Engenharia de Dados.
> Última atualização: 2026-07-21

---

## 1. Posição no roadmap

| Projeto | Escopo | Stack principal |
|---|---|---|
| 1 — bcb-pipeline | Ingestão: API BCB → S3 raw → S3 staging | Airflow, Lambda, PyArrow, Athena |
| **2 — bcb-warehouse (este)** | **Transformação + modelagem: staging → warehouse Iceberg** | **Glue PySpark, dbt, Iceberg, Airflow** |
| 3 — bcb-infra | Infraestrutura como código | Terraform, GitHub Actions |

O bcb-warehouse **consome** o output do bcb-pipeline e **não cria** infraestrutura de ingestão.
O contrato entre os projetos é o Airflow Dataset URI `s3://bcb-pipeline-staging/bcb/`.

---

## 2. Fluxo de dados

```
┌─────────────────────────────────────────────────────────────────────┐
│  bcb-pipeline (Projeto 1)                                           │
│                                                                     │
│  API BCB ──► S3 raw ──► Lambda ──► S3 staging (Parquet/Snappy)     │
│                                         │                           │
│                               Dataset s3://bcb-pipeline-staging/bcb/│
└─────────────────────────────────────────┼───────────────────────────┘
                                          │ trigger Airflow Dataset
                                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  bcb-warehouse (este projeto)                                       │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  AWS Glue Job (PySpark)                                      │   │
│  │  · cast tipos (DATE, DECIMAL(18,6))                         │   │
│  │  · deduplicação por (serie_id, data)                        │   │
│  │  · validação de ranges (out-of-range → NULL, não descarte)  │   │
│  │  · escrita Iceberg na camada intermediate                   │   │
│  └───────────────────────┬──────────────────────────────────────┘   │
│                           │                                          │
│                           ▼  s3://bcb-warehouse/warehouse/intermediate/
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  dbt Core + dbt-athena-community                             │   │
│  │                                                              │   │
│  │  staging/          ← views sobre tabelas Iceberg do Glue    │   │
│  │  stg_usd_brl                                                 │   │
│  │  stg_selic                                                   │   │
│  │  stg_ipca                                                    │   │
│  │       │                                                      │   │
│  │       ▼                                                      │   │
│  │  intermediate/     ← tabela Iceberg: union das 3 séries     │   │
│  │  int_indicadores_economicos                                  │   │
│  │       │                                                      │   │
│  │       ▼                                                      │   │
│  │  mart/             ← star schema Iceberg (consumo final)    │   │
│  │  dim_serie    dim_data                                       │   │
│  │  fct_cotacoes_diarias    fct_indicadores_mensais            │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Camadas

### 3.1 Glue Job — `bcb_staging_transform.py`

**Responsabilidade:** limpeza e tipagem de baixo nível. Produz tabelas Iceberg que servem
de contrato de entrada para o dbt.

| Operação | Detalhe |
|---|---|
| Leitura | Parquet/Snappy da `bcb-pipeline-staging`, particionado por `year=/month=` |
| Cast de tipos | `data → DATE`, `valor → DECIMAL(18,6)` |
| Deduplicação | `dropDuplicates(["serie_id", "data"])` — keep first |
| Validação de ranges | USD/BRL: 0.001–100 · Selic: 0–100 · IPCA: −10–50 · fora do range: NULL |
| Escrita | Iceberg via `.create()` (primeira execução) ou `MERGE INTO` (execuções seguintes) no Glue Data Catalog |

Tabelas produzidas no database `bcb_warehouse`:

| Tabela Iceberg | Série | Localização S3 |
|---|---|---|
| `usd_brl` | USD/BRL (código 1) | `s3://bcb-warehouse/warehouse/intermediate/usd_brl/` |
| `selic` | Selic (código 11) | `s3://bcb-warehouse/warehouse/intermediate/selic/` |
| `ipca` | IPCA (código 433) | `s3://bcb-warehouse/warehouse/intermediate/ipca/` |

> **Nota de nomenclatura:** o prefixo `stg_` é omitido intencionalmente nas tabelas do
> Glue Data Catalog para evitar conflito com as views dbt `stg_*` que são criadas no
> mesmo database `bcb_warehouse` (Athena não permite TABLE e VIEW com o mesmo nome).

### 3.2 dbt — camada `staging/`

**Responsabilidade:** renomear colunas para nomes semânticos. Sem transformações de dados.
Materialização: **view** (zero cópia de dados).

| Modelo | Fonte | Colunas renomeadas |
|---|---|---|
| `stg_usd_brl` | `bcb_warehouse.usd_brl` | `valor → taxa_cambio_brl_usd`, `data → data_referencia` |
| `stg_selic` | `bcb_warehouse.selic` | `valor → taxa_selic_aa`, `data → data_referencia` |
| `stg_ipca` | `bcb_warehouse.ipca` | `valor → variacao_mensal_pct`, `data → data_referencia` |

### 3.3 dbt — camada `intermediate/`

**Responsabilidade:** unificação das séries em formato longo normalizado. Entrada para o mart.
Materialização: **tabela Iceberg** (Parquet/Snappy).

| Modelo | Descrição |
|---|---|
| `int_indicadores_economicos` | UNION ALL das 3 séries com coluna `periodicidade` (D/M). Granularidade: `(serie_id, data_referencia)` |

Colunas: `serie_id`, `data_referencia`, `valor`, `periodicidade`, `data_processamento`

### 3.4 dbt — camada `mart/`

**Responsabilidade:** star schema final, pronto para consumo analítico.
Materialização: **tabela Iceberg** (Parquet/Snappy).

---

## 4. Modelo dimensional (star schema)

```
                    ┌──────────────┐
                    │  dim_serie   │
                    │──────────────│
                    │  serie_sk PK │◄──────────────────────┐
                    │  serie_id    │                        │
                    │  nome        │          ┌─────────────┴──────────────┐
                    │  unidade     │          │   fct_indicadores_mensais  │
                    │  periodicidade│         │────────────────────────────│
                    │  fonte       │          │  indicador_sk PK           │
                    │  data_inicio │          │  serie_fk FK               │
                    └──────────────┘          │  ano                       │
                                             │  mes                       │
                    ┌──────────────┐          │  valor                     │
                    │  dim_data    │          │  data_ingestao             │
                    │──────────────│          │  data_processamento        │
                    │  data_sk PK  │          └────────────────────────────┘
                    │  data        │
                    │  ano         │          ┌────────────────────────────┐
                    │  mes         │          │   fct_cotacoes_diarias     │
                    │  mes_nome    │◄─────────│────────────────────────────│
                    │  trimestre   │          │  cotacao_sk PK             │
                    │  semestre    │          │  serie_fk FK               │
                    │  dia_semana  │          │  data_fk FK                │
                    │  eh_dia_util │          │  valor                     │
                    │  eh_feriado  │          │  data_ingestao             │
                    └──────────────┘          │  data_processamento        │
                                             └────────────────────────────┘
                                                          │
                                                          │ serie_fk
                                                          ▼
                                                    dim_serie (acima)
```

### Decisões de design do modelo

| Decisão | Justificativa |
|---|---|
| Duas tabelas fato (diária + mensal) | USD/BRL e Selic compartilham granularidade diária. IPCA tem granularidade mensal e semântica distinta — forçar interpolação diária introduziria dados sintéticos (ADR-0004) |
| `fct_cotacoes_diarias` sem `dim_data` FK em `fct_indicadores_mensais` | IPCA referencia (ano, mes) — não um dia específico. Join com dim_data seria ambíguo |
| Surrogate keys via SHA-256 truncado (32 hex chars) | Estabilidade após reprocessamentos. MD5 do dbt_utils substituído por SHA-256 nativo do Athena (ADR-0004) |
| `dim_data` com `eh_dia_util` e `eh_feriado_nacional` | Habilita análises de janelas de dias úteis — ex: "Selic nos últimos 21 dias úteis" — sem lógica no cliente |
| Seeds CSV para `dim_serie` e `dim_data` | Dados estáticos; manutenção simples; sem dependência de API externa (ADR-0009) |

---

## 5. Infraestrutura S3

```
s3://bcb-pipeline-staging/          ← INPUT — somente leitura pelo warehouse
  bcb/{serie_id}/year=YYYY/month=MM/

s3://bcb-warehouse/                 ← OUTPUT — propriedade deste projeto
  warehouse/
    intermediate/
      usd_brl/                      ← Iceberg — output do Glue Job
      selic/
      ipca/
    mart/
      fct_cotacoes_diarias/         ← Iceberg — output do dbt
      fct_indicadores_mensais/
      dim_serie/
      dim_data/
  glue/
    scripts/                        ← script PySpark
    temp/                           ← diretório temporário do Spark
    logs/
  athena-results/                   ← resultados de queries (TTL 7 dias)
```

### Databases Glue / Athena

| Database | Proprietário | Tabelas |
|---|---|---|
| `bcb_pipeline` | bcb-pipeline | `usd_brl`, `selic`, `ipca` (Parquet) |
| `bcb_warehouse` | bcb-warehouse | `usd_brl`, `selic`, `ipca` (Iceberg intermediate) + `stg_*` views + `int_*`, `fct_*`, `dim_*` tabelas Iceberg |

---

## 6. Orquestração

### DAG `bcb_warehouse`

- **Trigger:** Airflow Dataset `s3://bcb-pipeline-staging/bcb/`
- **Acionamento:** automático após run bem-sucedida da DAG `bcb_ingestion` (Projeto 1)
- **Catchup:** desabilitado

```
[Dataset trigger]
    >> run_glue_job          ← GlueJobOperator (wait_for_completion=True)
    >> dbt_run_staging       ← BashOperator: dbt run --select staging
    >> dbt_run_intermediate  ← BashOperator: dbt run --select intermediate
    >> dbt_run_mart          ← BashOperator: dbt run --select mart
    >> dbt_test              ← BashOperator: dbt test
```

### DAG `bcb_warehouse_maintenance`

- **Schedule:** dia 15 de cada mês às 00:00 UTC — `0 0 15 * *`
- **Operações:** `OPTIMIZE BIN_PACK` seguido de `VACUUM` em todas as tabelas Iceberg

```
optimize_int_indicadores_economicos >> vacuum_int_indicadores_economicos
optimize_dim_serie                  >> vacuum_dim_serie
optimize_dim_data                   >> vacuum_dim_data
optimize_fct_cotacoes_diarias       >> vacuum_fct_cotacoes_diarias
optimize_fct_indicadores_mensais    >> vacuum_fct_indicadores_mensais
```

> OPTIMIZE primeiro: compacta arquivos pequenos e gera novo snapshot. VACUUM depois:
> remove os arquivos substituídos pelo OPTIMIZE que já expiraram do TTL de 7 dias (ADR-0008).

---

## 7. Estratégia de qualidade

Camada primária: **dbt tests declarativos** (ADR-0006). Great Expectations: não implementado.

| Camada | Testes aplicados |
|---|---|
| `staging/` | `not_null` em chaves naturais; `accepted_values` em `serie_id` |
| `intermediate/` | `not_null` em todas as colunas; `unique_combination_of_columns(serie_id, data_referencia)`; `relationships → dim_serie`; `accepted_values` em `periodicidade` |
| `mart/` | `not_null` + `unique` em surrogate keys; `relationships` em todas as FKs; `expression_is_true` em ranges de valor e atributos numéricos de `dim_data` |

**Totais:** 8 modelos · 3 seeds · 64 testes declarativos

---

## 8. Stack técnica

| Componente | Tecnologia | Versão |
|---|---|---|
| Transformação | AWS Glue Job (PySpark) | PySpark 3.5 / Glue 4.0 |
| Formato de armazenamento | Apache Iceberg | 1.0 (via Glue 4.0) |
| Modelagem dimensional | dbt Core + dbt-athena-community | 1.11.7 / 1.11.0 |
| Query engine | Amazon Athena | v3 (Trino/Presto) |
| Orquestração | Apache Airflow | 2.9+ |
| Metastore | AWS Glue Data Catalog | — |
| Armazenamento | Amazon S3 | us-east-1 |
| Linguagem | Python | 3.11 (prod) / 3.12 (dbt local) |
| Testes Glue | pytest + PyArrow | — |
| Linting | ruff + mypy | — |

---

## 9. Decisões arquiteturais (índice)

| ADR | Título | Status |
|---|---|---|
| 0001 | Iceberg como formato de tabela na camada warehouse | Aceito |
| 0002 | Bucket dedicado `bcb-warehouse`, organização por responsabilidade | Aceito |
| 0003 | Fronteira Glue Job / dbt na camada intermediate (staging Iceberg) | Aceito |
| 0004 | Star schema: `fct_cotacoes_diarias`, `fct_indicadores_mensais`, `dim_serie`, `dim_data` | Aceito |
| 0005 | DAG separada `bcb_warehouse` acionada via Airflow Dataset | Aceito |
| 0006 | dbt tests como camada primária de qualidade; GE não implementado | Aceito |
| 0007 | Estrutura de repositório: `glue/`, `dbt/bcb_warehouse/`, `dags/` | Aceito |
| 0008 | Manutenção periódica de tabelas Iceberg via VACUUM + OPTIMIZE | Aceito |
| 0009 | Fonte e estratégia de população de dimensões estáticas (seeds CSV) | Aceito |

Ver `docs/adr/` para o texto completo de cada decisão.
