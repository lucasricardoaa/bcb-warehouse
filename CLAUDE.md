# CLAUDE.md — bcb-warehouse

## Contexto do projeto

Pipeline de transformação e modelagem dimensional sobre os dados econômicos do Banco
Central do Brasil (USD/BRL, Selic, IPCA), consumindo o output do bcb-pipeline (Projeto 1).

Projeto 2 de 3 de um roadmap de portfólio para vaga de Engenheiro de Dados (foco pleno).
Continuação direta do bcb-pipeline — nao cria nova infraestrutura de ingestão.

## Posicionamento no roadmap

| Projeto | Escopo | Stack principal |
|---|---|---|
| 1 — bcb-pipeline | Ingestão: API BCB → S3 raw → S3 staging | Airflow, Lambda, PyArrow, Athena |
| 2 — bcb-warehouse (este) | Transformação + modelagem: staging → warehouse Iceberg | Glue PySpark, dbt, Iceberg, Airflow |
| 3 — bcb-infra | Infraestrutura como código | Terraform, GitHub Actions |

## O que o bcb-pipeline entrega (inputs deste projeto)

- Dados brutos em `s3://bcb-pipeline-raw/bcb/{serie_id}/year=YYYY/month=MM/` (JSON)
- Dados limpos em `s3://bcb-pipeline-staging/bcb/{serie_id}/year=YYYY/month=MM/` (Parquet/Snappy)
- Séries: USD/BRL (código 1), Selic (código 11), IPCA (código 433)
- Tabelas externas Glue/Athena: `usd_brl`, `selic`, `ipca` no database `bcb_pipeline`
- Infraestrutura: AWS us-east-1, conta `992382708036`

## Stack

- Python 3.11
- PySpark via AWS Glue Job (nao EMR)
- dbt Core com dbt-athena-community adapter
- Apache Iceberg + Glue Data Catalog (metastore)
- Apache Airflow 2.9+ (mesmo ambiente Docker do bcb-pipeline)
- AWS: S3, Glue, Athena, IAM
- pytest + ruff + mypy

## Arquitetura de camadas

### Fluxo de dados

```
bcb-pipeline-staging (Parquet/Snappy)
         |
         | leitura pelo Glue Job
         v
[AWS Glue Job — PySpark]
  - casting explícito de tipos
  - deduplicacao por (serie_id, data)
  - validacao de ranges (nulos para out-of-range, nao descarte)
  - escrita em Iceberg
         |
         v
s3://bcb-warehouse/warehouse/intermediate/stg_{serie}/ (Iceberg)
         |
         | leitura pelo dbt via Athena
         v
[dbt — camada staging]
  stg_usd_brl, stg_selic, stg_ipca
         |
         v
[dbt — camada intermediate]
  int_indicadores_economicos
         |
         v
[dbt — camada mart — star schema]
  fct_cotacoes_diarias, fct_indicadores_mensais
  dim_serie, dim_data
```

### Estrutura S3

```
s3://bcb-pipeline-staging/             # INPUT — somente leitura pelo warehouse
  bcb/{serie_id}/year=YYYY/month=MM/   # Parquet/Snappy (output do Projeto 1)

s3://bcb-warehouse/                    # OUTPUT — propriedade deste projeto
  warehouse/
    intermediate/
      stg_usd_brl/                     # Iceberg — output do Glue Job
      stg_selic/
      stg_ipca/
    mart/
      fct_cotacoes_diarias/            # Iceberg — output do dbt
      fct_indicadores_mensais/
      dim_serie/
      dim_data/
  glue/
    scripts/                           # script PySpark
    temp/                              # diretório temporário do Spark
    logs/
  athena-results/                      # resultados de queries (TTL 7 dias)
```

### Databases Glue / Athena

| Database | Proprietário | Tabelas |
|---|---|---|
| `bcb_pipeline` | bcb-pipeline | `usd_brl`, `selic`, `ipca` (Parquet) |
| `bcb_warehouse` | bcb-warehouse | `stg_*`, `int_*`, `fct_*`, `dim_*` (Iceberg) |

## Modelo dimensional (star schema)

### Tabelas fato

**`fct_cotacoes_diarias`** — USD/BRL e Selic (granularidade diária)
- Colunas: `cotacao_sk`, `serie_fk`, `data_fk`, `valor`, `data_ingestao`, `data_processamento`

**`fct_indicadores_mensais`** — IPCA (granularidade mensal)
- Colunas: `indicador_sk`, `serie_fk`, `ano`, `mes`, `valor`, `data_ingestao`, `data_processamento`

### Dimensoes

**`dim_serie`** — metadados das séries BCB
- Colunas: `serie_sk`, `serie_id`, `nome`, `unidade`, `periodicidade`, `fonte`, `data_inicio_serie`

**`dim_data`** — calendário 2020-2030
- Colunas: `data_sk`, `data`, `ano`, `mes`, `mes_nome`, `trimestre`, `semestre`,
  `dia_da_semana`, `dia_da_semana_nome`, `eh_dia_util`, `eh_feriado_nacional`

Surrogate keys: hash determinístico (SHA-256 truncado) da chave natural — garante
estabilidade após reprocessamentos.

## Orquestração

### DAG `bcb_warehouse`

- Trigger: Airflow Dataset `s3://bcb-pipeline-staging/bcb/` (declarado como outlet
  pela DAG `bcb_ingestion` do Projeto 1)
- Schedule: acionado automaticamente após cada run bem-sucedida da ingestão

```
wait_for_staging_dataset
  >> run_glue_job
  >> dbt_run_staging
  >> dbt_run_intermediate
  >> dbt_run_mart
  >> dbt_test
```

### Relacao com o bcb-pipeline

A DAG `bcb_warehouse` nao modifica a DAG `bcb_ingestion`. O contrato entre os projetos
é o Airflow Dataset URI. A DAG de ingestão deve declarar:

```python
outlets=[Dataset("s3://bcb-pipeline-staging/bcb/")]
```

na task `invoke_lambda_raw_to_staging`.

## Qualidade de dados

Estratégia: **dbt tests como camada primária** (ADR-0006).

Testes mínimos por camada:
- `staging/`: `not_null` em chaves naturais, `unique` em surrogate keys
- `intermediate/`: `not_null` em FKs, `relationships` verificando integridade referencial
- `mart/`: `not_null` + `unique` + `expression_is_true` para ranges + `recency` para completude

Great Expectations: **nao implementado** neste projeto (decisao explícita — ADR-0006).

## Estrutura do repositório

```
bcb-warehouse/
  dags/
    bcb_warehouse_dag.py

  glue/
    jobs/
      bcb_staging_transform.py     # script PySpark
    requirements.txt

  dbt/
    bcb_warehouse/
      dbt_project.yml
      profiles.yml.example
      packages.yml
      models/
        staging/                   # stg_usd_brl, stg_selic, stg_ipca
        intermediate/              # int_indicadores_economicos
        mart/                      # fct_*, dim_*
      seeds/
        dim_data_seed.csv
        feriados_nacionais.csv
      macros/
      tests/
        generic/

  tests/
    unit/
      glue/

  docs/
    adr/
    architecture/

  .env.example
  Makefile
  pyproject.toml
  CLAUDE.md
```

## Convencoes de código

- Mesmas convencoes do bcb-pipeline: logging estruturado JSON, type hints obrigatórios,
  sem credenciais hardcoded, sem `print()`
- Modelos dbt seguem prefixo por camada: `stg_`, `int_`, `fct_`, `dim_`
- `profiles.yml` nao é versionado — apenas `profiles.yml.example`
- Todo modelo dbt tem entrada correspondente em `schema.yml` com pelo menos `description`
  e os testes mandatórios da camada

## Fronteiras explícitas de responsabilidade

**Glue Job faz:**
- Leitura do Parquet da bcb-pipeline-staging
- Casting de tipos (DATE, DECIMAL)
- Deduplicacao por chave natural
- Validacao de ranges (nulos, nao descarte)
- Escrita em Iceberg na camada intermediate

**dbt faz:**
- Renomear colunas e aplicar aliases (camada staging)
- Joins e métricas derivadas (camada intermediate)
- Star schema final (camada mart)
- Testes de qualidade declarativos
- Documentacao de schema

**Regra de ouro:** se a lógica pode ser expressa em SQL sem ambiguidade, ela vive no dbt.
Se a lógica requer lógica imperativa, tipagem estrita ou processamento de dados brutos,
ela vive no Glue Job.

## Decisoes arquiteturais

Ver `docs/adr/` para todas as decisoes registradas.

| ADR | Decisão |
|---|---|
| 0001 | Iceberg como formato de tabela na camada warehouse |
| 0002 | Bucket dedicado `bcb-warehouse`, organização por responsabilidade |
| 0003 | Fronteira Glue Job / dbt na camada intermediate (staging Iceberg) |
| 0004 | Star schema: `fct_cotacoes_diarias`, `fct_indicadores_mensais`, `dim_serie`, `dim_data` |
| 0005 | DAG separada `bcb_warehouse` acionada via Airflow Dataset |
| 0006 | dbt tests como camada primária de qualidade; GE nao implementado |
| 0007 | Estrutura de repositório: `glue/`, `dbt/bcb_warehouse/`, `dags/` |

Antes de propor qualquer mudança arquitetural, verificar os ADRs existentes e identificar
qual decisao é afetada. Nunca implementar antes de decisao explícita.

## O que está fora do escopo deste projeto

- Streaming / Kafka
- Great Expectations (decisao explícita — ADR-0006)
- Infraestrutura como código (Terraform) — Projeto 3
- CI/CD automatizado — Projeto 3
- Múltiplas fontes além do BCB
- Ingestão incremental de novas séries (herdado do Projeto 1)

## Como rodar localmente

```bash
cp .env.example .env
# preencher AWS credentials, bucket names, Athena workgroup

# testes unitários do Glue Job
make test-glue

# executar dbt em modo dry-run (apenas compila SQL)
make dbt-compile

# executar dbt contra Athena real
make dbt-run

# executar testes dbt
make dbt-test

# lint e type check
make lint
make typecheck
```

## Contexto de portfólio

Este repositório faz parte de um conjunto de 3 projetos:
- **Projeto 1 (bcb-pipeline):** ingestão — Airflow + S3 + Athena + Lambda
- **Projeto 2 (este):** transformação — PySpark + dbt + Iceberg
- **Projeto 3 (bcb-infra):** infraestrutura — Terraform + GitHub Actions
