# bcb-warehouse

Pipeline de transformação e modelagem dimensional sobre dados econômicos do Banco Central
do Brasil (USD/BRL, Selic, IPCA). Projeto 2 de 3 de um portfólio de Engenharia de Dados
com foco em AWS.

> **Contexto do portfólio:**
> [bcb-pipeline](../bcb-pipeline) → **bcb-warehouse** (este) → [bcb-infra](../bcb-infra)
>
> O bcb-pipeline ingere os dados da API do BCB e os deposita em S3 (Parquet/Snappy).
> Este projeto transforma esses dados em um star schema Iceberg consultável via Athena.
> O bcb-infra provisiona toda a infraestrutura com Terraform.

---

## O que este projeto demonstra

| Competência | Como é demonstrada |
|---|---|
| PySpark / AWS Glue | Job com cast, dedup, validação de ranges e MERGE Iceberg com null-safe `<=>` |
| Apache Iceberg | Upsert via `MERGE INTO`, OPTIMIZE + VACUUM agendados, time travel |
| dbt Core + Athena | Star schema em 3 camadas, macro SHA-256, 68 testes declarativos |
| Modelagem dimensional | Duas granularidades separadas (diária/mensal), surrogate keys determinísticas |
| Airflow | Dataset triggers entre DAGs, GlueJobOperator, DAG de manutenção dedicada |
| Documentação técnica | 9 ADRs com erratas assinadas — decisões rastreáveis desde a concepção |
| Qualidade de código | 26 testes unitários do Glue Job, ruff, mypy, pytest com PyArrow fixtures |

---

## Arquitetura

```
bcb-pipeline-staging (S3 Parquet/Snappy)
           │
           ▼
  ┌─────────────────────┐
  │   AWS Glue Job      │  • cast de tipos (DATE, DECIMAL 18,6)
  │   (PySpark)         │  • deduplicação por (serie_id, data)
  │                     │  • validação de ranges → NULL out-of-range
  │                     │  • MERGE INTO Iceberg (null-safe <=>)
  └─────────────────────┘
           │
           ▼
  bcb_warehouse.{usd_brl, selic, ipca}   ← tabelas Iceberg (camada intermediate)
           │
           ▼
  ┌─────────────────────┐
  │   dbt Core          │  staging/      → views sobre Iceberg (renomeia colunas)
  │   (Athena adapter)  │  intermediate/ → int_indicadores_economicos (UNION ALL)
  │                     │  mart/         → star schema Iceberg
  └─────────────────────┘
           │
           ▼
  ┌─────────────────────────────────────────────┐
  │              Star Schema                    │
  │                                             │
  │  fct_cotacoes_diarias   fct_indicadores_mensais │
  │  (USD/BRL + Selic)      (IPCA)              │
  │         │  └───────────────┘  │             │
  │         ▼                     ▼             │
  │    dim_serie              dim_data          │
  │  (3 séries BCB)       (2020-2030 + feriados)│
  └─────────────────────────────────────────────┘
```

### Orquestração

```
DAG bcb_warehouse  (trigger: Dataset s3://bcb-pipeline-staging/bcb/)
  run_glue_job
    >> dbt_run_staging
    >> dbt_run_intermediate
    >> dbt_run_mart
    >> dbt_test

DAG bcb_warehouse_maintenance  (schedule: dia 15 de cada mês, 00:00 UTC)
  optimize_{tabela} >> vacuum_{tabela}   [5 tabelas Iceberg em paralelo]
```

---

## Stack

| Camada | Tecnologia |
|---|---|
| Processamento | PySpark 3.5 via AWS Glue Job (Glue 4.0) |
| Formato de tabela | Apache Iceberg (Glue Data Catalog como metastore) |
| Modelagem | dbt Core 1.11 + dbt-athena-community |
| Query engine | Amazon Athena (engine v3) |
| Armazenamento | Amazon S3 (`s3://bcb-warehouse/`) |
| Orquestração | Apache Airflow 2.9+ com Dataset triggers |
| Linguagem | Python 3.11 / 3.12 (dbt) |
| Testes | pytest, PyArrow, unittest.mock |
| Qualidade | ruff, mypy |

---

## Estrutura do repositório

```
bcb-warehouse/
  dags/
    bcb_warehouse_dag.py              # DAG principal (Dataset-triggered)
    bcb_warehouse_maintenance_dag.py  # OPTIMIZE + VACUUM mensal

  glue/
    jobs/bcb_staging_transform.py     # PySpark: limpeza + MERGE Iceberg

  dbt/bcb_warehouse/
    models/
      staging/        # stg_usd_brl, stg_selic, stg_ipca (views)
      intermediate/   # int_indicadores_economicos (tabela Iceberg)
      mart/           # fct_cotacoes_diarias, fct_indicadores_mensais,
                      # dim_serie, dim_data (tabelas Iceberg)
    seeds/
      dim_data_seed.csv         # calendário 2020-2030
      feriados_nacionais.csv    # 143 feriados nacionais brasileiros
      dim_serie_seed.csv        # metadados das 3 séries BCB
    macros/
      generate_surrogate_key.sql  # SHA-256 truncado (sobrepõe dbt_utils MD5)

  tests/unit/glue/              # 26 testes unitários do Glue Job
  tools/generate_dim_data.py    # gera dim_data_seed.csv e feriados_nacionais.csv
  docs/
    adr/                        # 9 ADRs com todas as decisões arquiteturais
    architecture/warehouse_architecture.md
```

---

## Decisões arquiteturais

Todas as decisões estruturais estão documentadas em [`docs/adr/`](docs/adr/):

| ADR | Decisão |
|---|---|
| [0001](docs/adr/0001-formato-armazenamento-iceberg-vs-parquet-plano.md) | Apache Iceberg como formato de tabela na camada warehouse |
| [0002](docs/adr/0002-estrutura-camadas-s3-warehouse.md) | Bucket dedicado `bcb-warehouse`, organização por responsabilidade |
| [0003](docs/adr/0003-divisao-responsabilidades-glue-job-dbt.md) | Fronteira Glue Job / dbt na camada intermediate |
| [0004](docs/adr/0004-modelo-dimensional-star-schema.md) | Star schema com duas granularidades (diária/mensal) |
| [0005](docs/adr/0005-orquestracao-dag-estendida-vs-dag-separada.md) | DAG separada acionada via Airflow Dataset |
| [0006](docs/adr/0006-estrategia-qualidade-dbt-tests-vs-great-expectations.md) | dbt tests como camada primária; Great Expectations não implementado |
| [0007](docs/adr/0007-estrutura-repositorio-bcb-warehouse.md) | Estrutura de repositório |
| [0008](docs/adr/0008-manutencao-tabelas-iceberg-optimize-vacuum.md) | Manutenção Iceberg: OPTIMIZE → VACUUM, DAG dedicada, dia 15/mês |
| [0009](docs/adr/0009-fonte-populacao-dimensoes-estaticas.md) | Seeds CSV para dimensões estáticas |

---

## Como executar localmente

### Pré-requisitos

- Python 3.11+ (testes do Glue Job)
- Python 3.12 (dbt — incompatibilidade com 3.14+)
- Java 8 ou 11 (PySpark)
- `winutils.exe` no PATH se Windows (ou executar em WSL/Linux)

### Setup

```bash
# 1. Dependências do Glue Job e ferramentas
pip install -e ".[dev]"

# 2. dbt em venv Python 3.12
make install-dbt

# 3. Configuração local
cp .env.example .env
# preencher AWS credentials

cp dbt/bcb_warehouse/profiles.yml.example dbt/bcb_warehouse/profiles.yml
# preencher s3_staging_dir e s3_data_dir se necessário
```

### Testes e validações

```bash
# Testes unitários do Glue Job (sem AWS)
make test-glue

# Lint e type check
make lint
make typecheck

# Validação de sintaxe dbt (sem AWS)
make dbt-parse

# Operações contra AWS real
make dbt-seed    # carrega seeds no Athena
make dbt-run     # executa todos os modelos
make dbt-test    # executa os 68 testes declarativos

# Regenerar seeds de calendário
python tools/generate_dim_data.py
```

---

## Séries cobertas

| Série BCB | Código | Granularidade | Tabela fato |
|---|---|---|---|
| Taxa de câmbio USD/BRL | 1 | Diária | `fct_cotacoes_diarias` |
| Taxa Selic | 11 | Diária | `fct_cotacoes_diarias` |
| IPCA | 433 | Mensal | `fct_indicadores_mensais` |

---

## Notas operacionais

- **`dim_data` cobre 2020–2030.** Datas fora desse intervalo são silenciosamente excluídas
  do join com as tabelas fato. Para expandir: `python tools/generate_dim_data.py --end 2035-12-31`
  seguido de `make dbt-seed`.

- **Teste `recency` no `dbt test`.** Após parada do pipeline por mais de 10 dias
  (cotações) ou 45 dias (indicadores), o teste de recência falhará. Para verificar
  a qualidade dos dados antes de limpar o alerta:
  `dbt test --exclude tag:recency --project-dir dbt/bcb_warehouse`.

- **Manutenção Iceberg.** A DAG `bcb_warehouse_maintenance` executa no dia 15 de cada mês.
  O OPTIMIZE cria novo snapshot; o VACUUM remove os arquivos substituídos que já expiraram
  do TTL de 7 dias. Há uma defasagem de ~1 mês entre OPTIMIZE e deleção física — comportamento
  esperado e documentado na ADR-0008.
