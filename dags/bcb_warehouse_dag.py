"""
DAG bcb_warehouse — Transforma dados BCB em star schema Iceberg via Glue + dbt.

Trigger: Dataset s3://bcb-pipeline-staging/bcb/ publicado pela DAG bcb_ingestion
         após cada run bem-sucedida de ingestão.

Fluxo:
    [Dataset trigger]
        >> run_glue_job          # PySpark: cast, dedup, validate, escreve Iceberg
        >> dbt_run_staging       # dbt views sobre Iceberg (renomeia colunas)
        >> dbt_run_intermediate  # dbt tabela Iceberg: union das 3 séries
        >> dbt_run_mart          # dbt tabelas Iceberg: star schema (dim_* + fct_*)
        >> dbt_test              # todos os testes declarativos do projeto
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# ── Constantes ──────────────────────────────────────────────────────────────

_STAGING_DATASET = Dataset("s3://bcb-pipeline-staging/bcb/")

_AWS_CONN_ID: str = "aws_default"
_GLUE_JOB_NAME: str = os.environ.get("GLUE_JOB_NAME", "bcb-staging-transform")
_STAGING_BUCKET: str = os.environ.get("STAGING_BUCKET", "bcb-pipeline-staging")
_SERIE_IDS: str = os.environ.get("SERIE_IDS", "1,11,433")

_DBT_PROJECT_DIR: str = os.environ.get(
    "DBT_PROJECT_DIR", "/opt/airflow/dbt/bcb_warehouse"
)
_DBT_PROFILES_DIR: str = os.environ.get(
    "DBT_PROFILES_DIR", "/opt/airflow/dbt/bcb_warehouse"
)


def _dbt_cmd(subcmd: str, selector: str | None = None) -> str:
    """Monta comando dbt com project-dir e profiles-dir padronizados."""
    cmd = (
        f"dbt {subcmd}"
        f" --project-dir {_DBT_PROJECT_DIR}"
        f" --profiles-dir {_DBT_PROFILES_DIR}"
    )
    if selector is not None:
        cmd += f" --select {selector}"
    return cmd


# ── DAG ─────────────────────────────────────────────────────────────────────

@dag(
    dag_id="bcb_warehouse",
    schedule=[_STAGING_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bcb", "warehouse", "dbt", "glue"],
    description=(
        "Transforma dados bcb-pipeline-staging em star schema Iceberg. "
        "Acionada automaticamente após run bem-sucedida da DAG bcb_ingestion."
    ),
    doc_md=__doc__,
)
def bcb_warehouse() -> None:

    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name=_GLUE_JOB_NAME,
        script_args={
            "--staging_bucket": _STAGING_BUCKET,
            "--serie_ids": _SERIE_IDS,
        },
        aws_conn_id=_AWS_CONN_ID,
        wait_for_completion=True,
        verbose=True,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=_dbt_cmd("run", "staging"),
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=_dbt_cmd("run", "intermediate"),
    )

    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=_dbt_cmd("run", "mart"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt_cmd("test"),
    )

    (
        run_glue_job
        >> dbt_run_staging
        >> dbt_run_intermediate
        >> dbt_run_mart
        >> dbt_test
    )


bcb_warehouse()
