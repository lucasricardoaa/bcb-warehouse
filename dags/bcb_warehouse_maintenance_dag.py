"""
DAG bcb_warehouse_maintenance — Manutenção periódica das tabelas Iceberg do warehouse.

Executa mensalmente no dia 15 às 00:00 UTC operações de compactação e limpeza:
- OPTIMIZE: compacta arquivos pequenos via bin-pack (melhora performance de leitura)
- VACUUM: remove snapshots antigos das tabelas Iceberg (retém últimos 7 dias)

Ordem: optimize primeiro, vacuum depois. O OPTIMIZE cria um snapshot novo com arquivos
compactados; o VACUUM em seguida remove os arquivos substituídos pelo OPTIMIZE, que já
não são referenciados por nenhum snapshot dentro do TTL (gap de 1-6 dias entre a última
run de ingestão e o dia 15 garante que o snapshot pré-OPTIMIZE expirou).

Schedule: dia 15 garante gap mínimo de 1 dia em relação à última ingestão semanal
(toda segunda-feira), mantendo a eficácia do VACUUM com TTL de 7 dias — ADR-0008.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# ── Constantes ──────────────────────────────────────────────────────────────

_AWS_CONN_ID: str = "aws_default"
_ATHENA_DATABASE: str = os.environ.get("ATHENA_DATABASE", "bcb_warehouse")
_ATHENA_WORKGROUP: str = os.environ.get("ATHENA_WORKGROUP", "primary")
_ATHENA_OUTPUT: str = os.environ.get(
    "ATHENA_RESULTS_LOCATION", "s3://bcb-warehouse/athena-results/"
)

# Tabelas Iceberg sujeitas a manutenção (intermediate + mart; staging são views)
_ICEBERG_TABLES: list[str] = [
    "int_indicadores_economicos",
    "dim_serie",
    "dim_data",
    "fct_cotacoes_diarias",
    "fct_indicadores_mensais",
]


def _vacuum_sql(table: str) -> str:
    return f"VACUUM {_ATHENA_DATABASE}.{table}"


def _optimize_sql(table: str) -> str:
    return f"OPTIMIZE {_ATHENA_DATABASE}.{table} REWRITE DATA USING BIN_PACK"


# ── DAG ─────────────────────────────────────────────────────────────────────

@dag(
    dag_id="bcb_warehouse_maintenance",
    schedule="0 0 15 * *",  # dia 15 de cada mês às 00:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bcb", "warehouse", "maintenance", "iceberg"],
    description="Manutenção mensal (dia 15) das tabelas Iceberg: OPTIMIZE + VACUUM.",
    doc_md=__doc__,
)
def bcb_warehouse_maintenance() -> None:

    vacuum_tasks = [
        AthenaOperator(
            task_id=f"vacuum_{table}",
            query=_vacuum_sql(table),
            database=_ATHENA_DATABASE,
            output_location=_ATHENA_OUTPUT,
            workgroup=_ATHENA_WORKGROUP,
            aws_conn_id=_AWS_CONN_ID,
        )
        for table in _ICEBERG_TABLES
    ]

    optimize_tasks = [
        AthenaOperator(
            task_id=f"optimize_{table}",
            query=_optimize_sql(table),
            database=_ATHENA_DATABASE,
            output_location=_ATHENA_OUTPUT,
            workgroup=_ATHENA_WORKGROUP,
            aws_conn_id=_AWS_CONN_ID,
        )
        for table in _ICEBERG_TABLES
    ]

    # optimize primeiro (compacta arquivos), vacuum depois (remove os substituídos)
    for optimize, vacuum in zip(optimize_tasks, vacuum_tasks, strict=False):
        optimize >> vacuum


bcb_warehouse_maintenance()
