"""
Testes unitários para as funções de leitura do Glue Job.

Os testes test_read_serie_* requerem PySpark com acesso ao filesystem local.
No Windows, isso exige winutils.exe (ver conftest.py).
Em CI (Linux) rodam sem restrições.

Fixtures Parquet são escritas com PyArrow (schema idêntico ao bcb-pipeline)
para evitar o createDataFrame, que falha com Python 3.14 + PySpark 3.5 (cloudpickle).
"""

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession

from glue.jobs.bcb_staging_transform import read_serie, staging_path
from tests.unit.glue.conftest import requires_spark

# Schema idêntico ao gerado pelo bcb-pipeline (src/storage/parquet.py)
_STAGING_SCHEMA = pa.schema(
    [
        pa.field("data", pa.date32()),
        pa.field("valor", pa.float64()),
    ]
)


def _write_parquet(path: Path, datas: list[date], valores: list[float | None]) -> None:
    """Escreve Parquet de staging no schema do bcb-pipeline via PyArrow."""
    table = pa.table(
        {
            "data": pa.array(datas, type=pa.date32()),
            "valor": pa.array(valores, type=pa.float64()),
        },
        schema=_STAGING_SCHEMA,
    )
    pq.write_table(table, str(path), compression="snappy")


# ── staging_path (sem Spark — sempre rodam) ───────────────────────────────────


def test_staging_path_usd_brl() -> None:
    assert staging_path("bcb-pipeline-staging", 1) == "s3://bcb-pipeline-staging/bcb/1/"


def test_staging_path_selic() -> None:
    assert staging_path("bcb-pipeline-staging", 11) == "s3://bcb-pipeline-staging/bcb/11/"


def test_staging_path_ipca() -> None:
    assert staging_path("bcb-pipeline-staging", 433) == "s3://bcb-pipeline-staging/bcb/433/"


# ── read_serie (requerem Spark + winutils no Windows) ────────────────────────


@requires_spark
def test_read_serie_columns(spark: SparkSession, tmp_path: Path) -> None:
    """Retorna exatamente as colunas [serie_id, data, valor] nessa ordem."""
    _write_parquet(
        tmp_path / "data.parquet",
        [date(2024, 1, 2), date(2024, 1, 3)],
        [4.9823, 4.9915],
    )

    result = read_serie(spark, str(tmp_path), serie_id=1)

    assert result.columns == ["serie_id", "data", "valor"]


@requires_spark
def test_read_serie_row_count(spark: SparkSession, tmp_path: Path) -> None:
    """Preserva o número de registros do Parquet de origem."""
    _write_parquet(
        tmp_path / "data.parquet",
        [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
        [4.9823, 4.9915, 5.0012],
    )

    result = read_serie(spark, str(tmp_path), serie_id=11)

    assert result.count() == 3


@requires_spark
def test_read_serie_populates_serie_id(spark: SparkSession, tmp_path: Path) -> None:
    """Todas as linhas têm serie_id igual ao valor fornecido."""
    _write_parquet(
        tmp_path / "data.parquet", [date(2024, 1, 2), date(2024, 2, 1)], [0.42, 0.39]
    )

    result = read_serie(spark, str(tmp_path), serie_id=433)

    serie_ids = [r.serie_id for r in result.collect()]
    assert all(sid == 433 for sid in serie_ids)


@requires_spark
def test_read_serie_tolerates_null_valor(spark: SparkSession, tmp_path: Path) -> None:
    """Linhas com valor nulo são preservadas (gaps são válidos na série)."""
    _write_parquet(
        tmp_path / "data.parquet", [date(2024, 1, 2), date(2024, 1, 3)], [4.9823, None]
    )

    result = read_serie(spark, str(tmp_path), serie_id=1)

    assert result.count() == 2
    assert result.filter("valor is null").count() == 1
