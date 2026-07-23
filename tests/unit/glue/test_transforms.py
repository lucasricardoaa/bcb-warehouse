"""
Testes unitários para as funções de transformação do Glue Job.

Fixtures Parquet são escritas com PyArrow e lidas via spark.read.parquet(),
contornando a incompatibilidade createDataFrame + Python 3.14 + PySpark 3.5.

Cobre:
  - cast_types:       valor cast para DecimalType(18,6)
  - deduplicate:      remoção de duplicatas por (serie_id, data)
  - validate_ranges:  valores out-of-range substituídos por NULL
  - transform_serie:  pipeline completo
"""

from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

from glue.jobs.bcb_staging_transform import (
    RANGE_RULES,
    cast_types,
    deduplicate,
    intermediate_table_name,
    read_serie,
    transform_serie,
    validate_ranges,
    write_iceberg,
)
from tests.unit.glue.conftest import requires_spark

_STAGING_SCHEMA = pa.schema(
    [
        pa.field("data", pa.date32()),
        pa.field("valor", pa.float64()),
    ]
)


def _write_parquet(path: Path, datas: list[date], valores: list[float | None]) -> None:
    table = pa.table(
        {
            "data": pa.array(datas, type=pa.date32()),
            "valor": pa.array(valores, type=pa.float64()),
        },
        schema=_STAGING_SCHEMA,
    )
    pq.write_table(table, str(path), compression="snappy")


def _read(spark: SparkSession, tmp_path: Path, serie_id: int) -> object:
    """Helper: escreve fixture e retorna DataFrame com read_serie."""
    return read_serie(spark, str(tmp_path), serie_id)


# ── intermediate_table_name (sem Spark) ───────────────────────────────────────


def test_intermediate_table_name_usd_brl() -> None:
    # Sem prefixo 'stg_' no catálogo — evita conflito com views dbt staging
    assert intermediate_table_name("usd_brl") == "glue_catalog.bcb_warehouse.usd_brl"


def test_intermediate_table_name_selic() -> None:
    assert intermediate_table_name("selic") == "glue_catalog.bcb_warehouse.selic"


def test_intermediate_table_name_ipca() -> None:
    assert intermediate_table_name("ipca") == "glue_catalog.bcb_warehouse.ipca"


# ── RANGE_RULES (sem Spark) ───────────────────────────────────────────────────


def test_range_rules_all_series_defined() -> None:
    """Todas as séries do pipeline têm range definido."""
    assert set(RANGE_RULES.keys()) == {1, 11, 433}


def test_range_rules_usd_brl_positive() -> None:
    min_val, _ = RANGE_RULES[1]
    assert min_val > 0, "USD/BRL nunca pode ser zero ou negativo"


def test_range_rules_selic_non_negative() -> None:
    min_val, _ = RANGE_RULES[11]
    assert min_val >= 0, "Selic nunca pode ser negativa"


def test_range_rules_ipca_allows_deflation() -> None:
    min_val, _ = RANGE_RULES[433]
    assert min_val < 0, "IPCA mensal pode ser negativo (deflação)"


def test_validate_ranges_unknown_serie_returns_df_unchanged(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Série sem range definido retorna o DataFrame inalterado."""
    _write_parquet(tmp_path / "d.parquet", [date(2024, 1, 2)], [1.0])
    df = cast_types(read_serie(spark, str(tmp_path), serie_id=1))

    result = validate_ranges(df, serie_id=999)

    assert result.count() == df.count()


# ── cast_types ────────────────────────────────────────────────────────────────


@requires_spark
def test_cast_types_valor_schema(spark: SparkSession, tmp_path: Path) -> None:
    """valor deve ser DecimalType(18, 6) após cast."""
    _write_parquet(tmp_path / "d.parquet", [date(2024, 1, 2)], [4.9823])
    df = read_serie(spark, str(tmp_path), serie_id=1)

    result = cast_types(df)

    valor_type = result.schema["valor"].dataType
    assert isinstance(valor_type, DecimalType)
    assert valor_type.precision == 18
    assert valor_type.scale == 6


@requires_spark
def test_cast_types_preserves_row_count(spark: SparkSession, tmp_path: Path) -> None:
    """cast_types não altera o número de registros."""
    _write_parquet(
        tmp_path / "d.parquet",
        [date(2024, 1, 2), date(2024, 1, 3)],
        [4.9823, None],
    )
    df = read_serie(spark, str(tmp_path), serie_id=1)

    assert cast_types(df).count() == 2


# ── deduplicate ───────────────────────────────────────────────────────────────


@requires_spark
def test_deduplicate_removes_exact_duplicates(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Duplicatas exatas de (serie_id, data) são removidas."""
    _write_parquet(
        tmp_path / "d.parquet",
        [date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 3)],
        [4.9823, 4.9823, 4.9915],
    )
    df = read_serie(spark, str(tmp_path), serie_id=1)

    assert deduplicate(df).count() == 2


@requires_spark
def test_deduplicate_keeps_unique_rows(spark: SparkSession, tmp_path: Path) -> None:
    """Linhas sem duplicata são preservadas integralmente."""
    _write_parquet(
        tmp_path / "d.parquet",
        [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
        [4.9823, 4.9915, 5.0012],
    )
    df = read_serie(spark, str(tmp_path), serie_id=1)

    assert deduplicate(df).count() == 3


# ── validate_ranges ───────────────────────────────────────────────────────────


@requires_spark
def test_validate_ranges_nullifies_negative_usd_brl(
    spark: SparkSession, tmp_path: Path
) -> None:
    """USD/BRL negativo é substituído por NULL."""
    _write_parquet(tmp_path / "d.parquet", [date(2024, 1, 2)], [-1.0])
    df = cast_types(read_serie(spark, str(tmp_path), serie_id=1))

    result = validate_ranges(df, serie_id=1)

    row = result.collect()[0]
    assert row.valor is None


@requires_spark
def test_validate_ranges_preserves_null_valor(
    spark: SparkSession, tmp_path: Path
) -> None:
    """NULL de origem é preservado (gap válido na série)."""
    _write_parquet(tmp_path / "d.parquet", [date(2024, 1, 2)], [None])
    df = cast_types(read_serie(spark, str(tmp_path), serie_id=1))

    result = validate_ranges(df, serie_id=1)

    assert result.filter("valor is null").count() == 1


@requires_spark
def test_validate_ranges_keeps_valid_value(spark: SparkSession, tmp_path: Path) -> None:
    """Valor dentro do range não é alterado."""
    _write_parquet(tmp_path / "d.parquet", [date(2024, 1, 2)], [4.9823])
    df = cast_types(read_serie(spark, str(tmp_path), serie_id=1))

    result = validate_ranges(df, serie_id=1)

    row = result.collect()[0]
    assert row.valor == Decimal("4.982300")


@requires_spark
def test_validate_ranges_ipca_negative_valid(
    spark: SparkSession, tmp_path: Path
) -> None:
    """IPCA negativo dentro do range (-10, 50) é preservado."""
    _write_parquet(tmp_path / "d.parquet", [date(2024, 1, 1)], [-0.61])
    df = cast_types(read_serie(spark, str(tmp_path), serie_id=433))

    result = validate_ranges(df, serie_id=433)

    row = result.collect()[0]
    assert row.valor is not None


@requires_spark
def test_validate_ranges_ipca_extreme_nullified(
    spark: SparkSession, tmp_path: Path
) -> None:
    """IPCA acima de 50% ao mês é substituído por NULL (hiperinflação irreal)."""
    _write_parquet(tmp_path / "d.parquet", [date(2024, 1, 1)], [99.9])
    df = cast_types(read_serie(spark, str(tmp_path), serie_id=433))

    result = validate_ranges(df, serie_id=433)

    row = result.collect()[0]
    assert row.valor is None


# ── transform_serie (pipeline completo) ───────────────────────────────────────


@requires_spark
def test_transform_serie_pipeline(spark: SparkSession, tmp_path: Path) -> None:
    """Pipeline completo: cast + dedup + validate em sequência."""
    _write_parquet(
        tmp_path / "d.parquet",
        [date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 3)],
        [4.9823, 4.9823, -999.0],  # duplicata + out-of-range
    )
    df = read_serie(spark, str(tmp_path), serie_id=1)

    result = transform_serie(df, serie_id=1)
    rows = result.orderBy("data").collect()

    assert len(rows) == 2
    assert rows[0].valor == Decimal("4.982300")  # deduplicado, in-range
    assert rows[1].valor is None                  # out-of-range → NULL
    valor_type = result.schema["valor"].dataType
    assert isinstance(valor_type, DecimalType)


# ── write_iceberg ──────────────────────────────────────────────────────────────
# Testes com mock puro — sem Glue catalog ou Spark real.


def test_write_iceberg_create_on_first_run() -> None:
    """Na primeira execução (DESCRIBE falha), deve chamar .create()."""
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = Exception("Table not found")

    mock_df = MagicMock()
    mock_writer = MagicMock()
    mock_df.writeTo.return_value = mock_writer
    mock_writer.tableProperty.return_value = mock_writer

    write_iceberg(mock_df, "glue_catalog.db.usd_brl", "usd_brl", mock_spark)

    mock_df.writeTo.assert_called_once_with("glue_catalog.db.usd_brl")
    mock_writer.create.assert_called_once()
    # .createOrReplace() não deve ser chamado
    mock_writer.createOrReplace.assert_not_called()
    # MERGE não deve ter sido disparado
    merge_calls = [c for c in mock_spark.sql.call_args_list if "MERGE" in str(c)]
    assert len(merge_calls) == 0


def test_write_iceberg_merge_on_subsequent_runs() -> None:
    """Em execuções seguintes (DESCRIBE bem-sucedida), deve executar MERGE."""
    mock_spark = MagicMock()
    # sql() retorna mock para DESCRIBE e para MERGE
    mock_spark.sql.return_value = MagicMock()

    mock_df = MagicMock()

    write_iceberg(mock_df, "glue_catalog.db.usd_brl", "usd_brl", mock_spark)

    # Temp view registrada com nome padronizado
    mock_df.createOrReplaceTempView.assert_called_once_with("_staging_usd_brl")
    # .writeTo() não deve ser chamado (tabela já existe)
    mock_df.writeTo.assert_not_called()
    # MERGE INTO deve ter sido disparado
    merge_calls = [c for c in mock_spark.sql.call_args_list if "MERGE INTO" in str(c)]
    assert len(merge_calls) == 1
