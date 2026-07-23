#!/usr/bin/env python3
"""
Glue Job: bcb-staging-transform

Lê os Parquets de staging do bcb-pipeline, aplica transformações e escreve
nas tabelas Iceberg da camada intermediate do bcb-warehouse (ADR-0003).

Pipeline por série:
  1. Leitura do Parquet (data: date32, valor: float64)
  2. Casting de tipos (valor → DecimalType(18,6))
  3. Deduplicação por chave natural (serie_id, data)
  4. Validação de ranges (out-of-range → NULL, sem descarte)
  5. Escrita em Iceberg via Glue Data Catalog

Parâmetros do job (via --key value na linha de comando do Glue):
  --JOB_NAME          Nome do Glue Job (obrigatório pelo runtime)
  --staging_bucket    Bucket de origem, ex: bcb-pipeline-staging
  --serie_ids         IDs das séries separados por vírgula, ex: "1,11,433"

Nota: o bucket de destino não é parâmetro do job — o path S3 das tabelas Iceberg
é configurado no warehouse_location do Glue Data Catalog (ADR-0002).
"""

import json
import logging
import sys

try:  # pragma: no cover
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions

    _IN_GLUE = True
except ImportError:
    _IN_GLUE = False

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# ── Logging estruturado JSON ──────────────────────────────────────────────────


class _JsonFormatter(logging.Formatter):  # pragma: no cover
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
log = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────

SERIES: dict[int, str] = {
    1: "usd_brl",
    11: "selic",
    433: "ipca",
}

# Glue Data Catalog — configurado automaticamente no runtime Glue 4.0
_CATALOG = "glue_catalog"
_DATABASE = "bcb_warehouse"

# Ranges válidos por série (min_inclusive, max_exclusive).
# Valores fora do range são substituídos por NULL (ADR-0003: não descartar).
RANGE_RULES: dict[int, tuple[float, float]] = {
    1: (0.001, 100.0),    # USD/BRL R$/US$: positivo e abaixo de 100
    11: (0.0, 100.0),     # Selic % a.a.: não-negativa e abaixo de 100
    433: (-10.0, 50.0),   # IPCA % mensal: deflação extrema a hiperinflação
}

# ── Funções de leitura ────────────────────────────────────────────────────────


def staging_path(staging_bucket: str, serie_id: int) -> str:
    """Retorna o caminho S3 raiz da série na camada staging do bcb-pipeline."""
    return f"s3://{staging_bucket}/bcb/{serie_id}/"


def read_serie(spark: SparkSession, path: str, serie_id: int) -> DataFrame:
    """
    Lê todos os Parquets de uma série e retorna DataFrame com colunas:
      - serie_id (IntegerType): identificador da série BCB
      - data     (DateType):    data da observação
      - valor    (DoubleType):  valor bruto (cast aplicado em cast_types)

    O Parquet de origem tem schema {data: date32, valor: float64} conforme
    escrito pelo bcb-pipeline. Colunas de partição Hive (year, month) são
    descartadas — a data já está na coluna `data`.
    """
    log.info("Lendo staging", extra={"path": path, "serie_id": serie_id})
    df = (
        spark.read.option("mergeSchema", "false")
        .parquet(path)
        .select("data", "valor")
        .withColumn("serie_id", F.lit(serie_id))
        .select("serie_id", "data", "valor")
    )
    count = df.count()
    log.info("Staging lido", extra={"serie_id": serie_id, "rows": count})
    return df


# ── Funções de transformação ──────────────────────────────────────────────────


def cast_types(df: DataFrame) -> DataFrame:
    """
    Aplica casting explícito de tipos (ADR-0003):
      - data:  DateType — já correto vindo do Parquet; mantido explicitamente
      - valor: DecimalType(18, 6) — precisão adequada para taxas e cotações
    """
    return df.withColumn("valor", F.col("valor").cast(DecimalType(18, 6)))


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicatas por chave natural (serie_id, data).

    Duplicatas podem surgir de reprocessamentos parciais ou sobreposição de
    partições no bcb-pipeline. Mantém a primeira ocorrência encontrada.
    """
    before = df.count()
    result = df.dropDuplicates(["serie_id", "data"])
    after = result.count()
    if before != after:
        log.warning(
            "Duplicatas removidas",
            extra={"removed": before - after},
        )
    return result


def validate_ranges(df: DataFrame, serie_id: int) -> DataFrame:
    """
    Substitui valores fora do range esperado por NULL (ADR-0003: não descartar).

    Valores NULL de origem são preservados — representam gaps válidos na série.
    Valores in-range são mantidos sem alteração.

    Args:
        df:       DataFrame com coluna `valor` (DecimalType(18,6)).
        serie_id: ID da série para lookup dos ranges em RANGE_RULES.
    """
    if serie_id not in RANGE_RULES:
        log.warning("Range não definido para série", extra={"serie_id": serie_id})
        return df

    min_val, max_val = RANGE_RULES[serie_id]
    in_range = F.col("valor").isNull() | (
        (F.col("valor") >= F.lit(min_val).cast(DecimalType(18, 6)))
        & (F.col("valor") < F.lit(max_val).cast(DecimalType(18, 6)))
    )
    result = df.withColumn(
        "valor",
        F.when(in_range, F.col("valor")).otherwise(
            F.lit(None).cast(DecimalType(18, 6))
        ),
    )

    nullified = (
        result.filter(F.col("valor").isNull()).count()
        - df.filter(F.col("valor").isNull()).count()
    )
    if nullified > 0:
        log.warning(
            "Valores out-of-range substituídos por NULL",
            extra={"serie_id": serie_id, "nullified": nullified},
        )
    return result


def transform_serie(df: DataFrame, serie_id: int) -> DataFrame:
    """
    Aplica o pipeline completo de transformação a uma série:
      cast_types → deduplicate → validate_ranges
    """
    return (
        df.transform(cast_types)
        .transform(deduplicate)
        .transform(lambda d: validate_ranges(d, serie_id))
    )


# ── Funções de escrita ────────────────────────────────────────────────────────


def intermediate_table_name(serie_name: str) -> str:
    """
    Retorna o nome qualificado da tabela Iceberg no Glue catalog.

    O nome no catálogo NÃO usa prefixo 'stg_' para evitar conflito com as
    views dbt staging (Athena não permite VIEW e TABLE com o mesmo nome no
    mesmo database). O prefixo 'stg_' é mantido nas views dbt e no S3 path
    (localização física dos arquivos Iceberg, definida pelo warehouse location).
    """
    return f"{_CATALOG}.{_DATABASE}.{serie_name}"


def write_iceberg(
    df: DataFrame,
    table_name: str,
    serie_name: str,
    spark: SparkSession,
) -> None:
    """
    Escreve DataFrame em tabela Iceberg no Glue Data Catalog (ADR-0001).

    Na primeira execução (tabela não existe), cria a tabela via .create().
    Nas execuções seguintes, executa MERGE por chave natural (serie_id, data):
      - MATCHED + valor alterado → UPDATE
      - NOT MATCHED              → INSERT

    O MERGE garante que apenas linhas novas ou corrigidas pelo BCB são
    reescritas, sem substituir dados históricos válidos (ADR-0001).

    Nota: não executável em modo local (sem Glue catalog). A escrita é
    condicional a _IN_GLUE no main().
    """
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        table_exists = True
    except Exception:
        table_exists = False

    if not table_exists:
        log.info("Primeira execução — criando tabela", extra={"table": table_name})
        (
            df.writeTo(table_name)
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .create()
        )
        log.info("Tabela criada", extra={"table": table_name})
    else:
        temp_view = f"_staging_{serie_name}"
        df.createOrReplaceTempView(temp_view)
        log.info("Executando MERGE", extra={"table": table_name, "source": temp_view})
        spark.sql(f"""
            MERGE INTO {table_name} t
            USING {temp_view} s
            ON t.serie_id = s.serie_id AND t.data = s.data
            WHEN MATCHED AND NOT (t.valor <=> s.valor)
                THEN UPDATE SET t.valor = s.valor
            WHEN NOT MATCHED
                THEN INSERT *
        """)
        log.info("MERGE concluído", extra={"table": table_name})


# ── Inicialização Spark / Glue ────────────────────────────────────────────────


def _init_spark(job_name: str) -> tuple[SparkSession, object | None]:
    if _IN_GLUE:  # pragma: no cover
        from pyspark import SparkContext

        sc = SparkContext()
        glue_ctx = GlueContext(sc)
        spark = glue_ctx.spark_session
        job = Job(glue_ctx)
        job.init(job_name, {})
        return spark, job

    spark = (
        SparkSession.builder.master("local[*]")
        .appName(job_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    return spark, None


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:  # pragma: no cover
    if _IN_GLUE:
        args = getResolvedOptions(
            sys.argv,
            ["JOB_NAME", "staging_bucket", "serie_ids"],
        )
        job_name: str = args["JOB_NAME"]
        staging_bucket: str = args["staging_bucket"]
        serie_ids = [int(s.strip()) for s in args["serie_ids"].split(",")]
    else:
        job_name = "bcb-staging-transform-local"
        staging_bucket = "bcb-pipeline-staging"
        serie_ids = list(SERIES.keys())

    log.info("Iniciando job", extra={"job_name": job_name, "serie_ids": serie_ids})

    spark, job = _init_spark(job_name)

    try:
        for serie_id in serie_ids:
            serie_name = SERIES[serie_id]
            path = staging_path(staging_bucket, serie_id)

            df_raw = read_serie(spark, path, serie_id)
            df_transformed = transform_serie(df_raw, serie_id)

            if _IN_GLUE:
                table_name = intermediate_table_name(serie_name)
                write_iceberg(df_transformed, table_name, serie_name, spark)

            log.info(
                "Série processada",
                extra={"serie_id": serie_id, "serie_name": serie_name},
            )

        if job is not None:
            job.commit()  # type: ignore[attr-defined]

    finally:
        spark.stop()

    log.info("Job concluído", extra={"job_name": job_name})


if __name__ == "__main__":  # pragma: no cover
    main()
