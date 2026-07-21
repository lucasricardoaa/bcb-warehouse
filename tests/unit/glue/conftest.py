"""
Configuração de fixtures para testes unitários do Glue Job.

Requisitos de ambiente (Windows):
  - JAVA_HOME apontando para JDK/JRE 8 ou 11 (Java 21+ incompatível com PySpark 3.5)
  - HADOOP_HOME apontando para diretório com bin/winutils.exe e bin/hadoop.dll
    Instalado em C:/hadoop (winutils 3.3.1, compatível com PySpark 3.5/Hadoop 3.3.4)

  Em CI (Linux) nenhuma dessas configurações é necessária.
"""

import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Caminhos fixos do ambiente Windows de desenvolvimento
_JAVA8_HOME = Path("C:/Program Files/Java/jre1.8.0_451")
_HADOOP_HOME = Path("C:/hadoop")


def _configure_windows_env() -> None:
    """Configura JAVA_HOME e HADOOP_HOME para PySpark no Windows."""
    if os.name != "nt":
        return
    if _JAVA8_HOME.exists():
        os.environ["JAVA_HOME"] = str(_JAVA8_HOME)
    if _HADOOP_HOME.exists():
        os.environ["HADOOP_HOME"] = str(_HADOOP_HOME)
        bin_dir = str(_HADOOP_HOME / "bin")
        if bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")


def _spark_available() -> bool:
    """Verifica se PySpark pode acessar o filesystem local neste ambiente."""
    if os.name != "nt":
        return True
    return (_HADOOP_HOME / "bin" / "winutils.exe").exists()


requires_spark = pytest.mark.skipif(
    not _spark_available(),
    reason=(
        "PySpark no Windows requer HADOOP_HOME com bin/winutils.exe. "
        "Instalado em C:/hadoop — verificar se o arquivo existe."
    ),
)

_configure_windows_env()


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName("bcb-warehouse-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
