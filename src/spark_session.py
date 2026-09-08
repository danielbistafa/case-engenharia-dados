"""Criacao padronizada da SparkSession local ou uso da sessao do Databricks."""
import os
import sys

from pyspark.sql import SparkSession

from config import IS_DATABRICKS, SPARK_WAREHOUSE


def create_spark_session(app_name: str = "CaseEngenhariaDados") -> SparkSession:
    """Retorna a sessao gerenciada do Databricks ou cria uma sessao Delta local."""
    if IS_DATABRICKS:
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    python_executable = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_executable)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(SPARK_WAREHOUSE))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
