"""Demonstra pseudonimizacao e mascaramento sobre dados publicos do Brasileirao."""

import os
import sys
from pathlib import Path

if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    REPO_ROOT = Path(os.getcwd()).parents[1]
else:
    REPO_ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(0, str(REPO_ROOT / "src"))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, length, lit, sha2, substring, when

from config import IS_DATABRICKS, SECURITY_DIR, SILVER_DIR  # noqa: E402
from spark_session import create_spark_session  # noqa: E402


def require_masking_salt(spark: SparkSession) -> str:
    """Obtem o segredo do ambiente local ou de um Databricks Secret Scope."""
    # Variavel de ambiente tem prioridade (util no Databricks Free Edition)
    salt = os.getenv("MASKING_SALT")
    if salt:
        return salt

    if IS_DATABRICKS:
        try:
            from pyspark.dbutils import DBUtils

            scope = os.getenv("DATABRICKS_SECRET_SCOPE", "case-engenharia-dados")
            key = os.getenv("DATABRICKS_SECRET_KEY", "masking-salt")
            return DBUtils(spark).secrets.get(scope=scope, key=key)
        except Exception as e:
            raise RuntimeError(
                "Nao foi possivel obter o segredo do Databricks. "
                "Defina a variavel MASKING_SALT ou configure um Secret Scope. "
                f"Erro original: {e}"
            )

    raise RuntimeError("Defina MASKING_SALT no ambiente local.")


def pseudonymize(value_column, salt: str):
    """Gera um identificador SHA-256 deterministico com salt externo."""
    return sha2(concat_ws("|", lit(salt), value_column), 256)


def mask_name(value_column):
    """Mantem apenas a primeira letra e mascara o restante do nome."""
    return when(
        value_column.isNull() | (length(value_column) == 0),
        lit(None),
    ).otherwise(concat_ws("", substring(value_column, 1, 1), lit("***")))


def create_jogadores_protegidos(df: DataFrame, salt: str) -> DataFrame:
    """Cria uma visao protegida sem expor nomes completos de jogadores."""
    return df.select(
        pseudonymize(col("jogador_id"), salt).alias("jogador_token"),
        mask_name(col("nome_jogador")).alias("nome_mascarado"),
        pseudonymize(col("ultimo_clube"), salt).alias("clube_token"),
        col("_processed_date").alias("origem_processada_em"),
        current_timestamp().alias("protegido_em"),
    )


def create_partidas_protegidas(df: DataFrame, salt: str) -> DataFrame:
    """Cria uma visao de partidas com tecnicos e clubes pseudonimizados."""
    return df.select(
        col("partida_id"),
        col("temporada"),
        col("rodada"),
        col("data_partida"),
        pseudonymize(col("mandante"), salt).alias("mandante_token"),
        pseudonymize(col("visitante"), salt).alias("visitante_token"),
        col("mandante_placar"),
        col("visitante_placar"),
        mask_name(col("tecnico_mandante")).alias("tecnico_mandante_mascarado"),
        mask_name(col("tecnico_visitante")).alias("tecnico_visitante_mascarado"),
        current_timestamp().alias("protegido_em"),
    )


def save_protected(df: DataFrame, table_name: str) -> None:
    """Persiste uma tabela protegida em Delta Lake."""
    path = SECURITY_DIR / table_name
    print(f"Salvando {table_name} em: {path}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )


def build_security_views(spark: SparkSession) -> dict[str, DataFrame]:
    """Gera as tabelas protegidas para demonstracao de governanca."""
    salt = require_masking_salt(spark)
    SECURITY_DIR.mkdir(parents=True, exist_ok=True)

    jogadores = spark.read.format("delta").load(str(SILVER_DIR / "dim_jogador"))
    partidas = spark.read.format("delta").load(str(SILVER_DIR / "fato_partida"))

    tables = {
        "jogadores_protegidos": create_jogadores_protegidos(jogadores, salt),
        "partidas_protegidas": create_partidas_protegidas(partidas, salt),
    }
    for table_name, df in tables.items():
        save_protected(df, table_name)
    return tables


if __name__ == "__main__":
    spark = create_spark_session(app_name="SegurancaMascaramentoLGPD")
    try:
        protected_tables = build_security_views(spark)
        print("\n=== RESUMO DA CAMADA PROTEGIDA ===")
        for name, table in protected_tables.items():
            print(f"{name}: {table.count()} registros")
        print("Nenhum nome completo e exibido no log.")
    finally:
        spark.stop()
