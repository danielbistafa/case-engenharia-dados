"""Cria schema e volume gerenciado usados pelo pipeline no Databricks."""

import os
import re
import sys
from pathlib import Path

# No Databricks o arquivo pode ser executado sem __file__; usamos o diretorio de trabalho como fallback.
if globals().get("__file__"):
    src_path = str(Path(__file__).resolve().parents[2] / "src")
else:
    src_path = os.path.join(os.getcwd(), "src")
sys.path.insert(0, src_path)

from config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA, DATABRICKS_VOLUME, IS_DATABRICKS  # noqa: E402
from spark_session import create_spark_session  # noqa: E402


def validate_identifier(value: str) -> str:
    """Aceita somente identificadores simples em comandos SQL administrativos."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Identificador invalido: {value}")
    return value


def main() -> None:
    if not IS_DATABRICKS:
        raise RuntimeError("Este setup deve ser executado no Databricks.")

    catalog = validate_identifier(DATABRICKS_CATALOG)
    schema = validate_identifier(DATABRICKS_SCHEMA)
    volume = validate_identifier(DATABRICKS_VOLUME)
    spark = create_spark_session(app_name="SetupDatabricksBrasileirao")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")

    print(f"Schema criado: {catalog}.{schema}")
    print(f"Volume criado: /Volumes/{catalog}/{schema}/{volume}")
    print("Envie os JSON para raw/Brasileirao_Dataset/data antes de executar a Bronze.")


if __name__ == "__main__":
    main()
