"""Cria schema e volume gerenciado usados pelo pipeline no Databricks."""

import os
import re
import sys
from pathlib import Path


def _repo_root() -> str:
    """Localiza a raiz do repositorio em execucao local ou no Databricks."""
    # Execucao local: __file__ sempre existe
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        return str(Path(__file__).resolve().parents[2])

    # Databricks: tenta usar o caminho do notebook via dbutils
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = globals().get("spark") or SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        if notebook_path:
            parts = Path(notebook_path).parts
            if "case-engenharia-dados" in parts:
                idx = parts.index("case-engenharia-dados")
                return str(Path(*parts[: idx + 1]))
    except Exception:
        pass

    # Fallbacks para Git folders comuns no Databricks
    username = os.getenv("DATABRICKS_USERNAME", "")
    candidates = [
        "/Workspace/Repos/case-engenharia-dados",
        f"/Workspace/Repos/{username}/case-engenharia-dados",
        "/Workspace/Users/case-engenharia-dados",
        f"/Workspace/Users/{username}/case-engenharia-dados",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return candidate

    raise RuntimeError(
        "Nao foi possivel localizar a raiz do repositorio no Databricks. "
        "Verifique se o Git folder foi importado ou defina CASE_PROJECT_ROOT."
    )


sys.path.insert(0, os.path.join(_repo_root(), "src"))

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
