"""Demonstra pseudonimizacao e mascaramento sobre dados publicos do Brasileirao."""

import os
import sys
from pathlib import Path


def _repo_root() -> str:
    """Localiza a raiz do repositorio em execucao local ou no Databricks."""
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        return str(Path(__file__).resolve().parents[2])

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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, length, lit, sha2, substring, when

from config import IS_DATABRICKS, SECURITY_DIR, SILVER_DIR  # noqa: E402
from spark_session import create_spark_session  # noqa: E402
