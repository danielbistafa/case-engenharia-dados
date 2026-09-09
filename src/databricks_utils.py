"""Utilitarios para execucao de scripts dentro do Databricks."""

import os
import sys
from pathlib import Path


def project_root_from_databricks_context() -> str:
    """
    Tenta descobrir a raiz do repositorio no Databricks usando variaveis de ambiente
    ou dbutils. Funciona tanto em notebooks quanto em jobs serverless.
    """
    # Caminho padrao quando o repositorio e importado como Git folder
    repo_name = os.getenv("CASE_REPO_NAME", "case-engenharia-dados")
    username = os.getenv("DATABRICKS_USERNAME", "")

    # 1. Tenta a variavel de ambiente explicita
    if os.getenv("CASE_PROJECT_ROOT"):
        return os.environ["CASE_PROJECT_ROOT"]

    # 2. Tenta inferir a partir do diretorio padrao de Repos
    candidates = [
        f"/Workspace/Repos/{username}/{repo_name}",
        f"/Workspace/Users/{username}/{repo_name}",
        f"/Workspace/Repos/{repo_name}",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return candidate

    # 3. Fallback para dbutils se estiver disponivel
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        notebook_path = DBUtils(spark).notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        if notebook_path:
            parts = Path(notebook_path).parts
            if repo_name in parts:
                idx = parts.index(repo_name)
                return str(Path(*parts[: idx + 1]))
    except Exception:
        pass

    raise RuntimeError(
        "Nao foi possivel determinar a raiz do projeto no Databricks. "
        "Defina a variavel CASE_PROJECT_ROOT ou DATABRICKS_USERNAME."
    )


def setup_src_path() -> None:
    """Adiciona src ao sys.path, funcionando local e no Databricks."""
    if "CASE_PROJECT_ROOT" in os.environ:
        root = os.environ["CASE_PROJECT_ROOT"]
    elif "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        # Execucao local: usa __file__ do chamador ou cwd
        frame = sys._getframe(1)
        caller_file = frame.f_globals.get("__file__")
        if caller_file:
            root = str(Path(caller_file).resolve().parents[2])
        else:
            root = os.getcwd()
    else:
        root = project_root_from_databricks_context()

    src_path = os.path.join(root, "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
