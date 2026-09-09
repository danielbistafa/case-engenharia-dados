"""Entrypoint parametrizado para tasks Python do workflow Databricks."""

import argparse
import os
import runpy
from pathlib import Path


STAGE_SCRIPTS = {
    "setup": "notebooks/00_setup/setup_databricks.py",
    "bronze": "notebooks/01_bronze/ingestao_bronze.py",
    "silver": "notebooks/02_silver/transformacao_silver.py",
    "gold": "notebooks/03_gold/agregacoes_gold.py",
    "health-check": "notebooks/05_observabilidade/health_check.py",
    "security": "notebooks/06_seguranca/mascaramento_lgpd.py",
    "register": "notebooks/07_databricks/registrar_tabelas.py",
}


def _project_root() -> Path:
    """Localiza a raiz do projeto, funcionando localmente e no Databricks."""
    if os.getenv("CASE_PROJECT_ROOT"):
        return Path(os.environ["CASE_PROJECT_ROOT"])

    if "__file__" in globals():
        return Path(__file__).resolve().parents[1]

    cwd = Path.cwd()
    for parent in [cwd] + list(cwd.parents):
        if (parent / "src").is_dir() and (parent / "notebooks").is_dir():
            return parent

    raise RuntimeError(
        "Nao foi possivel localizar a raiz do projeto. "
        "Defina a variavel de ambiente CASE_PROJECT_ROOT."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", required=True, choices=STAGE_SCRIPTS)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="brasileirao")
    parser.add_argument("--volume", default="case_dados")
    parser.add_argument("--secret-scope", default="case-engenharia-dados")
    parser.add_argument("--secret-key", default="masking-salt")
    parser.add_argument("--execution-id")
    args = parser.parse_args()

    os.environ["DATABRICKS_CATALOG"] = args.catalog
    os.environ["DATABRICKS_SCHEMA"] = args.schema
    os.environ["DATABRICKS_VOLUME"] = args.volume
    os.environ["DATABRICKS_SECRET_SCOPE"] = args.secret_scope
    os.environ["DATABRICKS_SECRET_KEY"] = args.secret_key

    project_root = _project_root()
    script_path = project_root / STAGE_SCRIPTS[args.stage]
    if args.stage in {"bronze", "silver", "gold", "security"}:
        import sys

        sys.path.insert(0, str(project_root / "src"))
        from observability import PipelineLogger
        from spark_session import create_spark_session

        spark = create_spark_session(app_name=f"CaseBrasileirao-{args.stage}")
        logger = PipelineLogger(spark, execution_id=args.execution_id)
        logger.run_task(
            stage=args.stage.upper(),
            task_name=f"databricks_{args.stage}",
            func=lambda: runpy.run_path(str(script_path), run_name="__main__"),
        )
    else:
        runpy.run_path(str(script_path), run_name="__main__")


if __name__ == "__main__":
    main()
