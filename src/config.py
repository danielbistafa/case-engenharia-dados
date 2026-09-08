"""Configuracoes portaveis para execucao local e no Databricks."""
import os
from pathlib import Path


IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
PROJECT_ROOT = Path(os.getenv("CASE_PROJECT_ROOT", Path(__file__).resolve().parents[1]))

DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "brasileirao")
DATABRICKS_VOLUME = os.getenv("DATABRICKS_VOLUME", "case_dados")
DEFAULT_DATA_ROOT = (
    Path(f"/Volumes/{DATABRICKS_CATALOG}/{DATABRICKS_SCHEMA}/{DATABRICKS_VOLUME}")
    if IS_DATABRICKS
    else PROJECT_ROOT / "data"
)
DATA_ROOT = Path(os.getenv("CASE_DATA_ROOT", DEFAULT_DATA_ROOT))

RAW_DIR = DATA_ROOT / "raw" / "Brasileirao_Dataset"
RAW_JSON_DIR = RAW_DIR / "data"
RAW_CSV_DIR = RAW_DIR
BRONZE_DIR = DATA_ROOT / "bronze"
SILVER_DIR = DATA_ROOT / "silver"
GOLD_DIR = DATA_ROOT / "gold"
SECURITY_DIR = DATA_ROOT / "security"
OBSERVABILITY_DIR = DATA_ROOT / "observability"

SPARK_WAREHOUSE = DATA_ROOT / "spark-warehouse"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
