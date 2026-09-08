"""Configuracoes de caminhos do projeto."""
from pathlib import Path

PROJECT_ROOT = Path(r"C:\SANTANDER\DATA_Master")

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "Brasileirao_Dataset"
RAW_JSON_DIR = RAW_DIR / "data"
RAW_CSV_DIR = RAW_DIR

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
SECURITY_DIR = PROJECT_ROOT / "data" / "security"

SPARK_WAREHOUSE = PROJECT_ROOT / "data" / "spark-warehouse"

# Caminho para os notebooks versionados (usados tambem no Databricks)
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
