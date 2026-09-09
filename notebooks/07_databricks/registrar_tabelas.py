"""Registra no Unity Catalog as tabelas Delta produzidas pelo pipeline."""

import os
import re
import sys
from pathlib import Path

if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    REPO_ROOT = Path(os.getcwd()).parents[1]
else:
    REPO_ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(0, str(REPO_ROOT / "src"))

from config import (  # noqa: E402
    BRONZE_DIR,
    DATABRICKS_CATALOG,
    DATABRICKS_SCHEMA,
    GOLD_DIR,
    IS_DATABRICKS,
    OBSERVABILITY_DIR,
    SECURITY_DIR,
    SILVER_DIR,
)
from spark_session import create_spark_session  # noqa: E402
