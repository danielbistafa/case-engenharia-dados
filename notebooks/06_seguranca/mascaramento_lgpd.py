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
