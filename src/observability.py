"""
Modulo de observabilidade do pipeline de dados.

Fornece funcoes para registrar a execucao de cada etapa (Bronze, Silver, Gold)
em uma tabela de auditoria. Localmente usa JSON Lines; no Databricks, pode ser
adaptado para Delta Lake.
"""

import json
import uuid
from datetime import datetime
from typing import Any, Callable, Optional

from pyspark.sql import DataFrame, SparkSession

from config import OBSERVABILITY_DIR  # noqa: E402


AUDIT_FILE_PATH = OBSERVABILITY_DIR / "pipeline_audit.jsonl"


def _now_iso() -> str:
    """Retorna timestamp atual no formato ISO 8601."""
    return datetime.now().isoformat()


class PipelineLogger:
    """
    Logger de auditoria para o pipeline de dados.
    Registra inicio, fim, status, registros processados e mensagens de erro.
    Localmente grava em JSON Lines para evitar problemas com Delta no Windows.
    """

    def __init__(self, spark: SparkSession, execution_id: Optional[str] = None):
        self.spark = spark
        self.execution_id = execution_id or str(uuid.uuid4())
        OBSERVABILITY_DIR.mkdir(parents=True, exist_ok=True)
        if not AUDIT_FILE_PATH.exists():
            AUDIT_FILE_PATH.write_text("", encoding="utf-8")

    def log(
        self,
        stage: str,
        task_name: str,
        status: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        records_processed: Optional[int] = None,
        source_path: Optional[str] = None,
        target_path: Optional[str] = None,
        message: Optional[str] = None,
    ) -> None:
        """Insere um registro de auditoria no arquivo JSON Lines."""
        record = {
            "execution_id": self.execution_id,
            "stage": stage,
            "task_name": task_name,
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "records_processed": records_processed,
            "source_path": source_path,
            "target_path": target_path,
            "message": message,
            "_log_time": _now_iso(),
        }
        with open(AUDIT_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def run_task(
        self,
        stage: str,
        task_name: str,
        func: Callable[..., Any],
        source_path: Optional[str] = None,
        target_path: Optional[str] = None,
        *args,
        **kwargs,
    ) -> Any:
        """
        Executa uma funcao e registra automaticamente o sucesso ou falha.
        Se a funcao retornar um DataFrame, tenta contar os registros processados.
        """
        start_time = _now_iso()
        try:
            result = func(*args, **kwargs)
            end_time = _now_iso()

            records_processed = None
            try:
                if isinstance(result, DataFrame):
                    records_processed = result.count()
                elif isinstance(result, dict):
                    records_processed = sum(
                        table.count() for table in result.values() if isinstance(table, DataFrame)
                    )
            except Exception:
                pass

            self.log(
                stage=stage,
                task_name=task_name,
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time,
                records_processed=records_processed,
                source_path=source_path,
                target_path=target_path,
                message="Task executada com sucesso",
            )
            return result

        except Exception as e:
            end_time = _now_iso()
            error_message = f"{type(e).__name__}: {str(e)}"
            self.log(
                stage=stage,
                task_name=task_name,
                status="FAILED",
                start_time=start_time,
                end_time=end_time,
                source_path=source_path,
                target_path=target_path,
                message=error_message[:2000],
            )
            raise


def get_audit_table(spark: SparkSession) -> DataFrame:
    """Retorna o DataFrame da tabela de auditoria."""
    if not AUDIT_FILE_PATH.exists() or AUDIT_FILE_PATH.stat().st_size == 0:
        # Retorna DataFrame vazio com schema
        from pyspark.sql.types import StructType, StructField, StringType, LongType
        schema = StructType([
            StructField("execution_id", StringType(), True),
            StructField("stage", StringType(), True),
            StructField("task_name", StringType(), True),
            StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True),
            StructField("status", StringType(), True),
            StructField("records_processed", LongType(), True),
            StructField("source_path", StringType(), True),
            StructField("target_path", StringType(), True),
            StructField("message", StringType(), True),
            StructField("_log_time", StringType(), True),
        ])
        return spark.createDataFrame([], schema)
    return spark.read.json(str(AUDIT_FILE_PATH))
