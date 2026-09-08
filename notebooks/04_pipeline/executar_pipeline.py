"""
Pipeline completo com observabilidade.

Executa sequencialmente:
1. Ingestao Bronze
2. Transformacao Silver
3. Agregacoes Gold

Todas as etapas sao registradas na tabela de auditoria
`data/observability/pipeline_audit`.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "notebooks" / "01_bronze"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "notebooks" / "02_silver"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "notebooks" / "03_gold"))

from config import BRONZE_DIR, SILVER_DIR, GOLD_DIR  # noqa: E402
from observability import PipelineLogger, get_audit_table  # noqa: E402
from spark_session import create_spark_session  # noqa: E402

# Importa as funcoes principais de cada camada
from ingestao_bronze import ingest_bronze  # noqa: E402
from transformacao_silver import transform_silver  # noqa: E402
from agregacoes_gold import build_gold_tables  # noqa: E402


def main() -> None:
    spark = create_spark_session(app_name="PipelineBrasileiraoComObservabilidade")
    logger = PipelineLogger(spark)

    try:
        print("\n" + "=" * 60)
        print("INICIANDO PIPELINE COMPLETO")
        print("=" * 60)

        # Bronze
        print("\n--- ETAPA 1: BRONZE ---")
        df_bronze = logger.run_task(
            stage="BRONZE",
            task_name="ingestao_bronze",
            func=ingest_bronze,
            source_path=str(Path("data/raw/Brasileirao_Dataset/data").resolve()),
            target_path=str(BRONZE_DIR / "partidas"),
            spark=spark,
        )

        # Silver
        print("\n--- ETAPA 2: SILVER ---")
        silver_tables = logger.run_task(
            stage="SILVER",
            task_name="transformacao_silver",
            func=transform_silver,
            source_path=str(BRONZE_DIR / "partidas"),
            target_path=str(SILVER_DIR),
            spark=spark,
        )

        # Gold
        print("\n--- ETAPA 3: GOLD ---")
        gold_tables = logger.run_task(
            stage="GOLD",
            task_name="agregacoes_gold",
            func=build_gold_tables,
            source_path=str(SILVER_DIR),
            target_path=str(GOLD_DIR),
            spark=spark,
        )

        print("\n" + "=" * 60)
        print("PIPELINE FINALIZADO COM SUCESSO")
        print("=" * 60)

        print("\nResumo da execucao:")
        df_audit = get_audit_table(spark).orderBy("start_time")
        df_audit.select(
            "execution_id",
            "stage",
            "task_name",
            "status",
            "records_processed",
            "start_time",
            "end_time",
        ).show(truncate=False)

    except Exception as e:
        print(f"\nPipeline finalizado com erro: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
