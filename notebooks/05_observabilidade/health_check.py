"""
Health check do pipeline.

Le a tabela de auditoria e exibe o status das ultimas execucoes,
alem de validar a existencia e contagem das tabelas nas camadas
Bronze, Silver e Gold.
"""

import os
import sys
from pathlib import Path

if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    REPO_ROOT = Path(os.getcwd()).parents[1]
else:
    REPO_ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(0, str(REPO_ROOT / "src"))

from config import BRONZE_DIR, GOLD_DIR, SILVER_DIR  # noqa: E402
from observability import get_audit_table  # noqa: E402
from spark_session import create_spark_session  # noqa: E402


def check_table(spark, table_path: Path, description: str) -> dict:
    """Verifica se uma tabela Delta existe e retorna contagem."""
    if not (table_path / "_delta_log").exists():
        return {"tabela": description, "status": "NAO_ENCONTRADA", "registros": 0}
    try:
        df = spark.read.format("delta").load(str(table_path))
        count = df.count()
        return {"tabela": description, "status": "OK", "registros": count}
    except Exception as e:
        return {"tabela": description, "status": f"ERRO: {e}", "registros": 0}


def main() -> None:
    spark = create_spark_session(app_name="HealthCheckPipeline")

    print("\n" + "=" * 60)
    print("HEALTH CHECK - PIPELINE BRASILEIRAO")
    print("=" * 60)

    # Lista de tabelas esperadas
    checks = [
        (BRONZE_DIR / "partidas", "Bronze - Partidas"),
        (SILVER_DIR / "fato_partida", "Silver - Fato Partida"),
        (SILVER_DIR / "dim_clube", "Silver - Dim Clube"),
        (SILVER_DIR / "dim_estadio", "Silver - Dim Estadio"),
        (SILVER_DIR / "fato_gols", "Silver - Fato Gols"),
        (SILVER_DIR / "fato_cartoes", "Silver - Fato Cartoes"),
        (SILVER_DIR / "fato_estatisticas", "Silver - Fato Estatisticas"),
        (SILVER_DIR / "dim_jogador", "Silver - Dim Jogador"),
        (GOLD_DIR / "classificacao_por_temporada", "Gold - Classificacao"),
        (GOLD_DIR / "artilharia", "Gold - Artilharia"),
        (GOLD_DIR / "evolucao_gols_por_temporada", "Gold - Evolucao Gols"),
        (GOLD_DIR / "aproveitamento_mandante_visitante", "Gold - Aproveitamento"),
        (GOLD_DIR / "cartoes_por_clube", "Gold - Cartoes por Clube"),
        (GOLD_DIR / "estatisticas_medias", "Gold - Estatisticas Medias"),
        (GOLD_DIR / "desempenho_treinador", "Gold - Desempenho Treinador"),
        (GOLD_DIR / "gols_por_minuto", "Gold - Gols por Minuto"),
    ]

    print("\nValidacao das tabelas:")
    for path, desc in checks:
        result = check_table(spark, path, desc)
        print(f"  {result['tabela']:<45} | {result['status']:<20} | {result['registros']:,} registros")

    print("\n" + "=" * 60)
    print("ULTIMAS EXECUCOES REGISTRADAS NA AUDITORIA")
    print("=" * 60)
    df_audit = (
        get_audit_table(spark)
        .orderBy("start_time", ascending=False)
        .select("execution_id", "stage", "task_name", "status", "records_processed", "start_time", "message")
    )
    df_audit.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
