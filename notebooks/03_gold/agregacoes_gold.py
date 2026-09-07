"""
Camada Gold: agregacoes e metricas de negocio.

A partir das tabelas Silver, cria tabelas analiticas prontas para
visualizacao em dashboards e apresentacoes.
"""

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    lit,
    max,
    min,
    regexp_replace,
    round,
    sum,
    trim,
    when,
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, dense_rank

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from config import SILVER_DIR, GOLD_DIR  # noqa: E402
from spark_session import create_spark_session  # noqa: E402
from utils import add_audit_columns  # noqa: E402


def read_silver_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Le uma tabela Delta da camada Silver."""
    path = SILVER_DIR / table_name
    print(f"Lendo tabela Silver: {path}")
    return spark.read.format("delta").load(str(path))


def save_gold(df: DataFrame, path: Path, table_name: str) -> None:
    """Salva um DataFrame na camada Gold em formato Delta Lake."""
    print(f"Salvando {table_name} em: {path}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )


def create_classificacao_por_temporada(df_partida: DataFrame) -> DataFrame:
    """
    Cria classificacao final do campeonato por temporada.
    Cada partida gera dois registros (mandante e visitante) para calculo.
    """
    # Registros do ponto de vista do mandante
    mandante = (
        df_partida.select(
            "temporada",
            col("mandante").alias("clube"),
            col("mandante_placar").alias("gols_pro"),
            col("visitante_placar").alias("gols_contra"),
            when(col("mandante_placar") > col("visitante_placar"), lit(3))
            .when(col("mandante_placar") == col("visitante_placar"), lit(1))
            .otherwise(lit(0)).alias("pontos"),
            when(col("mandante_placar") > col("visitante_placar"), lit(1)).otherwise(lit(0)).alias("vitorias"),
            when(col("mandante_placar") == col("visitante_placar"), lit(1)).otherwise(lit(0)).alias("empates"),
            when(col("mandante_placar") < col("visitante_placar"), lit(1)).otherwise(lit(0)).alias("derrotas"),
            lit(1).alias("jogos"),
        )
    )

    # Registros do ponto de vista do visitante
    visitante = (
        df_partida.select(
            "temporada",
            col("visitante").alias("clube"),
            col("visitante_placar").alias("gols_pro"),
            col("mandante_placar").alias("gols_contra"),
            when(col("visitante_placar") > col("mandante_placar"), lit(3))
            .when(col("visitante_placar") == col("mandante_placar"), lit(1))
            .otherwise(lit(0)).alias("pontos"),
            when(col("visitante_placar") > col("mandante_placar"), lit(1)).otherwise(lit(0)).alias("vitorias"),
            when(col("visitante_placar") == col("mandante_placar"), lit(1)).otherwise(lit(0)).alias("empates"),
            when(col("visitante_placar") < col("mandante_placar"), lit(1)).otherwise(lit(0)).alias("derrotas"),
            lit(1).alias("jogos"),
        )
    )

    df = (
        mandante.union(visitante)
        .groupBy("temporada", "clube")
        .agg(
            sum("pontos").alias("pontos"),
            sum("jogos").alias("jogos"),
            sum("vitorias").alias("vitorias"),
            sum("empates").alias("empates"),
            sum("derrotas").alias("derrotas"),
            sum("gols_pro").alias("gols_pro"),
            sum("gols_contra").alias("gols_contra"),
            (sum("gols_pro") - sum("gols_contra")).alias("saldo_gols"),
        )
        .withColumn("aproveitamento", round((col("pontos") / (col("jogos") * lit(3))) * lit(100), 2))
        .withColumn("rank", dense_rank().over(Window.partitionBy("temporada").orderBy(
            col("pontos").desc(), col("vitorias").desc(), col("saldo_gols").desc(), col("gols_pro").desc()
        )))
        .select(
            "temporada",
            "rank",
            "clube",
            "pontos",
            "jogos",
            "vitorias",
            "empates",
            "derrotas",
            "gols_pro",
            "gols_contra",
            "saldo_gols",
            "aproveitamento",
        )
    )
    return add_audit_columns(df, "gold_classificacao_por_temporada")


def create_aproveitamento_mandante_visitante(df_partida: DataFrame) -> DataFrame:
    """
    Aproveitamento dos clubes como mandante e como visitante.
    """
    mandante = (
        df_partida.select(
            "temporada",
            lit("MANDANTE").alias("tipo"),
            col("mandante").alias("clube"),
            when(col("mandante_placar") > col("visitante_placar"), lit(3))
            .when(col("mandante_placar") == col("visitante_placar"), lit(1))
            .otherwise(lit(0)).alias("pontos"),
            lit(1).alias("jogos"),
        )
    )
    visitante = (
        df_partida.select(
            "temporada",
            lit("VISITANTE").alias("tipo"),
            col("visitante").alias("clube"),
            when(col("visitante_placar") > col("mandante_placar"), lit(3))
            .when(col("visitante_placar") == col("mandante_placar"), lit(1))
            .otherwise(lit(0)).alias("pontos"),
            lit(1).alias("jogos"),
        )
    )

    df = (
        mandante.union(visitante)
        .groupBy("temporada", "clube", "tipo")
        .agg(
            sum("pontos").alias("pontos"),
            sum("jogos").alias("jogos"),
            round((sum("pontos") / (sum("jogos") * lit(3))) * lit(100), 2).alias("aproveitamento"),
        )
        .select("temporada", "clube", "tipo", "pontos", "jogos", "aproveitamento")
    )
    return add_audit_columns(df, "gold_aproveitamento_mandante_visitante")


def create_artilharia(df_gols: DataFrame) -> DataFrame:
    """
    Artilharia: gols por jogador e clube, com total e posicao no ranking.
    """
    df = (
        df_gols.filter(col("nome_jogador").isNotNull() & (trim(col("nome_jogador")) != ""))
        .groupBy("nome_jogador", "clube")
        .agg(
            count("*").alias("total_gols"),
            countDistinct("partida_id").alias("jogos_com_gol"),
        )
        .withColumn("rank", dense_rank().over(Window.orderBy(col("total_gols").desc())))
        .select("rank", "nome_jogador", "clube", "total_gols", "jogos_com_gol")
    )
    return add_audit_columns(df, "gold_artilharia")


def create_evolucao_gols_por_temporada(df_partida: DataFrame) -> DataFrame:
    """
    Evolucao historica da media de gols por partida ao longo das temporadas.
    """
    df = (
        df_partida.groupBy("temporada")
        .agg(
            count("*").alias("total_partidas"),
            sum(col("mandante_placar") + col("visitante_placar")).alias("total_gols"),
            avg(col("mandante_placar") + col("visitante_placar")).alias("media_gols_por_partida"),
            sum(when(col("empate"), lit(0)).otherwise(lit(1))).alias("partidas_com_vencedor"),
        )
        .withColumn("media_gols_por_partida", round(col("media_gols_por_partida"), 2))
        .select("temporada", "total_partidas", "total_gols", "media_gols_por_partida", "partidas_com_vencedor")
    )
    return add_audit_columns(df, "gold_evolucao_gols_por_temporada")


def create_estatisticas_medias(df_estatisticas: DataFrame) -> DataFrame:
    """
    Media de estatisticas por temporada (ex: chutes, posse de bola, faltas).
    Considera apenas registros com valores numericos preenchidos.
    """
    # Limpa valores vazios e converte para double
    df = (
        df_estatisticas
        .withColumn("valor_mandante_num", regexp_replace(col("valor_mandante"), "%", ""))
        .withColumn("valor_visitante_num", regexp_replace(col("valor_visitante"), "%", ""))
        .withColumn("valor_mandante_num", when(col("valor_mandante_num") == "", lit(None)).otherwise(col("valor_mandante_num")))
        .withColumn("valor_visitante_num", when(col("valor_visitante_num") == "", lit(None)).otherwise(col("valor_visitante_num")))
        .withColumn("valor_mandante_num", col("valor_mandante_num").cast("double"))
        .withColumn("valor_visitante_num", col("valor_visitante_num").cast("double"))
    )

    df_mandante = df.select(
        "temporada",
        "nome_estatistica",
        col("valor_mandante_num").alias("valor"),
        lit("MANDANTE").alias("tipo"),
    )
    df_visitante = df.select(
        "temporada",
        "nome_estatistica",
        col("valor_visitante_num").alias("valor"),
        lit("VISITANTE").alias("tipo"),
    )

    df_final = (
        df_mandante.unionByName(df_visitante)
        .filter(col("valor").isNotNull())
        .groupBy("temporada", "nome_estatistica", "tipo")
        .agg(round(avg("valor"), 2).alias("media"))
        .select("temporada", "nome_estatistica", "tipo", "media")
    )
    return add_audit_columns(df_final, "gold_estatisticas_medias")


def create_cartoes_por_clube(df_cartoes: DataFrame) -> DataFrame:
    """
    Quantidade de cartoes amarelos e vermelhos por clube e temporada.
    """
    df = (
        df_cartoes.groupBy("temporada", "clube", "cor_cartao")
        .agg(count("*").alias("quantidade"))
        .groupBy("temporada", "clube")
        .pivot("cor_cartao", ["AMARELO", "VERMELHO"])
        .agg(sum("quantidade"))
        .fillna(0)
        .withColumnRenamed("AMARELO", "cartoes_amarelos")
        .withColumnRenamed("VERMELHO", "cartoes_vermelhos")
        .select("temporada", "clube", "cartoes_amarelos", "cartoes_vermelhos")
    )
    return add_audit_columns(df, "gold_cartoes_por_clube")


def create_desempenho_treinador(df_partida: DataFrame) -> DataFrame:
    """
    Desempenho dos tecnicos por temporada.
    """
    mandante = (
        df_partida.select(
            "temporada",
            col("mandante").alias("clube"),
            col("tecnico_mandante").alias("tecnico"),
            when(col("mandante_placar") > col("visitante_placar"), lit(3))
            .when(col("mandante_placar") == col("visitante_placar"), lit(1))
            .otherwise(lit(0)).alias("pontos"),
            lit(1).alias("jogos"),
        )
    )
    visitante = (
        df_partida.select(
            "temporada",
            col("visitante").alias("clube"),
            col("tecnico_visitante").alias("tecnico"),
            when(col("visitante_placar") > col("mandante_placar"), lit(3))
            .when(col("visitante_placar") == col("mandante_placar"), lit(1))
            .otherwise(lit(0)).alias("pontos"),
            lit(1).alias("jogos"),
        )
    )

    df = (
        mandante.unionByName(visitante)
        .filter(col("tecnico").isNotNull() & (trim(col("tecnico")) != ""))
        .groupBy("temporada", "clube", "tecnico")
        .agg(
            sum("pontos").alias("pontos"),
            sum("jogos").alias("jogos"),
            round((sum("pontos") / (sum("jogos") * lit(3))) * lit(100), 2).alias("aproveitamento"),
        )
        .select("temporada", "clube", "tecnico", "pontos", "jogos", "aproveitamento")
    )
    return add_audit_columns(df, "gold_desempenho_treinador")


def create_gols_por_minuto(df_gols: DataFrame) -> DataFrame:
    """
    Distribuicao de gols por minuto de jogo.
    """
    df = (
        df_gols.filter(col("minuto").isNotNull() & (trim(col("minuto")) != ""))
        .withColumn("minuto_limpo", regexp_replace(col("minuto"), "[^0-9]", ""))
        .withColumn("minuto_num", col("minuto_limpo").cast("int"))
        .filter(col("minuto_num").isNotNull() & (col("minuto_num") <= 120))
        .withColumn(
            "intervalo",
            when(col("minuto_num") <= 15, lit("0-15"))
            .when(col("minuto_num") <= 30, lit("16-30"))
            .when(col("minuto_num") <= 45, lit("31-45"))
            .when(col("minuto_num") <= 60, lit("46-60"))
            .when(col("minuto_num") <= 75, lit("61-75"))
            .when(col("minuto_num") <= 90, lit("76-90"))
            .otherwise(lit("90+"))
        )
        .groupBy("intervalo")
        .agg(count("*").alias("quantidade_gols"))
        .select("intervalo", "quantidade_gols")
    )
    return add_audit_columns(df, "gold_gols_por_minuto")


def build_gold_tables(spark: SparkSession) -> dict:
    """Executa todas as agregacoes da camada Gold."""
    gold_output = GOLD_DIR
    gold_output.mkdir(parents=True, exist_ok=True)

    df_partida = read_silver_table(spark, "fato_partida")
    df_gols = read_silver_table(spark, "fato_gols")
    df_cartoes = read_silver_table(spark, "fato_cartoes")
    df_estatisticas = read_silver_table(spark, "fato_estatisticas")

    tables = {}

    print("Criando classificacao_por_temporada...")
    tables["classificacao_por_temporada"] = create_classificacao_por_temporada(df_partida)
    save_gold(tables["classificacao_por_temporada"], gold_output / "classificacao_por_temporada", "classificacao_por_temporada")

    print("Criando aproveitamento_mandante_visitante...")
    tables["aproveitamento_mandante_visitante"] = create_aproveitamento_mandante_visitante(df_partida)
    save_gold(tables["aproveitamento_mandante_visitante"], gold_output / "aproveitamento_mandante_visitante", "aproveitamento_mandante_visitante")

    print("Criando artilharia...")
    tables["artilharia"] = create_artilharia(df_gols)
    save_gold(tables["artilharia"], gold_output / "artilharia", "artilharia")

    print("Criando evolucao_gols_por_temporada...")
    tables["evolucao_gols_por_temporada"] = create_evolucao_gols_por_temporada(df_partida)
    save_gold(tables["evolucao_gols_por_temporada"], gold_output / "evolucao_gols_por_temporada", "evolucao_gols_por_temporada")

    print("Criando estatisticas_medias...")
    tables["estatisticas_medias"] = create_estatisticas_medias(df_estatisticas)
    save_gold(tables["estatisticas_medias"], gold_output / "estatisticas_medias", "estatisticas_medias")

    print("Criando cartoes_por_clube...")
    tables["cartoes_por_clube"] = create_cartoes_por_clube(df_cartoes)
    save_gold(tables["cartoes_por_clube"], gold_output / "cartoes_por_clube", "cartoes_por_clube")

    print("Criando desempenho_treinador...")
    tables["desempenho_treinador"] = create_desempenho_treinador(df_partida)
    save_gold(tables["desempenho_treinador"], gold_output / "desempenho_treinador", "desempenho_treinador")

    print("Criando gols_por_minuto...")
    tables["gols_por_minuto"] = create_gols_por_minuto(df_gols)
    save_gold(tables["gols_por_minuto"], gold_output / "gols_por_minuto", "gols_por_minuto")

    return tables


if __name__ == "__main__":
    spark = create_spark_session(app_name="AgregacoesGoldBrasileirao")
    tables = build_gold_tables(spark)

    print("\n=== RESUMO DA CAMADA GOLD ===")
    for name, df in tables.items():
        count = df.count()
        print(f"{name}: {count} registros")

    spark.stop()
