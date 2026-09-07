"""
Transformacao da camada Bronze para Silver.

Objetivos:
- Limpar e normalizar os dados brutos
- Criar identificadores unicos
- Converter tipos de dados
- Criar tabelas dimensionais e fatos desnormalizadas
- Tratar inconsistencias e valores nulos
"""

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    dayofmonth,
    explode,
    lit,
    md5,
    monotonically_increasing_id,
    month,
    regexp_replace,
    struct,
    to_date,
    trim,
    upper,
    when,
    year,
)
from pyspark.sql.types import IntegerType, DoubleType, StringType

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from config import BRONZE_DIR, SILVER_DIR  # noqa: E402
from spark_session import create_spark_session  # noqa: E402
from utils import add_audit_columns, normalize_text  # noqa: E402


def read_bronze(spark: SparkSession, bronze_path: Path = BRONZE_DIR / "partidas") -> DataFrame:
    """Le a tabela Bronze de partidas."""
    print(f"Lendo camada Bronze de: {bronze_path}")
    return spark.read.format("delta").load(str(bronze_path))


def generate_partida_id(temporada: str, rodada: str, mandante: str, visitante: str, data: str) -> str:
    """Gera um identificador unico para a partida baseado em chave natural."""
    chave = f"{temporada}|{rodada}|{normalize_text(mandante)}|{normalize_text(visitante)}|{data}"
    # Spark usa md5 via funcao; aqui e um placeholder para documentacao
    return chave


def create_fato_partida(df_bronze: DataFrame) -> DataFrame:
    """Cria a tabela fato de partidas a partir da camada Bronze."""
    df = (
        df_bronze
        .withColumn("mandante", col("clubs.home"))
        .withColumn("visitante", col("clubs.away"))
        .withColumn("mandante_placar", col("goals.home").cast(IntegerType()))
        .withColumn("visitante_placar", col("goals.away").cast(IntegerType()))
        .withColumn("tecnico_mandante", col("coach.home"))
        .withColumn("tecnico_visitante", col("coach.away"))
        .withColumn("formacao_mandante", col("formation.home"))
        .withColumn("formacao_visitante", col("formation.away"))
        .withColumn("data_partida", to_date(col("date"), "dd/MM/yy"))
        .withColumn(
            "vencedor",
            when(col("mandante_placar") > col("visitante_placar"), col("mandante"))
            .when(col("mandante_placar") < col("visitante_placar"), col("visitante"))
            .otherwise("EMPATE")
        )
        .withColumn("empate", (col("mandante_placar") == col("visitante_placar")).cast("boolean"))
        .withColumn(
            "partida_id",
            md5(concat_ws("_", col("temporada"), col("rodada"), col("mandante"), col("visitante"), col("data_partida")))
        )
        .select(
            "partida_id",
            "temporada",
            "rodada",
            "data_partida",
            "hour",
            "mandante",
            "visitante",
            "mandante_placar",
            "visitante_placar",
            "vencedor",
            "empate",
            col("stadium").alias("estadio"),
            "tecnico_mandante",
            "tecnico_visitante",
            "formacao_mandante",
            "formacao_visitante",
            "_source_file",
            "_ingestion_date",
        )
    )
    return add_audit_columns(df, "silver_fato_partida")


def create_dim_clube(df_fato: DataFrame) -> DataFrame:
    """Cria dimensao de clubes a partir dos mandantes e visitantes."""
    mandantes = df_fato.select(col("mandante").alias("nome_clube")).distinct()
    visitantes = df_fato.select(col("visitante").alias("nome_clube")).distinct()
    df = (
        mandantes.union(visitantes)
        .distinct()
        .withColumn("clube_id", md5(upper(trim(col("nome_clube")))))
        .select("clube_id", "nome_clube")
    )
    return add_audit_columns(df, "silver_dim_clube")


def create_dim_estadio(df_fato: DataFrame) -> DataFrame:
    """Cria dimensao de estadios."""
    df = (
        df_fato.select(col("estadio").alias("nome_estadio"))
        .distinct()
        .filter(col("nome_estadio").isNotNull() & (trim(col("nome_estadio")) != ""))
        .withColumn("estadio_id", md5(upper(trim(col("nome_estadio")))))
        .select("estadio_id", "nome_estadio")
    )
    return add_audit_columns(df, "silver_dim_estadio")


def create_fato_gols(df_bronze: DataFrame, df_fato: DataFrame) -> DataFrame:
    """
    Cria fato de gols a partir de goalsPlayer.
    Estrutura: goalsPlayer.home/away -> array de {player, gols: [minutos]}
    """
    # Home
    home = (
        df_bronze
        .select(
            col("temporada"),
            col("rodada"),
            col("date"),
            col("clubs.home").alias("clube"),
            col("clubs.away").alias("adversario"),
            col("goalsPlayer.home").alias("jogadores_gols"),
            lit(True).alias("eh_mandante"),
        )
    )
    # Away
    away = (
        df_bronze
        .select(
            col("temporada"),
            col("rodada"),
            col("date"),
            col("clubs.away").alias("clube"),
            col("clubs.home").alias("adversario"),
            col("goalsPlayer.away").alias("jogadores_gols"),
            lit(False).alias("eh_mandante"),
        )
    )

    df_unido = home.unionByName(away)
    df = (
        df_unido
        .withColumn("jogador", explode(col("jogadores_gols")))
        .withColumn("nome_jogador", col("jogador.player"))
        .withColumn("minuto", explode(col("jogador.gols")))
        .withColumn("data_partida", to_date(col("date"), "dd/MM/yy"))
        .withColumn(
            "partida_id",
            md5(concat_ws("_", col("temporada"), col("rodada"), col("clube"), col("adversario"), col("data_partida")))
        )
        .select(
            "partida_id",
            "temporada",
            "rodada",
            "data_partida",
            "clube",
            "adversario",
            "eh_mandante",
            "nome_jogador",
            "minuto",
        )
        .filter(col("nome_jogador").isNotNull() & (trim(col("nome_jogador")) != ""))
    )
    return add_audit_columns(df, "silver_fato_gols")


def create_fato_cartoes(df_bronze: DataFrame, df_fato: DataFrame) -> DataFrame:
    """
    Cria fato de cartoes a partir de cards.home/away.
    Estrutura: cards.home/away -> {yellow: [...], red: [...]}
    """
    cores = [("yellow", "AMARELO"), ("red", "VERMELHO")]
    fatos = []

    for lado in ["home", "away"]:
        adversario_lado = "away" if lado == "home" else "home"
        eh_mandante = lit(lado == "home")
        for cor_json, cor_label in cores:
            df = (
                df_bronze
                .select(
                    col("temporada"),
                    col("rodada"),
                    col("date"),
                    col(f"clubs.{lado}").alias("clube"),
                    col(f"clubs.{adversario_lado}").alias("adversario"),
                    col(f"cards.{lado}.{cor_json}").alias("cartoes"),
                )
                .withColumn("eh_mandante", eh_mandante)
                .withColumn("cartao", explode(col("cartoes")))
                .select(
                    col("temporada"),
                    col("rodada"),
                    col("date"),
                    col("clube"),
                    col("adversario"),
                    col("eh_mandante"),
                    col("cartao.player").alias("nome_jogador"),
                    col("cartao.position").alias("posicao"),
                    col("cartao.number").alias("numero_camisa"),
                    col("cartao.time").alias("minuto"),
                    lit(cor_label).alias("cor_cartao"),
                )
            )
            fatos.append(df)

    df = (
        fatos[0]
        .unionByName(fatos[1])
        .unionByName(fatos[2])
        .unionByName(fatos[3])
        .withColumn("data_partida", to_date(col("date"), "dd/MM/yy"))
        .withColumn(
            "partida_id",
            md5(concat_ws("_", col("temporada"), col("rodada"), col("clube"), col("adversario"), col("data_partida")))
        )
        .select(
            "partida_id",
            "temporada",
            "rodada",
            "data_partida",
            "clube",
            "adversario",
            "eh_mandante",
            "nome_jogador",
            "cor_cartao",
            "posicao",
            "numero_camisa",
            "minuto",
        )
        .filter(col("nome_jogador").isNotNull() & (trim(col("nome_jogador")) != ""))
    )
    return add_audit_columns(df, "silver_fato_cartoes")


def create_fato_estatisticas(df_bronze: DataFrame) -> DataFrame:
    """
    Cria fato de estatisticas a partir do campo stats.
    Estrutura: stats -> array de {home, stat, away}
    """
    df_stats = (
        df_bronze
        .select("temporada", "rodada", "date", "clubs", "stats")
        .withColumn("clube", col("clubs.home"))
        .withColumn("adversario", col("clubs.away"))
    )
    df = (
        df_stats
        .withColumn("stat", explode(col("stats")))
        .select(
            col("temporada"),
            col("rodada"),
            col("date"),
            col("clube"),
            col("adversario"),
            col("stat.stat").alias("nome_estatistica"),
            col("stat.home").alias("valor_mandante"),
            col("stat.away").alias("valor_visitante"),
        )
        .withColumn("data_partida", to_date(col("date"), "dd/MM/yy"))
        .withColumn(
            "partida_id",
            md5(concat_ws("_", col("temporada"), col("rodada"), col("clube"), col("adversario"), col("data_partida")))
        )
        .select(
            "partida_id",
            "temporada",
            "rodada",
            "data_partida",
            "nome_estatistica",
            "valor_mandante",
            "valor_visitante",
        )
        .filter(col("nome_estatistica").isNotNull() & (trim(col("nome_estatistica")) != ""))
    )
    return add_audit_columns(df, "silver_fato_estatisticas")


def create_dim_jogador(df_gols: DataFrame, df_cartoes: DataFrame) -> DataFrame:
    """Cria dimensao de jogadores a partir dos fatos de gols e cartoes."""
    jogadores_gols = df_gols.select(col("nome_jogador").alias("nome"), col("clube"))
    jogadores_cartoes = df_cartoes.select(col("nome_jogador").alias("nome"), col("clube"))
    df = (
        jogadores_gols.union(jogadores_cartoes)
        .distinct()
        .filter(col("nome").isNotNull() & (trim(col("nome")) != ""))
        .withColumn("jogador_id", md5(concat_ws("_", upper(trim(col("nome"))), upper(trim(col("clube"))))))
        .select("jogador_id", col("nome").alias("nome_jogador"), col("clube").alias("ultimo_clube"))
    )
    return add_audit_columns(df, "silver_dim_jogador")


def save_silver(df: DataFrame, path: Path, table_name: str) -> None:
    """Salva um DataFrame na camada Silver em formato Delta Lake."""
    print(f"Salvando {table_name} em: {path}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )


def transform_silver(spark: SparkSession) -> dict:
    """Executa todas as transformacoes da camada Silver."""
    silver_output = SILVER_DIR
    silver_output.mkdir(parents=True, exist_ok=True)

    df_bronze = read_bronze(spark)

    print("Criando fato_partida...")
    df_fato_partida = create_fato_partida(df_bronze)
    save_silver(df_fato_partida, silver_output / "fato_partida", "fato_partida")

    print("Criando dim_clube...")
    df_dim_clube = create_dim_clube(df_fato_partida)
    save_silver(df_dim_clube, silver_output / "dim_clube", "dim_clube")

    print("Criando dim_estadio...")
    df_dim_estadio = create_dim_estadio(df_fato_partida)
    save_silver(df_dim_estadio, silver_output / "dim_estadio", "dim_estadio")

    print("Criando fato_gols...")
    df_fato_gols = create_fato_gols(df_bronze, df_fato_partida)
    save_silver(df_fato_gols, silver_output / "fato_gols", "fato_gols")

    print("Criando fato_cartoes...")
    df_fato_cartoes = create_fato_cartoes(df_bronze, df_fato_partida)
    save_silver(df_fato_cartoes, silver_output / "fato_cartoes", "fato_cartoes")

    print("Criando fato_estatisticas...")
    df_fato_estatisticas = create_fato_estatisticas(df_bronze)
    save_silver(df_fato_estatisticas, silver_output / "fato_estatisticas", "fato_estatisticas")

    print("Criando dim_jogador...")
    df_dim_jogador = create_dim_jogador(df_fato_gols, df_fato_cartoes)
    save_silver(df_dim_jogador, silver_output / "dim_jogador", "dim_jogador")

    return {
        "fato_partida": df_fato_partida,
        "dim_clube": df_dim_clube,
        "dim_estadio": df_dim_estadio,
        "fato_gols": df_fato_gols,
        "fato_cartoes": df_fato_cartoes,
        "fato_estatisticas": df_fato_estatisticas,
        "dim_jogador": df_dim_jogador,
    }


if __name__ == "__main__":
    spark = create_spark_session(app_name="TransformacaoSilverBrasileirao")
    tables = transform_silver(spark)

    print("\n=== RESUMO DA CAMADA SILVER ===")
    for name, df in tables.items():
        count = df.count()
        print(f"{name}: {count} registros")

    spark.stop()
