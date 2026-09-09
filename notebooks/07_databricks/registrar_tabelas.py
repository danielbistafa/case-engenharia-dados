"""Registra no Unity Catalog as tabelas Delta produzidas pelo pipeline.

Como o Databricks Free Edition/Unity Catalog nao aceita LOCATION com esquema dbfs,
as tabelas sao criadas como managed tables a partir dos dados armazenados no Volume.
"""

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


TABLES = {
    "bronze_partidas": BRONZE_DIR / "partidas",
    "silver_fato_partida": SILVER_DIR / "fato_partida",
    "silver_dim_clube": SILVER_DIR / "dim_clube",
    "silver_dim_estadio": SILVER_DIR / "dim_estadio",
    "silver_fato_gols": SILVER_DIR / "fato_gols",
    "silver_fato_cartoes": SILVER_DIR / "fato_cartoes",
    "silver_fato_estatisticas": SILVER_DIR / "fato_estatisticas",
    "silver_dim_jogador": SILVER_DIR / "dim_jogador",
    "gold_classificacao_por_temporada": GOLD_DIR / "classificacao_por_temporada",
    "gold_aproveitamento_mandante_visitante": GOLD_DIR / "aproveitamento_mandante_visitante",
    "gold_artilharia": GOLD_DIR / "artilharia",
    "gold_evolucao_gols_por_temporada": GOLD_DIR / "evolucao_gols_por_temporada",
    "gold_estatisticas_medias": GOLD_DIR / "estatisticas_medias",
    "gold_cartoes_por_clube": GOLD_DIR / "cartoes_por_clube",
    "gold_desempenho_treinador": GOLD_DIR / "desempenho_treinador",
    "gold_gols_por_minuto": GOLD_DIR / "gols_por_minuto",
    "security_jogadores_protegidos": SECURITY_DIR / "jogadores_protegidos",
    "security_partidas_protegidas": SECURITY_DIR / "partidas_protegidas",
}


def validate_identifier(value: str) -> str:
    """Aceita somente identificadores simples em comandos SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Identificador invalido: {value}")
    return value


def _delta_table_exists(path: Path) -> bool:
    """Verifica se um diretorio contem uma tabela Delta."""
    return (path / "_delta_log").exists()


def main() -> None:
    if not IS_DATABRICKS:
        raise RuntimeError("O registro no Unity Catalog deve ser executado no Databricks.")

    catalog = validate_identifier(DATABRICKS_CATALOG)
    schema = validate_identifier(DATABRICKS_SCHEMA)
    spark = create_spark_session(app_name="RegistrarTabelasBrasileirao")

    for table_name, path in TABLES.items():
        table = validate_identifier(table_name)
        if not _delta_table_exists(path):
            print(f"Pulando {table}: dados nao encontrados em {path}")
            continue

        spark.sql(
            f"CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`{table}` "
            f"USING DELTA AS SELECT * FROM delta.`{path.as_posix()}`"
        )
        print(f"Registrada: {catalog}.{schema}.{table}")

    # Tabela de auditoria (somente se houver arquivos JSON)
    json_files = [f for f in OBSERVABILITY_DIR.iterdir() if f.is_file()] if OBSERVABILITY_DIR.exists() else []
    if json_files:
        spark.sql(
            f"CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`pipeline_audit` "
            f"USING JSON AS SELECT * FROM json.`{OBSERVABILITY_DIR.as_posix()}`"
        )
        print(f"Registrada: {catalog}.{schema}.pipeline_audit")
    else:
        print("Tabela pipeline_audit nao criada: nenhum arquivo JSON de auditoria encontrado.")


if __name__ == "__main__":
    main()
