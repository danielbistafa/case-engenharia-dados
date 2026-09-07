"""
Ingestao dos dados brutos do Brasileirao para a camada Bronze.

A camada Bronze preserva o schema original dos JSON, adicionando metadados
 de auditoria (temporada, rodada, arquivo de origem e data de ingestao).

Pode ser executado localmente (com PySpark) ou importado como notebook no Databricks.
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, input_file_name, col

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from config import RAW_JSON_DIR, BRONZE_DIR  # noqa: E402
from spark_session import create_spark_session  # noqa: E402


def extract_season(file_name: str) -> int:
    """Extrai o ano da temporada a partir do nome do arquivo."""
    match = re.search(r"(\d{4})", file_name)
    if not match:
        raise ValueError(f"Nao foi possivel extrair ano do arquivo: {file_name}")
    return int(match.group(1))


def extract_round(round_key: str) -> int:
    """Extrai o numero da rodada a partir de chaves como '1', '1 Rodada', '1a Rodada'."""
    match = re.search(r"(\d+)", str(round_key))
    if not match:
        raise ValueError(f"Nao foi possivel extrair numero da rodada: {round_key}")
    return int(match.group(1))


def ensure_dict(value: Any, default_keys: List[str]) -> Dict[str, Any]:
    """Garante que o valor seja um dicionario com as chaves padrao."""
    if isinstance(value, dict):
        return value
    return {key: "" for key in default_keys}


def ensure_goals_player(value: Any) -> Dict[str, Any]:
    """Garante que goalsPlayer seja um dict com home/away como listas."""
    default = {"home": [], "away": []}
    if isinstance(value, dict):
        return {
            key: value.get(key, default[key]) if isinstance(value.get(key), list) else default[key]
            for key in default
        }
    return default


def ensure_list(value: Any) -> List[Any]:
    """Garante que o valor seja uma lista."""
    if isinstance(value, list):
        return value
    return []


def load_matches_from_json(file_path: Path) -> List[Dict[str, Any]]:
    """
    Carrega um arquivo JSON brasileirao-AAAA.json e retorna uma lista
    de partidas, cada uma com os campos temporada e rodada.

    Padroniza campos com tipos mistos para manter um schema consistente.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        season_data = json.load(f)

    season = extract_season(file_path.name)
    matches = []

    for round_key, round_matches in season_data.items():
        if not isinstance(round_matches, list):
            continue
        round_number = extract_round(round_key)
        for match in round_matches:
            match_record = dict(match)
            match_record["clubs"] = ensure_dict(match_record.get("clubs"), ["home", "away"])
            match_record["goals"] = ensure_dict(match_record.get("goals"), ["home", "away"])
            match_record["cards"] = ensure_dict(match_record.get("cards"), ["home", "away"])
            match_record["coach"] = ensure_dict(match_record.get("coach"), ["home", "away"])
            match_record["formation"] = ensure_dict(match_record.get("formation"), ["home", "away"])
            match_record["goalsPlayer"] = ensure_goals_player(match_record.get("goalsPlayer"))
            match_record["stats"] = ensure_list(match_record.get("stats"))
            match_record["temporada"] = season
            match_record["rodada"] = round_number
            match_record["_source_file"] = str(file_path.name)
            matches.append(match_record)

    return matches


def ingest_bronze(
    spark: SparkSession,
    raw_dir: Path = RAW_JSON_DIR,
    bronze_output: Path = BRONZE_DIR / "partidas",
    temp_jsonl_dir: Path = BRONZE_DIR / "_temp_partidas_jsonl",
    rows_per_file: int = 500,
) -> DataFrame:
    """
    Le todos os JSON da pasta raw, cria o DataFrame Bronze e persiste em Delta Lake.
    Os registros sao salvos em multiplos arquivos JSON Lines pequenos para leitura
    distribuida pelo Spark, evitando sobrecarga no driver.
    """
    print(f"Lendo arquivos de: {raw_dir}")
    all_matches: List[Dict[str, Any]] = []

    json_files = sorted(raw_dir.glob("brasileirao-*.json"))
    print(f"Total de arquivos encontrados: {len(json_files)}")

    for file_path in json_files:
        matches = load_matches_from_json(file_path)
        all_matches.extend(matches)
        print(f"  {file_path.name}: {len(matches)} partidas")

    print(f"Total de partidas a serem ingeridas: {len(all_matches)}")

    # Escreve registros em varios arquivos JSON Lines pequenos
    bronze_output.parent.mkdir(parents=True, exist_ok=True)
    if temp_jsonl_dir.exists():
        # Limpa arquivos antigos
        for old_file in temp_jsonl_dir.glob("*.jsonl"):
            old_file.unlink()
    temp_jsonl_dir.mkdir(parents=True, exist_ok=True)

    for i in range(0, len(all_matches), rows_per_file):
        chunk = all_matches[i:i + rows_per_file]
        file_path = temp_jsonl_dir / f"partidas_{i:05d}.jsonl"
        with open(file_path, "w", encoding="utf-8") as f:
            for match in chunk:
                f.write(json.dumps(match, ensure_ascii=False) + "\n")

    print(f"Registros temporarios salvos em: {temp_jsonl_dir} ({len(list(temp_jsonl_dir.glob('*.jsonl')))} arquivos)")

    # Leitura distribuida pelo Spark a partir de uma pasta
    df = spark.read.json(str(temp_jsonl_dir / "*.jsonl"), multiLine=False)

    # Adiciona metadados de auditoria
    df = df.withColumn("_ingestion_date", current_timestamp())

    # Reorganiza colunas para facilitar leitura
    core_cols = ["temporada", "rodada"]
    other_cols = [c for c in df.columns if c not in core_cols + ["_ingestion_date"]]
    df = df.select(*core_cols, *other_cols, "_ingestion_date")

    print(f"\nSalvando camada Bronze em: {bronze_output}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(bronze_output))
    )

    print("Ingestao Bronze concluida com sucesso!")
    return df, temp_jsonl_dir


def cleanup_temp_files(temp_jsonl_dir: Path) -> None:
    """Remove arquivos JSON Lines temporarios."""
    if not temp_jsonl_dir.exists():
        return
    for temp_file in temp_jsonl_dir.glob("*.jsonl"):
        temp_file.unlink()
    temp_jsonl_dir.rmdir()
    print("Arquivos temporarios removidos.")


if __name__ == "__main__":
    spark = create_spark_session(app_name="IngestaoBronzeBrasileirao")
    df_bronze, temp_dir = ingest_bronze(spark)

    print("\nSchema da camada Bronze:")
    df_bronze.printSchema()

    print("\nAmostra de registros:")
    df_bronze.select("temporada", "rodada", "clubs", "goals", "date", "stadium").show(5, truncate=False)

    print(f"Total de registros: {df_bronze.count()}")

    cleanup_temp_files(temp_dir)
    spark.stop()
