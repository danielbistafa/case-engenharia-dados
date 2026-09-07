"""Funcoes utilitarias para transformacao e normalizacao de dados."""
import re
import unicodedata
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    dayofmonth,
    lit,
    lower,
    md5,
    month,
    regexp_replace,
    trim,
    upper,
    when,
    year,
)


def normalize_text(text: Optional[str]) -> Optional[str]:
    """
    Normaliza texto: remove espacos extras, converte para maiusculas,
    remove acentos e caracteres especiais de controle.
    Retorna None se o texto for vazio.
    """
    if text is None:
        return None
    text = str(text).strip()
    if text in ("", "-", "nan", "None", "null"):
        return None
    # Remove acentos mantendo caracteres base
    text = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("ASCII")
    text = re.sub(r"\s+", " ", text).strip()
    return text.upper()


def parse_date_pt(date_str: Optional[str], year_hint: Optional[int] = None) -> Optional[str]:
    """
    Converte data no formato brasileiro (dd/MM/aa ou dd/MM/yyyy) para ISO yyyy-MM-dd.
    Aceita anos com 2 ou 4 digitos.
    """
    if date_str is None:
        return None
    date_str = str(date_str).strip()
    if not date_str or date_str == "-":
        return None

    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            # Se ano veio com 2 digitos, pode ser necessario ajustar seculo
            if fmt == "%d/%m/%y" and year_hint and dt.year < 2000:
                # Assume que dados sao 2000+
                dt = dt.replace(year=dt.year + 100)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def safe_cast_int(value: Optional[str]) -> Optional[int]:
    """Converte string para inteiro, retornando None em caso de erro."""
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def safe_cast_float(value: Optional[str]) -> Optional[float]:
    """Converte string para float, retornando None em caso de erro."""
    if value is None:
        return None
    try:
        # Remove percentual e substitui virgula por ponto
        cleaned = str(value).replace("%", "").replace(",", ".").strip()
        return float(cleaned)
    except ValueError:
        return None


def add_audit_columns(df: DataFrame, stage: str = "silver") -> DataFrame:
    """Adiciona colunas de auditoria padrao."""
    return df.withColumn("_stage", lit(stage)).withColumn("_processed_date", current_timestamp())
