"""Carga del dataset final a DuckDB local. El archivo .duckdb es un
artefacto derivado (gitignored): se puede reconstruir corriendo el pipeline
completo en cualquier momento."""

from __future__ import annotations

import duckdb
import pandas as pd

from src.config import DUCKDB_PATH, SCHEMA_SQL_PATH, get_logger

logger = get_logger(__name__)


def get_connection(db_path=DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def init_schema(con: duckdb.DuckDBPyConnection, schema_sql_path=SCHEMA_SQL_PATH) -> None:
    sql = schema_sql_path.read_text(encoding="utf-8")
    con.execute(sql)
    logger.info("Schema aplicado desde %s", schema_sql_path)


def load_dataframe(
    con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str
) -> None:
    """Reemplaza el contenido de table_name con df, preservando el schema
    ya creado por init_schema (DELETE + INSERT en vez de DROP).

    Reordena las columnas de df segun el schema de la tabla: 'INSERT ...
    SELECT *' es posicional, y el orden de columnas de un DataFrame armado
    con pandas no esta garantizado que coincida con sql/schema.sql.
    """
    schema_cols = [row[1] for row in con.execute(f"PRAGMA table_info({table_name})").fetchall()]
    df = df[schema_cols]

    con.register("tmp_df", df)
    con.execute(f"DELETE FROM {table_name}")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM tmp_df")
    con.unregister("tmp_df")
    logger.info("Cargadas %d filas en %s", len(df), table_name)


if __name__ == "__main__":
    con = get_connection()
    init_schema(con)
    logger.info("Base DuckDB inicializada en %s", DUCKDB_PATH)
    con.close()
