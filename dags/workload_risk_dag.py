"""DAG de Airflow: extract -> transform -> load para el pipeline de riesgo
de carga vs congestion de calendario de CD Moquegua.

Parametrizado por snapshot_date (Airflow Param, no hardcodeado): filtra el
dataset a "todo lo ocurrido hasta esa fecha", simulando una corrida
incremental real aunque en la demo se dispare una sola vez contra el
snapshot completo de pretemporada.

Los datos intermedios entre tasks se pasan por archivo (data/processed/),
no por XCom, porque los DataFrames son demasiado grandes para el backend de
XCom por defecto de Airflow.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

# El proyecto no esta instalado como paquete: se agrega la raiz del repo al
# path para poder importar `src.*` igual que en local y en tests.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

PROCESSED_DIR = REPO_ROOT / "data" / "processed"

default_args = {
    "owner": "poncoy",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}


@dag(
    dag_id="workload_risk_dag",
    description="Riesgo de sobrecarga (ACWR) vs congestion de calendario - CD Moquegua",
    schedule=None,  # disparo manual en la demo; en produccion iria @daily
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    params={
        "snapshot_date": Param(
            default="2026-02-05",
            type="string",
            format="date",
            description="Procesar todos los datos con fecha <= snapshot_date",
        )
    },
    tags=["cd-moquegua", "workload-risk", "portfolio"],
)
def workload_risk_pipeline():
    @task
    def extract_gps(snapshot_date: str) -> str:
        import pandas as pd

        from src.extract_gps import extract_all

        df = extract_all()
        df = df[pd.to_datetime(df["fecha"]) <= pd.to_datetime(snapshot_date)]

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        out_path = PROCESSED_DIR / "gps_metrics.parquet"
        df.to_parquet(out_path, index=False)
        return str(out_path)

    @task
    def extract_fixtures(snapshot_date: str) -> dict:
        import pandas as pd

        from src.extract_fixtures import fetch_brasileirao_fixtures, scrape_moquegua_calendar

        moquegua = scrape_moquegua_calendar()
        brasil = fetch_brasileirao_fixtures()

        moquegua_path = PROCESSED_DIR / "moquegua_calendar.parquet"
        brasil_path = PROCESSED_DIR / "brasileirao_fixtures.parquet"
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        moquegua.to_parquet(moquegua_path, index=False)
        brasil.to_parquet(brasil_path, index=False)
        return {"moquegua": str(moquegua_path), "brasileirao": str(brasil_path)}

    @task
    def transform(gps_path: str, fixtures_paths: dict) -> str:
        import pandas as pd

        from src.transform_metrics import (
            build_risk_table,
            compute_acwr,
            compute_congestion_index,
            validate_gps_data,
        )

        gps = pd.read_parquet(gps_path)
        gps["fecha"] = pd.to_datetime(gps["fecha"])
        gps = validate_gps_data(gps)
        gps_valid = gps[gps["dato_valido"]]

        acwr = compute_acwr(gps_valid)

        moquegua = pd.read_parquet(fixtures_paths["moquegua"])
        moquegua["fecha"] = pd.to_datetime(moquegua["fecha"])
        congestion = compute_congestion_index(moquegua)

        risk_table = build_risk_table(acwr, congestion)

        out_path = PROCESSED_DIR / "risk_table.parquet"
        risk_table.to_parquet(out_path, index=False)
        return str(out_path)

    @task
    def load(risk_table_path: str, gps_path: str, fixtures_paths: dict) -> None:
        import pandas as pd

        from src.load_duckdb import get_connection, init_schema, load_dataframe
        from src.transform_metrics import compute_rest_days, validate_gps_data

        con = get_connection()
        init_schema(con)

        gps = pd.read_parquet(gps_path)
        gps["fecha"] = pd.to_datetime(gps["fecha"]).dt.date
        gps = validate_gps_data(gps)
        load_dataframe(con, gps, "gps_metrics")

        risk_table = pd.read_parquet(risk_table_path)
        risk_table["fecha"] = pd.to_datetime(risk_table["fecha"]).dt.date
        load_dataframe(con, risk_table, "acwr_risk")

        moquegua = pd.read_parquet(fixtures_paths["moquegua"])
        moquegua["fecha"] = pd.to_datetime(moquegua["fecha"])
        moquegua_rest = compute_rest_days(moquegua)
        moquegua_rest["fecha"] = moquegua_rest["fecha"].dt.date
        load_dataframe(con, moquegua_rest, "calendario_moquegua")

        brasil = pd.read_parquet(fixtures_paths["brasileirao"])
        brasil["fecha"] = pd.to_datetime(brasil["fecha"]).dt.date
        load_dataframe(con, brasil, "calendario_brasileirao")

        con.close()

    snapshot_date = "{{ params.snapshot_date }}"
    gps_path = extract_gps(snapshot_date)
    fixtures_paths = extract_fixtures(snapshot_date)
    risk_table_path = transform(gps_path, fixtures_paths)
    load(risk_table_path, gps_path, fixtures_paths)


workload_risk_pipeline()
