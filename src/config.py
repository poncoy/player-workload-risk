"""Configuracion centralizada: rutas, variables de entorno y logging."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DATA_RAW_DIR = PROJECT_ROOT / os.getenv("DATA_RAW_DIR", "data/raw")
DATA_MAPPING_DIR = PROJECT_ROOT / os.getenv("DATA_MAPPING_DIR", "data/mapping")
DATA_PROCESSED_DIR = PROJECT_ROOT / os.getenv("DATA_PROCESSED_DIR", "data/processed")
DATA_SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"

PLAYER_MAPPING_PATH = DATA_MAPPING_DIR / "player_mapping.csv"
DUCKDB_PATH = PROJECT_ROOT / os.getenv("DUCKDB_PATH", "data/processed/workload_risk.duckdb")
SCHEMA_SQL_PATH = PROJECT_ROOT / "sql" / "schema.sql"

FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "").strip()
FOOTBALL_DATA_COMPETITION = os.getenv("FOOTBALL_DATA_COMPETITION", "BSA")
FOOTBALL_DATA_SEASON = os.getenv("FOOTBALL_DATA_SEASON", "2026")
FOOTBALL_DATA_BASE_URL = "https://api.football-data.org/v4"

MOQUEGUA_FIXTURES_URL = os.getenv(
    "MOQUEGUA_FIXTURES_URL",
    "https://www.transfermarkt.com/ucv-moquegua/spielplandatum/verein/114625",
)

# Umbrales de negocio / validacion fisica
ACWR_RISK_THRESHOLD = 1.5
MAX_PLAUSIBLE_SPEED_KMH = 40.0
ACUTE_WINDOW_DAYS = 7
CHRONIC_WINDOW_DAYS = 28


def get_logger(name: str) -> logging.Logger:
    """Logger con formato consistente para todo el pipeline."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    return logger
