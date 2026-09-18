"""Extraccion de metricas GPS desde exports crudos de Catapult (CD Moquegua).

Cada archivo es una sesion (entrenamiento, amistoso u oficial) con una fila
por jugador por segmento ("Split Name"). Se conserva unicamente la fila
'all' (registro integrado de la sesion completa) para evitar duplicar carga
por segmentos parciales.

Regla de anonimizacion (no negociable): los nombres/apellidos reales de
'Player Name' se reemplazan por codigos Jugador_NN antes de que el dato
salga de este modulo. El mapeo nombre->codigo vive en
data/mapping/player_mapping.csv, listado en .gitignore, y nunca se commitea.
"""

from __future__ import annotations

import csv
import datetime as dt
import re
from pathlib import Path

import pandas as pd

from src.config import DATA_RAW_DIR, PLAYER_MAPPING_PATH, get_logger

logger = get_logger(__name__)

EXCEL_EPOCH = dt.date(1899, 12, 30)

# Columnas del export Catapult -> nombre de negocio usado aguas abajo.
# Ademas de las 5 metricas del alcance original (distancia, sprint, player
# load, velocidad), se suman 4 mas del mismo export que ya estaban en las
# 100 columnas y aportan valor practico para el cuerpo tecnico: intensidad
# de la sesion, impactos totales (carga mecanica) y aceleracion/
# desaceleracion maxima (frenadas y arranques, tipicos de sobrecarga en
# tejido blando).
COLUMN_MAP = {
    "Player Name": "jugador_raw",
    "Date": "fecha_serial",
    "Session Title": "session_title",
    "Distance (km)": "distancia_total_km",
    "Sprint Distance (m)": "distancia_sprint_m",
    "Player Load": "player_load",
    "Top Speed (km/h)": "velocidad_max_kmh",
    "Distance in Speed Zone 4  (km)": "dist_zona4_km",
    "Distance in Speed Zone 5  (km)": "dist_zona5_km",
    "Distance Per Min (m/min)": "intensidad_m_min",
    "Impacts": "impactos",
    "Max Acceleration (m/s/s)": "aceleracion_max",
    "Max Deceleration (m/s/s)": "desaceleracion_max",
}

OFFICIAL_MARKERS = ("LIGA1", "APERTURA", "CLAUSURA", "COPA")


def detect_delimiter(path: Path) -> str:
    """Catapult exporta a veces con coma, a veces con punto y coma.

    csv.Sniffer() es poco fiable en estos exports (falla con 'Could not
    determine delimiter' en varios archivos reales), asi que se cuenta la
    ocurrencia de cada separador candidato en la linea de encabezado.
    """
    with open(path, newline="", encoding="utf-8-sig") as fh:
        header_line = fh.readline()
    return ";" if header_line.count(";") > header_line.count(",") else ","


def excel_serial_to_date(serial: str | float) -> dt.date:
    return EXCEL_EPOCH + dt.timedelta(days=int(float(serial)))


def classify_session(filename: str, session_title: str) -> str:
    """Clasifica una sesion en entrenamiento / amistoso / oficial.

    Reglas observadas en los exports reales de CD Moquegua:
    - El nombre de archivo termina en una letra (A/B) antes de '.csv' cuando
      el dia tuvo dos partidos amistosos consecutivos (ej. S40...A / ...B).
    - 'Session Title' de partidos oficiales de Liga 1 incluye el tag de la
      competicion (LIGA1/APERTURA/CLAUSURA) junto al rival.
    - Todo lo demas (prefijo MA o numero de sesion suelto) es entrenamiento.
    """
    stem = Path(filename).stem
    if re.search(r"\d{8}[AB]$", stem):
        return "amistoso"
    title_upper = session_title.upper()
    if any(marker in title_upper for marker in OFFICIAL_MARKERS):
        return "oficial"
    return "entrenamiento"


def load_player_mapping() -> dict[str, str]:
    if not PLAYER_MAPPING_PATH.exists():
        return {}
    mapping_df = pd.read_csv(PLAYER_MAPPING_PATH)
    return dict(zip(mapping_df["jugador_raw"], mapping_df["jugador_codigo"]))


def save_player_mapping(mapping: dict[str, str]) -> None:
    PLAYER_MAPPING_PATH.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(mapping.items(), key=lambda kv: int(kv[1].split("_")[1]))
    pd.DataFrame(rows, columns=["jugador_raw", "jugador_codigo"]).to_csv(
        PLAYER_MAPPING_PATH, index=False
    )


def anonymize_players(raw_names: pd.Series, mapping: dict[str, str]) -> pd.Series:
    """Asigna Jugador_NN a cada nombre crudo normalizado, reutilizando el
    mapeo existente para mantener el mismo codigo entre corridas."""
    next_id = len(mapping) + 1
    codes = []
    for raw_name in raw_names:
        key = str(raw_name).strip()
        if key not in mapping:
            mapping[key] = f"Jugador_{next_id:02d}"
            next_id += 1
        codes.append(mapping[key])
    return pd.Series(codes, index=raw_names.index)


def extract_session_file(path: Path) -> pd.DataFrame:
    delimiter = detect_delimiter(path)
    df = pd.read_csv(path, sep=delimiter, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]

    if "Split Name" not in df.columns:
        logger.warning("Archivo sin columna 'Split Name', se omite: %s", path.name)
        return pd.DataFrame()

    df = df[df["Split Name"].str.strip().str.lower() == "all"].copy()
    if df.empty:
        logger.warning("Sin fila 'all' en %s, se omite", path.name)
        return pd.DataFrame()

    missing = [c for c in COLUMN_MAP if c not in df.columns]
    if missing:
        logger.warning("%s: columnas esperadas ausentes %s", path.name, missing)

    present = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=present)

    session_title = str(df["session_title"].iloc[0]) if "session_title" in df else ""
    df["session_type"] = classify_session(path.name, session_title)
    df["source_file"] = path.name
    df["fecha"] = df["fecha_serial"].apply(excel_serial_to_date)
    df["distancia_total_m"] = df["distancia_total_km"] * 1000.0

    zona4 = df["dist_zona4_km"] if "dist_zona4_km" in df else 0.0
    zona5 = df["dist_zona5_km"] if "dist_zona5_km" in df else 0.0
    # Alta velocidad = distancia recorrida en zonas de velocidad 4-5 (umbral
    # de zona definido por el club en Catapult, no expuesto en el export).
    # OJO: en este export, zona4+zona5 coincide casi exacto (corr=0.9999998)
    # con 'Sprint Distance' nativa de Catapult -> el club configuro el
    # umbral de sprint igual al piso de zona 4. Son, en la practica, la
    # misma metrica. Se conserva distancia_alta_velocidad_m por si el club
    # reconfigura las zonas en el futuro, pero no se debe tratar como una
    # senal independiente de distancia_sprint_m mientras esto siga asi.
    df["distancia_alta_velocidad_m"] = (zona4 + zona5) * 1000.0

    keep = [
        "jugador_raw",
        "fecha",
        "session_type",
        "session_title",
        "source_file",
        "distancia_total_m",
        "distancia_alta_velocidad_m",
        "distancia_sprint_m",
        "player_load",
        "velocidad_max_kmh",
        "intensidad_m_min",
        "impactos",
        "aceleracion_max",
        "desaceleracion_max",
    ]
    keep = [c for c in keep if c in df.columns]
    return df[keep]


def extract_all(raw_dir: Path = DATA_RAW_DIR, only_training: bool = False) -> pd.DataFrame:
    """Lee todos los CSV de raw_dir, anonimiza y consolida en un solo dataset
    jugador-sesion.

    Por defecto (only_training=False) se conservan TODAS las sesiones
    (entrenamiento + amistoso + oficial): el ACWR debe calcularse sobre la
    carga fisica real completa, y el partido es tipicamente la sesion de
    mayor carga de la semana -- excluirlo subestima justo el momento de
    mayor riesgo. session_type queda como columna para poder explicar un
    pico de carga ("este salto de ACWR coincide con el partido del 02/02").
    only_training=True existe por si se necesita replicar el alcance de
    otro analisis (ej. un clasificador de carga *planificada* de
    entrenamiento, donde el partido no aplica), pero no es el default para
    el calculo de riesgo de este proyecto.
    """
    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No hay CSV en {raw_dir}. Coloca los exports de Catapult (gitignored) ahi."
        )

    frames = [extract_session_file(p) for p in csv_files]
    frames = [f for f in frames if not f.empty]
    combined = pd.concat(frames, ignore_index=True)

    before = len(combined)
    combined = combined.drop_duplicates(subset=["jugador_raw", "fecha", "session_title"])
    dupes = before - len(combined)
    if dupes:
        logger.warning("Se eliminaron %d filas duplicadas (jugador+fecha+sesion)", dupes)

    mapping = load_player_mapping()
    combined["jugador"] = anonymize_players(combined["jugador_raw"], mapping)
    save_player_mapping(mapping)
    combined = combined.drop(columns=["jugador_raw"])

    logger.info(
        "Extraidas %d filas jugador-sesion de %d archivos (%d jugadores unicos)",
        len(combined),
        len(csv_files),
        combined["jugador"].nunique(),
    )

    counts = combined["session_type"].value_counts().to_dict()
    logger.info("Distribucion por tipo de sesion: %s", counts)

    if only_training:
        combined = combined[combined["session_type"] == "entrenamiento"].copy()

    return combined.sort_values(["jugador", "fecha"]).reset_index(drop=True)


if __name__ == "__main__":
    result = extract_all()
    out_path = Path("data/processed/gps_metrics.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(out_path, index=False)
    logger.info("Escrito %s (%d filas)", out_path, len(result))
