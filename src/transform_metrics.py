"""Calculo de ACWR, congestion de calendario y flags de riesgo combinado.

Funciones puras (reciben y devuelven DataFrames, sin I/O) para que sean
faciles de testear. La orquestacion de lectura/escritura vive en el DAG y en
load_duckdb.py.
"""

from __future__ import annotations

import pandas as pd

from src.config import ACUTE_WINDOW_DAYS, ACWR_RISK_THRESHOLD, CHRONIC_WINDOW_DAYS, MAX_PLAUSIBLE_SPEED_KMH, get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Validacion de calidad de datos GPS
# ---------------------------------------------------------------------------


def validate_gps_data(df: pd.DataFrame) -> pd.DataFrame:
    """Marca filas invalidas (nulos en metricas clave, valores fisicamente
    imposibles, duplicados exactos) en una columna 'dato_valido'. No elimina
    filas: deja la decision de filtrar a quien consuma el resultado, para
    que el motivo de invalidez quede auditable.
    """
    df = df.copy()
    metric_cols = ["distancia_total_m", "player_load", "velocidad_max_kmh"]

    null_mask = df[metric_cols].isna().any(axis=1)
    if null_mask.any():
        logger.warning("%d filas con nulos en metricas clave", null_mask.sum())

    impossible_speed = df["velocidad_max_kmh"] > MAX_PLAUSIBLE_SPEED_KMH
    if impossible_speed.any():
        logger.warning(
            "%d filas con velocidad_max_kmh > %.1f km/h (dato corrupto)",
            impossible_speed.sum(),
            MAX_PLAUSIBLE_SPEED_KMH,
        )

    negative_values = (df[metric_cols] < 0).any(axis=1)
    if negative_values.any():
        logger.warning("%d filas con metricas negativas", negative_values.sum())

    # Dos sesiones el mismo dia (doble turno AM/PM) son normales en
    # pretemporada: NO se consideran duplicado. Un duplicado real es la
    # misma sesion (jugador+fecha+session_title) repetida.
    dup_subset = [c for c in ["jugador", "fecha", "session_title"] if c in df.columns]
    dup_mask = df.duplicated(subset=dup_subset, keep="first")
    if dup_mask.any():
        logger.warning("%d filas duplicadas (%s)", dup_mask.sum(), "+".join(dup_subset))

    df["dato_valido"] = ~(null_mask | impossible_speed | negative_values | dup_mask)
    n_invalid = (~df["dato_valido"]).sum()
    if n_invalid:
        logger.info("Total filas marcadas invalidas: %d de %d", n_invalid, len(df))
    return df


# ---------------------------------------------------------------------------
# ACWR (Acute:Chronic Workload Ratio)
# ---------------------------------------------------------------------------


def _daily_load_per_player(
    df: pd.DataFrame, group_col: str, date_col: str, metric_col: str
) -> pd.DataFrame:
    """Suma la metrica por jugador-dia y rellena huecos de calendario con 0
    (dias sin sesion cuentan como carga cero en la ventana movil, convencion
    estandar de ACWR tipo Gabbett)."""
    daily = df.groupby([group_col, date_col], as_index=False)[metric_col].sum()

    filled_frames = []
    for player, group in daily.groupby(group_col):
        full_range = pd.date_range(group[date_col].min(), group[date_col].max(), freq="D")
        series = group.set_index(pd.to_datetime(group[date_col]))[metric_col]
        series = series.reindex(full_range, fill_value=0.0)
        out = series.rename(metric_col).rename_axis(date_col).reset_index()
        out[group_col] = player
        filled_frames.append(out)

    return pd.concat(filled_frames, ignore_index=True)


def compute_acwr(
    df: pd.DataFrame,
    group_col: str = "jugador",
    date_col: str = "fecha",
    metric_col: str = "player_load",
    acute_window: int = ACUTE_WINDOW_DAYS,
    chronic_window: int = CHRONIC_WINDOW_DAYS,
) -> pd.DataFrame:
    """ACWR = (media movil de acute_window dias) / (media movil de
    chronic_window dias), calculado por jugador sobre metric_col.

    El ACWR de un jugador solo es valido (columna 'historia_suficiente')
    una vez que acumulo al menos chronic_window dias de calendario desde su
    primer registro; antes de eso el denominador es estadisticamente pobre
    y el ratio no debe usarse para decisiones de riesgo.
    """
    daily = _daily_load_per_player(df, group_col, date_col, metric_col)
    daily = daily.sort_values([group_col, date_col])

    results = []
    for player, group in daily.groupby(group_col):
        group = group.sort_values(date_col).reset_index(drop=True)
        acute = group[metric_col].rolling(window=acute_window, min_periods=1).mean()
        chronic = group[metric_col].rolling(window=chronic_window, min_periods=1).mean()
        acwr = acute / chronic.replace(0, pd.NA)

        days_since_start = (group[date_col] - group[date_col].min()).dt.days
        historia_suficiente = days_since_start >= (chronic_window - 1)

        out = pd.DataFrame(
            {
                group_col: player,
                date_col: group[date_col],
                "carga_aguda_7d": acute,
                "carga_cronica_28d": chronic,
                "acwr": acwr,
                "historia_suficiente": historia_suficiente,
            }
        )
        results.append(out)

    return pd.concat(results, ignore_index=True)


def flag_acwr_risk(df: pd.DataFrame, threshold: float = ACWR_RISK_THRESHOLD) -> pd.DataFrame:
    df = df.copy()
    df["riesgo_acwr"] = df["historia_suficiente"] & (df["acwr"] > threshold)
    return df


# ---------------------------------------------------------------------------
# Congestion de calendario
# ---------------------------------------------------------------------------


def compute_rest_days(fixtures_df: pd.DataFrame, date_col: str = "fecha") -> pd.DataFrame:
    df = fixtures_df.sort_values(date_col).copy()
    df["fecha"] = pd.to_datetime(df[date_col])
    df["dias_descanso"] = df["fecha"].diff().dt.days
    return df


def compute_match_density(fixtures_df: pd.DataFrame, date_col: str = "fecha") -> pd.DataFrame:
    df = fixtures_df.copy()
    df["fecha"] = pd.to_datetime(df[date_col])
    df["anio_mes"] = df["fecha"].dt.to_period("M").astype(str)
    return df.groupby("anio_mes", as_index=False).size().rename(columns={"size": "partidos_en_mes"})


def compute_simultaneous_competitions(
    fixtures_df: pd.DataFrame, date_col: str = "fecha", competition_col: str = "competition"
) -> pd.DataFrame:
    df = fixtures_df.copy()
    df["fecha"] = pd.to_datetime(df[date_col])
    df["semana"] = df["fecha"].dt.to_period("W").astype(str)
    weekly = df.groupby("semana")[competition_col].nunique().rename("competiciones_simultaneas")
    return weekly.reset_index()


def compute_congestion_index(fixtures_df: pd.DataFrame, date_col: str = "fecha") -> pd.DataFrame:
    """Indice de congestion semanal normalizado [0, 1], combinando partidos
    por semana y descanso minimo previo. No mezcla datos fisicos: solo usa
    fechas de partido, por eso puede aplicarse igual a Moquegua o a un
    fixture externo como el Brasileirao (sirve de contexto, no de
    comparacion de carga real)."""
    rested = compute_rest_days(fixtures_df, date_col)
    rested["semana"] = rested["fecha"].dt.to_period("W").astype(str)

    weekly = rested.groupby("semana").agg(
        partidos_semana=("fecha", "count"),
        descanso_minimo=("dias_descanso", "min"),
    ).reset_index()

    max_partidos = max(weekly["partidos_semana"].max(), 1)
    weekly["score_partidos"] = weekly["partidos_semana"] / max_partidos

    descanso_min_obs = weekly["descanso_minimo"].min(skipna=True)
    descanso_max_obs = weekly["descanso_minimo"].max(skipna=True)
    rango = (descanso_max_obs - descanso_min_obs) or 1
    weekly["score_descanso"] = 1 - ((weekly["descanso_minimo"] - descanso_min_obs) / rango)
    weekly["score_descanso"] = weekly["score_descanso"].fillna(0.5)

    weekly["indice_congestion"] = (weekly["score_partidos"] + weekly["score_descanso"]) / 2
    return weekly[["semana", "partidos_semana", "descanso_minimo", "indice_congestion"]]


# ---------------------------------------------------------------------------
# Flag combinado (ACWR alto + calendario congestionado en la misma semana)
# ---------------------------------------------------------------------------


def build_risk_table(
    acwr_df: pd.DataFrame,
    moquegua_congestion_df: pd.DataFrame,
    date_col: str = "fecha",
) -> pd.DataFrame:
    """Combina ACWR por jugador con el indice de congestion de la semana de
    Moquegua. alerta_combinada = ACWR alto Y semana congestionada."""
    df = flag_acwr_risk(acwr_df)
    df = df.copy()
    df["semana"] = pd.to_datetime(df[date_col]).dt.to_period("W").astype(str)

    congestion_by_week = moquegua_congestion_df.set_index("semana")["indice_congestion"]
    umbral_congestion = congestion_by_week.median() if len(congestion_by_week) else 0.5

    df["indice_congestion_semana"] = df["semana"].map(congestion_by_week)
    df["semana_congestionada"] = df["indice_congestion_semana"] > umbral_congestion
    df["alerta_combinada"] = df["riesgo_acwr"] & df["semana_congestionada"].fillna(False)

    return df
