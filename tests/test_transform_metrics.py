"""Tests unitarios sobre el calculo de ACWR: la logica mas critica del
pipeline. Usan DataFrames sinteticos, sin depender de datos reales."""

from __future__ import annotations

import pandas as pd
import pytest

from src.transform_metrics import (
    compute_acwr,
    compute_congestion_index,
    flag_acwr_risk,
)


def _make_gps_df(player: str, start_date: str, loads: list[float]) -> pd.DataFrame:
    dates = pd.date_range(start_date, periods=len(loads), freq="D")
    return pd.DataFrame({"jugador": player, "fecha": dates, "player_load": loads})


def test_acwr_is_one_under_constant_load():
    """Carga constante -> aguda y cronica convergen, ACWR ~= 1.0."""
    df = _make_gps_df("Jugador_01", "2026-01-01", [100.0] * 10)
    result = compute_acwr(df, acute_window=3, chronic_window=5)

    last_row = result.iloc[-1]
    assert last_row["historia_suficiente"]
    assert last_row["acwr"] == pytest.approx(1.0, abs=0.01)


def test_acwr_spike_produces_high_ratio():
    """Una carga baja sostenida seguida de un salto brusco debe elevar el
    ACWR por encima del umbral de riesgo."""
    loads = [40.0] * 7 + [250.0] * 4
    df = _make_gps_df("Jugador_02", "2026-01-01", loads)
    result = compute_acwr(df, acute_window=4, chronic_window=7)

    last_row = result.iloc[-1]
    assert last_row["historia_suficiente"]
    assert last_row["acwr"] > 1.5


def test_acwr_insufficient_history_is_flagged_and_not_risky():
    """Con menos dias que la ventana cronica, el ACWR no debe usarse para
    marcar riesgo, sin importar cuan alta sea la carga reciente."""
    df = _make_gps_df("Jugador_03", "2026-01-01", [50.0, 500.0, 500.0])
    result = compute_acwr(df, acute_window=3, chronic_window=28)
    flagged = flag_acwr_risk(result)

    assert not flagged["historia_suficiente"].any()
    assert not flagged["riesgo_acwr"].any()


def test_flag_acwr_risk_threshold_is_strict_greater_than():
    """El flag de riesgo usa '>' 1.5, no '>='."""
    df = pd.DataFrame(
        {
            "jugador": ["Jugador_04", "Jugador_04"],
            "fecha": pd.to_datetime(["2026-02-01", "2026-02-02"]),
            "carga_aguda_7d": [150.0, 151.0],
            "carga_cronica_28d": [100.0, 100.0],
            "acwr": [1.5, 1.51],
            "historia_suficiente": [True, True],
        }
    )
    flagged = flag_acwr_risk(df, threshold=1.5)

    assert list(flagged["riesgo_acwr"]) == [False, True]


def test_congestion_index_is_bounded_between_zero_and_one():
    fixtures = pd.DataFrame(
        {
            "fecha": pd.to_datetime(
                [
                    "2026-02-02", "2026-02-06", "2026-02-09", "2026-02-13",
                    "2026-02-16", "2026-02-23", "2026-03-01",
                ]
            )
        }
    )
    result = compute_congestion_index(fixtures)

    assert not result.empty
    assert result["indice_congestion"].between(0, 1).all()
