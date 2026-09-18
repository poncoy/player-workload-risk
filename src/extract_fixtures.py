"""Extraccion de fixtures: Brasileirao Serie A (API publica) y calendario
de CD Moquegua (scraping de Transfermarkt).

Ambas fuentes tienen un fallback a datos cacheados en data/sample/ cuando la
fuente en vivo no esta disponible (sin API key, o el HTML de Transfermarkt
cambio de estructura). Esto es deliberado: el objetivo del proyecto es
demostrar el pipeline de datos, no depender de que dos servicios externos
esten arriba en el momento de una demo.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.config import (
    DATA_SAMPLE_DIR,
    FOOTBALL_DATA_API_KEY,
    FOOTBALL_DATA_BASE_URL,
    FOOTBALL_DATA_COMPETITION,
    FOOTBALL_DATA_SEASON,
    MOQUEGUA_FIXTURES_URL,
    get_logger,
)

logger = get_logger(__name__)

REQUEST_TIMEOUT = 15
USER_AGENT = "Mozilla/5.0 (compatible; player-workload-risk-portfolio/1.0)"


# ---------------------------------------------------------------------------
# Brasileirao Serie A (football-data.org)
# ---------------------------------------------------------------------------


def _brasileirao_mock_path() -> Path:
    return DATA_SAMPLE_DIR / "brasileirao_fixtures_mock.json"


def _matches_json_to_df(payload: dict) -> pd.DataFrame:
    rows = []
    for m in payload.get("matches", []):
        rows.append(
            {
                "fecha": pd.to_datetime(m["utcDate"]).date(),
                "matchday": m.get("matchday"),
                "status": m.get("status"),
                "local": m["homeTeam"]["name"],
                "visita": m["awayTeam"]["name"],
                "competicion": payload.get("competition", {}).get("name", "Brasileirao"),
            }
        )
    return pd.DataFrame(rows).sort_values("fecha").reset_index(drop=True)


def fetch_brasileirao_fixtures(
    api_key: str = FOOTBALL_DATA_API_KEY,
    competition: str = FOOTBALL_DATA_COMPETITION,
    season: str = FOOTBALL_DATA_SEASON,
) -> pd.DataFrame:
    """Fixtures del Brasileirao Serie A. Cae a mock si no hay api_key o si
    la llamada falla (rate limit, red, etc.)."""
    if not api_key:
        logger.warning(
            "FOOTBALL_DATA_API_KEY no configurada: usando fixtures mock de %s",
            _brasileirao_mock_path().name,
        )
        return _load_brasileirao_mock()

    url = f"{FOOTBALL_DATA_BASE_URL}/competitions/{competition}/matches"
    try:
        resp = requests.get(
            url,
            headers={"X-Auth-Token": api_key},
            params={"season": season},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        df = _matches_json_to_df(resp.json())
        logger.info("Obtenidos %d partidos de %s via API", len(df), competition)
        return df
    except requests.RequestException as exc:
        logger.error("Fallo la API de football-data.org (%s), usando mock", exc)
        return _load_brasileirao_mock()


def _load_brasileirao_mock() -> pd.DataFrame:
    import json

    with open(_brasileirao_mock_path(), encoding="utf-8") as fh:
        payload = json.load(fh)
    return _matches_json_to_df(payload)


# ---------------------------------------------------------------------------
# Calendario CD Moquegua (Transfermarkt scraping)
# ---------------------------------------------------------------------------

_MONTHS_ES = None  # no se usa; Transfermarkt entrega fechas en formato EN


def _parse_tm_date(raw: str) -> dt.date | None:
    """'Mon 02/02/26' -> date(2026, 2, 2). Devuelve None si no matchea."""
    parts = raw.strip().split()
    date_part = parts[-1] if parts else raw.strip()
    try:
        return dt.datetime.strptime(date_part, "%d/%m/%y").date()
    except ValueError:
        return None


def _moquegua_mock_path() -> Path:
    return DATA_SAMPLE_DIR / "moquegua_calendar_sample.csv"


def _parse_moquegua_html(html: str) -> pd.DataFrame:
    """Parsea la tabla 'Fixtures by date' de la pagina de un club en
    Transfermarkt. Estructura verificada manualmente en la pagina real de
    CD Moquegua (verein/114625): filas de seccion con
    <td class="extrarow ..." colspan="13"> marcan la competicion, filas de
    datos tienen >6 <td> con matchday/fecha/hora/local-visita/rival/resultado.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    fixtures_table = None
    for t in tables:
        if len(t.find_all("tr")) > 10 and t.find("th", string="Matchday"):
            fixtures_table = t
            break
    if fixtures_table is None:
        raise ValueError("No se encontro la tabla de fixtures en el HTML")

    rows = []
    competition = None
    for tr in fixtures_table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) <= 2:
            section_link = tr.find("a", title=True)
            if section_link:
                competition = section_link["title"]
            continue
        if len(tds) < 7:
            continue
        matchday = tds[0].get_text(strip=True)
        date_txt = tds[1].get_text(strip=True)
        time_txt = tds[2].get_text(strip=True)
        venue = tds[3].get_text(strip=True)
        opponent_cell = tds[6] if len(tds) > 6 else tds[-2]
        opponent_link = opponent_cell.find("a", title=True)
        opponent = opponent_link["title"] if opponent_link else opponent_cell.get_text(strip=True)
        result = tds[-1].get_text(strip=True)

        fecha = _parse_tm_date(date_txt)
        if fecha is None:
            continue
        rows.append(
            {
                "competition": competition,
                "matchday": matchday,
                "date": date_txt,
                "time": time_txt,
                "venue": venue,
                "opponent": opponent,
                "result": result,
                "fecha": fecha,
            }
        )
    return pd.DataFrame(rows)


def scrape_moquegua_calendar(url: str = MOQUEGUA_FIXTURES_URL) -> pd.DataFrame:
    """Calendario de CD Moquegua. Fragil por diseno: Transfermarkt puede
    cambiar su HTML en cualquier momento (ver limitaciones en README). Ante
    cualquier fallo, cae al snapshot cacheado en data/sample/.
    """
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        df = _parse_moquegua_html(resp.text)
        if df.empty:
            raise ValueError("Tabla de fixtures encontrada pero vacia")
        logger.info("Scrapeados %d partidos de CD Moquegua desde Transfermarkt", len(df))
        return df
    except Exception as exc:  # noqa: BLE001 - fuente externa fragil, se degrada a mock
        logger.error("Scraping de Transfermarkt fallo (%s); usando snapshot cacheado", exc)
        df = pd.read_csv(_moquegua_mock_path())
        df["fecha"] = pd.to_datetime(df["date"], format="%a %d/%m/%y").dt.date
        return df


if __name__ == "__main__":
    brasil = fetch_brasileirao_fixtures()
    moquegua = scrape_moquegua_calendar()
    out_dir = Path("data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)
    brasil.to_csv(out_dir / "brasileirao_fixtures.csv", index=False)
    moquegua.to_csv(out_dir / "moquegua_calendar.csv", index=False)
    logger.info("Brasileirao: %d filas | Moquegua: %d filas", len(brasil), len(moquegua))
