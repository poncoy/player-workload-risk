# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/).

## [0.1.0] - 2026-09-17

### Added
- `src/extract_gps.py`: extraccion de exports Catapult reales (100
  columnas, separador coma/punto y coma autodetectado), filtrado por
  `Split Name == 'all'`, clasificacion de sesion (entrenamiento / amistoso
  / oficial), anonimizacion de jugador con mapeo persistente gitignored.
- `src/extract_fixtures.py`: fixtures del Brasileirao Serie A via
  football-data.org (con fallback a mock si no hay API key) y scraping del
  calendario de CD Moquegua desde Transfermarkt (con fallback a snapshot
  cacheado).
- `src/transform_metrics.py`: validacion de calidad de datos GPS, calculo
  de ACWR (7d/28d) por jugador, indice de congestion de calendario semanal,
  flag combinado de riesgo.
- `src/load_duckdb.py`: carga a DuckDB local con schema versionado
  (`sql/schema.sql`).
- `dags/workload_risk_dag.py`: DAG de Airflow (TaskFlow API) parametrizado
  por `snapshot_date`, orquestando extract -> transform -> load.
- `docker-compose.yml`: Airflow local (LocalExecutor + Postgres) para
  correr el DAG end-to-end.
- `notebooks/analisis_riesgo.ipynb`: corrida end-to-end + tabla de
  jugadores en riesgo + grafico de ACWR.
- `tests/test_transform_metrics.py`: 5 tests unitarios sobre ACWR (ratio
  bajo carga constante, deteccion de pico, historia insuficiente, umbral
  estricto del flag, rango del indice de congestion).

### Changed
- Ventana de analisis de ACWR ampliada de "solo enero 2026" a
  diciembre 2025 - enero/febrero 2026 completos: con un mes solo, la
  ventana cronica de 28 dias quedaba incompleta para gran parte del
  periodo.

### Fixed
- Deteccion de delimitador CSV: `csv.Sniffer()` fallaba con
  `Could not determine delimiter` en varios exports reales con `;`;
  reemplazado por conteo directo de ocurrencias en la linea de encabezado.
- `load_dataframe()` reordenaba columnas de forma implicita (INSERT
  posicional): ahora reindexa el DataFrame contra `PRAGMA table_info` antes
  de insertar.
- Validacion de duplicados en `transform_metrics.py` marcaba como
  duplicado cualquier doble sesion (AM/PM) del mismo jugador en el mismo
  dia, un patron real y esperado en pretemporada. Se corrigio la clave de
  duplicado a `jugador + fecha + session_title`.

### Known limitations
Ver seccion "Limitaciones honestas" en `README.md`.
