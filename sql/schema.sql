-- Esquema DuckDB para player-workload-risk.
-- Se recrea en cada corrida del pipeline (CREATE OR REPLACE) porque el
-- .duckdb es un artefacto derivado, no una fuente de verdad versionada.

CREATE TABLE IF NOT EXISTS gps_metrics (
    jugador               VARCHAR NOT NULL,      -- codigo anonimizado, ej. Jugador_01
    fecha                 DATE NOT NULL,
    session_type          VARCHAR,
    session_title         VARCHAR,
    source_file           VARCHAR,
    distancia_total_m     DOUBLE,
    distancia_alta_velocidad_m DOUBLE,
    distancia_sprint_m    DOUBLE,
    player_load           DOUBLE,
    velocidad_max_kmh     DOUBLE,
    intensidad_m_min      DOUBLE,   -- Distance Per Min: metros/minuto, intensidad de la sesion
    impactos              INTEGER,  -- carga mecanica (colisiones/choques detectados por el chaleco)
    aceleracion_max       DOUBLE,   -- m/s/s, arranques
    desaceleracion_max    DOUBLE,   -- m/s/s, frenadas (asociadas a sobrecarga de tejido blando)
    dato_valido           BOOLEAN
);

CREATE TABLE IF NOT EXISTS acwr_risk (
    jugador               VARCHAR NOT NULL,
    fecha                 DATE NOT NULL,
    carga_aguda_7d        DOUBLE,
    carga_cronica_28d     DOUBLE,
    acwr                  DOUBLE,
    historia_suficiente   BOOLEAN,
    riesgo_acwr           BOOLEAN,
    semana                VARCHAR,
    indice_congestion_semana DOUBLE,
    semana_congestionada  BOOLEAN,
    alerta_combinada      BOOLEAN
);

CREATE TABLE IF NOT EXISTS calendario_moquegua (
    competition   VARCHAR,
    matchday      VARCHAR,
    fecha         DATE,
    venue         VARCHAR,
    opponent      VARCHAR,
    result        VARCHAR,
    dias_descanso INTEGER
);

CREATE TABLE IF NOT EXISTS calendario_brasileirao (
    fecha        DATE,
    matchday     INTEGER,
    status       VARCHAR,
    local        VARCHAR,
    visita       VARCHAR,
    competicion  VARCHAR
);

CREATE TABLE IF NOT EXISTS congestion_moquegua (
    semana              VARCHAR,
    partidos_semana     INTEGER,
    descanso_minimo     INTEGER,
    indice_congestion   DOUBLE
);

CREATE TABLE IF NOT EXISTS congestion_brasileirao (
    semana              VARCHAR,
    partidos_semana     INTEGER,
    descanso_minimo     INTEGER,
    indice_congestion   DOUBLE
);
