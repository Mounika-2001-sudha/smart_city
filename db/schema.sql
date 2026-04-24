-- Smart City IoT Platform - Database Schema
-- Run: psql -U smartcity -d smartcity -f schema.sql

-- ──────────────────────────────────────────────────────────────────────────
-- Extensions
-- ──────────────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ──────────────────────────────────────────────────────────────────────────
-- Traffic Data
-- ──────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS traffic_data (
    id               BIGSERIAL PRIMARY KEY,
    sensor_id        TEXT        NOT NULL,
    zone             TEXT        NOT NULL,
    intersection_id  INT         NOT NULL,
    timestamp        TIMESTAMPTZ NOT NULL,
    vehicle_count    INT         NOT NULL,
    avg_speed_kmh    FLOAT       NOT NULL,
    congestion_index FLOAT       NOT NULL CHECK (congestion_index BETWEEN 0 AND 1),
    incident_flag    BOOLEAN     NOT NULL DEFAULT FALSE,
    weather_impact   FLOAT       NOT NULL DEFAULT 1.0,
    lat              FLOAT,
    lon              FLOAT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_traffic_timestamp ON traffic_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_traffic_zone       ON traffic_data (zone);
CREATE INDEX IF NOT EXISTS idx_traffic_sensor     ON traffic_data (sensor_id);

-- ──────────────────────────────────────────────────────────────────────────
-- Air Quality Data
-- ──────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS air_quality_data (
    id              BIGSERIAL PRIMARY KEY,
    sensor_id       TEXT        NOT NULL,
    station         TEXT        NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    pm25            FLOAT       NOT NULL,
    pm10            FLOAT       NOT NULL,
    co              FLOAT       NOT NULL,
    no2             FLOAT       NOT NULL,
    so2             FLOAT       NOT NULL,
    o3              FLOAT       NOT NULL,
    aqi             INT         NOT NULL CHECK (aqi >= 0),
    aqi_category    TEXT        NOT NULL,
    temperature_c   FLOAT,
    humidity_pct    FLOAT,
    wind_speed_ms   FLOAT,
    lat             FLOAT,
    lon             FLOAT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aq_timestamp ON air_quality_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_aq_station   ON air_quality_data (station);
CREATE INDEX IF NOT EXISTS idx_aq_aqi       ON air_quality_data (aqi DESC);

-- ──────────────────────────────────────────────────────────────────────────
-- Weather Data
-- ──────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_data (
    id                     BIGSERIAL PRIMARY KEY,
    sensor_id              TEXT        NOT NULL,
    station                TEXT        NOT NULL,
    timestamp              TIMESTAMPTZ NOT NULL,
    temperature_c          FLOAT       NOT NULL,
    feels_like_c           FLOAT       NOT NULL,
    humidity_pct           FLOAT       NOT NULL,
    pressure_hpa           FLOAT       NOT NULL,
    wind_speed_ms          FLOAT       NOT NULL,
    wind_direction_deg     INT         NOT NULL,
    precipitation_mm       FLOAT       NOT NULL DEFAULT 0,
    visibility_km          FLOAT       NOT NULL,
    cloud_cover_pct        INT         NOT NULL,
    uv_index               FLOAT       NOT NULL,
    condition              TEXT        NOT NULL,
    weather_impact_traffic FLOAT       NOT NULL DEFAULT 1.0,
    weather_impact_aqi     FLOAT       NOT NULL DEFAULT 1.0,
    lat                    FLOAT,
    lon                    FLOAT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_weather_station   ON weather_data (station);

-- ──────────────────────────────────────────────────────────────────────────
-- Alerts Log
-- ──────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts_log (
    id          BIGSERIAL PRIMARY KEY,
    alert_type  TEXT        NOT NULL,  -- aqi_spike, congestion, incident, low_visibility, combined_hazard
    severity    TEXT        NOT NULL,  -- low, medium, high, critical
    zone        TEXT        NOT NULL,
    message     TEXT        NOT NULL,
    value       FLOAT       NOT NULL,
    threshold   FLOAT       NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    resolved    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts_log (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity  ON alerts_log (severity);
CREATE INDEX IF NOT EXISTS idx_alerts_type      ON alerts_log (alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_resolved  ON alerts_log (resolved);

-- ──────────────────────────────────────────────────────────────────────────
-- Useful Views
-- ──────────────────────────────────────────────────────────────────────────

-- Zone summary (latest 10 minutes of traffic)
CREATE OR REPLACE VIEW v_traffic_zone_summary AS
SELECT
    zone,
    COUNT(*)                        AS reading_count,
    AVG(vehicle_count)::INT         AS avg_vehicles,
    ROUND(AVG(avg_speed_kmh)::NUMERIC, 1)   AS avg_speed_kmh,
    ROUND(AVG(congestion_index)::NUMERIC, 3) AS avg_congestion,
    SUM(CASE WHEN incident_flag THEN 1 ELSE 0 END) AS incident_count,
    MAX(timestamp)                  AS latest_update
FROM traffic_data
WHERE timestamp > NOW() - INTERVAL '10 minutes'
GROUP BY zone
ORDER BY avg_congestion DESC;

-- AQI status per station
CREATE OR REPLACE VIEW v_aq_station_summary AS
SELECT DISTINCT ON (station)
    station,
    aqi,
    aqi_category,
    pm25,
    pm10,
    co,
    wind_speed_ms,
    timestamp
FROM air_quality_data
ORDER BY station, timestamp DESC;

-- Active (unresolved) alerts
CREATE OR REPLACE VIEW v_active_alerts AS
SELECT * FROM alerts_log
WHERE resolved = FALSE
ORDER BY timestamp DESC;

-- Correlation: join traffic + AQ by nearest timestamp
CREATE OR REPLACE VIEW v_traffic_aq_correlation AS
SELECT
    t.zone,
    DATE_TRUNC('minute', t.timestamp) AS bucket,
    AVG(t.congestion_index) AS avg_congestion,
    AVG(t.avg_speed_kmh)    AS avg_speed,
    AVG(a.aqi)              AS avg_aqi,
    AVG(a.pm25)             AS avg_pm25,
    AVG(a.wind_speed_ms)    AS avg_wind
FROM traffic_data t
JOIN air_quality_data a
    ON DATE_TRUNC('minute', t.timestamp) = DATE_TRUNC('minute', a.timestamp)
   AND a.station = 'aq_' || t.zone
WHERE t.timestamp > NOW() - INTERVAL '1 hour'
GROUP BY t.zone, bucket
ORDER BY bucket DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- TimescaleDB Hypertables (optional - uncomment if TimescaleDB installed)
-- ──────────────────────────────────────────────────────────────────────────
-- SELECT create_hypertable('traffic_data', 'timestamp', if_not_exists => TRUE);
-- SELECT create_hypertable('air_quality_data', 'timestamp', if_not_exists => TRUE);
-- SELECT create_hypertable('weather_data', 'timestamp', if_not_exists => TRUE);
-- SELECT create_hypertable('alerts_log', 'timestamp', if_not_exists => TRUE);
