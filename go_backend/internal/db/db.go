package db

import (
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"smart_city_backend/internal/models"
)

type DB struct {
	conn *sqlx.DB
}

func NewDB(dsn string) (*DB, error) {
	conn, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("db connect: %w", err)
	}
	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(10)
	conn.SetConnMaxLifetime(5 * time.Minute)
	return &DB{conn: conn}, nil
}

func (d *DB) Close() {
	d.conn.Close()
}

// ─── Schema Migrations ────────────────────────────────────────────────────

func (d *DB) Migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS traffic_data (
		id              BIGSERIAL PRIMARY KEY,
		sensor_id       TEXT NOT NULL,
		zone            TEXT NOT NULL,
		intersection_id INT NOT NULL,
		timestamp       TIMESTAMPTZ NOT NULL,
		vehicle_count   INT NOT NULL,
		avg_speed_kmh   FLOAT NOT NULL,
		congestion_index FLOAT NOT NULL,
		incident_flag   BOOLEAN NOT NULL DEFAULT FALSE,
		weather_impact  FLOAT NOT NULL DEFAULT 1.0,
		lat             FLOAT,
		lon             FLOAT,
		created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_traffic_timestamp ON traffic_data(timestamp DESC);
	CREATE INDEX IF NOT EXISTS idx_traffic_zone ON traffic_data(zone);

	CREATE TABLE IF NOT EXISTS air_quality_data (
		id              BIGSERIAL PRIMARY KEY,
		sensor_id       TEXT NOT NULL,
		station         TEXT NOT NULL,
		timestamp       TIMESTAMPTZ NOT NULL,
		pm25            FLOAT NOT NULL,
		pm10            FLOAT NOT NULL,
		co              FLOAT NOT NULL,
		no2             FLOAT NOT NULL,
		so2             FLOAT NOT NULL,
		o3              FLOAT NOT NULL,
		aqi             INT NOT NULL,
		aqi_category    TEXT NOT NULL,
		temperature_c   FLOAT,
		humidity_pct    FLOAT,
		wind_speed_ms   FLOAT,
		lat             FLOAT,
		lon             FLOAT,
		created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_aq_timestamp ON air_quality_data(timestamp DESC);
	CREATE INDEX IF NOT EXISTS idx_aq_station ON air_quality_data(station);

	CREATE TABLE IF NOT EXISTS weather_data (
		id                    BIGSERIAL PRIMARY KEY,
		sensor_id             TEXT NOT NULL,
		station               TEXT NOT NULL,
		timestamp             TIMESTAMPTZ NOT NULL,
		temperature_c         FLOAT NOT NULL,
		feels_like_c          FLOAT NOT NULL,
		humidity_pct          FLOAT NOT NULL,
		pressure_hpa          FLOAT NOT NULL,
		wind_speed_ms         FLOAT NOT NULL,
		wind_direction_deg    INT NOT NULL,
		precipitation_mm      FLOAT NOT NULL DEFAULT 0,
		visibility_km         FLOAT NOT NULL,
		cloud_cover_pct       INT NOT NULL,
		uv_index              FLOAT NOT NULL,
		condition             TEXT NOT NULL,
		weather_impact_traffic FLOAT NOT NULL DEFAULT 1.0,
		weather_impact_aqi    FLOAT NOT NULL DEFAULT 1.0,
		lat                   FLOAT,
		lon                   FLOAT,
		created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(timestamp DESC);

	CREATE TABLE IF NOT EXISTS alerts_log (
		id          BIGSERIAL PRIMARY KEY,
		alert_type  TEXT NOT NULL,
		severity    TEXT NOT NULL,
		zone        TEXT NOT NULL,
		message     TEXT NOT NULL,
		value       FLOAT NOT NULL,
		threshold   FLOAT NOT NULL,
		timestamp   TIMESTAMPTZ NOT NULL,
		resolved    BOOLEAN NOT NULL DEFAULT FALSE,
		created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts_log(timestamp DESC);
	CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts_log(severity);
	`
	_, err := d.conn.Exec(schema)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	log.Println("[DB] Schema migration complete")
	return nil
}

// ─── Traffic Writes ───────────────────────────────────────────────────────

func (d *DB) InsertTraffic(r models.TrafficReading) {
	const q = `
	INSERT INTO traffic_data
		(sensor_id, zone, intersection_id, timestamp, vehicle_count, avg_speed_kmh,
		 congestion_index, incident_flag, weather_impact, lat, lon)
	VALUES
		(:sensor_id, :zone, :intersection_id, :timestamp, :vehicle_count, :avg_speed_kmh,
		 :congestion_index, :incident_flag, :weather_impact, :lat, :lon)`
	if _, err := d.conn.NamedExec(q, r); err != nil {
		log.Printf("[DB] Traffic insert error: %v", err)
	}
}

// ─── Air Quality Writes ───────────────────────────────────────────────────

func (d *DB) InsertAirQuality(r models.AirQualityReading) {
	const q = `
	INSERT INTO air_quality_data
		(sensor_id, station, timestamp, pm25, pm10, co, no2, so2, o3,
		 aqi, aqi_category, temperature_c, humidity_pct, wind_speed_ms, lat, lon)
	VALUES
		(:sensor_id, :station, :timestamp, :pm25, :pm10, :co, :no2, :so2, :o3,
		 :aqi, :aqi_category, :temperature_c, :humidity_pct, :wind_speed_ms, :lat, :lon)`
	if _, err := d.conn.NamedExec(q, r); err != nil {
		log.Printf("[DB] AQ insert error: %v", err)
	}
}

// ─── Weather Writes ───────────────────────────────────────────────────────

func (d *DB) InsertWeather(r models.WeatherReading) {
	const q = `
	INSERT INTO weather_data
		(sensor_id, station, timestamp, temperature_c, feels_like_c, humidity_pct,
		 pressure_hpa, wind_speed_ms, wind_direction_deg, precipitation_mm,
		 visibility_km, cloud_cover_pct, uv_index, condition,
		 weather_impact_traffic, weather_impact_aqi, lat, lon)
	VALUES
		(:sensor_id, :station, :timestamp, :temperature_c, :feels_like_c, :humidity_pct,
		 :pressure_hpa, :wind_speed_ms, :wind_direction_deg, :precipitation_mm,
		 :visibility_km, :cloud_cover_pct, :uv_index, :condition,
		 :weather_impact_traffic, :weather_impact_aqi, :lat, :lon)`
	if _, err := d.conn.NamedExec(q, r); err != nil {
		log.Printf("[DB] Weather insert error: %v", err)
	}
}

// ─── Alert Writes ─────────────────────────────────────────────────────────

func (d *DB) InsertAlert(a models.Alert) {
	const q = `
	INSERT INTO alerts_log
		(alert_type, severity, zone, message, value, threshold, timestamp)
	VALUES
		(:alert_type, :severity, :zone, :message, :value, :threshold, :timestamp)`
	if _, err := d.conn.NamedExec(q, a); err != nil {
		log.Printf("[DB] Alert insert error: %v", err)
	}
}
