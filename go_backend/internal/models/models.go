package models

import "time"

// ─── Traffic ───────────────────────────────────────────────────────────────

type TrafficReading struct {
	SensorID        string    `json:"sensor_id"        db:"sensor_id"`
	Zone            string    `json:"zone"             db:"zone"`
	IntersectionID  int       `json:"intersection_id"  db:"intersection_id"`
	Timestamp       time.Time `json:"timestamp"        db:"timestamp"`
	VehicleCount    int       `json:"vehicle_count"    db:"vehicle_count"`
	AvgSpeedKmh     float64   `json:"avg_speed_kmh"    db:"avg_speed_kmh"`
	CongestionIndex float64   `json:"congestion_index" db:"congestion_index"`
	IncidentFlag    bool      `json:"incident_flag"    db:"incident_flag"`
	WeatherImpact   float64   `json:"weather_impact"   db:"weather_impact"`
	Lat             float64   `json:"lat"              db:"lat"`
	Lon             float64   `json:"lon"              db:"lon"`
}

type TrafficLatest struct {
	Zone            string    `json:"zone"`
	AvgCongestion   float64   `json:"avg_congestion"`
	AvgSpeed        float64   `json:"avg_speed"`
	TotalVehicles   int       `json:"total_vehicles"`
	IncidentCount   int       `json:"incident_count"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// ─── Air Quality ────────────────────────────────────────────────────────────

type AirQualityReading struct {
	SensorID      string    `json:"sensor_id"      db:"sensor_id"`
	Station       string    `json:"station"        db:"station"`
	Timestamp     time.Time `json:"timestamp"      db:"timestamp"`
	PM25          float64   `json:"pm25"           db:"pm25"`
	PM10          float64   `json:"pm10"           db:"pm10"`
	CO            float64   `json:"co"             db:"co"`
	NO2           float64   `json:"no2"            db:"no2"`
	SO2           float64   `json:"so2"            db:"so2"`
	O3            float64   `json:"o3"             db:"o3"`
	AQI           int       `json:"aqi"            db:"aqi"`
	AQICategory   string    `json:"aqi_category"   db:"aqi_category"`
	TemperatureC  float64   `json:"temperature_c"  db:"temperature_c"`
	HumidityPct   float64   `json:"humidity_pct"   db:"humidity_pct"`
	WindSpeedMS   float64   `json:"wind_speed_ms"  db:"wind_speed_ms"`
	Lat           float64   `json:"lat"            db:"lat"`
	Lon           float64   `json:"lon"            db:"lon"`
}

// ─── Weather ────────────────────────────────────────────────────────────────

type WeatherReading struct {
	SensorID              string    `json:"sensor_id"               db:"sensor_id"`
	Station               string    `json:"station"                 db:"station"`
	Timestamp             time.Time `json:"timestamp"               db:"timestamp"`
	TemperatureC          float64   `json:"temperature_c"           db:"temperature_c"`
	FeelsLikeC            float64   `json:"feels_like_c"            db:"feels_like_c"`
	HumidityPct           float64   `json:"humidity_pct"            db:"humidity_pct"`
	PressureHpa           float64   `json:"pressure_hpa"            db:"pressure_hpa"`
	WindSpeedMS           float64   `json:"wind_speed_ms"           db:"wind_speed_ms"`
	WindDirectionDeg      int       `json:"wind_direction_deg"      db:"wind_direction_deg"`
	PrecipitationMM       float64   `json:"precipitation_mm"        db:"precipitation_mm"`
	VisibilityKm          float64   `json:"visibility_km"           db:"visibility_km"`
	CloudCoverPct         int       `json:"cloud_cover_pct"         db:"cloud_cover_pct"`
	UVIndex               float64   `json:"uv_index"                db:"uv_index"`
	Condition             string    `json:"condition"               db:"condition"`
	WeatherImpactTraffic  float64   `json:"weather_impact_traffic"  db:"weather_impact_traffic"`
	WeatherImpactAQI      float64   `json:"weather_impact_aqi"      db:"weather_impact_aqi"`
	Lat                   float64   `json:"lat"                     db:"lat"`
	Lon                   float64   `json:"lon"                     db:"lon"`
}

// ─── Alerts ─────────────────────────────────────────────────────────────────

type Alert struct {
	ID          int64     `json:"id"           db:"id"`
	AlertType   string    `json:"alert_type"   db:"alert_type"`   // aqi_spike, congestion, weather_impact
	Severity    string    `json:"severity"     db:"severity"`     // low, medium, high, critical
	Zone        string    `json:"zone"         db:"zone"`
	Message     string    `json:"message"      db:"message"`
	Value       float64   `json:"value"        db:"value"`
	Threshold   float64   `json:"threshold"    db:"threshold"`
	Timestamp   time.Time `json:"timestamp"    db:"timestamp"`
	Resolved    bool      `json:"resolved"     db:"resolved"`
}

// ─── Correlation ─────────────────────────────────────────────────────────────

type CorrelationPoint struct {
	Zone            string    `json:"zone"`
	Timestamp       time.Time `json:"timestamp"`
	CongestionIndex float64   `json:"congestion_index"`
	AQI             int       `json:"aqi"`
	PM25            float64   `json:"pm25"`
	WindSpeedMS     float64   `json:"wind_speed_ms"`
	Condition       string    `json:"condition"`
}

type CorrelationStats struct {
	TrafficVsAQI      float64 `json:"traffic_vs_aqi"`
	TrafficVsPM25     float64 `json:"traffic_vs_pm25"`
	WindVsAQI         float64 `json:"wind_vs_aqi"`
	HumidityVsPM25    float64 `json:"humidity_vs_pm25"`
	ComputedAt        time.Time `json:"computed_at"`
}
