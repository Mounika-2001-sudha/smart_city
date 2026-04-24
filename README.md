# 🏙️ Smart City IoT Simulation Platform

A fully simulated Smart City system that generates, streams, processes, and visualises real-world urban sensor data — no hardware required.

```
Python Simulators → Apache Kafka → Go Backend (REST API) → PostgreSQL → Streamlit Dashboard
```

## 📋 Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start (Docker)](#quick-start-docker)
- [Manual Setup](#manual-setup)
- [API Reference](#api-reference)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Analytics](#analytics)

---

## 🏗️ Architecture

| Layer | Technology | Role |
|-------|-----------|------|
| Simulation | Python 3.11 | Generates realistic IoT sensor data |
| Streaming | Apache Kafka 3.x | Real-time message bus (3 topics) |
| Backend | Go 1.21 + Gin | High-performance consumer + REST API |
| Storage | PostgreSQL 16 | Persistent time-series storage |
| Dashboard | Streamlit + Plotly | Live visualisation |

### Kafka Topics

| Topic | Producers | Consumers | Partitions |
|-------|-----------|-----------|-----------|
| `traffic_stream` | Traffic Simulator | Go Backend | 3 |
| `air_quality_stream` | AQ Simulator | Go Backend | 3 |
| `weather_stream` | Weather Simulator | Go Backend | 2 |

### City Zones Simulated

- Downtown, North Suburb, South Suburb, East Corridor, West Highway, Airport Road
- Each zone has 4 traffic intersections + 1 air quality station + 1 weather station

---

## ✅ Prerequisites

### Docker (Recommended)
- Docker 24+ with Docker Compose v2

### Manual
- Python 3.11+
- Go 1.21+
- Apache Kafka 3.6+
- PostgreSQL 16+

---

## 🚀 Quick Start (Docker)

```bash
# 1. Clone the repo
git clone https://github.com/yourname/smart-city-iot.git
cd smart-city-iot

# 2. Start all services
docker compose -f docker/docker-compose.yml up -d

# 3. Wait ~30s for services to initialise, then open:
#    Dashboard:    http://localhost:8501
#    Go API:       http://localhost:8080/api/v1/traffic/latest
#    Kafka UI:     http://localhost:8090

# 4. Watch logs
docker compose -f docker/docker-compose.yml logs -f
```

---

## 🔧 Manual Setup

### Step 1 — Start Kafka

```bash
# Using docker just for Kafka/Postgres
docker compose -f docker/docker-compose.yml up -d zookeeper kafka postgres

# Create topics
cd simulator
pip install -r requirements.txt
python setup_topics.py
```

### Step 2 — Start Python Simulators

```bash
cd simulator
pip install -r requirements.txt

# Run all three simulators concurrently
python orchestrator.py

# Or run individually:
python weather_simulator.py &
python traffic_simulator.py &
python air_quality_simulator.py &
```

### Step 3 — Start Go Backend

```bash
cd go_backend

# Download dependencies
go mod tidy

# Run
export DATABASE_URL="postgres://smartcity:smartcity@localhost:5432/smartcity?sslmode=disable"
export KAFKA_BROKERS="localhost:9092"
export API_PORT=":8080"

go run ./cmd/server
```

### Step 4 — Start Streamlit Dashboard

```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
# Opens at http://localhost:8501
```

---

## 📡 API Reference

Base URL: `http://localhost:8080/api/v1`

### Traffic

```
GET /traffic/latest
```
Returns latest aggregated traffic stats per zone.

```json
{
  "count": 6,
  "data": [
    {
      "zone": "downtown",
      "avg_congestion": 0.72,
      "avg_speed": 28.5,
      "total_vehicles": 3200,
      "incident_count": 1,
      "updated_at": "2024-01-15T08:32:10Z"
    }
  ]
}
```

### Air Quality

```
GET /air/latest
```
Returns latest air quality reading per station including AQI, PM2.5, CO, NO2, SO2.

### Weather

```
GET /weather/latest
```
Returns latest weather reading per station including condition, wind, temperature, visibility.

### Alerts

```
GET /alerts?limit=20&severity=high
```

Parameters:
- `limit` — number of alerts (default: 20, max: 200)
- `severity` — filter: `critical`, `high`, `medium`, `low`

### Correlation

```
GET /correlation?limit=200
```
Returns time-bucketed data points merging traffic + AQ for scatter analysis.

```
GET /correlation/stats
```
Returns Pearson correlation coefficients between streams.

---

## 📁 Project Structure

```
smart_city/
├── simulator/
│   ├── traffic_simulator.py      # Rush hour + incident logic
│   ├── air_quality_simulator.py  # PM2.5, AQI with EPA breakpoints
│   ├── weather_simulator.py      # Seasonal + diurnal patterns
│   ├── orchestrator.py           # Runs all three, shares weather state
│   ├── setup_topics.py           # Creates Kafka topics
│   ├── requirements.txt
│   └── Dockerfile
│
├── go_backend/
│   ├── cmd/server/main.go        # Entry point
│   ├── internal/
│   │   ├── models/models.go      # Shared data structures
│   │   ├── consumer/consumer.go  # Kafka goroutines (3 streams)
│   │   ├── processor/processor.go # Business logic + alert engine
│   │   ├── api/server.go         # Gin REST endpoints
│   │   └── db/db.go              # PostgreSQL writes
│   ├── go.mod
│   └── Dockerfile
│
├── dashboard/
│   ├── app.py                    # Streamlit multi-tab dashboard
│   ├── requirements.txt
│   └── Dockerfile
│
├── db/
│   └── schema.sql                # Tables, indexes, views
│
├── analytics/
│   ├── analytics_engine.py       # Standalone analytics (rush hour, spikes, correlation)
│   └── requirements.txt
│
└── docker/
    └── docker-compose.yml        # Full stack orchestration
```

---

## ⚙️ Configuration

All services are configured via environment variables:

### Go Backend

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `DATABASE_URL` | `postgres://...` | PostgreSQL DSN |
| `API_PORT` | `:8080` | HTTP listen port |
| `GIN_MODE` | `debug` | `debug` or `release` |

### Streamlit Dashboard

| Variable | Default | Description |
|----------|---------|-------------|
| `API_BASE` | `http://localhost:8080/api/v1` | Go backend URL |

### Alert Thresholds (processor.go)

| Constant | Default | Trigger |
|----------|---------|---------|
| `AQIHighThreshold` | 150 | High AQI alert |
| `AQICriticalThreshold` | 200 | Critical AQI alert |
| `CongestionHighThreshold` | 0.7 | Congestion alert |
| `LowVisibilityThreshold` | 2.0 km | Visibility alert |

---

## 📊 Analytics

Run the standalone analytics engine against the PostgreSQL database:

```bash
cd analytics
pip install -r requirements.txt
python analytics_engine.py
```

Generates:
- `rush_hour_analysis.png` — hourly congestion patterns + zone heatmap
- `aqi_spikes.png` — AQI time series with Z-score spike detection
- `correlation_heatmap.png` — cross-stream Pearson correlation matrix
- `alert_summary.png` — alert frequency by severity and hour

---

## 🚨 Alert Types

| Type | Trigger | Severity |
|------|---------|---------|
| `aqi_spike` | AQI > 150 | high |
| `aqi_spike` | AQI > 200 | critical |
| `congestion` | Congestion index > 0.7 | high |
| `congestion` | Congestion index > 0.9 | critical |
| `incident` | Incident flag set | medium |
| `low_visibility` | Visibility < 2 km | medium |
| `combined_hazard` | Congestion > 0.6 AND AQI > 150 | high |

---

## 🧪 Simulated Patterns

### Traffic Simulator
- **Morning rush**: 8–10 AM (1.6–2.2× base volume)
- **Evening rush**: 5–7 PM (1.8–2.4× base volume)
- **Overnight lull**: 12–5 AM (5–15% base volume)
- **Weekend shopping peak**: 11 AM–3 PM
- **Random incidents**: 2% base probability, +4% during rush hours
- **Weather impact**: speed reduction based on current conditions

### Air Quality Simulator
- **Traffic correlation**: PM2.5 rises with congestion
- **Industrial proximity**: stations near industrial zones have higher baselines
- **Wind dispersion**: higher wind = lower pollution
- **Humidity trap**: high humidity increases particulate concentration
- **EPA AQI calculation**: proper linear interpolation across breakpoints

### Weather Simulator
- **Diurnal temperature cycle**: coolest at 5 AM, warmest at 2 PM
- **Seasonal baselines**: monthly temperature profiles (Delhi-like)
- **Monsoon season**: July–August produces rain/heavy_rain conditions
- **Winter fog**: December–February, high humidity → fog
- **Weather impacts**: directly fed to Traffic and AQ simulators

---

## 🐳 Docker Services

| Service | Port | Description |
|---------|------|-------------|
| `zookeeper` | 2181 | Kafka coordinator |
| `kafka` | 9092 | Kafka broker |
| `kafka-ui` | 8090 | Kafka topic browser |
| `postgres` | 5432 | Time-series storage |
| `simulator` | — | Python sensor simulators |
| `go-backend` | 8080 | REST API + stream processor |
| `dashboard` | 8501 | Streamlit UI |

---

## 📝 License

MIT — free to use for academic and commercial projects.
