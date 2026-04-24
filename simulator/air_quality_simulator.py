"""
Air Quality Sensor Simulator
Generates PM2.5, PM10, CO, NO2, SO2 and computed AQI.
Correlates with traffic density and weather conditions.
"""

import json
import random
import time
import math
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from kafka import KafkaProducer
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "air_quality_stream"

# Monitoring stations (co-located with traffic zones)
STATIONS = {
    "aq_downtown":      {"lat": 28.6139, "lon": 77.2090, "industrial_proximity": 0.7},
    "aq_north_suburb":  {"lat": 28.7041, "lon": 77.1025, "industrial_proximity": 0.3},
    "aq_south_suburb":  {"lat": 28.5244, "lon": 77.1855, "industrial_proximity": 0.4},
    "aq_east_corridor": {"lat": 28.6353, "lon": 77.2975, "industrial_proximity": 0.6},
    "aq_west_highway":  {"lat": 28.6128, "lon": 77.0369, "industrial_proximity": 0.5},
    "aq_airport_road":  {"lat": 28.5562, "lon": 77.1000, "industrial_proximity": 0.4},
}

# AQI breakpoints (US EPA standard)
PM25_BREAKPOINTS = [
    (0.0, 12.0,   0, 50),
    (12.1, 35.4,  51, 100),
    (35.5, 55.4,  101, 150),
    (55.5, 150.4, 151, 200),
    (150.5, 250.4,201, 300),
    (250.5, 350.4,301, 400),
    (350.5, 500.4,401, 500),
]

AQI_CATEGORIES = {
    (0, 50):   ("Good", "green"),
    (51, 100): ("Moderate", "yellow"),
    (101, 150):("Unhealthy for Sensitive Groups", "orange"),
    (151, 200):("Unhealthy", "red"),
    (201, 300):("Very Unhealthy", "purple"),
    (301, 500):("Hazardous", "maroon"),
}


@dataclass
class AirQualityReading:
    sensor_id: str
    station: str
    timestamp: str
    pm25: float      # µg/m³
    pm10: float      # µg/m³
    co: float        # ppm
    no2: float       # ppb
    so2: float       # ppb
    o3: float        # ppb
    aqi: int
    aqi_category: str
    temperature_c: float
    humidity_pct: float
    wind_speed_ms: float
    lat: float
    lon: float


def calculate_aqi(pm25: float) -> int:
    """Convert PM2.5 concentration to AQI using EPA linear interpolation."""
    for (c_lo, c_hi, i_lo, i_hi) in PM25_BREAKPOINTS:
        if c_lo <= pm25 <= c_hi:
            aqi = ((i_hi - i_lo) / (c_hi - c_lo)) * (pm25 - c_lo) + i_lo
            return int(round(aqi))
    return 500


def get_aqi_category(aqi: int) -> str:
    for (lo, hi), (category, _) in AQI_CATEGORIES.items():
        if lo <= aqi <= hi:
            return category
    return "Hazardous"


def get_traffic_correlation_factor(hour: int, is_weekend: bool) -> float:
    """Pollution correlates with traffic density."""
    if is_weekend:
        if 11 <= hour <= 15:
            return random.uniform(1.15, 1.35)
        return random.uniform(0.7, 0.9)
    else:
        if 8 <= hour <= 10 or 17 <= hour <= 19:
            return random.uniform(1.4, 1.8)
        elif 0 <= hour <= 5:
            return random.uniform(0.4, 0.6)
        return random.uniform(0.8, 1.1)


def generate_reading(station_id: str, weather_params: dict) -> AirQualityReading:
    now = datetime.now(timezone.utc)
    hour = now.hour
    is_weekend = now.weekday() >= 5
    station = STATIONS[station_id]

    traffic_factor = get_traffic_correlation_factor(hour, is_weekend)
    industrial = station["industrial_proximity"]

    # Wind disperses pollutants; humidity can trap them
    wind = weather_params.get("wind_speed_ms", 3.0)
    humidity = weather_params.get("humidity_pct", 60.0)
    temp = weather_params.get("temperature_c", 25.0)

    dispersion = max(0.3, 1.0 - (wind / 15.0))  # more wind = less pollution
    trap_factor = 1.0 + (humidity - 50.0) / 200.0  # humidity traps particles

    # Base PM2.5 for the city (Delhi-like urban baseline)
    base_pm25 = 45.0 + (industrial * 30.0)
    pm25 = base_pm25 * traffic_factor * dispersion * trap_factor
    pm25 = round(max(5.0, pm25 + random.gauss(0, 5)), 1)

    pm10 = round(pm25 * random.uniform(1.5, 2.2), 1)
    co = round((0.5 + traffic_factor * 1.5 * industrial) + random.gauss(0, 0.3), 2)
    no2 = round((20 + traffic_factor * 40 + industrial * 20) + random.gauss(0, 5), 1)
    so2 = round((5 + industrial * 25) + random.gauss(0, 3), 1)
    o3 = round(max(0, (30 + temp * 0.5 - traffic_factor * 10)) + random.gauss(0, 5), 1)

    aqi = calculate_aqi(pm25)
    category = get_aqi_category(aqi)

    return AirQualityReading(
        sensor_id=f"aq_{station_id[-4:]}",
        station=station_id,
        timestamp=now.isoformat(),
        pm25=pm25,
        pm10=pm10,
        co=co,
        no2=no2,
        so2=so2,
        o3=o3,
        aqi=aqi,
        aqi_category=category,
        temperature_c=round(temp, 1),
        humidity_pct=round(humidity, 1),
        wind_speed_ms=round(wind, 1),
        lat=station["lat"],
        lon=station["lon"],
    )


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def run_simulator(weather_params: dict = None, interval_sec: float = 5.0):
    """Main simulation loop."""
    if weather_params is None:
        weather_params = {"wind_speed_ms": 3.0, "humidity_pct": 60.0, "temperature_c": 28.0}

    producer = create_producer()
    print(f"[Air Quality Simulator] Starting... topic={TOPIC}, interval={interval_sec}s")

    try:
        while True:
            for station_id in STATIONS:
                reading = generate_reading(station_id, weather_params)
                producer.send(
                    TOPIC,
                    key=reading.sensor_id,
                    value=asdict(reading),
                )
            producer.flush()
            print(f"[AQ] Sent {len(STATIONS)} readings | avg AQI computed @ {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("[Air Quality Simulator] Stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_simulator()
