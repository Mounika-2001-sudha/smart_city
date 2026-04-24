"""
Weather Sensor Simulator
Generates realistic weather data with temporal patterns,
seasonal variation, and weather event simulation.
Broadcasts weather_params that other simulators consume.
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
TOPIC = "weather_stream"

WEATHER_STATIONS = {
    "wth_center":  {"lat": 28.6139, "lon": 77.2090},
    "wth_north":   {"lat": 28.7200, "lon": 77.1000},
    "wth_south":   {"lat": 28.5100, "lon": 77.1900},
    "wth_east":    {"lat": 28.6400, "lon": 77.3100},
    "wth_west":    {"lat": 28.6100, "lon": 77.0200},
}

# Seasonal temperature baselines (Delhi-like)
MONTH_TEMP_BASELINE = {
    1: 14, 2: 17, 3: 23, 4: 30, 5: 36, 6: 34,
    7: 31, 8: 30, 9: 29, 10: 25, 11: 19, 12: 15
}


@dataclass
class WeatherReading:
    sensor_id: str
    station: str
    timestamp: str
    temperature_c: float
    feels_like_c: float
    humidity_pct: float
    pressure_hpa: float
    wind_speed_ms: float
    wind_direction_deg: int
    precipitation_mm: float
    visibility_km: float
    cloud_cover_pct: int
    uv_index: float
    condition: str           # clear, cloudy, rain, heavy_rain, fog, haze
    weather_impact_traffic: float   # multiplier for traffic speed
    weather_impact_aqi: float       # multiplier for pollution dispersion
    lat: float
    lon: float


def get_temperature(hour: int, month: int) -> float:
    """Diurnal temperature variation."""
    baseline = MONTH_TEMP_BASELINE.get(month, 25)
    daily_range = 8.0
    # Coolest at 5 AM, warmest at 2 PM
    hour_offset = math.sin(math.radians((hour - 5) * 15)) * (daily_range / 2)
    return round(baseline + hour_offset + random.gauss(0, 1.5), 1)


def determine_condition(humidity: float, month: int) -> str:
    """Determine weather condition based on season and humidity."""
    if month in [7, 8]:  # Monsoon
        if humidity > 90:
            return "heavy_rain"
        elif humidity > 80:
            return "rain"
        elif humidity > 70:
            return "cloudy"
    if month in [12, 1, 2] and humidity > 75:  # Winter fog
        return "fog"
    if humidity > 70:
        return "haze"
    if random.random() < 0.1:
        return "cloudy"
    return "clear"


def get_traffic_impact(condition: str) -> float:
    """Speed reduction factor from weather."""
    return {
        "clear":      1.0,
        "cloudy":     0.97,
        "haze":       0.90,
        "rain":       0.75,
        "heavy_rain": 0.55,
        "fog":        0.60,
    }.get(condition, 1.0)


def get_aqi_impact(condition: str, wind_speed: float) -> float:
    """Dispersion factor: rain washes out particulates, fog traps them."""
    base = {
        "clear":      1.0,
        "cloudy":     0.95,
        "haze":       1.2,
        "rain":       0.70,
        "heavy_rain": 0.50,
        "fog":        1.35,
    }.get(condition, 1.0)
    wind_effect = max(0.5, 1.0 - wind_speed / 20.0)
    return round(base * wind_effect, 3)


def generate_reading(station_id: str) -> WeatherReading:
    now = datetime.now(timezone.utc)
    hour = now.hour
    month = now.month
    station = WEATHER_STATIONS[station_id]

    temperature = get_temperature(hour, month)
    humidity = round(random.uniform(40, 95), 1)
    pressure = round(random.gauss(1013, 8), 1)
    wind_speed = round(abs(random.gauss(3, 2.5)), 1)
    wind_dir = random.randint(0, 359)
    condition = determine_condition(humidity, month)

    # Rain intensity
    precip = 0.0
    if condition == "rain":
        precip = round(random.uniform(0.5, 5.0), 2)
    elif condition == "heavy_rain":
        precip = round(random.uniform(5.0, 25.0), 2)

    visibility = {
        "clear": round(random.uniform(8, 12), 1),
        "cloudy": round(random.uniform(6, 10), 1),
        "haze": round(random.uniform(2, 5), 1),
        "rain": round(random.uniform(3, 7), 1),
        "heavy_rain": round(random.uniform(0.5, 3), 1),
        "fog": round(random.uniform(0.1, 1.5), 1),
    }.get(condition, 8.0)

    cloud_cover = {
        "clear": random.randint(0, 15),
        "cloudy": random.randint(60, 90),
        "haze": random.randint(20, 50),
        "rain": random.randint(75, 95),
        "heavy_rain": random.randint(90, 100),
        "fog": random.randint(80, 100),
    }.get(condition, 0)

    # UV based on time, cloud cover
    uv_base = max(0, math.sin(math.radians(max(0, hour - 6) * 15))) * 10
    uv = round(uv_base * (1 - cloud_cover / 150), 1)

    # Feels like (heat index / wind chill approximation)
    feels_like = round(temperature - (wind_speed * 0.3) + (humidity - 50) * 0.05, 1)

    return WeatherReading(
        sensor_id=f"wth_{station_id[-3:]}",
        station=station_id,
        timestamp=now.isoformat(),
        temperature_c=temperature,
        feels_like_c=feels_like,
        humidity_pct=humidity,
        pressure_hpa=pressure,
        wind_speed_ms=wind_speed,
        wind_direction_deg=wind_dir,
        precipitation_mm=precip,
        visibility_km=visibility,
        cloud_cover_pct=cloud_cover,
        uv_index=uv,
        condition=condition,
        weather_impact_traffic=get_traffic_impact(condition),
        weather_impact_aqi=get_aqi_impact(condition, wind_speed),
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


def run_simulator(interval_sec: float = 10.0):
    producer = create_producer()
    print(f"[Weather Simulator] Starting... topic={TOPIC}, interval={interval_sec}s")

    try:
        while True:
            conditions = []
            for station_id in WEATHER_STATIONS:
                reading = generate_reading(station_id)
                producer.send(
                    TOPIC,
                    key=reading.sensor_id,
                    value=asdict(reading),
                )
                conditions.append(reading.condition)
            producer.flush()
            print(f"[Weather] Sent {len(WEATHER_STATIONS)} readings | conditions: {set(conditions)}")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("[Weather Simulator] Stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_simulator()
