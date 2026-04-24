"""
Traffic Sensor Simulator
Generates realistic traffic data with rush hour logic,
weekend patterns, and incident simulation.
"""

import json
import random
import time
import math
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from kafka import KafkaProducer
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "traffic_stream"

# City zones with base traffic characteristics
ZONES = {
    "downtown":      {"base_volume": 800, "base_speed": 35, "lat": 28.6139, "lon": 77.2090},
    "north_suburb":  {"base_volume": 400, "base_speed": 55, "lat": 28.7041, "lon": 77.1025},
    "south_suburb":  {"base_volume": 350, "base_speed": 58, "lat": 28.5244, "lon": 77.1855},
    "east_corridor": {"base_volume": 600, "base_speed": 45, "lat": 28.6353, "lon": 77.2975},
    "west_highway":  {"base_volume": 700, "base_speed": 60, "lat": 28.6128, "lon": 77.0369},
    "airport_road":  {"base_volume": 500, "base_speed": 50, "lat": 28.5562, "lon": 77.1000},
}

# Intersection IDs per zone
INTERSECTIONS_PER_ZONE = 4


@dataclass
class TrafficReading:
    sensor_id: str
    zone: str
    intersection_id: int
    timestamp: str
    vehicle_count: int       # vehicles per minute
    avg_speed_kmh: float     # average speed
    congestion_index: float  # 0.0 (free flow) to 1.0 (gridlock)
    incident_flag: bool
    weather_impact: float    # speed reduction factor
    lat: float
    lon: float


def get_rush_hour_multiplier(hour: int, is_weekend: bool) -> float:
    """Return traffic multiplier based on time of day."""
    if is_weekend:
        # Weekend: shopping peak around noon-3pm
        if 11 <= hour <= 15:
            return random.uniform(1.2, 1.5)
        elif 19 <= hour <= 22:
            return random.uniform(1.1, 1.3)
        return random.uniform(0.4, 0.7)
    else:
        # Weekday: morning rush 8-10, evening rush 17-19
        if 8 <= hour <= 10:
            return random.uniform(1.6, 2.2)
        elif 17 <= hour <= 19:
            return random.uniform(1.8, 2.4)
        elif 12 <= hour <= 13:
            return random.uniform(1.1, 1.3)
        elif 0 <= hour <= 5:
            return random.uniform(0.05, 0.15)
        return random.uniform(0.5, 0.9)


def calculate_congestion(vehicle_count: int, speed: float, zone_capacity: int) -> float:
    """Compute congestion index from volume and speed."""
    volume_ratio = min(vehicle_count / zone_capacity, 1.0)
    speed_ratio = 1.0 - (speed / 80.0)  # 80 kmh = free flow
    congestion = (volume_ratio * 0.6 + speed_ratio * 0.4)
    return round(min(max(congestion + random.uniform(-0.05, 0.05), 0.0), 1.0), 3)


def simulate_incident(hour: int) -> bool:
    """Randomly trigger traffic incidents (higher probability during rush hours)."""
    base_prob = 0.02
    rush_boost = 0.04 if (8 <= hour <= 10 or 17 <= hour <= 19) else 0.0
    return random.random() < (base_prob + rush_boost)


def generate_reading(zone: str, intersection_id: int, weather_impact: float = 1.0) -> TrafficReading:
    now = datetime.now(timezone.utc)
    hour = now.hour
    is_weekend = now.weekday() >= 5
    zone_data = ZONES[zone]

    multiplier = get_rush_hour_multiplier(hour, is_weekend)
    noise = random.gauss(0, 0.1)

    vehicle_count = int(zone_data["base_volume"] * multiplier * (1 + noise))
    vehicle_count = max(0, vehicle_count)

    # Speed decreases with more vehicles and weather
    speed_factor = max(0.3, 1.0 - (vehicle_count / (zone_data["base_volume"] * 2.5)))
    avg_speed = zone_data["base_speed"] * speed_factor * weather_impact
    avg_speed = round(max(5.0, avg_speed + random.gauss(0, 2)), 1)

    incident = simulate_incident(hour)
    if incident:
        avg_speed *= 0.5
        vehicle_count = int(vehicle_count * 1.3)

    congestion = calculate_congestion(vehicle_count, avg_speed, int(zone_data["base_volume"] * 2))

    # Slightly offset lat/lon per intersection
    lat_offset = (intersection_id * 0.002) - 0.004
    lon_offset = (intersection_id * 0.003) - 0.006

    return TrafficReading(
        sensor_id=f"trf_{zone[:4]}_{intersection_id:02d}",
        zone=zone,
        intersection_id=intersection_id,
        timestamp=now.isoformat(),
        vehicle_count=vehicle_count,
        avg_speed_kmh=avg_speed,
        congestion_index=congestion,
        incident_flag=incident,
        weather_impact=round(weather_impact, 3),
        lat=round(zone_data["lat"] + lat_offset, 6),
        lon=round(zone_data["lon"] + lon_offset, 6),
    )


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def run_simulator(weather_impact: float = 1.0, interval_sec: float = 2.0):
    """Main simulation loop. weather_impact: 1.0=clear, 0.7=rain, 0.5=heavy rain."""
    producer = create_producer()
    print(f"[Traffic Simulator] Starting... topic={TOPIC}, interval={interval_sec}s")

    try:
        while True:
            for zone in ZONES:
                for i in range(INTERSECTIONS_PER_ZONE):
                    reading = generate_reading(zone, i, weather_impact)
                    producer.send(
                        TOPIC,
                        key=reading.sensor_id,
                        value=asdict(reading),
                    )
            producer.flush()
            print(f"[Traffic] Sent {len(ZONES) * INTERSECTIONS_PER_ZONE} readings @ {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("[Traffic Simulator] Stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_simulator()
