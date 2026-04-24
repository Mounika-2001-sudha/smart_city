"""
Smart City Simulator Orchestrator
Launches all three simulators as concurrent threads,
sharing weather state so AQ and Traffic react to weather.
"""

import threading
import time
import json
import random
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from traffic_simulator import run_simulator as run_traffic
from air_quality_simulator import run_simulator as run_aq
from weather_simulator import run_simulator as run_weather

# Shared state: weather updates inform other simulators
weather_state = {
    "wind_speed_ms": 3.0,
    "humidity_pct": 60.0,
    "temperature_c": 28.0,
    "condition": "clear",
    "weather_impact_traffic": 1.0,
    "weather_impact_aqi": 1.0,
}
weather_lock = threading.Lock()


def weather_thread():
    """Run weather simulator."""
    run_weather(interval_sec=10.0)


def traffic_thread():
    """Run traffic simulator, adapting to current weather."""
    from traffic_simulator import ZONES, INTERSECTIONS_PER_ZONE, generate_reading, create_producer, TOPIC
    import json

    producer = create_producer()
    print("[Orchestrator] Traffic thread started")

    while True:
        with weather_lock:
            impact = weather_state["weather_impact_traffic"]

        for zone in ZONES:
            for i in range(INTERSECTIONS_PER_ZONE):
                reading = generate_reading(zone, i, weather_impact=impact)
                producer.send(
                    TOPIC,
                    key=reading.sensor_id,
                    value=vars(reading) if hasattr(reading, '__dict__') else reading.__dict__,
                )
        from dataclasses import asdict
        producer.flush()
        time.sleep(2.0)


def aq_thread():
    """Run AQ simulator, adapting to current weather."""
    from air_quality_simulator import STATIONS, generate_reading, create_producer, TOPIC
    from dataclasses import asdict

    producer = create_producer()
    print("[Orchestrator] Air quality thread started")

    while True:
        with weather_lock:
            params = dict(weather_state)

        for station_id in STATIONS:
            reading = generate_reading(station_id, params)
            producer.send(
                TOPIC,
                key=reading.sensor_id,
                value=asdict(reading),
            )
        producer.flush()
        time.sleep(5.0)


def weather_consumer_thread():
    """Consume weather topic to update shared state."""
    consumer = KafkaConsumer(
        "weather_stream",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="orchestrator-weather-consumer",
    )
    print("[Orchestrator] Weather consumer thread started")

    for msg in consumer:
        data = msg.value
        with weather_lock:
            weather_state["wind_speed_ms"] = data.get("wind_speed_ms", 3.0)
            weather_state["humidity_pct"] = data.get("humidity_pct", 60.0)
            weather_state["temperature_c"] = data.get("temperature_c", 28.0)
            weather_state["condition"] = data.get("condition", "clear")
            weather_state["weather_impact_traffic"] = data.get("weather_impact_traffic", 1.0)
            weather_state["weather_impact_aqi"] = data.get("weather_impact_aqi", 1.0)


def main():
    print("=" * 60)
    print("  Smart City IoT Simulator Orchestrator")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    threads = [
        threading.Thread(target=weather_thread,          daemon=True, name="weather-sim"),
        threading.Thread(target=weather_consumer_thread, daemon=True, name="weather-consumer"),
        threading.Thread(target=traffic_thread,          daemon=True, name="traffic-sim"),
        threading.Thread(target=aq_thread,               daemon=True, name="aq-sim"),
    ]

    # Start weather first so others can sync
    threads[0].start()
    threads[1].start()
    time.sleep(3)  # let weather emit first reading
    threads[2].start()
    threads[3].start()

    print(f"[Orchestrator] All simulators running. Ctrl+C to stop.")

    try:
        while True:
            time.sleep(30)
            with weather_lock:
                cond = weather_state["condition"]
                temp = weather_state["temperature_c"]
            print(f"[Status] Weather: {cond}, {temp}°C | All streams active")
    except KeyboardInterrupt:
        print("\n[Orchestrator] Shutting down...")


if __name__ == "__main__":
    main()
