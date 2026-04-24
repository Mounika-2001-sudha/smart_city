"""
Smart City IoT Dashboard
Real-time visualization powered by the Go REST API.
Auto-refreshes every few seconds.
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime
from typing import Optional

# ── Config ────────────────────────────────────────────────────────────────────
import os

API_BASE = os.getenv("API_BASE", "http://localhost:8080/api/v1")
REFRESH_INTERVAL = 5  # seconds

st.set_page_config(
    page_title="Smart City IoT Dashboard",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
.metric-card {
    background: #1e1e2e;
    border-radius: 12px;
    padding: 1rem 1.5rem;
    border-left: 4px solid #7c5cbf;
}
.alert-critical { border-left-color: #e24b4a !important; }
.alert-high     { border-left-color: #ef9f27 !important; }
.alert-medium   { border-left-color: #378add !important; }
.status-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 0.8em;
    font-weight: 600;
}
</style>
""", unsafe_allow_html=True)


# ── API Helpers ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch(endpoint: str) -> Optional[dict]:
    try:
        r = requests.get(f"{API_BASE}{endpoint}", timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        st.error(f"⚠️ Cannot connect to Go API at {API_BASE}. Is the backend running?")
        return None
    except Exception as e:
        st.warning(f"API error [{endpoint}]: {e}")
        return None


def aqi_color(aqi: int) -> str:
    if aqi <= 50:   return "🟢"
    if aqi <= 100:  return "🟡"
    if aqi <= 150:  return "🟠"
    if aqi <= 200:  return "🔴"
    if aqi <= 300:  return "🟣"
    return "⚫"


def congestion_color(ci: float) -> str:
    if ci < 0.3:  return "#1D9E75"
    if ci < 0.6:  return "#EF9F27"
    if ci < 0.8:  return "#D85A30"
    return "#E24B4A"


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🏙️ Smart City IoT")
    st.caption("Real-time urban sensor dashboard")
    st.divider()

    auto_refresh = st.toggle("Auto-refresh", value=True)
    refresh_rate = st.slider("Refresh every (s)", 3, 30, REFRESH_INTERVAL)
    st.divider()

    st.subheader("🔌 API Status")
    health = fetch("/health" if False else "")  # use health endpoint
    try:
        r = requests.get(f"http://localhost:8080/health", timeout=2)
        if r.ok:
            st.success("Go backend: Online")
        else:
            st.error("Go backend: Error")
    except Exception:
        st.error("Go backend: Offline")

    st.divider()
    st.caption(f"Last update: {datetime.now().strftime('%H:%M:%S')}")


# ── Page Title ────────────────────────────────────────────────────────────────
st.title("🏙️ Smart City IoT Simulation Platform")
st.caption("Live data from Python simulators → Kafka → Go backend → Dashboard")

tabs = st.tabs(["📈 Live Overview", "🚦 Traffic", "🌫️ Air Quality", "🌦️ Weather", "🚨 Alerts", "📊 Correlation"])


# ── Tab 1: Live Overview ──────────────────────────────────────────────────────
with tabs[0]:
    traffic_data = fetch("/traffic/latest")
    aq_data      = fetch("/air/latest")
    weather_data = fetch("/weather/latest")
    alert_data   = fetch("/alerts?limit=5")

    col1, col2, col3, col4 = st.columns(4)

    if traffic_data and traffic_data.get("data"):
        df_t = pd.DataFrame(traffic_data["data"])
        avg_cong = df_t["avg_congestion"].mean()
        with col1:
            st.metric(
                "🚦 Avg Congestion",
                f"{avg_cong:.2f}",
                delta=f"{(avg_cong - 0.5):.2f} vs baseline",
                delta_color="inverse",
            )
    else:
        col1.metric("🚦 Avg Congestion", "—")

    if aq_data and aq_data.get("data"):
        df_aq = pd.DataFrame(aq_data["data"])
        avg_aqi = int(df_aq["aqi"].mean())
        category = df_aq["aqi_category"].mode()[0] if len(df_aq) > 0 else "—"
        with col2:
            st.metric("🌫️ Avg AQI", f"{aqi_color(avg_aqi)} {avg_aqi}", delta=category)
    else:
        col2.metric("🌫️ Avg AQI", "—")

    if weather_data and weather_data.get("data"):
        df_w = pd.DataFrame(weather_data["data"])
        avg_temp = df_w["temperature_c"].mean()
        condition = df_w["condition"].mode()[0] if len(df_w) > 0 else "—"
        with col3:
            st.metric("🌡️ Temperature", f"{avg_temp:.1f}°C", delta=condition)
    else:
        col3.metric("🌡️ Temperature", "—")

    if alert_data and alert_data.get("data"):
        active_alerts = len(alert_data["data"])
        critical = sum(1 for a in alert_data["data"] if a["severity"] == "critical")
        with col4:
            st.metric("🚨 Recent Alerts", active_alerts, delta=f"{critical} critical" if critical else "All clear")
    else:
        col4.metric("🚨 Recent Alerts", "—")

    st.divider()

    # Zone congestion bar chart
    if traffic_data and traffic_data.get("data"):
        df_t = pd.DataFrame(traffic_data["data"])
        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("Zone Congestion Index")
            fig = px.bar(
                df_t.sort_values("avg_congestion", ascending=True),
                x="avg_congestion",
                y="zone",
                orientation="h",
                color="avg_congestion",
                color_continuous_scale=["#1D9E75", "#EF9F27", "#D85A30", "#E24B4A"],
                range_color=[0, 1],
                labels={"avg_congestion": "Congestion Index", "zone": "Zone"},
            )
            fig.update_layout(showlegend=False, height=300, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            if aq_data and aq_data.get("data"):
                st.subheader("AQI by Station")
                df_aq = pd.DataFrame(aq_data["data"])
                fig2 = px.bar(
                    df_aq.sort_values("aqi", ascending=True),
                    x="aqi",
                    y="station",
                    orientation="h",
                    color="aqi",
                    color_continuous_scale=["#1D9E75", "#EF9F27", "#D85A30", "#E24B4A"],
                    range_color=[0, 300],
                    labels={"aqi": "AQI", "station": "Station"},
                )
                fig2.update_layout(showlegend=False, height=300, margin=dict(l=0, r=0, t=10, b=0))
                st.plotly_chart(fig2, use_container_width=True)

    # Geo map
    if aq_data and aq_data.get("data"):
        st.subheader("🗺️ City Sensor Map")
        df_map = pd.DataFrame(aq_data["data"])
        if "lat" in df_map.columns and "lon" in df_map.columns:
            fig_map = px.scatter_mapbox(
                df_map,
                lat="lat",
                lon="lon",
                color="aqi",
                size="pm25",
                hover_name="station",
                hover_data={"aqi": True, "pm25": True, "aqi_category": True},
                color_continuous_scale=["green", "yellow", "orange", "red", "purple"],
                range_color=[0, 300],
                zoom=10,
                mapbox_style="carto-positron",
                title="Air Quality Index by Station",
            )
            fig_map.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_map, use_container_width=True)


# ── Tab 2: Traffic ────────────────────────────────────────────────────────────
with tabs[1]:
    st.subheader("🚦 Traffic Analysis")
    traffic_data = fetch("/traffic/latest")

    if traffic_data and traffic_data.get("data"):
        df = pd.DataFrame(traffic_data["data"])

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Zones", len(df))
        col2.metric("Total Vehicles", df["total_vehicles"].sum())
        col3.metric("Active Incidents", df["incident_count"].sum())

        # Speed vs Congestion scatter
        fig = px.scatter(
            df,
            x="avg_congestion",
            y="avg_speed",
            size="total_vehicles",
            color="zone",
            hover_name="zone",
            title="Speed vs Congestion by Zone",
            labels={"avg_congestion": "Congestion Index", "avg_speed": "Avg Speed (km/h)"},
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            df[["zone", "avg_congestion", "avg_speed", "total_vehicles", "incident_count", "updated_at"]],
            use_container_width=True,
        )
    else:
        st.info("Waiting for traffic data from the Go backend...")


# ── Tab 3: Air Quality ────────────────────────────────────────────────────────
with tabs[2]:
    st.subheader("🌫️ Air Quality Analysis")
    aq_data = fetch("/air/latest")

    if aq_data and aq_data.get("data"):
        df = pd.DataFrame(aq_data["data"])

        # AQI gauge-style chart
        fig = go.Figure()
        for _, row in df.iterrows():
            fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=row["aqi"],
                title={"text": row["station"][:15]},
                gauge={
                    "axis": {"range": [0, 300]},
                    "bar": {"color": "#7c5cbf"},
                    "steps": [
                        {"range": [0, 50],   "color": "#1D9E75"},
                        {"range": [50, 100], "color": "#9FE1CB"},
                        {"range": [100, 150],"color": "#EF9F27"},
                        {"range": [150, 200],"color": "#D85A30"},
                        {"range": [200, 300],"color": "#E24B4A"},
                    ],
                },
            ))

        fig.update_layout(
            grid={"rows": 2, "columns": 3, "pattern": "independent"},
            height=500,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Pollutant comparison
        poll_cols = ["pm25", "pm10", "co", "no2", "so2", "o3"]
        fig2 = px.bar(
            df.melt(id_vars=["station"], value_vars=poll_cols, var_name="pollutant", value_name="value"),
            x="station",
            y="value",
            color="pollutant",
            barmode="group",
            title="Pollutant Levels by Station",
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Waiting for air quality data...")


# ── Tab 4: Weather ────────────────────────────────────────────────────────────
with tabs[3]:
    st.subheader("🌦️ Weather Conditions")
    weather_data = fetch("/weather/latest")

    if weather_data and weather_data.get("data"):
        df = pd.DataFrame(weather_data["data"])

        col1, col2 = st.columns(2)
        with col1:
            fig = px.scatter(
                df,
                x="humidity_pct",
                y="temperature_c",
                color="condition",
                size="wind_speed_ms",
                hover_name="station",
                title="Temperature vs Humidity",
                labels={"humidity_pct": "Humidity (%)", "temperature_c": "Temperature (°C)"},
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig2 = px.bar_polar(
                df,
                r="wind_speed_ms",
                theta="wind_direction_deg",
                color="condition",
                title="Wind Speed & Direction",
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.dataframe(
            df[["station", "temperature_c", "feels_like_c", "humidity_pct",
                "wind_speed_ms", "precipitation_mm", "visibility_km", "condition"]],
            use_container_width=True,
        )
    else:
        st.info("Waiting for weather data...")


# ── Tab 5: Alerts ─────────────────────────────────────────────────────────────
with tabs[4]:
    st.subheader("🚨 Real-Time Alert Feed")

    severity_filter = st.selectbox("Filter by severity", ["all", "critical", "high", "medium", "low"])
    endpoint = f"/alerts?limit=50" if severity_filter == "all" else f"/alerts?limit=50&severity={severity_filter}"
    alert_data = fetch(endpoint)

    if alert_data and alert_data.get("data"):
        SEVERITY_EMOJI = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}
        for alert in alert_data["data"]:
            sev = alert.get("severity", "low")
            emoji = SEVERITY_EMOJI.get(sev, "⚪")
            with st.expander(f"{emoji} [{sev.upper()}] {alert.get('alert_type','?')} — {alert.get('zone','?')}"):
                col1, col2, col3 = st.columns(3)
                col1.write(f"**Message:** {alert.get('message', '—')}")
                col2.write(f"**Value:** {alert.get('value', '—'):.2f}")
                col3.write(f"**Threshold:** {alert.get('threshold', '—')}")
                st.caption(f"🕐 {alert.get('timestamp', '—')}")
    else:
        st.info("No alerts to display.")


# ── Tab 6: Correlation ────────────────────────────────────────────────────────
with tabs[5]:
    st.subheader("📊 Cross-Stream Correlation Analysis")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Correlation Statistics")
        stats = fetch("/correlation/stats")
        if stats:
            st.metric("Traffic vs AQI",   f"{stats.get('traffic_vs_aqi', 0):.3f}")
            st.metric("Traffic vs PM2.5", f"{stats.get('traffic_vs_pm25', 0):.3f}")
            st.metric("Wind vs AQI",      f"{stats.get('wind_vs_aqi', 0):.3f}")
            st.caption("Pearson correlation coefficient: −1 to +1")
        else:
            st.info("Computing correlation stats...")

    with col2:
        corr_data = fetch("/correlation?limit=200")
        if corr_data and corr_data.get("data"):
            df_corr = pd.DataFrame(corr_data["data"])
            if len(df_corr) > 0:
                fig = px.scatter(
                    df_corr,
                    x="congestion_index",
                    y="aqi",
                    color="zone",
                    size="pm25",
                    hover_data=["wind_speed_ms", "condition"],
                    title="Congestion Index vs AQI",
                    labels={"congestion_index": "Congestion Index", "aqi": "AQI"},
                    trendline="ols",
                )
                st.plotly_chart(fig, use_container_width=True)

    # Heatmap
    if corr_data and corr_data.get("data") and len(corr_data["data"]) > 5:
        df_h = pd.DataFrame(corr_data["data"])[["congestion_index", "aqi", "pm25", "wind_speed_ms"]]
        corr_matrix = df_h.corr()
        fig_heat = px.imshow(
            corr_matrix,
            text_auto=True,
            color_continuous_scale="RdBu",
            title="Correlation Heatmap",
            zmin=-1, zmax=1,
        )
        st.plotly_chart(fig_heat, use_container_width=True)


# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
