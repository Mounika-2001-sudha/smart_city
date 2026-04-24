"""
Smart City IoT Analytics Engine
Standalone analytics: pattern detection, anomaly detection,
rush hour analysis, and correlation reports.
Run: python analytics_engine.py
"""

import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from scipy import stats
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "postgresql://smartcity:smartcity@localhost:5432/smartcity")

sns.set_theme(style="darkgrid", palette="muted")
plt.rcParams["figure.figsize"] = (14, 6)
plt.rcParams["figure.dpi"] = 120


def get_engine():
    return create_engine(DB_URL)


# ─── Data Loaders ─────────────────────────────────────────────────────────────

def load_traffic(hours: int = 24) -> pd.DataFrame:
    engine = get_engine()
    q = text("""
        SELECT zone, timestamp, vehicle_count, avg_speed_kmh, congestion_index, incident_flag
        FROM traffic_data
        WHERE timestamp > NOW() - INTERVAL ':hours hours'
        ORDER BY timestamp
    """.replace(":hours", str(hours)))
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def load_air_quality(hours: int = 24) -> pd.DataFrame:
    engine = get_engine()
    q = text(f"""
        SELECT station, timestamp, pm25, pm10, co, aqi, aqi_category, wind_speed_ms, humidity_pct
        FROM air_quality_data
        WHERE timestamp > NOW() - INTERVAL '{hours} hours'
        ORDER BY timestamp
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def load_alerts(days: int = 7) -> pd.DataFrame:
    engine = get_engine()
    q = text(f"""
        SELECT alert_type, severity, zone, message, value, threshold, timestamp
        FROM alerts_log
        WHERE timestamp > NOW() - INTERVAL '{days} days'
        ORDER BY timestamp DESC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


# ─── Rush Hour Detection ───────────────────────────────────────────────────────

def analyze_rush_hours(df_traffic: pd.DataFrame):
    """Detect rush hours from congestion patterns."""
    print("\n=== Rush Hour Analysis ===")

    df = df_traffic.copy()
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek

    # Average congestion by hour
    hourly = df.groupby("hour")["congestion_index"].agg(["mean", "std", "count"]).reset_index()
    hourly.columns = ["hour", "mean_congestion", "std_congestion", "count"]

    # Detect rush hours: > mean + 0.5*std
    threshold = hourly["mean_congestion"].mean() + 0.5 * hourly["mean_congestion"].std()
    rush_hours = hourly[hourly["mean_congestion"] > threshold]["hour"].tolist()
    print(f"Rush hours detected: {rush_hours}")

    # Plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 5))

    ax1.plot(hourly["hour"], hourly["mean_congestion"], "o-", color="#7c5cbf", linewidth=2)
    ax1.fill_between(
        hourly["hour"],
        hourly["mean_congestion"] - hourly["std_congestion"],
        hourly["mean_congestion"] + hourly["std_congestion"],
        alpha=0.2, color="#7c5cbf",
    )
    ax1.axhline(threshold, color="#E24B4A", linestyle="--", label=f"Rush threshold ({threshold:.2f})")
    ax1.set_xlabel("Hour of Day")
    ax1.set_ylabel("Average Congestion Index")
    ax1.set_title("Congestion by Hour of Day")
    ax1.legend()
    ax1.set_xticks(range(0, 24, 2))

    # Heatmap: zone × hour
    if "zone" in df.columns:
        pivot = df.pivot_table(values="congestion_index", index="zone", columns="hour", aggfunc="mean")
        sns.heatmap(pivot, cmap="YlOrRd", ax=ax2, cbar_kws={"label": "Congestion Index"})
        ax2.set_title("Congestion Heatmap: Zone × Hour")

    plt.tight_layout()
    plt.savefig("rush_hour_analysis.png", bbox_inches="tight")
    plt.close()
    print("Saved: rush_hour_analysis.png")
    return rush_hours


# ─── Pollution Spike Detection ─────────────────────────────────────────────────

def detect_pollution_spikes(df_aq: pd.DataFrame):
    """Detect statistically significant AQI spikes using Z-score method."""
    print("\n=== Pollution Spike Detection ===")

    df = df_aq.copy()
    df["z_score"] = stats.zscore(df["aqi"].dropna())
    spikes = df[df["z_score"].abs() > 2.5]
    print(f"Detected {len(spikes)} AQI spikes (|Z| > 2.5)")

    if len(spikes) > 0:
        print(spikes[["station", "timestamp", "aqi", "aqi_category", "z_score"]].head(10).to_string())

    # Plot AQI time series with spikes highlighted
    fig, ax = plt.subplots(figsize=(16, 5))
    for station, grp in df.groupby("station"):
        ax.plot(grp["timestamp"], grp["aqi"], alpha=0.5, linewidth=0.8, label=station)

    if len(spikes) > 0:
        ax.scatter(spikes["timestamp"], spikes["aqi"], color="#E24B4A", s=80, zorder=5, label="Spike")

    ax.axhline(150, color="#EF9F27", linestyle="--", alpha=0.7, label="Unhealthy threshold (150)")
    ax.axhline(200, color="#E24B4A", linestyle="--", alpha=0.7, label="Very Unhealthy (200)")
    ax.set_xlabel("Time")
    ax.set_ylabel("AQI")
    ax.set_title("AQI Time Series with Spike Detection")
    ax.legend(fontsize=7, ncol=4)
    plt.tight_layout()
    plt.savefig("aqi_spikes.png", bbox_inches="tight")
    plt.close()
    print("Saved: aqi_spikes.png")
    return spikes


# ─── Cross-stream Correlation ──────────────────────────────────────────────────

def compute_correlations(df_traffic: pd.DataFrame, df_aq: pd.DataFrame):
    """Compute correlation between traffic congestion and air quality metrics."""
    print("\n=== Cross-Stream Correlation Analysis ===")

    # Bucket both to 5-minute intervals
    df_t = df_traffic.copy()
    df_a = df_aq.copy()
    df_t["bucket"] = df_t["timestamp"].dt.floor("5min")
    df_a["bucket"] = df_a["timestamp"].dt.floor("5min")

    t_agg = df_t.groupby("bucket")[["congestion_index", "vehicle_count", "avg_speed_kmh"]].mean()
    a_agg = df_a.groupby("bucket")[["aqi", "pm25", "pm10", "co", "wind_speed_ms"]].mean()

    merged = pd.merge(t_agg, a_agg, left_index=True, right_index=True, how="inner")
    print(f"Merged {len(merged)} 5-min buckets for correlation analysis")

    if len(merged) < 10:
        print("Not enough data for correlation analysis yet.")
        return None

    corr = merged.corr()

    # Focus columns
    focus = ["congestion_index", "vehicle_count", "avg_speed_kmh", "aqi", "pm25", "pm10", "wind_speed_ms"]
    corr_focus = corr.loc[[c for c in focus if c in corr.columns], [c for c in focus if c in corr.columns]]

    print("\nCorrelation matrix:")
    print(corr_focus.round(3))

    fig, ax = plt.subplots(figsize=(10, 8))
    mask = np.triu(np.ones_like(corr_focus, dtype=bool))
    sns.heatmap(
        corr_focus,
        annot=True,
        fmt=".2f",
        cmap="RdBu_r",
        vmin=-1, vmax=1,
        mask=mask,
        ax=ax,
        linewidths=0.5,
    )
    ax.set_title("Cross-Stream Correlation Heatmap\n(Traffic × Air Quality)")
    plt.tight_layout()
    plt.savefig("correlation_heatmap.png", bbox_inches="tight")
    plt.close()
    print("Saved: correlation_heatmap.png")
    return corr_focus


# ─── Alert Summary ─────────────────────────────────────────────────────────────

def summarize_alerts(df_alerts: pd.DataFrame):
    """Generate alert summary statistics."""
    print("\n=== Alert Summary ===")

    if df_alerts.empty:
        print("No alerts in the selected time range.")
        return

    print(f"\nTotal alerts: {len(df_alerts)}")
    print("\nBy severity:")
    print(df_alerts["severity"].value_counts().to_string())
    print("\nBy type:")
    print(df_alerts["alert_type"].value_counts().to_string())
    print("\nBy zone:")
    print(df_alerts["zone"].value_counts().head(10).to_string())

    # Alert timeline
    fig, axes = plt.subplots(1, 2, figsize=(16, 5))

    sev_counts = df_alerts["severity"].value_counts()
    colors_sev = {"critical": "#E24B4A", "high": "#EF9F27", "medium": "#378ADD", "low": "#1D9E75"}
    bar_colors = [colors_sev.get(s, "#888") for s in sev_counts.index]
    axes[0].bar(sev_counts.index, sev_counts.values, color=bar_colors)
    axes[0].set_title("Alerts by Severity")
    axes[0].set_xlabel("Severity")
    axes[0].set_ylabel("Count")

    df_alerts["hour"] = df_alerts["timestamp"].dt.hour
    hourly_alerts = df_alerts.groupby(["hour", "severity"]).size().unstack(fill_value=0)
    hourly_alerts.plot(kind="bar", stacked=True, ax=axes[1],
                       color=[colors_sev.get(c, "#888") for c in hourly_alerts.columns])
    axes[1].set_title("Alert Frequency by Hour")
    axes[1].set_xlabel("Hour of Day")
    axes[1].set_ylabel("Alert Count")
    axes[1].legend(title="Severity")

    plt.tight_layout()
    plt.savefig("alert_summary.png", bbox_inches="tight")
    plt.close()
    print("Saved: alert_summary.png")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Smart City IoT Analytics Engine")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        df_traffic = load_traffic(hours=24)
        df_aq = load_air_quality(hours=24)
        df_alerts = load_alerts(days=7)

        print(f"Loaded: {len(df_traffic)} traffic rows, {len(df_aq)} AQ rows, {len(df_alerts)} alerts")

        if not df_traffic.empty:
            analyze_rush_hours(df_traffic)

        if not df_aq.empty:
            detect_pollution_spikes(df_aq)

        if not df_traffic.empty and not df_aq.empty:
            compute_correlations(df_traffic, df_aq)

        if not df_alerts.empty:
            summarize_alerts(df_alerts)

        print("\n✅ Analytics complete. Outputs saved as PNG files.")

    except Exception as e:
        print(f"\n❌ Analytics error: {e}")
        print("Make sure the database is running and has data.")
        print("You can run simulators first to generate data.")


if __name__ == "__main__":
    main()
