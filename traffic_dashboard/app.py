import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta

# ==============================
# DATABASE CONNECTION
# ==============================
def get_connection():
    return psycopg2.connect(
        dbname="traffic_db",
        user="postgres",
        password="732019",   # 🔁 adapte selon ton mot de passe
        host="localhost",
        port="5432"
    )

def run_sql(query, params=None):
    conn = get_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# ==============================
# STREAMLIT DASHBOARD
# ==============================
st.set_page_config(page_title="Traffic Analysis Dashboard", layout="wide")
st.title("🚦 Traffic Monitoring Dashboard")
st.caption("Données issues du fichier SQL : 2SD04_WS01.sql")

# Choix de la période d’analyse
hours = st.slider("Durée d'analyse (heures)", 1, 24, 6)

# ==============================
# 1️⃣ Traffic Peaks & Flow Intensity
# ==============================
st.header("1️⃣ Traffic Peaks and Flow Intensity")

query1 = """
SELECT 
    sensor_id,
    date_trunc('hour', record_time) AS hour_slot,
    COUNT(vehiicule_id) AS vehicle_count,
    AVG(speed) AS avg_speed
FROM raw_traffic_readings
WHERE record_time >= NOW() - INTERVAL %s
GROUP BY sensor_id, date_trunc('hour', record_time)
ORDER BY sensor_id, hour_slot;
"""
df1 = run_sql(query1, (f"{hours} hours",))
fig1 = px.line(df1, x="hour_slot", y="vehicle_count", color="sensor_id",
               title="Vehicle Count per Hour by Sensor")
st.plotly_chart(fig1, use_container_width=True)

# ==============================
# 2️⃣ Movement Efficiency & Slowdowns
# ==============================
st.header("2️⃣ Movement Efficiency & Possible Slowdowns")

query2 = """
WITH speed_stats AS (
    SELECT 
        sensor_id,
        date_trunc('minute', record_time) AS time_slot,
        AVG(speed) AS avg_speed
    FROM raw_traffic_readings
    WHERE record_time >= NOW() - INTERVAL %s
    GROUP BY sensor_id, date_trunc('minute', record_time)
),
sensor_baseline AS (
    SELECT 
        sensor_id,
        AVG(avg_speed) AS normal_speed,
        STDDEV(avg_speed) AS speed_std
    FROM speed_stats
    GROUP BY sensor_id
)
SELECT 
    s.sensor_id,
    ROUND(s.avg_speed, 2) AS avg_speed,
    ROUND(b.normal_speed, 2) AS normal_speed,
    CASE 
        WHEN s.avg_speed < b.normal_speed - 1.5 * b.speed_std THEN 'Slowdown'
        WHEN s.avg_speed > b.normal_speed + 1.5 * b.speed_std THEN 'Free Flow'
        ELSE 'Normal'
    END AS status
FROM speed_stats s
JOIN sensor_baseline b USING(sensor_id)
ORDER BY s.sensor_id, s.time_slot;
"""
df2 = run_sql(query2, (f"{hours} hours",))
fig2 = px.histogram(df2, x="status", color="status", title="Traffic Status Distribution")
st.plotly_chart(fig2, use_container_width=True)

# ==============================
# 3️⃣ Dynamic Traffic Evaluation
# ==============================
st.header("3️⃣ Dynamic Traffic Conditions by Road Type")

query3 = """
WITH traffic_summary AS (
    SELECT 
        se.sensor_id,
        se.location_name,
        se.road_type,
        date_trunc('minute', r.record_time) AS time_slot,
        AVG(r.speed) AS avg_speed
    FROM raw_traffic_readings r
    JOIN sensors se ON r.sensor_id = se.sensor_id
    WHERE record_time >= NOW() - INTERVAL %s
    GROUP BY se.sensor_id, se.location_name, se.road_type, date_trunc('minute', r.record_time)
)
SELECT
    sensor_id,
    location_name,
    road_type,
    time_slot,
    ROUND(avg_speed, 2) AS avg_speed,
    CASE 
        WHEN road_type = 'Highway' AND avg_speed >= 80 THEN 'Free Flow'
        WHEN road_type = 'Highway' AND avg_speed >= 50 THEN 'Moderate'
        WHEN road_type = 'Highway' THEN 'Congested'
        WHEN road_type = 'Urban' AND avg_speed >= 40 THEN 'Free Flow'
        WHEN road_type = 'Urban' AND avg_speed >= 25 THEN 'Moderate'
        WHEN road_type = 'Urban' THEN 'Congested'
        WHEN road_type = 'Suburban' AND avg_speed >= 60 THEN 'Free Flow'
        WHEN road_type = 'Suburban' AND avg_speed >= 35 THEN 'Moderate'
        ELSE 'Congested'
    END AS traffic_condition
FROM traffic_summary;
"""
df3 = run_sql(query3, (f"{hours} hours",))
fig3 = px.bar(df3, x="location_name", color="traffic_condition", title="Traffic Conditions by Sensor")
st.plotly_chart(fig3, use_container_width=True)

# ==============================
# 4️⃣ Density Impact on Flow
# ==============================
st.header("4️⃣ Density Impact on Flow")

query4 = """
WITH flow_density AS (
    SELECT 
        s.sensor_id,
        se.location_name,
        se.road_type,
        date_trunc('hour', r.record_time) AS hour_slot,
        COUNT(r.vehiicule_id) AS vehicle_count,
        AVG(r.speed) AS avg_speed
    FROM raw_traffic_readings r
    JOIN sensors se ON r.sensor_id = se.sensor_id
    WHERE record_time >= NOW() - INTERVAL %s
    GROUP BY s.sensor_id, se.location_name, se.road_type, date_trunc('hour', r.record_time)
)
SELECT 
    sensor_id,
    location_name,
    road_type,
    hour_slot,
    vehicle_count,
    ROUND(avg_speed, 2) AS avg_speed
FROM flow_density
ORDER BY sensor_id, hour_slot;
"""
df4 = run_sql(query4, (f"{hours} hours",))
fig4 = px.scatter(df4, x="vehicle_count", y="avg_speed", color="road_type",
                  title="Vehicle Density vs Average Speed")
st.plotly_chart(fig4, use_container_width=True)

# ==============================
# 5️⃣ Daily Evolution
# ==============================
st.header("5️⃣ Traffic Evolution Throughout the Day")

query5 = """
SELECT 
    date_trunc('hour', record_time) AS hour_slot,
    AVG(speed) AS avg_speed
FROM raw_traffic_readings
WHERE record_time >= NOW() - INTERVAL %s
GROUP BY date_trunc('hour', record_time)
ORDER BY hour_slot;
"""
df5 = run_sql(query5, (f"{hours} hours",))
fig5 = px.line(df5, x="hour_slot", y="avg_speed", title="Average Speed Evolution")
st.plotly_chart(fig5, use_container_width=True)

# ==============================
# 6️⃣ Irregular Patterns / Incidents
# ==============================
st.header("6️⃣ Incident Detection (Speed Anomalies)")

query6 = """
WITH speed_stats AS (
    SELECT 
        sensor_id,
        date_trunc('minute', record_time) AS time_slot,
        AVG(speed) AS avg_speed
    FROM raw_traffic_readings
    WHERE record_time >= NOW() - INTERVAL %s
    GROUP BY sensor_id, date_trunc('minute', record_time)
),
sensor_baseline AS (
    SELECT 
        sensor_id,
        AVG(avg_speed) AS normal_speed,
        STDDEV(avg_speed) AS speed_std
    FROM speed_stats
    GROUP BY sensor_id
)
SELECT 
    s.sensor_id,
    ROUND(s.avg_speed, 2) AS avg_speed,
    ROUND(b.normal_speed, 2) AS normal_speed,
    CASE 
        WHEN s.avg_speed < b.normal_speed - 2 * b.speed_std THEN 'Possible Incident - Sudden slowdown'
        WHEN s.avg_speed > b.normal_speed + 2 * b.speed_std THEN 'Unusual acceleration'
        ELSE 'Normal'
    END AS anomaly_flag
FROM speed_stats s
JOIN sensor_baseline b USING(sensor_id)
ORDER BY s.sensor_id, s.time_slot;
"""
df6 = run_sql(query6, (f"{hours} hours",))
fig6 = px.histogram(df6, x="anomaly_flag", color="anomaly_flag", title="Detected Anomalies")
st.plotly_chart(fig6, use_container_width=True)

# ==============================
# 7️⃣ Compare Road Types by Time of Day
# ==============================
st.header("7️⃣ Compare Average Speed Across Road Types")

query7 = """
SELECT 
    road_type,
    date_trunc('hour', record_time) AS hour_slot,
    ROUND(AVG(speed), 2) AS avg_speed
FROM raw_traffic_readings r
JOIN sensors s ON r.sensor_id = s.sensor_id
WHERE record_time >= NOW() - INTERVAL %s
GROUP BY road_type, date_trunc('hour', record_time)
ORDER BY road_type, hour_slot;
"""
df7 = run_sql(query7, (f"{hours} hours",))
fig7 = px.line(df7, x="hour_slot", y="avg_speed", color="road_type",
               title="Average Speed per Road Type")
st.plotly_chart(fig7, use_container_width=True)

st.success("✅ Dashboard loaded successfully with data from 2SD04_WS01.sql")
