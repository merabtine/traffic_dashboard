import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# ==============================
# DATABASE CONNECTION
# ==============================
@st.cache_data(ttl=60)
def run_sql(query, params=None):
    conn = psycopg2.connect(
        dbname="Traffic_Dash",
        user="neondb_owner",
        password="npg_VCtug9iSxDr5",
        host="ep-polished-meadow-adynfmd5-pooler.c-2.us-east-1.aws.neon.tech",
        port="5432",
        sslmode="require",
    )
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# ==============================
# STREAMLIT CONFIG
# ==============================
st.set_page_config(page_title="🚦 Real-Time Traffic Dashboard", layout="wide")
st.title("🚦 Real-Time Traffic Analytics Dashboard")
st.caption("Data extracted from 2SD04_WS01.sql (Neon Database)")

# Choix de la période d’analyse
hours = st.slider("Durée d'analyse (heures)", 1, 24, 6)

# ======================================================
# 1️⃣ Traffic Peaks and Flow Intensity
# ======================================================
st.header("1️⃣ Traffic Peaks and Flow Intensity")

query1 = """
WITH hourly_stats AS (
    SELECT 
        sensor_id,
        date_trunc('hour', record_time) AS hour_slot,
        COUNT(vehiicule_id) AS vehicle_count,
        AVG(speed) AS avg_speed
    FROM raw_traffic_readings
    WHERE record_time >= NOW() - INTERVAL %s
    GROUP BY sensor_id, date_trunc('hour', record_time)
)
SELECT 
    h.*,
    CASE 
        WHEN vehicle_count = MAX(vehicle_count) OVER (PARTITION BY sensor_id)
        THEN 'Peak'
        ELSE 'Normal'
    END AS traffic_status
FROM hourly_stats h
ORDER BY sensor_id, hour_slot;
"""
df1 = run_sql(query1, (f"{hours} hours",))
fig1 = px.bar(df1, x="hour_slot", y="vehicle_count", color="traffic_status",
              barmode="group", title="Traffic Peaks and Flow Intensity")
st.plotly_chart(fig1, use_container_width=True)

# ======================================================
# 2️⃣ Movement Efficiency & Slowdowns
# ======================================================
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
    se.location_name,
    s.time_slot,
    ROUND(s.avg_speed, 2) AS avg_speed,
    ROUND(b.normal_speed, 2) AS normal_speed,
    CASE 
        WHEN s.avg_speed < b.normal_speed - 1.5 * b.speed_std THEN 'Slowdown'
        WHEN s.avg_speed > b.normal_speed + 1.5 * b.speed_std THEN 'Free Flow'
        ELSE 'Normal'
    END AS traffic_status
FROM speed_stats s
JOIN sensor_baseline b USING(sensor_id)
JOIN sensors se USING(sensor_id)
ORDER BY s.sensor_id, s.time_slot;
"""
df2 = run_sql(query2, (f"{hours} hours",))
fig2 = px.histogram(df2, x="traffic_status", color="traffic_status",
                    title="Traffic Efficiency Status Distribution")
st.plotly_chart(fig2, use_container_width=True)

# ======================================================
# 3️⃣ Dynamic Traffic Evaluation
# ======================================================
st.header("3️⃣ Dynamic Traffic Conditions by Road Type")

query3 = """
WITH traffic_summary AS (
    SELECT 
        se.sensor_id,
        se.location_name,
        se.road_type,
        date_trunc('minute', r.record_time) AS time_slot,
        COUNT(r.vehiicule_id) AS vehicle_count,
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
    vehicle_count,
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
FROM traffic_summary
ORDER BY sensor_id, time_slot;
"""
df3 = run_sql(query3, (f"{hours} hours",))
fig3 = px.bar(df3, x="location_name", y="avg_speed", color="traffic_condition",
              title="Traffic Conditions by Road Type")
st.plotly_chart(fig3, use_container_width=True)

# ======================================================
# 4️⃣ Density Impact on Flow
# ======================================================
st.header("4️⃣ Density Impact on Flow")

query4 = """
WITH flow_density AS (
    SELECT 
        se.sensor_id,
        se.location_name,
        se.road_type,
        date_trunc('hour', r.record_time) AS hour_slot,
        COUNT(r.vehiicule_id) AS vehicle_count,
        AVG(r.speed) AS avg_speed
    FROM raw_traffic_readings r
    JOIN sensors se ON r.sensor_id = se.sensor_id
    WHERE record_time >= NOW() - INTERVAL %s
    GROUP BY se.sensor_id, se.location_name, se.road_type, date_trunc('hour', r.record_time)
)
SELECT 
    sensor_id,
    location_name,
    road_type,
    hour_slot,
    vehicle_count,
    ROUND(avg_speed, 2) AS avg_speed,
    CASE 
        WHEN vehicle_count > 80 AND avg_speed < 30 THEN 'High density - Low flow (Congestion)'
        WHEN vehicle_count BETWEEN 40 AND 80 AND avg_speed BETWEEN 30 AND 60 THEN 'Medium density - Stable flow'
        WHEN vehicle_count < 40 AND avg_speed > 60 THEN 'Low density - Free flow'
        ELSE 'Transitional state'
    END AS density_impact
FROM flow_density
ORDER BY sensor_id, hour_slot;
"""
df4 = run_sql(query4, (f"{hours} hours",))
fig4 = px.scatter(df4, x="vehicle_count", y="avg_speed", color="density_impact",
                  title="Density Impact on Traffic Flow")
st.plotly_chart(fig4, use_container_width=True)

# ======================================================
# 5️⃣ Daily Evolution
# ======================================================
st.header("5️⃣ Traffic Evolution Throughout the Day")

query5 = """
WITH hourly_speed AS (
    SELECT 
        se.sensor_id,
        se.location_name,
        se.road_type,
        date_trunc('hour', r.record_time) AS hour_slot,
        AVG(r.speed) AS avg_speed
    FROM raw_traffic_readings r
    JOIN sensors se ON r.sensor_id = se.sensor_id
    WHERE record_time >= NOW() - INTERVAL %s
    GROUP BY se.sensor_id, se.location_name, se.road_type, date_trunc('hour', r.record_time)
)
SELECT 
    sensor_id,
    location_name,
    road_type,
    hour_slot,
    ROUND(avg_speed, 2) AS avg_speed,
    LAG(avg_speed) OVER (PARTITION BY sensor_id ORDER BY hour_slot) AS previous_hour_speed,
    CASE 
        WHEN avg_speed > LAG(avg_speed) OVER (PARTITION BY sensor_id ORDER BY hour_slot) THEN 'Improving'
        WHEN avg_speed < LAG(avg_speed) OVER (PARTITION BY sensor_id ORDER BY hour_slot) THEN 'Deteriorating'
        ELSE 'Stable'
    END AS traffic_trend
FROM hourly_speed
ORDER BY sensor_id, hour_slot;
"""
df5 = run_sql(query5, (f"{hours} hours",))
fig5 = px.line(df5, x="hour_slot", y="avg_speed", color="traffic_trend",
               title="Traffic Evolution Throughout the Day")
st.plotly_chart(fig5, use_container_width=True)

# ======================================================
# 6️⃣ Incident Detection
# ======================================================
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
    se.location_name,
    s.time_slot,
    ROUND(s.avg_speed, 2) AS avg_speed,
    ROUND(b.normal_speed, 2) AS normal_speed,
    CASE 
        WHEN s.avg_speed < b.normal_speed - 2 * b.speed_std THEN 'Possible Incident - Sudden slowdown'
        WHEN s.avg_speed > b.normal_speed + 2 * b.speed_std THEN 'Unusual acceleration'
        ELSE 'Normal'
    END AS anomaly_flag
FROM speed_stats s
JOIN sensor_baseline b USING(sensor_id)
JOIN sensors se USING(sensor_id)
ORDER BY s.sensor_id, s.time_slot;
"""
df6 = run_sql(query6, (f"{hours} hours",))
fig6 = px.histogram(df6, x="anomaly_flag", color="anomaly_flag",
                    title="Detected Traffic Anomalies")
st.plotly_chart(fig6, use_container_width=True)

# ======================================================
# 7️⃣ Comparison by Road Type
# ======================================================
st.header("7️⃣ Compare Average Speed Across Road Types")

query7 = """
SELECT 
    road_type,
    date_trunc('hour', record_time) AS hour_slot,
    ROUND(AVG(speed), 2) AS avg_speed,
    COUNT(vehiicule_id) AS vehicle_count
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

st.success("✅ Dashboard connected successfully to Neon Database")
