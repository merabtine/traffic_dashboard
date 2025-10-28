import os
import time
from datetime import timedelta
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Traffic Analytics", layout="wide")

# DB connection via SQLAlchemy (convenient)
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "Workshop01")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "theworldwidechampion")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}", pool_pre_ping=True)

# Sidebar
st.sidebar.title("Controls")
hours = st.sidebar.slider("Time window (hours)", 1, 24, 3)
with engine.connect() as conn:
    sensors_df = pd.read_sql("SELECT sensor_id, location_name FROM sensors ORDER BY sensor_id;", conn)
sensor_options = st.sidebar.multiselect("Sensors (empty = all)", options=sensors_df['sensor_id'].tolist(),
                                        format_func=lambda x: sensors_df.loc[sensors_df.sensor_id==x, 'location_name'].values[0])
refresh_sec = st.sidebar.number_input("Auto-refresh interval (seconds, 0 = off)", min_value=0, max_value=3600, value=0, step=5)
manual_refresh = st.sidebar.button("Refresh Now")

# auto-refresh
if 'last_run' not in st.session_state:
    st.session_state['last_run'] = 0
if refresh_sec > 0:
    if time.time() - st.session_state['last_run'] > refresh_sec:
        st.session_state['last_run'] = time.time()
        st.experimental_rerun()
if manual_refresh:
    st.session_state['last_run'] = time.time()
    st.experimental_rerun()

# helper to run SQL
def run_sql(query, params=None):
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)

# Flow intensity
sensor_filter = ""
params = {"hours": f"{hours} hours"}
if sensor_options:
    sensor_filter = "AND r.sensor_id = any(:sensors)"
    params["sensors"] = sensor_options

sql_flow = f"""
SELECT date_trunc('minute', record_time) AS minute,
       COUNT(*) AS vehicle_count
FROM raw_traffic_readings r
WHERE record_time >= NOW() - INTERVAL :hours
{sensor_filter}
GROUP BY 1
ORDER BY 1;
"""
flow_df = run_sql(sql_flow, params=params)

st.title("Real-Time Traffic Analytics Dashboard")
st.markdown(f"**Window:** last {hours} hours")

if not flow_df.empty:
    fig_flow = px.line(flow_df, x='minute', y='vehicle_count', title="Vehicles per minute (flow intensity)")
    st.plotly_chart(fig_flow, use_container_width=True)
else:
    st.info("No flow data in the selected window/sensors.")

# Percent slow per sensor
sql_pct_slow = """
WITH thresholds AS (
  SELECT sensor_id,
         CASE road_type
           WHEN 'Highway' THEN 60
           WHEN 'Urban' THEN 30
           WHEN 'Suburban' THEN 40
         END AS slow_threshold
  FROM sensors
)
SELECT s.sensor_id, s.location_name, s.road_type,
       COUNT(r.reading_id) AS total_reads,
       SUM(CASE WHEN r.speed < t.slow_threshold THEN 1 ELSE 0 END) AS slow_count,
       ROUND(100.0 * SUM(CASE WHEN r.speed < t.slow_threshold THEN 1 ELSE 0 END) / NULLIF(COUNT(r.reading_id),0),2) AS pct_slow
FROM sensors s
LEFT JOIN thresholds t ON t.sensor_id = s.sensor_id
LEFT JOIN raw_traffic_readings r ON r.sensor_id = s.sensor_id AND r.record_time >= NOW() - INTERVAL :hours
GROUP BY s.sensor_id, s.location_name, s.road_type, t.slow_threshold
ORDER BY pct_slow DESC;
"""
pct_slow_df = run_sql(sql_pct_slow, params={"hours": f"{hours} hours"})
if not pct_slow_df.empty:
    fig_bar = px.bar(pct_slow_df, x='location_name', y='pct_slow', hover_data=['road_type','total_reads','slow_count'], title="% slow readings per sensor")
    st.plotly_chart(fig_bar, use_container_width=True)
else:
    st.info("No slow-speed data for the selected window.")

# moving average speed per sensor (5-minute window)
sql_mavg = """
SELECT sensor_id, minute, moving_avg_speed
FROM (
  SELECT sensor_id, minute,
         ROUND(AVG(speed) OVER (PARTITION BY sensor_id ORDER BY minute ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),2) AS moving_avg_speed
  FROM (
    SELECT sensor_id, date_trunc('minute', record_time) AS minute, AVG(speed) AS speed
    FROM raw_traffic_readings
    WHERE record_time >= NOW() - INTERVAL :hours
    GROUP BY sensor_id, date_trunc('minute', record_time)
  ) AS per_minute
) t
ORDER BY sensor_id, minute;
"""
mavg_df = run_sql(sql_mavg, params={"hours": f"{hours} hours"})
st.subheader("Moving average speed (5-minute window)")
if not mavg_df.empty:
    sel_sensor = st.selectbox("Select sensor to view", options=sorted(mavg_df['sensor_id'].unique()),
                              format_func=lambda x: sensors_df.loc[sensors_df.sensor_id==x, 'location_name'].values[0])
    sel_df = mavg_df[mavg_df['sensor_id'] == sel_sensor]
    fig_mavg = px.line(sel_df, x='minute', y='moving_avg_speed', title=f"Sensor {sel_sensor} — 5-min moving avg speed")
    st.plotly_chart(fig_mavg, use_container_width=True)
else:
    st.info("No moving-average speed data.")

# anomalies (z-score)
sql_anom = """
WITH per_min AS (
  SELECT sensor_id, date_trunc('minute', record_time) AS minute, COUNT(*) AS cnt
  FROM raw_traffic_readings
  WHERE record_time >= NOW() - INTERVAL :hours2
  GROUP BY sensor_id, date_trunc('minute', record_time)
),
stats AS (
  SELECT sensor_id, AVG(cnt) AS mean_cnt, STDDEV_POP(cnt) AS sd_cnt
  FROM per_min
  GROUP BY sensor_id
)
SELECT p.sensor_id, s.location_name, p.minute, p.cnt,
       ROUND((p.cnt - st.mean_cnt) / NULLIF(st.sd_cnt,0),2) AS zscore
FROM per_min p
JOIN stats st ON st.sensor_id = p.sensor_id
JOIN sensors s ON s.sensor_id = p.sensor_id
WHERE st.sd_cnt IS NOT NULL AND ABS((p.cnt - st.mean_cnt) / st.sd_cnt) >= 2
ORDER BY ABS((p.cnt - st.mean_cnt) / st.sd_cnt) DESC
LIMIT 200;
"""
anom_df = run_sql(sql_anom, params={"hours2": f"{hours*2} hours"})
st.subheader("Anomalous minutes (zscore >= 2)")
if not anom_df.empty:
    st.table(anom_df)
else:
    st.info("No anomalies detected in the selected period.")

st.markdown("---")
st.markdown("**Tips:** peak flow + low avg speed = congestion. High pct_slow for single sensor = local incident.")
#xxx