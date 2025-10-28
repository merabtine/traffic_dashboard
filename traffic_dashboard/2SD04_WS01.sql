CREATE TABLE sensors(
sensor_id SERIAL primary key,
location_name text not null,
latitude numeric(9,6),
longitude numeric(9,6),
road_type text check(road_type in ('Highway', 'Urban', 'Suburban'))
);
CREATE TABLE raw_traffic_readings(
reading_id SERIAL primary key,
sensor_id int references sensors(sensor_id),
record_time timestamp not null,
vehiicule_id text,
speed numeric (5,2) check (speed >= 0)
);
insert into sensors(location_name,latitude,longitude,road_type)
values('Didouche Mourad Street', 36.7538, 3.0588,'Urban'),
('Boulevard Mohamed Khemisti', 36.7520, 3.0420,'Urban'),
('Corniche El-Biar', 36.7800, 3.0500,'Urban'),
('Autoroute Est-Ouest', 36.7400, 3.0800,'Highway'),
('Boulevard Zighout Youcef', 36.7600, 3.0600,'Suburban');

select setseed(0.24);

INSERT INTO raw_traffic_readings(sensor_id, record_time, vehiicule_id, speed)
SELECT 
    s.sensor_id,
    now() - (INTERVAL '1 minute' * gs) - (INTERVAL '1 second' * floor(random() * 60)),
    'V' || (floor(1000 + random() * 9000)::int),
    CASE s.road_type 
        WHEN 'Highway' THEN 80 + (random() * 40) --80-120 km/h
        WHEN 'Urban' THEN 20 + (random() * 40)--20-60 km/h
        WHEN 'Suburban' THEN 30 + (random() * 50) --30-80 km/h
    END
FROM generate_series(0, 300) AS gs
CROSS JOIN sensors s;


-------------------------------------------
------1-------
-- Identifies traffic peaks and flow intensity by counting vehicles and averaging speed per sensor
-- each hour, revealing high-congestion periods or calm flow trends.
SELECT 
    sensor_id,
    date_trunc('hour', record_time) AS hour_slot,
    COUNT(vehiicule_id) AS vehicle_count,
    AVG(speed) AS avg_speed
FROM raw_traffic_readings
GROUP BY sensor_id, date_trunc('hour', record_time)
ORDER BY sensor_id, hour_slot;


---2-----
-- Evaluates movement efficiency by comparing average speeds to normal baselines,
-- detecting slowdowns or free-flow periods that reflect traffic stability or congestion.
WITH speed_stats AS (
    SELECT 
        sensor_id,
        date_trunc('minute', record_time) AS time_slot,
        AVG(speed) AS avg_speed
    FROM raw_traffic_readings
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
        WHEN s.avg_speed > b.normal_speed + 1.5 * b.speed_std THEN 'Free flow'
        ELSE 'Normal'
    END AS traffic_status
FROM speed_stats s
JOIN sensor_baseline b USING(sensor_id)
JOIN sensors se USING(sensor_id)
ORDER BY s.sensor_id, s.time_slot;


---3
-- Dynamically evaluates traffic conditions by classifying real-time average speeds 
-- into congestion levels, showing evolving flow states across road types.
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

----4
-- Explores how vehicle density influences traffic flow by comparing hourly counts and average speeds, revealing when high density leads to congestion or efficient flow.
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

----5
-- Tracks traffic trends throughout the day by comparing average speeds per hour
-- with the previous hour, identifying periods of improvement or deterioration.
WITH hourly_speed AS (
    SELECT 
        se.sensor_id,
        se.location_name,
        se.road_type,
        date_trunc('hour', r.record_time) AS hour_slot,
        AVG(r.speed) AS avg_speed
    FROM raw_traffic_readings r
    JOIN sensors se ON r.sensor_id = se.sensor_id
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

---6
-- Detects unusual traffic patterns by flagging readings with speeds significantly lower
-- or higher than the normal baseline for each sensor, highlighting possible incidents.
WITH speed_stats AS (
    SELECT 
        sensor_id,
        date_trunc('minute', record_time) AS time_slot,
        AVG(speed) AS avg_speed
    FROM raw_traffic_readings
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

---7
-- Compares average speeds across road types by hour,
--highlighting differences in traffic flow patterns during the day.
SELECT 
    road_type,
    date_trunc('hour', record_time) AS hour_slot,
    ROUND(AVG(speed), 2) AS avg_speed,
    COUNT(vehiicule_id) AS vehicle_count
FROM raw_traffic_readings r
JOIN sensors s ON r.sensor_id = s.sensor_id
GROUP BY road_type, date_trunc('hour', record_time)
ORDER BY road_type, hour_slot;
