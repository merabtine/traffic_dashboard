-- Workshop #01: Real-Time Traffic Data Analytics Dashboard

DROP TABLE IF EXISTS raw_traffic_readings;
DROP TABLE IF EXISTS sensors;

CREATE TABLE sensors (
  sensor_id SERIAL PRIMARY KEY,
  location_name TEXT NOT NULL,
  latitude NUMERIC(9,6),
  longitude NUMERIC(9,6),
  road_type TEXT CHECK (road_type IN ('Highway', 'Urban', 'Suburban'))
);

CREATE TABLE raw_traffic_readings (
  reading_id SERIAL PRIMARY KEY,
  sensor_id INT REFERENCES sensors(sensor_id),
  record_time TIMESTAMP NOT NULL,
  vehicle_id TEXT,
  speed NUMERIC(5,2) CHECK (speed >= 0)
);


INSERT INTO sensors (location_name, latitude, longitude, road_type) VALUES
('Didouche Mourad Street', 36.7538, 3.0588, 'Urban'),
('Boulevard Mohamed Khemisti', 36.7520, 3.0420, 'Urban'),
('Corniche El-Biar', 36.7800, 3.0500, 'Urban'),
('Autoroute Est-Ouest', 36.7400, 3.0800, 'Highway'),
('Boulevard Zighout Youcef', 36.7600, 3.0600, 'Suburban');

INSERT INTO raw_traffic_readings (sensor_id, record_time, vehicle_id, speed)
SELECT
  s.sensor_id,
  NOW() - (INTERVAL '1 minute' * gs) - (INTERVAL '1 second' * FLOOR(RANDOM() * 60)),
  'V' || (FLOOR(1000 + RANDOM() * 9000)::int),
  CASE s.road_type
    WHEN 'Highway' THEN ROUND((80 + RANDOM() * 40)::numeric, 2)      -- 80–120 km/h
    WHEN 'Urban'   THEN ROUND((20 + RANDOM() * 40)::numeric, 2)      -- 20–60 km/h
    WHEN 'Suburban' THEN ROUND((30 + RANDOM() * 50)::numeric, 2)     -- 30–80 km/h
  END
FROM sensors s
CROSS JOIN generate_series(0, 300) AS gs;
