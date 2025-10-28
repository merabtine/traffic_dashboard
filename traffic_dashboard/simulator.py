import os
import time
import random
from datetime import datetime
import psycopg2

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "Workshop01")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "theworldwidechampion")

conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
cur = conn.cursor()

# fetch sensor ids & types
cur.execute("SELECT sensor_id, road_type FROM sensors")
sensors = cur.fetchall()

def gen_speed(road_type):
    if road_type == 'Highway':
        return round(80 + random.random()*40, 2)
    elif road_type == 'Urban':
        return round(20 + random.random()*40, 2)
    else:
        return round(30 + random.random()*50, 2)

try:
    while True:
        # insert N readings each loop (simulate multiple sensors)
        for sensor_id, road_type in sensors:
            vehicle_id = 'V' + str(random.randint(1000, 9999))
            speed = gen_speed(road_type)
            ts = datetime.now()
            cur.execute(
                "INSERT INTO raw_traffic_readings (sensor_id, record_time, vehicle_id, speed) VALUES (%s, %s, %s, %s)",
                (sensor_id, ts, vehicle_id, speed)
            )
        conn.commit()
        # wait
        time.sleep(5) # change to 1-10 seconds for faster/slower simulation
except KeyboardInterrupt:
    print("Simulator stopped.")
finally:
    cur.close()
    conn.close()
