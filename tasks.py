import os
import sys
import psycopg2
import logging
from celery_config import make_celery
from flask import Flask
from datetime import datetime

# Add the project directory to the Python path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

app = Flask(__name__)
app.config.from_object('config.Config')
celery = make_celery(app)

@celery.task
def save_data_to_db_task(validated_data):
    try:
        with psycopg2.connect(
            dbname="iot_db",
            user="postgres",
            password="786143143",
            host="localhost",
            port="5432"
        ) as conn:
            with conn.cursor() as cursor:
                timestamp = validated_data.get("timestamp", datetime.utcnow().isoformat())
                latitude = validated_data.get("latitude")
                longitude = validated_data.get("longitude")
                device_name = validated_data.get("device_name")
                device_type = validated_data.get("device_type")
                status = validated_data.get("status")
                water_level = validated_data.get("water_level")
                action = validated_data.get("action")
                current_action = validated_data.get("current_action")

                cursor.execute("""
                    SELECT id FROM device_data 
                    WHERE device_name = %s AND timestamp = %s
                """, (device_name, timestamp))

                result = cursor.fetchone()

                if result:
                    cursor.execute("""
                        UPDATE device_data 
                        SET latitude = %s, longitude = %s, status = %s, water_level = %s, action = %s, current_action = %s
                        WHERE id = %s
                    """, (
                        latitude,
                        longitude,
                        status,
                        water_level,
                        action,
                        current_action,
                        result[0]
                    ))
                else:
                    cursor.execute("""
                        INSERT INTO device_data (device_name, device_type, timestamp, latitude, longitude, status, water_level, action, current_action)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        device_name,
                        device_type,
                        timestamp,
                        latitude,
                        longitude,
                        status,
                        water_level,
                        action,
                        current_action
                    ))

                conn.commit()
                logging.info("Data saved successfully")
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        conn.rollback()  # Rollback in case of error to avoid partial commits
    except Exception as e:
        logging.error(f"Error saving data to DB: {e}")

if __name__ == '__main__':
    app.run(debug=True)
