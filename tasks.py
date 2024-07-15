import os
import sys
import logging
from celery_config import make_celery
from flask import Flask
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from config import SQLALCHEMY_DATABASE_URI

# Add the project directory to the Python path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

app = Flask(__name__)
app.config.from_object('config.Config')
celery = make_celery(app)

# Set up the SQLAlchemy engine and session
engine = create_engine(SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=engine)

@celery.task
def save_data_to_db_task(validated_data):
    session = Session()
    try:
        timestamp = validated_data.get("timestamp", datetime.utcnow().isoformat())
        latitude = validated_data.get("latitude")
        longitude = validated_data.get("longitude")
        device_name = validated_data.get("device_name")
        device_type = validated_data.get("device_type")
        status = validated_data.get("status")
        water_level = validated_data.get("water_level")
        action = validated_data.get("action")
        current_action = validated_data.get("current_action")

        result = session.execute(text("""
            SELECT id FROM device_data 
            WHERE device_name = :device_name AND timestamp = :timestamp
        """), {'device_name': device_name, 'timestamp': timestamp}).fetchone()

        if result:
            session.execute(text("""
                UPDATE device_data 
                SET latitude = :latitude, longitude = :longitude, status = :status, water_level = :water_level, action = :action, current_action = :current_action
                WHERE id = :id
            """), {
                'latitude': latitude,
                'longitude': longitude,
                'status': status,
                'water_level': water_level,
                'action': action,
                'current_action': current_action,
                'id': result[0]
            })
        else:
            session.execute(text("""
                INSERT INTO device_data (device_name, device_type, timestamp, latitude, longitude, status, water_level, action, current_action)
                VALUES (:device_name, :device_type, :timestamp, :latitude, :longitude, :status, :water_level, :action, :current_action)
            """), {
                'device_name': device_name,
                'device_type': device_type,
                'timestamp': timestamp,
                'latitude': latitude,
                'longitude': longitude,
                'status': status,
                'water_level': water_level,
                'action': action,
                'current_action': current_action
            })

        session.commit()
        logging.info("Data saved successfully")
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
        session.rollback()  # Rollback in case of error to avoid partial commits
    except Exception as e:
        logging.error(f"Error saving data to DB: {e}")
    finally:
        session.close()

if __name__ == '__main__':
    app.run(debug=True)
