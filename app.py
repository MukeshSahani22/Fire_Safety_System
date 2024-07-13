from flask import Flask, request, jsonify, render_template
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from marshmallow import ValidationError
import json
from datetime import datetime
import threading
from simulated_devices import run_simulated_devices
from threading import Lock
import paho.mqtt.client as mqtt
import time
from config import Config_Redis, Config
from schemas import DeviceDataSchema, LoginSchema
import logging
from celery_config import make_celery
from tasks import save_data_to_db_task
import psycopg2

# Setup logging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
app.config.from_object(Config)
app.json_encoder = Config.JSON_ENCODER

jwt = JWTManager(app)
celery = make_celery(app)

# Track processed message IDs to avoid duplicates
processed_message_ids = set()
message_processing_lock = Lock()

# Initialize schemas
device_data_schema = DeviceDataSchema()
login_schema = LoginSchema()

# MQTT client setup
mqtt_client = mqtt.Client()
mqtt_client.connected_once = False

def on_connect(client, userdata, flags, rc):
    if not client.connected_once:
        logging.info(f"Connected with result code {rc}")
        client.connected_once = True
    client.subscribe("fire_sensor", qos=2)
    client.subscribe("water_level_detector", qos=2)
    client.subscribe("sprinkler_handler", qos=2)

def on_message(client, userdata, msg):
    with message_processing_lock:
        logging.debug(f"Message received on topic {msg.topic}: {msg.payload.decode()}")
        try:
            data = json.loads(msg.payload.decode())
            logging.debug(f"Parsed data: {data}")
            save_data_to_db_task(data)             
        except json.JSONDecodeError:
            logging.error("Failed to decode JSON message")

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect("localhost", 1883, 60)

# Function to handle MQTT loop
def mqtt_loop():
    while True:
        mqtt_client.loop(timeout=1.0)  # Process network traffic, callbacks, and reconnecting
        time.sleep(1)  # Wait for 1 second before the next loop

# Utility function to generate a token
def generate_token(username):
    with app.app_context():
        return create_access_token(identity={'username': username})

# Serve the index.html file
@app.route('/')
def index():
    return render_template('index.html')

# Flask routes for authentication
@app.route('/login', methods=['POST'])
def login():
    try:
        data = login_schema.load(request.json)
    except ValidationError as err:
        return jsonify(err.messages), 400

    username = data['username']
    password = data['password']
    if username == 'admin' and password == 'admin':
        access_token = generate_token(username)
        return jsonify(access_token=access_token)
    return jsonify({"msg": "Bad username or password"}), 401

@app.route('/token', methods=['GET'])
def get_token():
    access_token = generate_token('admin')
    return jsonify(access_token=access_token)

# API endpoint to handle incoming data from IoT devices
@app.route('/api/devices/data', methods=['POST'])
@jwt_required()
def post_device_data():
    try:
        data = device_data_schema.load(request.json)
        logging.debug(f"Received data: {data}")
        save_data_to_db_task.delay(data)
        return jsonify({"msg": "Data saved successfully"}), 201
    except ValidationError as err:
        return jsonify(err.messages), 400

# API endpoint to get the latest status of a specific device
@app.route('/api/devices/<device_name>/status', methods=['GET'])
@jwt_required()
def get_device_status(device_name):
    try:
        with psycopg2.connect(
            dbname="iot_db",
            user="postgres",
            password="786143143",
            host="localhost",
            port="5432"
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM device_data WHERE device_name = %s ORDER BY timestamp DESC LIMIT 1", (device_name,))
                device = cursor.fetchone()
                if device:
                    return jsonify({
                        "device_name": device[1],
                        "device_type": device[2],
                        "timestamp": device[3],
                        "latitude": device[4],
                        "longitude": device[5],
                        "status": device[6],
                        "water_level": device[7],
                        "action": device[8],
                        "current_action": device[9]
                    })
                return jsonify({"msg": "No device data found"}), 404
    except Exception as e:
        logging.error(f"Error fetching device status: {e}")
        return jsonify({"msg": "Error fetching device status"}), 500

# API endpoint to get reports for a specific device over a date range
@app.route('/api/devices/<device_name>/reports', methods=['GET'])
@jwt_required()
def get_device_reports(device_name):
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')

    try:
        datetime.fromisoformat(from_date)
        datetime.fromisoformat(to_date)
    except ValueError:
        return jsonify({"msg": "Invalid date format"}), 400

    try:
        with psycopg2.connect(
            dbname="iot_db",
            user="postgres",
            password="786143143",
            host="localhost",
            port="5432"
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM device_data 
                    WHERE device_name = %s AND timestamp BETWEEN %s AND %s 
                    ORDER BY timestamp DESC
                """, (device_name, from_date, to_date))
                devices = cursor.fetchall()
                return jsonify([{
                    "device_name": device[1],
                    "device_type": device[2],
                    "timestamp": device[3],
                    "latitude": device[4],
                    "longitude": device[5],
                    "status": device[6],
                    "water_level": device[7],
                    "action": device[8],
                    "current_action": device[9]
                } for device in devices])
    except Exception as e:
        logging.error(f"Error fetching device reports: {e}")
        return jsonify({"msg": "Error fetching device reports"}), 500

# API endpoint to send command to sprinkler_handler
@app.route('/api/devices/sprinkler_handler/action', methods=['POST'])
@jwt_required()
def set_sprinkler_action():
    action = request.json.get('action')
    if action not in ['start', 'stop']:
        return jsonify({"msg": "Invalid action"}), 400
    
    payload = {
        "device_name": "sprinkler_handler",
        "device_type": "sprinkler_handler",
        "action": action,
        "timestamp": datetime.utcnow().isoformat(),
        "latitude": 0.0,
        "longitude": 0.0  # Dummy location for the action
    }
    mqtt_client.publish("sprinkler_handler", json.dumps(payload), qos=2)
    return jsonify({"msg": "Action command sent"}), 200

# Error handlers
@app.errorhandler(404)
def resource_not_found(e):
    return jsonify({"msg": "Resource not found"}), 404

@app.errorhandler(500)
def internal_server_error(e):
    return jsonify({"msg": "Internal server error"}), 500

if __name__ == '__main__':
    simulated_devices_thread = threading.Thread(target=run_simulated_devices)
    simulated_devices_thread.start()

    mqtt_loop_thread = threading.Thread(target=mqtt_loop)
    mqtt_loop_thread.start()

    app.run(debug=True)
