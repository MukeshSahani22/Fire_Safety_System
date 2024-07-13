import paho.mqtt.client as mqtt
import json
import time
import random
from datetime import datetime

# Utility function to generate random location
def generate_random_location():
    return {
        "latitude": round(random.uniform(-90, 90), 6),
        "longitude": round(random.uniform(-180, 180), 6)
    }

class MQTTDevice:
    def __init__(self, client_id, broker='localhost', port=1883, topic='', qos=2):
        self.client = mqtt.Client(client_id)
        self.broker = broker
        self.port = port
        self.topic = topic
        self.qos = qos
        self.connected_once = False  # Flag to indicate the connection message has been shown
        self.client.on_connect = self.on_connect

    def connect(self):
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        if not self.connected_once:
            print(f"Connected with result code {rc}")
            self.connected_once = True
        self.client.subscribe(self.topic, qos=self.qos)

    def send_data(self, payload):
        self.client.publish(self.topic, json.dumps(payload), qos=self.qos)

    def generate_base_payload(self):
        location = generate_random_location()
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "latitude": location['latitude'],
            "longitude": location['longitude']
        }

class FireSensor(MQTTDevice):
    def __init__(self, client_id, broker='localhost', port=1883, topic='fire_sensor', qos=2):
        super().__init__(client_id, broker, port, topic, qos)

    def detect_fire(self):
        fire_detected = random.choice([True, False])
        payload = self.generate_base_payload()
        payload.update({
            'device_name': 'fire_sensor',
            'device_type': 'fire_sensor',
            'status': 'fire_detected' if fire_detected else 'no_fire'
        })
        self.send_data(payload)

class WaterLevelDetector(MQTTDevice):
    def __init__(self, client_id, broker='localhost', port=1883, topic='water_level_detector', qos=2):
        super().__init__(client_id, broker, port, topic, qos)

    def check_water_level(self):
        water_level = random.randint(0, 100)
        payload = self.generate_base_payload()
        payload.update({
            'device_name': 'water_level_detector',
            'device_type': 'water_level_detector',
            'water_level': water_level
        })
        self.send_data(payload)

class SprinklerHandler(MQTTDevice):
    def __init__(self, client_id, broker='localhost', port=1883, topic='sprinkler_handler', qos=2):
        super().__init__(client_id, broker, port, topic, qos)
        self.current_action = "stopped"

    def handle_message(self, message):
        try:
            data = json.loads(message)
            if data['device_name'] == 'sprinkler_handler' and 'action' in data:
                self.current_action = data['action']
                self.send_status()
        except json.JSONDecodeError:
            print("Failed to decode JSON message")

    def send_status(self):
        payload = self.generate_base_payload()
        payload.update({
            'device_name': 'sprinkler_handler',
            'device_type': 'sprinkler_handler',
            'current_action': self.current_action
        })
        self.send_data(payload)

def run_simulated_devices():
    fire_sensor = FireSensor(client_id='fire_sensor_1', qos=2)
    fire_sensor.connect()

    water_level_detector = WaterLevelDetector(client_id='water_level_detector_1', qos=2)
    water_level_detector.connect()

    sprinkler_handler = SprinklerHandler(client_id='sprinkler_handler_1', qos=2)
    sprinkler_handler.connect()

    try:
        while True:
            fire_sensor.detect_fire()
            time.sleep(1)  # Adjusted to reduce frequency

            water_level_detector.check_water_level()
            time.sleep(1)  # Adjusted to reduce frequency

            sprinkler_handler.send_status()
            time.sleep(1)  # Adjusted to reduce frequency
    except KeyboardInterrupt:
        print("Exiting...")

if __name__ == '__main__':
    run_simulated_devices()
