# central_service.py

import json
import logging

import paho.mqtt.client as mqtt

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MQTT Configuration - Should match the publisher's broker settings
MQTT_BROKER = "192.168.100.191"
MQTT_PORT = 1883
MQTT_USERNAME = None  # If authentication is needed
MQTT_PASSWORD = None

TOPICS = [("PoiDataMessage", 0)]

# Initialize MQTT Client
client = mqtt.Client()

# Set username and password if provided
if MQTT_USERNAME and MQTT_PASSWORD:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Central Service connected to MQTT Broker!")
        for topic in TOPICS:
            client.subscribe(topic)
            print(f"Subscribed to topic: {topic[0]}")
    else:
        logger.error(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        poi_data = json.loads(payload)
        logger.info(f"Received POI Approved Message: {poi_data}")
        # Process the POI data as needed
        # For example, store it in a database, trigger workflows, etc.
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning("Unexpected disconnection from MQTT Broker.")
    else:
        logger.info("Disconnected from MQTT Broker.")

# Assign event callbacks
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

# Connect to MQTT Broker
client.connect(MQTT_BROKER, MQTT_PORT)

# Start MQTT loop
client.loop_forever()
