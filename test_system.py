import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime

def publish_test_data():
    client = mqtt.Client()
    client.connect("localhost", 1883)
    
    # Test 1: Temperature data (goes to PostgreSQL)
    temp_data = {
        "device_id": "env_sensor_001",
        "temperature": 25.5,
        "humidity": 60.0,
        "location": "room_a",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    client.publish("sensors/temperature", json.dumps(temp_data))
    print(f"Published temperature data: {temp_data}")
    
    # Test 2: Air quality data (goes to MongoDB)
    air_data = {
        "device_id": "air_sensor_001",
        "pm2_5": 15,
        "pm10": 30,
        "co2": 450,
        "location": "room_a",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    client.publish("sensors/air_quality", json.dumps(air_data))
    print(f"Published air quality data: {air_data}")
    
    # Test 3: Device status (goes to both)
    status_data = {
        "device_id": "env_sensor_001",
        "status": "online",
        "battery_level": 85,
        "signal_strength": -45,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    client.publish("sensors/device/status", json.dumps(status_data))
    print(f"Published device status: {status_data}")
    
    client.disconnect()
    print("\nTest data published. Check your database handlers for storage.")

if __name__ == "__main__":
    publish_test_data()