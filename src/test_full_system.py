import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

def test_all_databases():
    client = mqtt.Client()
    client.connect("localhost", 1883)
    
    print("="*60)
    print("FULL SYSTEM TEST - All 3 Databases")
    print("="*60)
    
    # Test 1: Environmental data (PostgreSQL + Neo4j)
    env_data = {
        "device_id": "building_sensor_001",
        "temperature": 22.5,
        "humidity": 55.0,
        "location": "main_lab",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    client.publish("sensors/temperature", json.dumps(env_data))
    print(f"1. Published environmental data → PostgreSQL & Neo4j")
    print(f"   Device: {env_data['device_id']}, Temp: {env_data['temperature']}°C")
    
    time.sleep(1)
    
    # Test 2: Air quality data (MongoDB + Neo4j)
    air_data = {
        "device_id": "air_monitor_001",
        "pm2_5": 12,
        "pm10": 25,
        "co2": 420,
        "location": "main_lab",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    client.publish("sensors/air_quality", json.dumps(air_data))
    print(f"\n2. Published air quality data → MongoDB & Neo4j")
    print(f"   Device: {air_data['device_id']}, PM2.5: {air_data['pm2_5']}")
    
    time.sleep(1)
    
    # Test 3: Device status (MongoDB + Neo4j + PostgreSQL)
    status_data = {
        "device_id": "building_sensor_001",
        "status": "online",
        "battery_level": 92,
        "signal_strength": -50,
        "location": "main_lab",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    client.publish("sensors/device/status", json.dumps(status_data))
    print(f"\n3. Published device status → All 3 databases")
    print(f"   Device: {status_data['device_id']}, Battery: {status_data['battery_level']}%")
    
    client.disconnect()
    
    print("\n" + "="*60)
    print("TEST COMPLETE - Check data processor window for processing logs")
    print("="*60)
    print("\nExpected storage:")
    print("- PostgreSQL: Temperature/Humidity readings")
    print("- MongoDB: Air quality data + Device status")
    print("- Neo4j: Device relationships + Location hierarchy")
    print("\nYou can also check Neo4j Browser at: http://localhost:7474")
    print("Login: neo4j/iot_password")

if __name__ == "__main__":
    test_all_databases()