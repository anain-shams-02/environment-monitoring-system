import paho.mqtt.client as mqtt
import psycopg2
from pymongo import MongoClient
import time

def test_mqtt():
    print("Testing MQTT connection...")
    client = mqtt.Client()
    try:
        client.connect("localhost", 1883, 60)
        print("✅ MQTT: Connected successfully")
        return True
    except Exception as e:
        print(f"❌ MQTT: Connection failed - {e}")
        return False

def test_postgres():
    print("\nTesting PostgreSQL connection...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="iot_monitoring",
            user="iot_user",
            password="iot_password"
        )
        print("✅ PostgreSQL: Connected successfully")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ PostgreSQL: Connection failed - {e}")
        return False

def test_mongodb():
    print("\nTesting MongoDB connection...")
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client.admin
        db.command('ping')
        print("✅ MongoDB: Connected successfully")
        return True
    except Exception as e:
        print(f"❌ MongoDB: Connection failed - {e}")
        return False

if __name__ == "__main__":
    print("="*50)
    print("Testing All Connections")
    print("="*50)
    
    mqtt_ok = test_mqtt()
    pg_ok = test_postgres()
    mongo_ok = test_mongodb()
    
    print("\n" + "="*50)
    print("Summary:")
    print(f"MQTT: {'✅ OK' if mqtt_ok else '❌ FAILED'}")
    print(f"PostgreSQL: {'✅ OK' if pg_ok else '❌ FAILED'}")
    print(f"MongoDB: {'✅ OK' if mongo_ok else '❌ FAILED'}")
    print("="*50)