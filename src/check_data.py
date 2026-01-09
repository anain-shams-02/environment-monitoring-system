from database.postgres_handler import PostgresHandler
from database.mongo_handler import MongoHandler

print("="*50)
print("DATABASE DATA CHECK")
print("="*50)

# Check PostgreSQL
print("\n1. PostgreSQL Data:")
try:
    pg_handler = PostgresHandler()
    readings = pg_handler.get_recent_readings(10)
    print(f"   Total records in environmental_data: {len(readings)}")
    
    if readings:
        for r in readings:
            print(f"   - ID: {r['id']}, Device: {r['device_id']}, "
                  f"Temp: {r['temperature']}°C, Humidity: {r['humidity']}%, "
                  f"Time: {r['recorded_at']}")
    else:
        print("   No records found.")
        
except Exception as e:
    print(f"   Error checking PostgreSQL: {e}")

# Check MongoDB
print("\n2. MongoDB Data:")
try:
    mongo_handler = MongoHandler()
    
    # Count all documents
    total_count = mongo_handler.db.sensor_data.count_documents({})
    print(f"   Total documents in sensor_data: {total_count}")
    
    # Count by type
    env_count = mongo_handler.db.sensor_data.count_documents({"sensor_type": "environmental"})
    air_count = mongo_handler.db.sensor_data.count_documents({"sensor_type": "air_quality"})
    status_count = mongo_handler.db.sensor_data.count_documents({"sensor_type": "device_status"})
    
    print(f"   - Environmental sensors: {env_count}")
    print(f"   - Air quality sensors: {air_count}")
    print(f"   - Device status: {status_count}")
    
    # Show recent documents
    recent_data = mongo_handler.get_recent_sensor_data(5)
    print(f"\n   Recent 5 documents:")
    for doc in recent_data:
        doc_id = str(doc.get('_id', 'N/A'))[:8]
        print(f"   - ID: {doc_id}..., Device: {doc.get('device_id')}, "
              f"Type: {doc.get('sensor_type', 'N/A')}, "
              f"Time: {doc.get('timestamp', 'N/A')}")
    
    mongo_handler.close()
    
except Exception as e:
    print(f"   Error checking MongoDB: {e}")

print("\n" + "="*50)