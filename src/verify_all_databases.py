from database.postgres_handler import PostgresHandler
from database.mongo_handler import MongoHandler
from database.neo4j_handler import Neo4jHandler
import logging

logging.basicConfig(level=logging.INFO)

print("="*70)
print("VERIFICATION - Check All 3 Databases")
print("="*70)

# 1. Check PostgreSQL
print("\n1. 📊 POSTGRESQL (Temperature/Humidity):")
pg = PostgresHandler()
pg_data = pg.get_recent_readings(5)
print(f"   Found {len(pg_data)} environmental records")
for i, record in enumerate(pg_data[:3], 1):
    print(f"   {i}. Device: {record['device_id']}, "
          f"Temp: {record['temperature']}°C, "
          f"Humidity: {record['humidity']}%, "
          f"Time: {record['recorded_at'].strftime('%H:%M:%S')}")

# 2. Check MongoDB
print("\n2. 🍃 MONGODB (Air Quality/Device Status):")
mongo = MongoHandler()
total = mongo.db.sensor_data.count_documents({})
print(f"   Total documents: {total}")

# Count by type
types = mongo.db.sensor_data.distinct("sensor_type")
for t in types:
    count = mongo.db.sensor_data.count_documents({"sensor_type": t})
    print(f"   - {t}: {count} documents")

# Show recent
recent = mongo.get_recent_sensor_data(3)
for i, doc in enumerate(recent, 1):
    print(f"   {i}. Device: {doc['device_id']}, Type: {doc.get('sensor_type')}")

# 3. Check Neo4j
print("\n3. 🕸️  NEO4J (Device Relationships):")
neo4j = Neo4jHandler()
if neo4j.driver:
    # Count devices
    with neo4j.driver.session() as session:
        result = session.run("MATCH (d:Device) RETURN count(d) as device_count")
        count = result.single()["device_count"]
        print(f"   Total devices in graph: {count}")
    
    # Get location hierarchy
    hierarchy = neo4j.get_location_hierarchy()
    print(f"   Locations with devices: {len(hierarchy)}")
    for loc in hierarchy:
        print(f"   - {loc['location']}: {loc['device_count']} devices")
    
    # Check specific device
    devices = ["building_sensor_001", "air_monitor_001"]
    for device in devices:
        rels = neo4j.get_device_relationships(device)
        print(f"   Device '{device}' has {len(rels)} relationships")
    
    neo4j.close()
else:
    print("   ❌ Neo4j not connected")

print("\n" + "="*70)
print("VERIFICATION COMPLETE")
print("="*70)