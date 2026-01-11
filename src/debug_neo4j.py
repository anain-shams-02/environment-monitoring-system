from database.neo4j_handler import Neo4jHandler
import logging

logging.basicConfig(level=logging.DEBUG)

handler = Neo4jHandler()

# Test data
test_data = {
    'device_id': 'debug_sensor_001',
    'timestamp': '2024-01-09T10:30:00',
    'temperature': 25.5,
    'humidity': 60.0,
    'location': 'debug_room'
}

print("Testing Neo4j storage...")
success = handler.store_environmental_data(test_data)
print(f"Storage success: {success}")

# Check what was created
print("\nChecking created device...")
with handler.driver.session() as session:
    # Check if device exists
    result = session.run(
        "MATCH (d:Device {device_id: $device_id}) RETURN d",
        device_id='debug_sensor_001'
    )
    device = result.single()
    print(f"Device found: {bool(device)}")
    
    # Check relationships
    result = session.run("""
        MATCH (d:Device {device_id: $device_id})-[r]-(connected)
        RETURN type(r) as rel_type, labels(connected) as connected_type
    """, device_id='debug_sensor_001')
    
    relationships = list(result)
    print(f"Relationships found: {len(relationships)}")
    for rel in relationships:
        print(f"  - {rel['rel_type']} to {rel['connected_type']}")

handler.close()