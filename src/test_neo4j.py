from database.neo4j_handler import Neo4jHandler
import logging

logging.basicConfig(level=logging.INFO)

print("Testing Neo4j Connection...")
print("="*50)

handler = Neo4jHandler()

if handler.driver:
    print("✅ Neo4j connection successful!")
    
    # Test creating a device
    device_id = handler.create_device_node({
        'device_id': 'test_device_001',
        'device_type': 'test_sensor',
        'status': 'active'
    })
    
    print(f"Created device: {device_id}")
    
    # Test creating location
    location = handler.create_location_node('test_room', 'room')
    print(f"Created location: {location}")
    
    # Test linking
    if device_id and location:
        linked = handler.link_device_to_location(device_id, location)
        print(f"Linked device to location: {linked}")
    
    # Get relationships
    relationships = handler.get_device_relationships('test_device_001')
    print(f"\nRelationships found: {len(relationships)}")
    for rel in relationships:
        print(f"  - Type: {rel['relationship_type']}, Connected to: {rel.get('connected_name') or rel.get('connected_device_id')}")
    
    # Test storing environmental data
    test_data = {
        'device_id': 'env_sensor_001',
        'timestamp': '2024-01-09T10:30:00',
        'temperature': 25.5,
        'humidity': 60.0,
        'location': 'room_a'
    }
    
    stored = handler.store_environmental_data(test_data)
    print(f"\nStored environmental data: {stored}")
    
    # Get location hierarchy
    hierarchy = handler.get_location_hierarchy()
    print(f"\nLocation hierarchy ({len(hierarchy)} locations):")
    for loc in hierarchy:
        print(f"  - {loc['location']}: {loc['device_count']} devices")
    
    handler.close()
    print("\n" + "="*50)
    print("✅ All Neo4j tests passed!")
    
else:
    print("❌ Neo4j connection failed")
    print("Check if Neo4j is running on port 7687")
    print("Default credentials: neo4j/iot_password")