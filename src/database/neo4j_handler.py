from neo4j import GraphDatabase
import logging
from typing import Dict, Any, List
import os

class Neo4jHandler:
    def __init__(self, uri=None, user=None, password=None, database=None):
        self.uri = uri or os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        self.user = user or os.getenv('NEO4J_USER', 'neo4j')
        self.password = password or os.getenv('NEO4J_PASSWORD', 'iot_password')
        self.database = database or os.getenv('NEO4J_DATABASE', 'neo4j')
        
        self.driver = None
        self.logger = logging.getLogger(__name__)
        
        # Initialize connection and schema
        self.connect()
        self.init_schema()
    
    def connect(self):
        """Connect to Neo4j database"""
        try:
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
                database=self.database
            )
            # Test connection
            self.driver.verify_connectivity()
            self.logger.info(f"Connected to Neo4j at {self.uri}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Neo4j: {e}")
            return False
    
    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            self.logger.info("Closed Neo4j connection")
    
    def init_schema(self):
        """Initialize Neo4j schema with constraints and indexes"""
        try:
            with self.driver.session() as session:
                # Create constraints for uniqueness
                session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Device) REQUIRE d.device_id IS UNIQUE")
                session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (l:Location) REQUIRE l.name IS UNIQUE")
                session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:SensorType) REQUIRE s.type IS UNIQUE")
                
                # Create indexes for better performance
                session.run("CREATE INDEX IF NOT EXISTS FOR (r:Reading) ON (r.timestamp)")
                session.run("CREATE INDEX IF NOT EXISTS FOR (r:Reading) ON (r.sensor_type)")
                
                self.logger.info("Neo4j schema initialized with constraints and indexes")
                return True
        except Exception as e:
            self.logger.error(f"Failed to initialize Neo4j schema: {e}")
            return False
    
    def create_device_node(self, device_data: Dict[str, Any]):
        """Create or update a device node"""
        try:
            with self.driver.session() as session:
                query = """
                MERGE (d:Device {device_id: $device_id})
                SET d.device_type = $device_type,
                    d.model = $model,
                    d.manufacturer = $manufacturer,
                    d.installation_date = $installation_date,
                    d.last_seen = $last_seen,
                    d.status = $status,
                    d.updated_at = timestamp()
                RETURN d.device_id as device_id
                """
                
                result = session.run(query, 
                    device_id=device_data.get('device_id'),
                    device_type=device_data.get('device_type', 'environmental_sensor'),
                    model=device_data.get('model', 'unknown'),
                    manufacturer=device_data.get('manufacturer', 'unknown'),
                    installation_date=device_data.get('installation_date'),
                    last_seen=device_data.get('timestamp'),
                    status=device_data.get('status', 'active')
                )
                
                device_id = result.single()["device_id"]
                self.logger.debug(f"Created/updated device node: {device_id}")
                return device_id
        except Exception as e:
            self.logger.error(f"Failed to create device node: {e}")
            return None
    
    def create_location_node(self, location_name: str, location_type: str = "room"):
        """Create or get a location node"""
        try:
            with self.driver.session() as session:
                query = """
                MERGE (l:Location {name: $name})
                SET l.type = $type,
                    l.updated_at = timestamp()
                RETURN l.name as location_name
                """
                
                result = session.run(query, name=location_name, type=location_type)
                location_name = result.single()["location_name"]
                return location_name
        except Exception as e:
            self.logger.error(f"Failed to create location node: {e}")
            return None
    
    def link_device_to_location(self, device_id: str, location_name: str):
        """Create LOCATED_IN relationship between device and location"""
        try:
            with self.driver.session() as session:
                query = """
                MATCH (d:Device {device_id: $device_id})
                MATCH (l:Location {name: $location_name})
                MERGE (d)-[r:LOCATED_IN]->(l)
                SET r.since = coalesce(r.since, timestamp()),
                    r.updated_at = timestamp()
                RETURN d.device_id, l.name
                """
                
                result = session.run(query, device_id=device_id, location_name=location_name)
                record = result.single()
                if record:
                    self.logger.debug(f"Linked device {record['d.device_id']} to location {record['l.name']}")
                    return True
                return False
        except Exception as e:
            self.logger.error(f"Failed to link device to location: {e}")
            return False
    
    def create_sensor_reading(self, reading_data: Dict[str, Any]):
        """Create a sensor reading node and link to device"""
        try:
            with self.driver.session() as session:
                # Create reading node
                query = """
                MATCH (d:Device {device_id: $device_id})
                CREATE (r:Reading {
                    timestamp: $timestamp,
                    sensor_type: $sensor_type,
                    value: $value,
                    unit: $unit,
                    raw_data: $raw_data
                })
                CREATE (d)-[:GENERATED]->(r)
                RETURN id(r) as reading_id, d.device_id as device_id
                """
                
                result = session.run(query,
                    device_id=reading_data.get('device_id'),
                    timestamp=reading_data.get('timestamp'),
                    sensor_type=reading_data.get('sensor_type', 'environmental'),
                    value=reading_data.get('value'),
                    unit=reading_data.get('unit'),
                    raw_data=str(reading_data.get('raw_data', {}))
                )
                
                record = result.single()
                if record:
                    self.logger.debug(f"Created reading {record['reading_id']} for device {record['device_id']}")
                    
                    # Link to sensor type
                    self.link_reading_to_sensor_type(
                        record['reading_id'],
                        reading_data.get('sensor_type', 'environmental')
                    )
                    
                    return record['reading_id']
                return None
        except Exception as e:
            self.logger.error(f"Failed to create sensor reading: {e}")
            return None
    
    def link_reading_to_sensor_type(self, reading_id: int, sensor_type: str):
        """Link reading to sensor type node"""
        try:
            with self.driver.session() as session:
                query = """
                MATCH (r:Reading) WHERE id(r) = $reading_id
                MERGE (st:SensorType {type: $sensor_type})
                MERGE (r)-[:OF_TYPE]->(st)
                """
                
                session.run(query, reading_id=reading_id, sensor_type=sensor_type)
                return True
        except Exception as e:
            self.logger.error(f"Failed to link reading to sensor type: {e}")
            return False
    
    def create_correlation(self, device1_id: str, device2_id: str, correlation_type: str, strength: float):
        """Create correlation relationship between two devices"""
        try:
            with self.driver.session() as session:
                query = """
                MATCH (d1:Device {device_id: $device1_id})
                MATCH (d2:Device {device_id: $device2_id})
                MERGE (d1)-[r:CORRELATES_WITH {type: $correlation_type}]->(d2)
                SET r.strength = $strength,
                    r.updated_at = timestamp()
                RETURN d1.device_id, d2.device_id, r.type
                """
                
                result = session.run(query,
                    device1_id=device1_id,
                    device2_id=device2_id,
                    correlation_type=correlation_type,
                    strength=strength
                )
                
                record = result.single()
                if record:
                    self.logger.debug(f"Created correlation between {record['d1.device_id']} and {record['d2.device_id']}")
                    return True
                return False
        except Exception as e:
            self.logger.error(f"Failed to create correlation: {e}")
            return False
    
    def store_environmental_data(self, data: Dict[str, Any]):
        """Store environmental data in Neo4j graph"""
        try:
            device_id = data.get('device_id')
            location = data.get('location')
            
            if not device_id:
                self.logger.error("No device_id provided")
                return False
            
            # Create/update device node
            device_node_id = self.create_device_node({
                'device_id': device_id,
                'device_type': 'environmental_sensor',
                'last_seen': data.get('timestamp'),
                'status': 'active'
            })
            
            if not device_node_id:
                return False
            
            # Create/link location if provided
            if location:
                location_name = self.create_location_node(location, "sensor_location")
                if location_name:
                    self.link_device_to_location(device_id, location_name)
            
            # Store temperature reading if available
            if data.get('temperature') is not None:
                self.create_sensor_reading({
                    'device_id': device_id,
                    'timestamp': data.get('timestamp'),
                    'sensor_type': 'temperature',
                    'value': data.get('temperature'),
                    'unit': 'celsius',
                    'raw_data': data
                })
            
            # Store humidity reading if available
            if data.get('humidity') is not None:
                self.create_sensor_reading({
                    'device_id': device_id,
                    'timestamp': data.get('timestamp'),
                    'sensor_type': 'humidity',
                    'value': data.get('humidity'),
                    'unit': 'percent',
                    'raw_data': data
                })
            
            # Store air quality readings if available
            air_quality_params = ['pm2_5', 'pm10', 'co2']
            for param in air_quality_params:
                if data.get(param) is not None:
                    self.create_sensor_reading({
                        'device_id': device_id,
                        'timestamp': data.get('timestamp'),
                        'sensor_type': param,
                        'value': data.get(param),
                        'unit': 'ppm' if param == 'co2' else 'µg/m³',
                        'raw_data': data
                    })
            
            self.logger.info(f"Stored environmental data in Neo4j for device: {device_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store environmental data in Neo4j: {e}")
            return False
    
    def get_device_relationships(self, device_id: str):
        """Get all relationships for a device"""
        try:
            with self.driver.session() as session:
                query = """
                MATCH (d:Device {device_id: $device_id})-[r]-(connected)
                RETURN type(r) as relationship_type, 
                       labels(connected) as connected_labels,
                       connected.device_id as connected_device_id,
                       connected.name as connected_name
                """
                
                result = session.run(query, device_id=device_id)
                return [record.data() for record in result]
        except Exception as e:
            self.logger.error(f"Failed to get device relationships: {e}")
            return []
    
    def find_correlated_devices(self, device_id: str, threshold: float = 0.7):
        """Find devices with strong correlations"""
        try:
            with self.driver.session() as session:
                query = """
                MATCH (d1:Device {device_id: $device_id})-[r:CORRELATES_WITH]-(d2:Device)
                WHERE r.strength >= $threshold
                RETURN d2.device_id as correlated_device,
                       r.type as correlation_type,
                       r.strength as strength
                ORDER BY r.strength DESC
                """
                
                result = session.run(query, device_id=device_id, threshold=threshold)
                return [record.data() for record in result]
        except Exception as e:
            self.logger.error(f"Failed to find correlated devices: {e}")
            return []
    
    def get_location_hierarchy(self):
        """Get the hierarchy of locations and devices"""
        try:
            with self.driver.session() as session:
                query = """
                MATCH (l:Location)<-[:LOCATED_IN]-(d:Device)
                RETURN l.name as location, 
                       l.type as location_type,
                       collect(d.device_id) as devices,
                       count(d) as device_count
                ORDER BY device_count DESC
                """
                
                result = session.run(query)
                return [record.data() for record in result]
        except Exception as e:
            self.logger.error(f"Failed to get location hierarchy: {e}")
            return []

# Test the handler
if __name__ == "__main__":
    import json
    from datetime import datetime
    import logging
    
    logging.basicConfig(level=logging.INFO)
    
    handler = Neo4jHandler()
    
    if handler.driver:
        # Test data
        test_data = {
            'device_id': 'env_sensor_001',
            'timestamp': datetime.utcnow().isoformat(),
            'temperature': 25.5,
            'humidity': 60.0,
            'pm2_5': 15,
            'location': 'room_a'
        }
        
        # Test storage
        success = handler.store_environmental_data(test_data)
        print(f"Storage successful: {success}")
        
        # Test relationships
        relationships = handler.get_device_relationships('env_sensor_001')
        print(f"\nDevice relationships: {len(relationships)} found")
        for rel in relationships:
            print(f"  - {rel}")
        
        # Test location hierarchy
        hierarchy = handler.get_location_hierarchy()
        print(f"\nLocation hierarchy: {len(hierarchy)} locations")
        for loc in hierarchy:
            print(f"  - {loc['location']}: {loc['device_count']} devices")
        
        # Close connection
        handler.close()
    else:
        print("Failed to connect to Neo4j")