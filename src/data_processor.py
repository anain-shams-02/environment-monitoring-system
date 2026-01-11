import warnings
warnings.filterwarnings("ignore", message="datetime.datetime.utcnow()")
import json
import logging
from typing import Dict, Any
from datetime import datetime
import os

# Import our handlers
from database.postgres_handler import PostgresHandler
from database.mongo_handler import MongoHandler
from mqtt_handler import MQTTHandler
from database.neo4j_handler import Neo4jHandler

class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Initialize database handlers
        self.postgres_handler = PostgresHandler()
        self.mongo_handler = MongoHandler()
        self.neo4j_handler = Neo4jHandler()  # Keep this ONE line
        
        # Initialize MQTT handler
        self.mqtt_handler = MQTTHandler()
        
        # Register MQTT topic handlers
        self.register_mqtt_handlers()
    
    def register_mqtt_handlers(self):  # FIX INDENTATION - remove extra space
        """Register handlers for different MQTT topics"""
        
        # Handler for temperature data (store in PostgreSQL AND Neo4j)
        def handle_temperature(data):
            self.logger.info(f"Processing temperature data: {data}")
            
            # Store in PostgreSQL
            pg_data = {
                'device_id': data.get('device_id', 'unknown'),
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'temperature': data.get('temperature'),
                'humidity': data.get('humidity'),
                'location': data.get('location', 'unknown'),
                'metadata': {
                    'topic': data.get('_topic'),
                    'raw_data': data
                }
            }
            
            # Insert into PostgreSQL
            if pg_data['temperature'] is not None or pg_data['humidity'] is not None:
                success = self.postgres_handler.insert_environmental_data(pg_data)
                if success:
                    self.logger.debug(f"Stored temperature data in PostgreSQL for device: {pg_data['device_id']}")
                else:
                    self.logger.error(f"Failed to store temperature data in PostgreSQL")
            
            # ALSO store in Neo4j for relationships
            neo4j_data = {
                'device_id': data.get('device_id', 'unknown'),
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'temperature': data.get('temperature'),
                'humidity': data.get('humidity'),
                'location': data.get('location', 'unknown'),
                'pm2_5': data.get('pm2_5'),
                'pm10': data.get('pm10'),
                'co2': data.get('co2')
            }
            
            neo4j_success = self.neo4j_handler.store_environmental_data(neo4j_data)
            if neo4j_success:
                self.logger.debug(f"Stored temperature data in Neo4j for device: {neo4j_data['device_id']}")
            else:
                self.logger.error(f"Failed to store temperature data in Neo4j")
        
        # Handler for air quality data (store in MongoDB)
        def handle_air_quality(data):
            self.logger.info(f"Processing air quality data: {data}")
            
            # Store in MongoDB
            mongo_data = {
                'device_id': data.get('device_id', 'unknown'),
                'sensor_type': 'air_quality',
                'location': data.get('location', 'unknown'),
                'pm2_5': data.get('pm2_5'),
                'pm10': data.get('pm10'),
                'co2': data.get('co2'),
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'metadata': data
            }
            
            # Insert into MongoDB
            inserted_id = self.mongo_handler.insert_sensor_data(mongo_data)
            if inserted_id:
                self.logger.debug(f"Stored air quality data in MongoDB with ID: {inserted_id}")
            else:
                self.logger.error(f"Failed to store air quality data in MongoDB")
            
            # ALSO store relevant data in Neo4j
            neo4j_data = {
                'device_id': data.get('device_id', 'unknown'),
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'pm2_5': data.get('pm2_5'),
                'pm10': data.get('pm10'),
                'co2': data.get('co2'),
                'location': data.get('location', 'unknown')
            }
            
            self.neo4j_handler.store_environmental_data(neo4j_data)
            self.logger.debug(f"Stored air quality data in Neo4j")
        
        # Handler for device health/status (store in both MongoDB AND Neo4j)
        def handle_device_status(data):
            self.logger.info(f"Processing device status: {data}")
            
            # Store in MongoDB for flexible schema
            mongo_data = {
                'device_id': data.get('device_id', 'unknown'),
                'sensor_type': 'device_status',
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'status': data.get('status'),
                'battery_level': data.get('battery_level'),
                'signal_strength': data.get('signal_strength'),
                'uptime': data.get('uptime'),
                'raw_data': data
            }
            
            inserted_id = self.mongo_handler.insert_sensor_data(mongo_data)
            if inserted_id:
                self.logger.debug(f"Stored device status in MongoDB")
            
            # Also store basic info in PostgreSQL
            pg_data = {
                'device_id': data.get('device_id', 'unknown'),
                'sensor_type': 'device_status',
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'value': data.get('battery_level') or data.get('status'),
                'unit': 'percent' if 'battery_level' in data else 'status',
                'location': data.get('location', 'unknown'),
                'topic': data.get('_topic'),
                'raw_data': json.dumps(data) if data else None
            }
            
            if pg_data['value'] is not None:
                self.postgres_handler.insert_sensor_reading(pg_data)
            
            # ALSO store in Neo4j for device relationships
            neo4j_device_data = {
                'device_id': data.get('device_id', 'unknown'),
                'device_type': data.get('device_type', 'environmental_sensor'),
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'status': data.get('status', 'active'),
                'location': data.get('location', 'unknown')
            }
            
            # Create/update device node in Neo4j
            device_id = self.neo4j_handler.create_device_node(neo4j_device_data)
            if device_id and neo4j_device_data.get('location'):
                self.neo4j_handler.link_device_to_location(
                    device_id, 
                    neo4j_device_data['location']
                )
                self.logger.debug(f"Updated device node in Neo4j: {device_id}")
        
        # Test topic handler
        def handle_test(data):
            self.logger.info(f"Test message received: {data}")
            
            # Try to auto-detect data type
            if 'temperature' in data or 'humidity' in data:
                # Store in PostgreSQL
                pg_data = {
                    'device_id': data.get('device_id', 'test_device'),
                    'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                    'temperature': data.get('temperature'),
                    'humidity': data.get('humidity'),
                    'location': data.get('location', 'test_location'),
                    'metadata': {'source': 'test', 'topic': data.get('_topic')}
                }
                self.postgres_handler.insert_environmental_data(pg_data)
                
                # Store in Neo4j
                neo4j_data = {
                    'device_id': data.get('device_id', 'test_device'),
                    'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                    'temperature': data.get('temperature'),
                    'humidity': data.get('humidity'),
                    'location': data.get('location', 'test_location')
                }
                self.neo4j_handler.store_environmental_data(neo4j_data)
                
                self.logger.info(f"Auto-stored temperature test data")
                
            elif any(key in data for key in ['pm2_5', 'pm10', 'co2', 'aqi']):
                # Store in MongoDB
                mongo_data = {
                    'device_id': data.get('device_id', 'test_device'),
                    'sensor_type': 'air_quality',
                    'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                    'pm2_5': data.get('pm2_5'),
                    'pm10': data.get('pm10'),
                    'co2': data.get('co2'),
                    'location': data.get('location', 'test_location'),
                    'raw_data': data
                }
                self.mongo_handler.insert_sensor_data(mongo_data)
                
                # Store in Neo4j
                neo4j_data = {
                    'device_id': data.get('device_id', 'test_device'),
                    'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                    'pm2_5': data.get('pm2_5'),
                    'pm10': data.get('pm10'),
                    'co2': data.get('co2'),
                    'location': data.get('location', 'test_location')
                }
                self.neo4j_handler.store_environmental_data(neo4j_data)
                
                self.logger.info(f"Auto-stored air quality test data")
                
            else:
                self.logger.info(f"Unclassified test data stored in MongoDB")
                self.mongo_handler.insert_sensor_data({
                    'device_id': data.get('device_id', 'test_device'),
                    'sensor_type': 'test',
                    'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                    'data': data
                })
        
        # Register handlers with MQTT
        self.mqtt_handler.register_handler("sensors/temperature", handle_temperature)
        self.mqtt_handler.register_handler("sensors/humidity", handle_temperature)  # Same handler
        self.mqtt_handler.register_handler("sensors/air_quality", handle_air_quality)
        self.mqtt_handler.register_handler("sensors/device/status", handle_device_status)
        self.mqtt_handler.register_handler("sensors/device/health", handle_device_status)
        self.mqtt_handler.register_handler("test/topic", handle_test)
        
        # Handler for temperature data (store in PostgreSQL)
        def handle_temperature(data: Dict[str, Any]):
            self.logger.info(f"Processing temperature data: {data}")
            
            # Store in PostgreSQL
            pg_data = {
                'device_id': data.get('device_id', 'unknown'),
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'temperature': data.get('temperature'),
                'humidity': data.get('humidity'),
                'location': data.get('location', 'unknown'),
                'metadata': {
                    'topic': data.get('_topic'),
                    'raw_data': data
                }
            }
            
            # Insert into PostgreSQL
            if pg_data['temperature'] is not None or pg_data['humidity'] is not None:
                success = self.postgres_handler.insert_environmental_data(pg_data)
                if success:
                    self.logger.debug(f"Stored temperature data in PostgreSQL for device: {pg_data['device_id']}")
                else:
                    self.logger.error(f"Failed to store temperature data in PostgreSQL")
        
        # Handler for air quality data (store in MongoDB)
        def handle_air_quality(data: Dict[str, Any]):
            self.logger.info(f"Processing air quality data: {data}")
            
            # Store in MongoDB
            mongo_data = {
                'device_id': data.get('device_id', 'unknown'),
                'sensor_type': 'air_quality',
                'location': data.get('location', 'unknown'),
                'pm2_5': data.get('pm2_5'),
                'pm10': data.get('pm10'),
                'co2': data.get('co2'),
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'metadata': data
            }
            
            # Insert into MongoDB
            inserted_id = self.mongo_handler.insert_sensor_data(mongo_data)
            if inserted_id:
                self.logger.debug(f"Stored air quality data in MongoDB with ID: {inserted_id}")
            else:
                self.logger.error(f"Failed to store air quality data in MongoDB")
        
        # Handler for device health/status (store in both)
        def handle_device_status(data: Dict[str, Any]):
            self.logger.info(f"Processing device status: {data}")
            
            # Store in MongoDB for flexible schema
            mongo_data = {
                'device_id': data.get('device_id', 'unknown'),
                'sensor_type': 'device_status',
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'status': data.get('status'),
                'battery_level': data.get('battery_level'),
                'signal_strength': data.get('signal_strength'),
                'uptime': data.get('uptime'),
                'raw_data': data
            }
            
            inserted_id = self.mongo_handler.insert_sensor_data(mongo_data)
            if inserted_id:
                self.logger.debug(f"Stored device status in MongoDB")
            
            # Also store basic info in PostgreSQL
            pg_data = {
                'device_id': data.get('device_id', 'unknown'),
                'sensor_type': 'device_status',
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'value': data.get('battery_level') or data.get('status'),
                'unit': 'percent' if 'battery_level' in data else 'status',
                'location': data.get('location', 'unknown'),
                'topic': data.get('_topic'),
                'raw_data': json.dumps(data) if data else None
            }
            
            if pg_data['value'] is not None:
                self.postgres_handler.insert_sensor_reading(pg_data)
        
        # Register handlers with MQTT
        self.mqtt_handler.register_handler("sensors/temperature", handle_temperature)
        self.mqtt_handler.register_handler("sensors/humidity", handle_temperature)  # Same handler
        self.mqtt_handler.register_handler("sensors/air_quality", handle_air_quality)
        self.mqtt_handler.register_handler("sensors/device/status", handle_device_status)
        self.mqtt_handler.register_handler("sensors/device/health", handle_device_status)
        
        # Test topic
        self.mqtt_handler.register_handler("test/topic", self.handle_test)
    
    def handle_test(self, data: Dict[str, Any]):
        """Handler for test messages"""
        self.logger.info(f"Test message received: {data}")
        
        # Try to auto-detect data type
        if 'temperature' in data or 'humidity' in data:
            self.handle_temperature_auto(data)
        elif any(key in data for key in ['pm2_5', 'pm10', 'co2', 'aqi']):
            self.handle_air_quality_auto(data)
        else:
            self.logger.info(f"Unclassified test data stored in MongoDB")
            self.mongo_handler.insert_sensor_data({
                'device_id': data.get('device_id', 'test_device'),
                'sensor_type': 'test',
                'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
                'data': data
            })
    
    def handle_temperature_auto(self, data: Dict[str, Any]):
        """Auto-handle temperature/humidity data"""
        pg_data = {
            'device_id': data.get('device_id', 'test_device'),
            'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
            'temperature': data.get('temperature'),
            'humidity': data.get('humidity'),
            'location': data.get('location', 'test_location'),
            'metadata': {'source': 'auto_detected', 'topic': data.get('_topic')}
        }
        
        self.postgres_handler.insert_environmental_data(pg_data)
        self.logger.info(f"Auto-stored temperature data in PostgreSQL")
    
    def handle_air_quality_auto(self, data: Dict[str, Any]):
        """Auto-handle air quality data"""
        mongo_data = {
            'device_id': data.get('device_id', 'test_device'),
            'sensor_type': 'air_quality',
            'timestamp': data.get('timestamp') or datetime.utcnow().isoformat(),
            'pm2_5': data.get('pm2_5'),
            'pm10': data.get('pm10'),
            'co2': data.get('co2'),
            'aqi': data.get('aqi'),
            'location': data.get('location', 'test_location'),
            'raw_data': data
        }
        
        self.mongo_handler.insert_sensor_data(mongo_data)
        self.logger.info(f"Auto-stored air quality data in MongoDB")
    
    def start(self):
        """Start the data processor"""
        self.logger.info("Starting Data Processor...")
        self.mqtt_handler.connect()
        self.logger.info("Data Processor started. Waiting for messages...")
    
        def stop(self):
        """Stop the data processor"""
        self.logger.info("Stopping Data Processor...")
        self.mqtt_handler.disconnect()
        self.mongo_handler.close()
        self.neo4j_handler.close()  # ADD THIS LINE
        self.logger.info("Data Processor stopped.")

# Main entry point
if __name__ == "__main__":
    import time
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = DataProcessor()
    
    try:
        processor.start()
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
        processor.stop()