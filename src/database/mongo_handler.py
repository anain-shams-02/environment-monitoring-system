from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List
import os

class MongoHandler:
    def __init__(self, host=None, port=None, database=None):
        self.host = host or os.getenv('MONGO_HOST', 'localhost')
        self.port = port or int(os.getenv('MONGO_PORT', 27017))
        self.database_name = database or os.getenv('MONGO_DATABASE', 'iot_data')
        
        self.client = None
        self.db = None
        self.logger = logging.getLogger(__name__)
        
        # Initialize connection
        self.connect()
        
    def connect(self):
        """Connect to MongoDB"""
        try:
            # Connect without auth for now (you have MongoDB running locally)
            self.client = MongoClient(
                host=self.host,
                port=self.port,
                serverSelectionTimeoutMS=5000
            )
            
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            
            self.logger.info(f"Connected to MongoDB at {self.host}:{self.port}/{self.database_name}")
            
            # Initialize collections and indexes
            self.init_collections()
            return True
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def init_collections(self):
        """Initialize collections and create indexes"""
        try:
            # Create indexes for sensor_data collection
            self.db.sensor_data.create_index([("timestamp", -1)])
            self.db.sensor_data.create_index([("device_id", 1)])
            self.db.sensor_data.create_index([("sensor_type", 1)])
            self.db.sensor_data.create_index([("location", 1)])
            
            # Create indexes for device_metadata collection
            self.db.device_metadata.create_index([("device_id", 1)], unique=True)
            self.db.device_metadata.create_index([("last_seen", -1)])
            
            # Create indexes for alerts collection
            self.db.alerts.create_index([("timestamp", -1)])
            self.db.alerts.create_index([("device_id", 1)])
            self.db.alerts.create_index([("resolved", 1)])
            
            self.logger.info("MongoDB collections initialized with indexes")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize collections: {e}")
    
    def insert_sensor_data(self, data: Dict[str, Any]):
        """Insert sensor data into MongoDB"""
        try:
            # Add timestamp if not present
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now(timezone.utc)
            elif isinstance(data['timestamp'], str):
                # Convert string timestamp to datetime
                data['timestamp'] = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            
            # Insert into sensor_data collection
            result = self.db.sensor_data.insert_one(data)
            self.logger.debug(f"Inserted sensor data with ID: {result.inserted_id}")
            
            # Update device metadata
            self.update_device_metadata(data.get('device_id'), data)
            
            return str(result.inserted_id)
            
        except Exception as e:
            self.logger.error(f"Failed to insert sensor data: {e}")
            return None
    
    def update_device_metadata(self, device_id: str, latest_data: Dict[str, Any]):
        """Update or create device metadata"""
        try:
            if not device_id:
                return
            
            update_data = {
                'device_id': device_id,
                'last_seen': datetime.now(timezone.utc),
                'last_values': {
                    'temperature': latest_data.get('temperature'),
                    'humidity': latest_data.get('humidity'),
                    'pm2_5': latest_data.get('pm2_5'),
                    'pm10': latest_data.get('pm10'),
                    'co2': latest_data.get('co2')
                },
                'location': latest_data.get('location'),
                'sensor_types': list(set(
                    [k for k in latest_data.keys() 
                     if k in ['temperature', 'humidity', 'pm2_5', 'pm10', 'co2'] 
                     and latest_data[k] is not None]
                ))
            }
            
            # Remove None values
            update_data = {k: v for k, v in update_data.items() if v is not None}
            
            self.db.device_metadata.update_one(
                {'device_id': device_id},
                {'$set': update_data},
                upsert=True
            )
            
            self.logger.debug(f"Updated metadata for device: {device_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to update device metadata: {e}")
    
    def insert_alert(self, alert_data: Dict[str, Any]):
        """Insert an alert into the alerts collection"""
        try:
            alert_data.setdefault('timestamp', datetime.now(timezone.utc))
            alert_data.setdefault('resolved', False)
            
            result = self.db.alerts.insert_one(alert_data)
            self.logger.info(f"Inserted alert: {alert_data.get('message')}")
            return str(result.inserted_id)
            
        except Exception as e:
            self.logger.error(f"Failed to insert alert: {e}")
            return None
    
    def get_recent_sensor_data(self, limit: int = 100, **filters):
        """Get recent sensor data with optional filters"""
        try:
            query = {}
            
            # Apply filters
            if 'device_id' in filters:
                query['device_id'] = filters['device_id']
            if 'sensor_type' in filters:
                query['sensor_type'] = filters['sensor_type']
            if 'location' in filters:
                query['location'] = filters['location']
            if 'start_time' in filters:
                query['timestamp'] = {'$gte': filters['start_time']}
            if 'end_time' in filters:
                query.setdefault('timestamp', {})['$lte'] = filters['end_time']
            
            cursor = self.db.sensor_data.find(query).sort('timestamp', -1).limit(limit)
            return list(cursor)
            
        except Exception as e:
            self.logger.error(f"Failed to fetch sensor data: {e}")
            return []
    
    def get_device_metadata(self, device_id: str = None):
        """Get device metadata for all devices or a specific device"""
        try:
            query = {'device_id': device_id} if device_id else {}
            cursor = self.db.device_metadata.find(query).sort('last_seen', -1)
            return list(cursor)
            
        except Exception as e:
            self.logger.error(f"Failed to fetch device metadata: {e}")
            return []
    
    def get_active_alerts(self, resolved: bool = False):
        """Get active or resolved alerts"""
        try:
            cursor = self.db.alerts.find({'resolved': resolved}).sort('timestamp', -1)
            return list(cursor)
            
        except Exception as e:
            self.logger.error(f"Failed to fetch alerts: {e}")
            return []
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.logger.info("Closed MongoDB connection")

# Test the handler
if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    
    handler = MongoHandler()
    
    if handler.client:
        # Test data for environmental monitoring
        test_data = {
            'device_id': 'env_sensor_001',
            'sensor_type': 'environmental',
            'location': 'room_a',
            'temperature': 25.5,
            'humidity': 60.0,
            'pm2_5': 15,
            'pm10': 30,
            'co2': 450,
            'battery_level': 85,
            'metadata': {
                'calibration_date': '2024-01-01',
                'firmware_version': '1.2.3'
            }
        }
        
        # Test insertion
        inserted_id = handler.insert_sensor_data(test_data)
        print(f"Inserted document ID: {inserted_id}")
        
        # Test retrieval
        recent_data = handler.get_recent_sensor_data(limit=5)
        print(f"\nRecent sensor data ({len(recent_data)} records):")
        for doc in recent_data:
            print(f"  {doc['timestamp']} - Device: {doc['device_id']}, Temp: {doc.get('temperature')}°C")
        
        # Test device metadata
        metadata = handler.get_device_metadata()
        print(f"\nDevice metadata ({len(metadata)} devices):")
        for device in metadata:
            print(f"  Device: {device['device_id']}, Last seen: {device['last_seen']}")
        
        # Close connection
        handler.close()
    else:
        print("Failed to connect to MongoDB")