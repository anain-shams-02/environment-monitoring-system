import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, Any, List
import os
import json

class PostgresHandler:
    def __init__(self, host=None, port=None, database=None, user=None, password=None):
        self.host = host or os.getenv('PG_HOST', 'localhost')
        self.port = port or int(os.getenv('PG_PORT', 5432))
        self.database = database or os.getenv('PG_DATABASE', 'iot_monitoring')
        self.user = user or os.getenv('PG_USER', 'iot_user')
        self.password = password or os.getenv('PG_PASSWORD', 'iot_password')
        
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
        # Initialize database schema
        self.init_database()
    
    def connect(self):
        """Establish connection to PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            self.logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from PostgreSQL")
    
    def init_database(self):
        """Initialize database tables if they don't exist"""
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                # Create sensor_readings table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sensor_readings (
                        id SERIAL PRIMARY KEY,
                        device_id VARCHAR(50) NOT NULL,
                        sensor_type VARCHAR(50) NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        value DECIMAL(10,4),
                        unit VARCHAR(20),
                        location VARCHAR(100),
                        topic VARCHAR(255),
                        raw_data JSONB
                    )
                """)
                
                # Create environmental_data table for your project
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS environmental_data (
                        id SERIAL PRIMARY KEY,
                        device_id VARCHAR(50) NOT NULL,
                        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        temperature DECIMAL(5,2),
                        humidity DECIMAL(5,2),
                        pm2_5 INTEGER,
                        pm10 INTEGER,
                        co2 INTEGER,
                        location VARCHAR(100),
                        metadata JSONB
                    )
                """)
                
                # Create indexes for better performance
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_env_data_timestamp 
                    ON environmental_data(recorded_at)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_env_data_device 
                    ON environmental_data(device_id)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_env_data_location 
                    ON environmental_data(location)
                """)
                
                self.connection.commit()
                self.logger.info("Database tables initialized successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            self.connection.rollback()
            return False
        finally:
            self.disconnect()
    
    def insert_sensor_reading(self, data: Dict[str, Any]):
        """Insert a sensor reading into the database"""
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO sensor_readings 
                    (device_id, sensor_type, timestamp, value, unit, location, topic, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data.get('device_id'),
                    data.get('sensor_type'),
                    data.get('timestamp'),
                    data.get('value'),
                    data.get('unit'),
                    data.get('location'),
                    data.get('topic'),
                    data.get('raw_data')
                ))
                
                self.connection.commit()
                self.logger.debug(f"Inserted sensor reading: {data.get('device_id')}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to insert sensor reading: {e}")
            self.connection.rollback()
            return False
        finally:
            self.disconnect()
    
    def insert_environmental_data(self, data: Dict[str, Any]):
        """Insert environmental data (temperature, humidity, air quality)"""
        if not self.connect():
            return False
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO environmental_data 
                    (device_id, recorded_at, temperature, humidity, pm2_5, pm10, co2, location, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data.get('device_id'),
                    data.get('timestamp'),
                    data.get('temperature'),
                    data.get('humidity'),
                    data.get('pm2_5'),
                    data.get('pm10'),
                    data.get('co2'),
                    data.get('location'),
                    json.dumps(data.get('metadata')) if data.get('metadata') else None
                ))
                
                self.connection.commit()
                self.logger.info(f"Inserted environmental data from device: {data.get('device_id')}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to insert environmental data: {e}")
            self.connection.rollback()
            return False
        finally:
            self.disconnect()
    
    def get_recent_readings(self, limit: int = 100):
        """Get recent environmental readings"""
        if not self.connect():
            return []
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM environmental_data 
                    ORDER BY recorded_at DESC 
                    LIMIT %s
                """, (limit,))
                
                return cursor.fetchall()
                
        except Exception as e:
            self.logger.error(f"Failed to fetch readings: {e}")
            return []
        finally:
            self.disconnect()
    
    def get_device_readings(self, device_id: str, limit: int = 50):
        """Get readings for a specific device"""
        if not self.connect():
            return []
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM environmental_data 
                    WHERE device_id = %s 
                    ORDER BY recorded_at DESC 
                    LIMIT %s
                """, (device_id, limit))
                
                return cursor.fetchall()
                
        except Exception as e:
            self.logger.error(f"Failed to fetch device readings: {e}")
            return []
        finally:
            self.disconnect()

# Test the handler
if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    
    handler = PostgresHandler()
    
    # Test data
    test_data = {
        'device_id': 'sensor_001',
        'timestamp': '2024-01-09T10:30:00',
        'temperature': 25.5,
        'humidity': 60.0,
        'pm2_5': 15,
        'pm10': 30,
        'co2': 450,
        'location': 'room_a',
        'metadata': {'battery': 85}
    }
    
    # Test insertion
    success = handler.insert_environmental_data(test_data)
    print(f"Insertion successful: {success}")
    
    # Test retrieval
    readings = handler.get_recent_readings(5)
    print(f"Recent readings: {len(readings)} records")
    
    for reading in readings:
        print(f"  {reading['recorded_at']} - Temp: {reading['temperature']}°C")