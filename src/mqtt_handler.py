import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import logging
from typing import Dict, Callable
import os

class MQTTHandler:
    def __init__(self, broker="localhost", port=1883, keepalive=60):
        self.broker = broker
        self.port = port
        self.keepalive = keepalive
        
        # Create MQTT client
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        
        # Setup callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
        # Message handlers dictionary: topic -> function
        self.message_handlers: Dict[str, Callable] = {}
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        self.connected = False
    
    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            self.connected = True
            self.logger.info(f"Connected to MQTT Broker at {self.broker}:{self.port}")
            
            # Subscribe to all registered topics
            for topic in self.message_handlers.keys():
                client.subscribe(topic)
                self.logger.info(f"Subscribed to topic: {topic}")
        else:
            self.logger.error(f"Connection failed with code: {reason_code}")
    
    def on_message(self, client, userdata, msg):
        try:
            self.logger.debug(f"Received message on [{msg.topic}]: {msg.payload}")
            
            # Decode payload
            payload_str = msg.payload.decode('utf-8')
            
            # Try to parse as JSON
            try:
                data = json.loads(payload_str)
                data['_topic'] = msg.topic
                data['_timestamp'] = datetime.utcnow().isoformat()
            except json.JSONDecodeError:
                # If not JSON, store as raw data
                data = {
                    'raw_data': payload_str,
                    '_topic': msg.topic,
                    '_timestamp': datetime.utcnow().isoformat()
                }
            
            # Call appropriate handler
            if msg.topic in self.message_handlers:
                self.message_handlers[msg.topic](data)
            else:
                self.logger.warning(f"No handler registered for topic: {msg.topic}")
                
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
    
    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self.connected = False
        self.logger.warning(f"Disconnected from MQTT broker. Reason: {reason_code}")
    
    def register_handler(self, topic: str, handler: Callable):
        """Register a handler function for a specific topic"""
        self.message_handlers[topic] = handler
        self.logger.info(f"Registered handler for topic: {topic}")
        
        # If already connected, subscribe to the topic
        if self.connected:
            self.client.subscribe(topic)
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.logger.info(f"Connecting to MQTT broker at {self.broker}:{self.port}...")
            self.client.connect(self.broker, self.port, self.keepalive)
            # Start network loop in background
            self.client.loop_start()
        except Exception as e:
            self.logger.error(f"Failed to connect to MQTT broker: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from MQTT broker"""
        self.client.loop_stop()
        self.client.disconnect()
        self.logger.info("Disconnected from MQTT broker")
    
    def publish(self, topic: str, message, qos=0, retain=False):
        """Publish a message to a topic"""
        try:
            if isinstance(message, dict):
                message = json.dumps(message)
            elif not isinstance(message, str):
                message = str(message)
            
            result = self.client.publish(topic, message, qos=qos, retain=retain)
            self.logger.debug(f"Published to [{topic}]: {message}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to publish message: {e}")
            return None

# Example usage
if __name__ == "__main__":
    # Test the MQTT handler
    handler = MQTTHandler()
    
    # Define a simple handler for testing
    def test_handler(data):
        print(f"Test handler received: {data}")
    
    # Register handler for test topic
    handler.register_handler("test/topic", test_handler)
    
    # Connect
    handler.connect()
    
    try:
        # Keep running
        print("MQTT Handler running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        handler.disconnect()