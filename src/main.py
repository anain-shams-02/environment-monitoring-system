import os
from pathlib import Path
from dotenv import load_dotenv

# __file__ = "D:\database mod b\environment_monitoring\src\main.py"
# .parent = "D:\database mod b\environment_monitoring\src\"
# .parent.parent = "D:\database mod b\environment_monitoring\"
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Now test
print("Environment Monitoring System")
print("="*50)
print(f"MQTT: {os.getenv('MQTT_BROKER')}:{os.getenv('MQTT_PORT')}")
print(f"PostgreSQL: {os.getenv('PG_HOST')}:{os.getenv('PG_DATABASE')}")
print(f"MongoDB: {os.getenv('MONGO_HOST')}:{os.getenv('MONGO_DATABASE')}")
print("="*50)

# Test actual values
mqtt_broker = os.getenv('MQTT_BROKER', 'NOT FOUND')
print(f"MQTT Broker value: '{mqtt_broker}'")