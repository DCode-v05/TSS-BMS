import ssl
import paho.mqtt.client as mqtt
import json
import random
from datetime import datetime, timezone
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS IoT configuration
AWS_IOT_ENDPOINT = "agx4cnp1fkwi-ats.iot.ap-south-1.amazonaws.com"
PORT = 8883
CLIENT_ID = "ki_tss_publisher_1"
SENSOR_TOPIC = "ki/tss/sensor"
WIND_TOPIC = "ki/tss/all"
CA_PATH = "AmazonRootCA1.pem"
CERT_PATH = "certificate.crt"
KEY_PATH = "private.key"

# Initialize previous values for gradual changes
previous_values = {
    'voltage': 350.0,
    'current': 50.0,
    'soc': 80.0,
    'temp': 25.0,
    'pdutemp': 22.0,
    'power': 100.0,
    'lat': 12.345678,
    'long': 78.901234,
    'wind_speed': 10.0,
    'wind_direction': 180.0
}

def generate_dummy_payloads():
    global previous_values
    # Generate current timestamp in seconds since epoch (UTC)
    timestamp = datetime.now(timezone.utc).timestamp()
    
    # Generate new values with small deviations from previous values
    new_values = {
        'timestamp': timestamp,
        'speed': 0.0,  # Always zero as per original code
        'voltage': round(previous_values['voltage'] + random.uniform(-0.1, 0.1), 2),  # ±0.1 V
        'current': round(previous_values['current'] + random.uniform(-0.1, 0.1), 2),  # ±0.1 A
        'soc': round(previous_values['soc'] + random.uniform(-0.05, 0.05), 2),       # ±0.05%
        'temp': round(previous_values['temp'] + random.uniform(-0.1, 0.1), 2),       # ±0.1°C
        'pdutemp': round(previous_values['pdutemp'] + random.uniform(-0.1, 0.1), 2), # ±0.1°C
        'power': round(previous_values['power'] + random.uniform(-1.0, 1.0), 2),     # ±1.0 W
        'lat': round(previous_values['lat'] + random.uniform(-0.00001, 0.00001), 6), # ±0.00001°
        'long': round(previous_values['long'] + random.uniform(-0.00001, 0.00001), 6), # ±0.00001°
        'wind_speed': round(previous_values['wind_speed'] + random.uniform(-0.1, 0.1), 2), # ±0.1 knots
        'wind_direction': round(previous_values['wind_direction'] + random.uniform(-1.0, 1.0), 1), # ±1°
    }
    
    # Update previous values for the next iteration with bounds
    previous_values.update({
        'voltage': max(349.0, min(351.0, new_values['voltage'])),  # Keep within 349-351 V
        'current': max(49.0, min(51.0, new_values['current'])),    # Keep within 49-51 A
        'soc': max(79.0, min(81.0, new_values['soc'])),            # Keep within 79-81%
        'temp': max(24.0, min(26.0, new_values['temp'])),          # Keep within 24-26°C
        'pdutemp': max(21.0, min(23.0, new_values['pdutemp'])),    # Keep within 21-23°C
        'power': max(90.0, min(110.0, new_values['power'])),       # Keep within 90-110 W
        'lat': new_values['lat'],                                  # Allow small drift
        'long': new_values['long'],                                # Allow small drift
        'wind_speed': max(9.5, min(10.5, new_values['wind_speed'])), # Keep within 9.5-10.5 knots
        'wind_direction': max(175.0, min(185.0, new_values['wind_direction'])) # Keep within 175-185°
    })
    
    # Split into sensor and wind payloads
    sensor_payload = {
        'timestamp': timestamp,
        'speed': new_values['speed'],
        'voltage': new_values['voltage'],
        'current': new_values['current'],
        'soc': new_values['soc'],
        'temp': new_values['temp'],
        'pdutemp': new_values['pdutemp'],
        'power': new_values['power'],
        'lat': new_values['lat'],
        'long': new_values['long']
    }
    
    wind_payload = {
        'timestamp': timestamp,
        'wind_speed': new_values['wind_speed'],
        'wind_direction': new_values['wind_direction']
    }
    
    return sensor_payload, wind_payload

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT broker successfully")
    else:
        logger.error(f"Connection failed with code {rc}")

def on_publish(client, userdata, mid):
    logger.debug(f"Message published with id: {mid}")

def main():
    client = mqtt.Client(client_id=CLIENT_ID)

    # Configure TLS
    client.tls_set(
        ca_certs=CA_PATH,
        certfile=CERT_PATH,
        keyfile=KEY_PATH,
        tls_version=ssl.PROTOCOL_TLSv1_2,
        ciphers=None
    )

    client.on_connect = on_connect
    client.on_publish = on_publish
    client.enable_logger()

    try:
        client.connect(AWS_IOT_ENDPOINT, PORT, keepalive=60)
        client.loop_start()
    except Exception as e:
        logger.error(f"Failed to connect to MQTT broker: {str(e)}")
        raise

    try:
        while True:
            sensor_payload, wind_payload = generate_dummy_payloads()
            
            # Publish to sensor topic
            result_sensor = client.publish(SENSOR_TOPIC, json.dumps(sensor_payload), qos=1)
            if result_sensor[0] == 0:
                logger.info(f"Sent sensor data to {SENSOR_TOPIC}: {sensor_payload}")
            else:
                logger.error(f"Failed to send sensor data to {SENSOR_TOPIC}")
            
            # Publish to wind topic
            result_wind = client.publish(WIND_TOPIC, json.dumps(wind_payload), qos=1)
            if result_wind[0] == 0:
                logger.info(f"Sent wind data to {WIND_TOPIC}: {wind_payload}")
            else:
                logger.error(f"Failed to send wind data to {WIND_TOPIC}")
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Exiting...")
    except Exception as e:
        logger.error(f"Error in main loop: {str(e)}")
    finally:
        client.loop_stop()
        client.disconnect()
        logger.info("Disconnected from MQTT broker")

if __name__ == "__main__":
    main()