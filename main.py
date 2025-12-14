from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt
import json
import logging
import ssl
import asyncio
import os
from collections import deque
from zoneinfo import ZoneInfo
import math
import joblib
import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Boat Monitoring and Battery API",
    description="Real-time boat monitoring and battery management API via MQTT",
    version="1.0.0"
)

# CORS config
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files
STATIC_DIR = os.getenv("STATIC_DIR", "./static")
if not os.path.exists(STATIC_DIR):
    logger.error(f"Static directory {STATIC_DIR} not found")
    raise RuntimeError(f"Static directory {STATIC_DIR} not found")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Load AI model
MODEL_PATH = "xgb_speed_model.pkl"
if not os.path.exists(MODEL_PATH):
    logger.error(f"Model file {MODEL_PATH} not found")
    raise RuntimeError(f"Model file {MODEL_PATH} not found")
try:
    xgb_model = joblib.load(MODEL_PATH)
    logger.info(f"Loaded XGBoost model from {MODEL_PATH}")
except Exception as e:
    logger.error(f"Failed to load model {MODEL_PATH}: {str(e)}")
    raise RuntimeError(f"Failed to load model {MODEL_PATH}: {str(e)}")

# Data models
class SensorData(BaseModel):
    timestamp: str
    speed: float
    voltage: float
    current: float
    soc: float
    temperature: float
    pdutemp: float
    power: float
    lat: float
    long: float
    wind_speed: float
    wind_direction: float
    boat_direction: float
    wind_type: str
    lap_count: int
    ai_predicted_speed: float = 0.0

class SensorDataResponse(BaseModel):
    data: List[SensorData]
    count: int
    latest_timestamp: str
    status: str

class PredictSpeedRequest(BaseModel):
    voltage: float
    current: float
    soc: float
    temperature: float
    pdutemp: float
    power: float
    wind_dir_text: str
    wind_speed: float
    wind_direction: float

class BuoyData(BaseModel):
    buoy1_lat: float
    buoy1_lon: float
    buoy2_lat: float
    buoy2_lon: float
    buoy3_lat: float
    buoy3_lon: float

# MQTT configuration
AWS_IOT_ENDPOINT = "agx4cnp1fkwi-ats.iot.ap-south-1.amazonaws.com"
PORT = 8883
CLIENT_ID = "ki_tss_receiver_134"
SENSOR_TOPIC = "ki/tss/sensor"
WEATHER_TOPIC = "ki/tss/all"
CA_PATH = "AmazonRootCA1.pem"
CERT_PATH = "certificate.crt"
KEY_PATH = "private.key"

# In-memory storage
MAX_DATA_POINTS = 1000
sensor_data = deque(maxlen=MAX_DATA_POINTS)
data_lock = asyncio.Lock()

# Separate storage for incomplete data from each topic
sensor_buffer = {}  # Store sensor data temporarily
weather_buffer = {}  # Store weather data temporarily
BUFFER_TIMEOUT = 5  # seconds - how long to wait for matching data

last_data_time = None
active_connections = 0
last_position = None
start_position = None
lap_count = 0
race_started = False
buoy_positions = []

# Haversine formula
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# AI model prediction
def predict_speed(data: PredictSpeedRequest) -> float:
    try:
        wind_dir_map = {
            'East': np.int64(0), 'NE': np.int64(1), 'NW': np.int64(2), 'North': np.int64(3),
            'SE': np.int64(4), 'SW': np.int64(5), 'South': np.int64(6), 'West': np.int64(7)
        }
        wind_dir_num = wind_dir_map.get(data.wind_dir_text, 0)
        
        features = pd.DataFrame([[
            data.voltage, data.current, data.soc, data.temperature, data.pdutemp,
            data.power, wind_dir_num, data.wind_speed, data.wind_direction
        ]], columns=[
            'voltage', 'current', 'soc', 'temp', 'pdutemp', 'power',
            'wind_dir_text', 'wind_speed', 'wind_dir_deg'
        ])
        
        predicted_speed = xgb_model.predict(features)[0]
        return max(0, min(float(predicted_speed), 100))
    except Exception as e:
        logger.error(f"Error predicting speed with XGBoost model: {str(e)}")
        return 0.0

# Calculate boat direction
def calculate_boat_direction(current_lat, current_long, last_lat, last_long):
    if last_lat is None or last_long is None:
        return 0.0
    delta_lat = math.radians(current_lat - last_lat)
    delta_long = math.radians(current_long - last_long)
    x = math.sin(delta_long) * math.cos(math.radians(current_lat))
    y = math.cos(math.radians(last_lat)) * math.sin(math.radians(current_lat)) - \
        math.sin(math.radians(last_lat)) * math.cos(math.radians(current_lat)) * math.cos(delta_long)
    bearing = math.degrees(math.atan2(x, y))
    return (bearing + 360) % 360

# Calculate wind type
def calculate_wind_type(boat_direction, wind_direction):
    angle_diff = (wind_direction - boat_direction + 360) % 360
    if 150 <= angle_diff <= 210:
        return "Headwind"
    elif 0 <= angle_diff <= 30 or 330 <= angle_diff <= 360:
        return "Tailwind"
    else:
        return "Crosswind"

# Clean up old buffer entries
def cleanup_buffers():
    current_time = datetime.now()
    timeout_threshold = timedelta(seconds=BUFFER_TIMEOUT)
    
    # Clean sensor buffer
    expired_keys = [
        key for key, data in sensor_buffer.items()
        if current_time - data['received_at'] > timeout_threshold
    ]
    for key in expired_keys:
        del sensor_buffer[key]
        logger.warning(f"Removed expired sensor data for timestamp {key}")
    
    # Clean weather buffer
    expired_keys = [
        key for key, data in weather_buffer.items()
        if current_time - data['received_at'] > timeout_threshold
    ]
    for key in expired_keys:
        del weather_buffer[key]
        logger.warning(f"Removed expired weather data for timestamp {key}")

# Try to create complete data point from buffers
def try_create_complete_data():
    global last_position, start_position, lap_count, race_started, last_data_time
    
    cleanup_buffers()
    
    # Find matching timestamps between sensor and weather data
    sensor_timestamps = set(sensor_buffer.keys())
    weather_timestamps = set(weather_buffer.keys())
    
    # Look for exact matches first
    matching_timestamps = sensor_timestamps.intersection(weather_timestamps)
    
    if not matching_timestamps:
        # If no exact matches, try to find close timestamps (within 2 seconds)
        for s_ts in sensor_timestamps:
            for w_ts in weather_timestamps:
                if abs(s_ts - w_ts) <= 2:  # Within 2 seconds
                    matching_timestamps.add(s_ts)  # Use sensor timestamp as primary
                    break
    
    complete_data_points = []
    
    for timestamp in matching_timestamps:
        try:
            # Get sensor data
            if timestamp in sensor_buffer:
                sensor_payload = sensor_buffer[timestamp]['data']
            else:
                # Find closest sensor data within tolerance
                closest_sensor_ts = min(
                    sensor_timestamps,
                    key=lambda x: abs(x - timestamp)
                )
                if abs(closest_sensor_ts - timestamp) <= 2:
                    sensor_payload = sensor_buffer[closest_sensor_ts]['data']
                else:
                    continue
            
            # Get weather data
            if timestamp in weather_buffer:
                weather_payload = weather_buffer[timestamp]['data']
            else:
                # Find closest weather data within tolerance
                closest_weather_ts = min(
                    weather_timestamps,
                    key=lambda x: abs(x - timestamp)
                )
                if abs(closest_weather_ts - timestamp) <= 2:
                    weather_payload = weather_buffer[closest_weather_ts]['data']
                else:
                    continue
            
            # Create complete data point
            iso_timestamp = datetime.fromtimestamp(timestamp, tz=ZoneInfo('Asia/Kolkata')).isoformat()
            current_lat = float(sensor_payload['lat'])
            current_long = float(sensor_payload['long'])
            
            # Race tracking logic
            if not race_started:
                start_position = {'lat': current_lat, 'long': current_long}
                race_started = True
                lap_count = 0
                logger.info(f"Race started. Start position: {start_position}")

            if start_position:
                distance = haversine(current_lat, current_long, start_position['lat'], start_position['long'])
                if distance <= 15 and last_position and haversine(last_position['lat'], last_position['long'], start_position['lat'], start_position['long']) > 15:
                    lap_count += 1
                    logger.info(f"Lap completed: {lap_count}")

            # Calculate boat direction
            boat_direction = calculate_boat_direction(
                current_lat, current_long,
                last_position['lat'] if last_position else None,
                last_position['long'] if last_position else None
            )
            last_position = {'lat': current_lat, 'long': current_long}
            
            # Wind calculations
            wind_direction = float(weather_payload['wind_direction'])
            wind_type = calculate_wind_type(boat_direction, wind_direction)

            wind_angle = wind_direction % 360
            if 22.5 <= wind_angle < 67.5:
                wind_dir_text = 'NE'
            elif 67.5 <= wind_angle < 112.5:
                wind_dir_text = 'East'
            elif 112.5 <= wind_angle < 157.5:
                wind_dir_text = 'SE'
            elif 157.5 <= wind_angle < 202.5:
                wind_dir_text = 'South'
            elif 202.5 <= wind_angle < 247.5:
                wind_dir_text = 'SW'
            elif 247.5 <= wind_angle < 292.5:
                wind_dir_text = 'West'
            elif 292.5 <= wind_angle < 337.5:
                wind_dir_text = 'NW'
            else:
                wind_dir_text = 'North'

            # AI prediction
            ai_input = PredictSpeedRequest(
                voltage=float(sensor_payload['voltage']),
                current=float(sensor_payload['current']),
                soc=float(sensor_payload['soc']),
                temperature=float(sensor_payload['temp']),
                pdutemp=float(sensor_payload['pdutemp']),
                power=float(sensor_payload['power']),
                wind_dir_text=wind_dir_text,
                wind_speed=float(weather_payload['wind_speed']),
                wind_direction=wind_direction
            )
            ai_predicted_speed = predict_speed(ai_input)

            # Create complete data point
            data_point = {
                'timestamp': iso_timestamp,
                'speed': float(sensor_payload['speed']),
                'voltage': float(sensor_payload['voltage']),
                'current': float(sensor_payload['current']),
                'soc': float(sensor_payload['soc']),
                'temperature': float(sensor_payload['temp']),
                'pdutemp': float(sensor_payload['pdutemp']),
                'power': float(sensor_payload['power']),
                'lat': current_lat,
                'long': current_long,
                'wind_speed': float(weather_payload['wind_speed']),
                'wind_direction': wind_direction,
                'boat_direction': boat_direction,
                'wind_type': wind_type,
                'lap_count': lap_count,
                'ai_predicted_speed': ai_predicted_speed
            }
            
            complete_data_points.append(data_point)
            
            # Remove processed data from buffers
            if timestamp in sensor_buffer:
                del sensor_buffer[timestamp]
            if timestamp in weather_buffer:
                del weather_buffer[timestamp]
            
            logger.info(f"Created complete data point: {iso_timestamp}, Lap: {lap_count}, Wind: {wind_type}, AI speed: {ai_predicted_speed}")
            
        except Exception as e:
            logger.error(f"Error creating complete data point for timestamp {timestamp}: {str(e)}")
            continue
    
    # Add complete data points to main storage
    if complete_data_points:
        async def add_data():
            async with data_lock:
                for data_point in complete_data_points:
                    sensor_data.append(data_point)
                global last_data_time
                last_data_time = datetime.now()
        asyncio.run(add_data())

# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    logger.info(f"Connected to MQTT broker with result code {rc}")
    if rc == 0:
        client.subscribe(SENSOR_TOPIC)
        client.subscribe(WEATHER_TOPIC)
        logger.info(f"Subscribed to topics: {SENSOR_TOPIC}, {WEATHER_TOPIC}")
    else:
        logger.error(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        logger.debug(f"Received message from topic '{msg.topic}': {payload}")
        
        if 'timestamp' not in payload:
            logger.warning(f"Missing timestamp in payload from topic {msg.topic}: {payload}")
            return
        
        timestamp = int(payload['timestamp'])
        current_time = datetime.now()
        
        if msg.topic == SENSOR_TOPIC:
            # Validate sensor data fields
            required_sensor_fields = [
                'timestamp', 'speed', 'voltage', 'current', 'soc', 'temp', 'pdutemp',
                'power', 'lat', 'long'
            ]
            if not all(field in payload for field in required_sensor_fields):
                logger.warning(f"Missing sensor fields in payload: {payload}")
                return
            
            sensor_buffer[timestamp] = {
                'data': payload,
                'received_at': current_time
            }
            logger.debug(f"Stored sensor data for timestamp {timestamp}")
            
        elif msg.topic == WEATHER_TOPIC:
            # Validate weather data fields
            required_weather_fields = ['timestamp', 'wind_speed', 'wind_direction']
            if not all(field in payload for field in required_weather_fields):
                logger.warning(f"Missing weather fields in payload: {payload}")
                return
            
            weather_buffer[timestamp] = {
                'data': payload,
                'received_at': current_time
            }
            logger.debug(f"Stored weather data for timestamp {timestamp}")
        
        # Try to create complete data points
        try_create_complete_data()
        
    except Exception as e:
        logger.error(f"Error processing MQTT message from topic {msg.topic}: {str(e)}")

def on_disconnect(client, userdata, rc):
    logger.warning(f"Disconnected from MQTT broker with result code {rc}")
    if rc != 0:
        logger.info("Attempting to reconnect...")
        try:
            client.reconnect()
        except Exception as e:
            logger.error(f"Reconnection failed: {str(e)}")

# Initialize MQTT client
mqtt_client = mqtt.Client(client_id=CLIENT_ID)
mqtt_client.tls_set(
    ca_certs=CA_PATH,
    certfile=CERT_PATH,
    keyfile=KEY_PATH,
    tls_version=ssl.PROTOCOL_TLSv1_2,
    ciphers=None
)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.enable_logger()

try:
    mqtt_client.connect(AWS_IOT_ENDPOINT, PORT, keepalive=60)
    mqtt_client.loop_start()
except Exception as e:
    logger.error(f"Failed to connect to MQTT broker: {str(e)}")
    raise RuntimeError(f"Failed to connect to MQTT broker: {str(e)}")

# Periodic cleanup task
async def periodic_cleanup():
    while True:
        await asyncio.sleep(30)  # Run every 30 seconds
        cleanup_buffers()

# Start background cleanup task
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(periodic_cleanup())

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down MQTT client")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

@app.get("/")
async def root():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(index_path):
        logger.error(f"index.html not found at {index_path}")
        raise HTTPException(status_code=404, detail="index.html not found")
    logger.info("Serving index.html")
    return FileResponse(index_path)

@app.get("/battery/data", response_model=SensorDataResponse)
async def get_battery_data(
    start_time: str = Query(None, description="ISO timestamp for start of range"),
    end_time: str = Query(None, description="ISO timestamp for end of range"),
    limit: int = Query(1000, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    try:
        logger.info("Received request for /battery/data")
        async with data_lock:
            data = list(sensor_data)
        
        if not data:
            logger.info("No data available, waiting for MQTT messages")
            return SensorDataResponse(
                data=[],
                count=0,
                latest_timestamp="",
                status="waiting"
            )

        filtered_data = data
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time)
                filtered_data = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) >= start_dt]
            except ValueError:
                raise HTTPException(status_code=422, detail="Invalid start_time format")
        if end_time:
            try:
                end_dt = datetime.fromisoformat(end_time)
                filtered_data = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) <= end_dt]
            except ValueError:
                raise HTTPException(status_code=422, detail="Invalid end_time format")

        paginated_data = filtered_data[offset:offset + limit]
        logger.info(f"Returning {len(paginated_data)} rows of battery data")
        return SensorDataResponse(
            data=paginated_data,
            count=len(paginated_data),
            latest_timestamp=paginated_data[-1]["timestamp"] if paginated_data else "",
            status="available"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving battery data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving battery data: {str(e)}")

@app.post("/battery/predict_speed")
async def get_predicted_speed(data: PredictSpeedRequest):
    try:
        predicted_speed = predict_speed(data)
        return {"predicted_speed": predicted_speed}
    except Exception as e:
        logger.error(f"Error predicting speed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error predicting speed: {str(e)}")

@app.get("/battery/stream")
async def stream_battery_data():
    global active_connections
    async def event_generator():
        global active_connections
        try:
            async with data_lock:
                active_connections += 1
            logger.info(f"New battery SSE connection opened. Active connections: {active_connections}")
            last_count = len(sensor_data)
            last_ping = datetime.now()
            while True:
                async with data_lock:
                    current_count = len(sensor_data)
                    if current_count > last_count:
                        new_data = list(sensor_data)[last_count:current_count]
                        for data_point in new_data:
                            yield f"data: {json.dumps(data_point)}\n\n"
                        last_count = current_count
                if (datetime.now() - last_ping).total_seconds() >= 1:
                    yield "data: ping\n\n"
                    last_ping = datetime.now()
                await asyncio.sleep(0.1)
        finally:
            async with data_lock:
                active_connections -= 1
            logger.info(f"Battery SSE connection closed. Active connections: {active_connections}")
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/battery/health")
async def battery_health_check():
    mqtt_status = "connected" if mqtt_client.is_connected() else "disconnected"
    data_fresh = last_data_time is not None and (datetime.now() - last_data_time).total_seconds() < 10
    logger.info("Battery health check requested")
    return {
        "status": "healthy" if mqtt_status == "connected" and data_fresh else "unhealthy",
        "timestamp": datetime.now().isoformat(),
        "mqtt_status": mqtt_status,
        "data_count": len(sensor_data),
        "last_data_time": last_data_time.isoformat() if last_data_time else None,
        "data_fresh": data_fresh,
        "active_connections": active_connections,
        "lap_count": lap_count,
        "sensor_buffer_size": len(sensor_buffer),
        "weather_buffer_size": len(weather_buffer)
    }

@app.get("/map/data", response_model=SensorDataResponse)
async def get_map_data(
    start_time: str = Query(None, description="ISO timestamp for start of range"),
    end_time: str = Query(None, description="ISO timestamp for end of range"),
    limit: int = Query(1000, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    try:
        logger.info("Received request for /map/data")
        async with data_lock:
            data = list(sensor_data)
        
        if not data:
            logger.info("No data available, waiting for MQTT messages")
            return SensorDataResponse(
                data=[],
                count=0,
                latest_timestamp="",
                status="waiting"
            )

        filtered_data = data
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time)
                filtered_data = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) >= start_dt]
            except ValueError:
                raise HTTPException(status_code=422, detail="Invalid start_time format")
        if end_time:
            try:
                end_dt = datetime.fromisoformat(end_time)
                filtered_data = [d for d in filtered_data if datetime.fromisoformat(d['timestamp']) <= end_dt]
            except ValueError:
                raise HTTPException(status_code=422, detail="Invalid end_time format")

        paginated_data = filtered_data[offset:offset + limit]
        logger.info(f"Returning {len(paginated_data)} rows of map data")
        return SensorDataResponse(
            data=paginated_data,
            count=len(paginated_data),
            latest_timestamp=paginated_data[-1]["timestamp"] if paginated_data else "",
            status="available"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving map data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving map data: {str(e)}")

@app.get("/map/stream")
async def stream_map_data():
    global active_connections
    async def event_generator():
        global active_connections
        try:
            async with data_lock:
                active_connections += 1
            logger.info(f"New map SSE connection opened. Active connections: {active_connections}")
            last_count = len(sensor_data)
            last_ping = datetime.now()
            while True:
                async with data_lock:
                    current_count = len(sensor_data)
                    if current_count > last_count:
                        new_data = list(sensor_data)[last_count:current_count]
                        for data_point in new_data:
                            yield f"data: {json.dumps(data_point)}\n\n"
                        last_count = current_count
                if (datetime.now() - last_ping).total_seconds() >= 1:
                    yield "data: ping\n\n"
                    last_ping = datetime.now()
                await asyncio.sleep(0.1)
        finally:
            async with data_lock:
                active_connections -= 1
            logger.info(f"Map SSE connection closed. Active connections: {active_connections}")
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/map/buoys")
async def submit_buoys(buoys: BuoyData):
    buoy_positions.clear()
    buoy_positions.extend([
        {"lat": buoys.buoy1_lat, "lon": buoys.buoy1_lon},
        {"lat": buoys.buoy2_lat, "lon": buoys.buoy2_lon},
        {"lat": buoys.buoy3_lat, "lon": buoys.buoy3_lon}
    ])
    logger.info("Buoy positions updated: %s", buoy_positions)
    return {"status": "success", "buoys": buoy_positions}

@app.get("/map/buoys")
async def get_buoys():
    logger.info("Returning buoy positions: %s", buoy_positions)
    return buoy_positions

@app.get("/map/health")
async def map_health_check():
    mqtt_status = "connected" if mqtt_client.is_connected() else "disconnected"
    data_fresh = last_data_time is not None and (datetime.now() - last_data_time).total_seconds() < 10
    logger.info("Map health check requested")
    return {
        "status": "healthy" if mqtt_status == "connected" and data_fresh else "unhealthy",
        "timestamp": datetime.now().isoformat(),
        "mqtt_status": mqtt_status,
        "data_count": len(sensor_data),
        "last_data_time": last_data_time.isoformat() if last_data_time else None,
        "data_fresh": data_fresh,
        "active_connections": active_connections,
        "buoy_count": len(buoy_positions),
        "sensor_buffer_size": len(sensor_buffer),
        "weather_buffer_size": len(weather_buffer)
    }

# Debug endpoint to view buffer status
@app.get("/debug/buffers")
async def debug_buffers():
    return {
        "sensor_buffer": {
            "size": len(sensor_buffer),
            "timestamps": list(sensor_buffer.keys()),
            "oldest": min(sensor_buffer.keys()) if sensor_buffer else None,
            "newest": max(sensor_buffer.keys()) if sensor_buffer else None
        },
        "weather_buffer": {
            "size": len(weather_buffer),
            "timestamps": list(weather_buffer.keys()),
            "oldest": min(weather_buffer.keys()) if weather_buffer else None,
            "newest": max(weather_buffer.keys()) if weather_buffer else None
        },
        "complete_data_count": len(sensor_data)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)