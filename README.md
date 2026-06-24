# TSS-BMS

**Real-time telemetry, battery monitoring, and AI speed prediction for an electric race boat — one FastAPI service fed by AWS IoT Core over MQTT.**

![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat&logo=python&logoColor=white) ![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white) ![XGBoost](https://img.shields.io/badge/XGBoost-EB5E28?style=flat&logo=xgboost&logoColor=white) ![AWS IoT Core](https://img.shields.io/badge/AWS%20IoT%20Core-232F3E?style=flat&logo=amazonaws&logoColor=white) ![MQTT](https://img.shields.io/badge/MQTT-660066?style=flat&logo=mqtt&logoColor=white) ![Leaflet](https://img.shields.io/badge/Leaflet-199900?style=flat&logo=leaflet&logoColor=white) ![Chart.js](https://img.shields.io/badge/Chart.js-FF6384?style=flat&logo=chartdotjs&logoColor=white) ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

## Overview

TSS-BMS is the monitoring and battery-management dashboard I built for **Team Sea Sakthi**, an electric/solar race boat, while working on iQube's projects around May 2025. The boat carries a battery pack, motor, GPS, and a wind sensor, and during a race the pilot and the shore crew need to see what's happening *right now* — state of charge, voltage, current draw, temperatures, position on the course, lap count, and how fast the boat should be going given the power it's pulling.

This repository is the deployable backend-plus-dashboard that does that. A FastAPI service subscribes to two MQTT topics on **AWS IoT Core**, fuses the boat's sensor stream with a separate weather (wind) stream, derives race information like heading, wind type, and lap count, runs a trained **XGBoost** model to predict boat speed from electrical and wind inputs, and pushes everything to two browser dashboards over Server-Sent Events. It runs from a single container and exposes a small REST + SSE API.

The endpoint, certificates, and topic names in the code point at the actual deployment that ran during testing. A bundled MQTT publisher lets you replay realistic dummy telemetry so the whole thing works on a laptop without the boat.

## Key Features

- **Dual-stream MQTT ingestion** — subscribes to a sensor topic (`ki/tss/sensor`) and a weather topic (`ki/tss/all`) on AWS IoT Core over mutual-TLS (port 8883, TLS 1.2 with device certificates).
- **Timestamp fusion** — sensor and wind messages arrive independently. The service buffers each stream keyed by epoch timestamp and pairs them when timestamps match exactly or fall within 2 seconds, producing one combined data point. Unmatched entries expire after a 5-second timeout, with a periodic cleanup task every 30 seconds.
- **Automated lap counting** — uses the Haversine formula against a captured start position. The first fused point sets the start; each time the boat re-enters a 15 m geofence around it (having previously been outside), the lap counter increments. Race start is detected automatically.
- **Boat heading and wind classification** — computes the boat's bearing from consecutive GPS fixes, then classifies the wind as **Headwind**, **Tailwind**, or **Crosswind** based on the angle between heading and wind direction. Wind direction in degrees is also bucketed into 8-point compass text (N/NE/E/SE/S/SW/W/NW).
- **AI speed prediction** — a pre-trained XGBoost regressor (`xgb_speed_model.pkl`) predicts boat speed from voltage, current, SOC, temperature, PDU temperature, power, and wind features. Output is clamped to a sane 0–100 range. Available both inline (added to every data point) and as a standalone `POST /battery/predict_speed` endpoint.
- **Live push via SSE** — `/battery/stream` and `/map/stream` are Server-Sent Event endpoints that emit each new fused data point as it lands, with a 1-second keep-alive ping.
- **REST API with time filtering** — `/battery/data` and `/map/data` return the in-memory history with optional `start_time` / `end_time` ISO filtering plus `limit` / `offset` pagination.
- **Race-buoy management** — `POST /map/buoys` sets three course-buoy positions and `GET /map/buoys` returns them, so the map dashboard can draw the course.
- **Health and debug endpoints** — `/battery/health` and `/map/health` report MQTT connectivity, data freshness (<10 s), buffer sizes, lap count, and active SSE connections; `/debug/buffers` exposes the raw fusion-buffer state.
- **Two dashboards** — a battery/wind dashboard (`batteryIndex.html`, Chart.js gauges and time-series) and a live race map (`index.html`, Leaflet with boat position and buoys).
- **Telemetry simulator** — `MQTT_Data_Push.py` publishes realistic dummy sensor + wind payloads once per second (bounded random walk: ~350 V, ~50 A, ~80% SOC, ~10 kn wind), so you can run the full pipeline with no hardware.
- **Containerized** — single Dockerfile on `python:3.12-slim`, served by Uvicorn.

## How It Works

The service is a single FastAPI app (`main.py`, ~720 lines) that owns an MQTT client, an in-memory data store, and the HTTP/SSE API. The flow is: MQTT in → fuse → enrich → store → stream out.

### 1. Ingestion (MQTT → buffers)

On startup the app builds a `paho-mqtt` client, configures TLS with `AmazonRootCA1.pem` / `certificate.crt` / `private.key`, connects to the AWS IoT Core endpoint, and subscribes to both topics. Every message hits `on_message`, which validates required fields and drops the payload into either `sensor_buffer` or `weather_buffer`, keyed by its integer epoch timestamp and tagged with the local receive time. A disconnect handler attempts reconnection.

### 2. Fusion (two streams → one point)

After each message, `try_create_complete_data()` looks for timestamps present in both buffers — exact matches first, then any pair within 2 seconds. For each match it pulls the closest sensor and weather payloads, then builds a single fused record. Buffer entries older than 5 seconds are pruned so a half-arrived point doesn't sit around forever.

### 3. Enrichment (the race logic)

For every fused point the service computes:

- **Lap tracking** — Haversine distance to the recorded start position; a 15 m re-entry geofence drives the lap counter.
- **Heading** — great-circle bearing from the previous GPS fix to the current one.
- **Wind type** — angle difference between boat heading and wind direction, bucketed into Headwind (150–210°), Tailwind (0–30° / 330–360°), or Crosswind otherwise.
- **Predicted speed** — wind direction is converted to compass text, then voltage, current, SOC, temperature, PDU temp, power, and wind features are assembled into a pandas DataFrame and run through the XGBoost model.

The finished record (raw sensor values + wind + heading + wind type + lap count + AI speed, timestamped in IST) is appended to a `deque(maxlen=1000)` — a fixed 1000-point ring buffer that holds the recent history, guarded by an `asyncio` lock.

### 4. Serving (REST + SSE → dashboards)

The API layer reads from that ring buffer. REST endpoints support time-range filtering and pagination; the two SSE endpoints watch the buffer length and yield each newly appended point to connected browsers, with a 1 Hz ping to keep the connection alive. CORS is open so the static dashboards (served from `/static` and `/`) can talk to it. The map endpoints additionally manage the three race buoys.

### 5. The model

`xgb_speed_model.pkl` is a saved XGBoost regressor loaded with `joblib` at boot (the app refuses to start if the file is missing). It maps battery/electrical state and wind conditions to a speed estimate, so the dashboard can show **actual vs. AI-predicted speed** side by side — useful for spotting when the boat is under- or over-performing for the power it's drawing. The model is shipped pre-trained; training happened offline against captured race telemetry and isn't part of this repository.

### Running without the boat

`MQTT_Data_Push.py` is a standalone publisher that connects to the same AWS IoT endpoint with the same certs and emits a sensor payload and a wind payload every second. Values drift gradually inside fixed bounds (voltage 349–351 V, current 49–51 A, SOC 79–81%, temp 24–26°C, wind 9.5–10.5 kn, wind direction 175–185°) and the GPS coordinates jitter slightly, so the dashboards animate as if a real boat were on the water.

## Tech Stack

- **Languages:** Python 3.12, JavaScript, HTML/CSS
- **Backend / API:** FastAPI, Uvicorn, Pydantic, Server-Sent Events
- **IoT / messaging:** AWS IoT Core, MQTT (paho-mqtt) over mutual-TLS
- **Data / ML:** XGBoost, joblib, pandas, NumPy
- **Frontend:** Chart.js (battery + wind charts), Leaflet.js (race map), vanilla JS
- **Infra:** Docker (python:3.12-slim base, Uvicorn)

## Getting Started

### Prerequisites

- Python 3.12 (or Docker)
- AWS IoT Core device certificates placed in the project root: `AmazonRootCA1.pem`, `certificate.crt`, `private.key`
- An AWS IoT endpoint / topics, or use the bundled simulator to generate data

### Installation

```bash
git clone https://github.com/DCode-v05/TSS-BMS.git
cd TSS-BMS
pip install -r requirements.txt
```

### Running

```bash
# start the FastAPI service
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# in a second terminal, feed it simulated telemetry (optional)
python MQTT_Data_Push.py
```

Or with Docker:

```bash
docker build -t tss-bms .
docker run -p 8000:8000 tss-bms
```

The app loads `xgb_speed_model.pkl` and connects to MQTT on startup, so the model file and the three certificate files must be present in the working directory or it will refuse to boot.

## Usage

Open `http://localhost:8000/` for the race map dashboard, or `http://localhost:8000/static/batteryIndex.html` for the battery/wind dashboard. With the simulator running you'll see live gauges, the boat moving on the map, lap counts ticking up, and actual-vs-predicted speed.

API quick reference:

| Method | Endpoint | What it does |
| --- | --- | --- |
| GET | `/battery/data` | Battery + sensor history (filterable, paginated) |
| GET | `/map/data` | Same dataset, for the map view |
| POST | `/battery/predict_speed` | Predict speed from a raw feature payload |
| GET | `/battery/stream` | SSE live battery feed |
| GET | `/map/stream` | SSE live map feed |
| POST/GET | `/map/buoys` | Set / read the three race-course buoys |
| GET | `/battery/health`, `/map/health` | MQTT + data-freshness status |
| GET | `/debug/buffers` | Inspect the fusion buffers |

Example prediction request:

```bash
curl -X POST http://localhost:8000/battery/predict_speed \
  -H "Content-Type: application/json" \
  -d '{"voltage":350,"current":50,"soc":80,"temperature":25,"pdutemp":22,"power":100,"wind_dir_text":"South","wind_speed":10,"wind_direction":180}'
```

## Project Structure

```
TSS-BMS/
├── main.py                   # FastAPI app: MQTT ingest, stream fusion, lap/heading/wind logic,
│                             #   XGBoost inference, REST + SSE endpoints
├── MQTT_Data_Push.py         # Telemetry simulator — publishes dummy sensor + wind data over MQTT
├── xgb_speed_model.pkl       # Pre-trained XGBoost speed regressor (loaded with joblib)
├── requirements.txt          # Python dependencies
├── Dockerfile                # python:3.12-slim + Uvicorn
├── static/
│   ├── index.html            # Leaflet race-map dashboard (boat position, buoys, laps)
│   └── batteryIndex.html     # Chart.js battery + wind dashboard (SOC, V, A, temps, speed)
└── README.md
```

---

## Contact

**Portfolio:** [Denistan](https://www.denistan.me)<br>
**LinkedIn:** [Denistan](https://www.linkedin.com/in/denistanb)<br>
**GitHub:** [DCode-v05](https://github.com/DCode-v05)<br>
**LeetCode:** [Denistan_B](https://leetcode.com/u/Denistan_B)<br>
**Email:** [denistanb05@gmail.com](mailto:denistanb05@gmail.com)

Made with ❤️ by **Denistan B**
