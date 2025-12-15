# TSS-BMS (Boat Monitoring System)

## Project Description
This project is a comprehensive Boat Monitoring and Battery Management System (BMS) designed for real-time telemetry and race tracking of electric boats. It integrates IoT sensor data, weather conditions, and AI-powered predictive analytics to provide actionable insights. The system is built with a FastAPI backend, connects to AWS IoT Core for data streaming, and features a web-based dashboard for live monitoring of battery health, boat speed, and race progress.

---

## Project Details

### Problem Statement
Efficiently managing electric boat performance requires real-time visibility into battery parameters and environmental conditions. Pilots and race engineers need immediate access to critical data like State of Charge (SOC), voltage, and estimated speed to optimize performance and tracking during competitive events.

### Data Processing
- **Real-time Ingestion:** Consumes MQTT messages from AWS IoT Core for two streams:
  - **Sensors:** Voltage, Current, Power, Temperatures, GPS (Lat/Long).
  - **Weather:** Wind Speed, Wind Direction.
- **Synchronization:** Implements intelligent buffering to time-align asynchronous sensor and weather data streams into unified data points.
- **Race Logic:**
  - Automated **Lap Counting** using GPS geofencing and the Haversine formula.
  - **Wind Analysis**: Calculates "Headwind", "Tailwind", or "Crosswind" relative to the boat's heading.

### Model Training & Evaluation
- **Model Used:** XGBoost Regressor (`xgb_speed_model.pkl`).
- **Feature Engineering:**
  - Inputs: Battery Voltage, Current, SOC, PDU Temperature, and Weather metrics.
  - Target: Boat Speed.
- **Purpose:** Predicts the theoretical speed of the boat based on current power consumption and environmental resistance.

### Web Application
- **Backend:** FastAPI server handling MQTT subscriptions, data processing, and SSE (Server-Sent Events) streaming.
- **Frontend:** Static HTML/JS dashboards (`index.html`, `batteryIndex.html`) visualizing:
  - Battery Gauges (Amps, Volts, SOC).
  - Live Map Tracking with Buoy positions.
  - Speedometer comparing Actual vs. AI-Predicted Speed.

---

## Tech Stack
- **Languages:** Python 3.x, JavaScript, HTML/CSS
- **Frameworks:** FastAPI, Uvicorn
- **IoT & Cloud:** AWS IoT Core, Paho MQTT
- **Machine Learning:** XGBoost, Scikit-learn, Pandas, Joblib
- **Containerization:** Docker

---

## Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/DCode-v05/TSS-BMS.git
cd TSS-BMS
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Setup AWS Credentials
> **Important:** This application requires AWS IoT certificates to connect.
Place the following files in the project root:
- `AmazonRootCA1.pem`
- `certificate.crt`
- `private.key`

### 4. Run the Application
Start the FastAPI backend server:
```bash
python main.py
# OR
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

To simulate sensor data (if no physical device is connected):
```bash
python MQTT_Data_Push.py
```

---

## Usage
- Open your browser and navigate to `http://localhost:8000` to view the main dashboard.
- Access `/battery/data` or `/map/data` for REST API endpoints.
- The system automatically logs lap counts and predicts speed as data flows in.

---

## Project Structure
```
TSS-BMS/
│
├── main.py                 # Core FastAPI application & MQTT Logic
├── MQTT_Data_Push.py       # Data simulator for testing
├── requirements.txt        # Python dependencies
├── xgb_speed_model.pkl     # Pre-trained XGBoost model
├── Dockerfile              # Container configuration
│
├── static/                 # Frontend Assets
│   ├── index.html          # Main Dashboard
│   └── batteryIndex.html   # Battery Specific Dashboard
│
└── README.md               # Project documentation
```

---

## Contributing

Contributions are welcome! To contribute:
1. Fork the repository
2. Create a new branch:
   ```bash
   git checkout -b feature/your-feature
   ```
3. Commit your changes:
   ```bash
   git commit -m "Add your feature"
   ```
4. Push to your branch:
   ```bash
   git push origin feature/your-feature
   ```
5. Open a pull request describing your changes.

---

## Contact
- **GitHub:** [DCode-v05](https://github.com/DCode-v05)
- **Email:** denistanb05@gmail.com
