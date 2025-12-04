## Weather Prediction Dashboard (Vietnam)

A **Streamlit** web application to visualize and analyze weather data for major cities in Vietnam (Ha Noi, Ho Chi Minh, Da Nang), powered by real observations and LSTM-based prediction models running on **Databricks**.

![Home page screenshot](graphics/home_page.png)

### 🌐 Live Demo

- Deployed app:  
  `https://weather-prediction-app-j5zagaxajexjyxszokvbdn.streamlit.app/`

### 🔍 Key Features

- **Overview page**
  - 24-hour temperature forecast for the selected city.
  - Rain probability for today (from LSTM rain model).
  - Daily summary stats (average / max temperature, total rainfall, average humidity).
  - Comparison chart: predicted vs actual temperature.

- **Daily weather (`fact_vn_weather_daily`)**
  - Daily temperature chart (UTC+7).
  - Detailed data table and summary metrics.

- **Hourly weather (`fact_vn_weather_hourly`)**
  - Hourly temperature chart (UTC+7, last 7 days or configurable number of days).
  - Sample hourly data table.

- **24h temperature forecast (`lstm_weather_24h`)**
  - Line chart for the next 24-hour temperature forecast.
  - Metrics: average, maximum, minimum predicted temperature + detailed table.

- **Today's rain probability (`lstm_rain_daily`)**
  - Rain probability from LSTM model (0–1) converted to percentage.
  - Gauge chart with qualitative risk levels and guidance.

- **Forecast vs actual comparison**
  - Join `lstm_weather_24h` with `fact_vn_weather_hourly` to compare model vs reality.
  - Compute MAE, RMSE and visualize the difference over time.

All time axes in charts are converted from UTC+0 to **UTC+7 (Asia/Ho_Chi_Minh)**.

### 📁 Project Structure

- `app.py`: main Streamlit application (UI, data fetching, and visualizations).
- `requirements.txt`: Python dependencies.
- `.streamlit/secrets.toml`: Databricks connection configuration.

### 🧪 Run Locally

1. Create a virtual environment (recommended):

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the Streamlit app:

```bash
streamlit run app.py
```

### 🔐 Databricks Configuration (`secrets.toml`)

In the `.streamlit/` directory, create a `secrets.toml` file:

```toml
[databricks]
server_hostname = "YOUR_DATABRICKS_HOST"
http_path       = "YOUR_DATABRICKS_HTTP_PATH"
access_token    = "YOUR_DATABRICKS_PERSONAL_ACCESS_TOKEN"
```

- This file is ignored by `.gitignore`, so it will not be committed to GitHub.
- These values are used by `app.py` to create the Databricks SQL connection via `databricks-sql-connector`.

### 🔧 Tech Stack

- **Streamlit** – interactive dashboard UI.
- **Databricks SQL Connector** – querying the lakehouse.
- **Pandas** – tabular data processing.
- **Plotly (Express & Graph Objects)** – interactive charts (line, gauge, bar, heatmap, …).

### 🚀 Deployment

The app is deployed on **Streamlit Community Cloud**:

- Deployed link:  
  `https://weather-prediction-app-j5zagaxajexjyxszokvbdn.streamlit.app/`

When deploying to Streamlit Cloud, configure `secrets` under **App settings → Secrets** using the same format as `secrets.toml` above.
