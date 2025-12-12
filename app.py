import streamlit as st
from databricks import sql
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ============================================================================
# CONFIGURATION
# ============================================================================
st.set_page_config(page_title="Weather Forecast Dashboard", layout="wide")

# Múi giờ Việt Nam
VIETNAM_TZ = "Asia/Ho_Chi_Minh"

# Danh sách thành phố
CITIES = {
    "Hà Nội": "Ha Noi City",
    "Hồ Chí Minh": "Ho Chi Minh City", 
    "Đà Nẵng": "Da Nang City"
}

# ============================================================================
# DATABASE CONNECTION HELPER
# ============================================================================
def get_connection():
    """Tạo kết nối đến Databricks"""
    return sql.connect(
        server_hostname=st.secrets["databricks"]["server_hostname"],
        http_path=st.secrets["databricks"]["http_path"],
        access_token=st.secrets["databricks"]["access_token"]
    )

# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================
@st.cache_data(ttl=600)
def get_daily_weather(city):
    """Lấy dữ liệu thời tiết hàng ngày"""
    connection = get_connection()
    cursor = connection.cursor()
    
    query = """
    SELECT *
    FROM hcmut.gold.fact_vn_weather_daily
    WHERE ds_location = :city
    ORDER BY dt_date_record DESC
    LIMIT 30
    """
    
    cursor.execute(query, parameters={"city": city})
    result = cursor.fetchall_arrow()
    df = result.to_pandas()
    
    cursor.close()
    connection.close()
    return df

@st.cache_data(ttl=600)
def get_hourly_weather(city, days=7):
    """Lấy dữ liệu thời tiết hàng giờ"""
    connection = get_connection()
    cursor = connection.cursor()
    
    query = """
    SELECT *
    FROM hcmut.gold.fact_vn_weather_hourly
    WHERE ds_location = :city
    ORDER BY dt_date_record DESC
    LIMIT :limit
    """
    
    limit = days * 24
    cursor.execute(query, parameters={"city": city, "limit": limit})
    result = cursor.fetchall_arrow()
    df = result.to_pandas()
    
    cursor.close()
    connection.close()
    return df

@st.cache_data(ttl=600)
def get_temperature_forecast_24h(city):
    """Lấy dự đoán nhiệt độ 24 giờ tiếp theo"""
    connection = get_connection()
    cursor = connection.cursor()
    
    query = """
    WITH latest_forecast AS (
      SELECT *
      FROM hcmut.gold.lstm_weather_24h
      WHERE ds_location = :city
      QUALIFY row_number() OVER (
        PARTITION BY dt_forecast_time, ds_location
        ORDER BY dt_model_run_time DESC
      ) = 1
    )
    SELECT 
      dt_forecast_time AS forecast_time,
      nr_predicted_temperature AS predicted_temperature,
      ds_location AS location,
      dt_model_run_time AS model_run_time
    FROM latest_forecast
    WHERE dt_forecast_time >= CURRENT_TIMESTAMP()
    ORDER BY dt_forecast_time ASC
    LIMIT 24
    """
    
    cursor.execute(query, parameters={"city": city})
    result = cursor.fetchall_arrow()
    df = result.to_pandas()
    
    cursor.close()
    connection.close()
    return df

@st.cache_data(ttl=600)
def get_rain_probability_today(city):
    """Lấy xác suất mưa dự đoán cho ngày hôm nay (theo logic tham khảo từ Databricks)"""
    connection = get_connection()
    cursor = connection.cursor()
    
    query = """
    SELECT 
      dt_forecast_date AS forecast_date,
      prediction_probability AS rain_probability,
      prediction_label AS rain_label,
      ds_location AS location,
      dt_model_run_time AS model_run_time
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY dt_forecast_date, ds_location
          ORDER BY dt_model_run_time DESC
        ) AS rn
      FROM hcmut.gold.lstm_rain_daily
      WHERE ds_location = :city
        AND DATE(from_utc_timestamp(dt_forecast_date, 'Asia/Ho_Chi_Minh')) >= CURRENT_DATE()
    )
    WHERE rn = 1
    ORDER BY dt_forecast_date ASC
    LIMIT 5
    """
    
    cursor.execute(query, parameters={"city": city})
    result = cursor.fetchall_arrow()
    df = result.to_pandas()
    
    cursor.close()
    connection.close()
    return df

@st.cache_data(ttl=600)
def get_temperature_comparison(city, days=7):
    """Lấy dữ liệu so sánh nhiệt độ dự đoán vs thực tế"""
    connection = get_connection()
    cursor = connection.cursor()
    
    query = """
    WITH latest_lstm AS (
      SELECT *
      FROM hcmut.gold.lstm_weather_24h
      WHERE ds_location = :city
      QUALIFY row_number() OVER (
        PARTITION BY dt_forecast_time, ds_location
        ORDER BY dt_model_run_time DESC
      ) = 1
    )
    SELECT
      latest_lstm.dt_forecast_time AS date,
      latest_lstm.nr_predicted_temperature AS predicted_temperature,
      w.nr_temperature_2m AS actual_temperature,
      w.ds_location AS location
    FROM latest_lstm
    INNER JOIN hcmut.gold.fact_vn_weather_hourly w
      ON latest_lstm.dt_forecast_time = w.dt_date_record
      AND latest_lstm.ds_location = w.ds_location
    WHERE w.ds_location = :city
    ORDER BY date DESC
    LIMIT :limit
    """
    
    limit = days * 24
    cursor.execute(query, parameters={"city": city, "limit": limit})
    result = cursor.fetchall_arrow()
    df = result.to_pandas()
    
    cursor.close()
    connection.close()
    return df

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================
def create_temperature_forecast_chart(df):
    """Biểu đồ dự đoán nhiệt độ 24h"""
    if df.empty:
        st.warning("Không có dữ liệu dự đoán.")
        return

    # Chuyển forecast_time sang giờ Việt Nam
    if "forecast_time" in df.columns:
        df = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df["forecast_time"]):
            df["forecast_time"] = pd.to_datetime(df["forecast_time"], utc=True, errors="coerce")
        if df["forecast_time"].dt.tz is None:
            df["forecast_time"] = df["forecast_time"].dt.tz_localize("UTC")
        df["forecast_time_vn"] = df["forecast_time"].dt.tz_convert(VIETNAM_TZ)
        x_col = "forecast_time_vn"
    else:
        x_col = "forecast_time"

    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df[x_col],
        y=df['predicted_temperature'],
        mode='lines+markers',
        name='Nhiệt Độ Dự Đoán',
        line=dict(color='#FF6B6B', width=3),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        title="🌡️ Dự Đoán Nhiệt Độ 24 Giờ Tiếp Theo",
        xaxis_title="Thời Gian (UTC+7)",
        yaxis_title="Nhiệt Độ (°C)",
        height=500,
        hovermode='x unified',
        template='plotly_white'
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_rain_probability_gauge(df):
    """Biểu đồ gauge xác suất mưa - hỗ trợ nhiều dự đoán"""
    if df.empty:
        st.warning("Không có dữ liệu xác suất mưa.")
        return

    # Lấy dòng đầu tiên (mới nhất theo model_run_time)
    row = df.iloc[0]
    rain_prob_raw = row['rain_probability']
    rain_prob = rain_prob_raw * 100
    
    # Lấy thông tin forecast date
    forecast_date = pd.to_datetime(row['forecast_date'], utc=True)
    forecast_date_vn = forecast_date.tz_convert(VIETNAM_TZ).strftime('%d-%m-%Y %H:%M')
    
    # Xác định màu sắc và nhãn
    if rain_prob < 30:
        color = '#4ECDC4'  # Xanh lá - Ít khả năng mưa
        label = "Ít Khả Năng Mưa"
    elif rain_prob < 50:
        color = '#FFD93D'  # Vàng - Có thể mưa
        label = "Có Thể Mưa"
    elif rain_prob < 70:
        color = '#FFA07A'  # Cam - Khả năng mưa cao
        label = "Khả Năng Mưa Cao"
    else:
        color = '#FF6B6B'  # Đỏ - Rất có khả năng mưa
        label = "Rất Có Khả Năng Mưa"
    
    # Tạo gauge chart
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = rain_prob,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': f"🌧️ Xác Suất Mưa ({forecast_date_vn})<br><span style='font-size:0.8em;color:gray'>{label}</span>"},
        delta = {'reference': 50, 'position': "top"},
        gauge = {
            'axis': {'range': [None, 100]},
            'bar': {'color': color},
            'steps': [
                {'range': [0, 30], 'color': "lightgray"},
                {'range': [30, 50], 'color': "gray"},
                {'range': [50, 70], 'color': "lightgray"},
                {'range': [70, 100], 'color': "gray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 50
            }
        }
    ))
    
    fig.update_layout(
        height=400,
        template='plotly_white'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Hiển thị bảng chi tiết nếu có nhiều dự đoán
    if len(df) > 1:
        st.subheader("📋 Chi Tiết Dự Đoán Mưa")
        display_df = df.copy()
        display_df['forecast_date'] = pd.to_datetime(display_df['forecast_date'], utc=True)
        display_df['forecast_date_vn'] = display_df['forecast_date'].dt.tz_convert(VIETNAM_TZ).dt.strftime('%d-%m-%Y %H:%M')
        display_df['rain_probability'] = (display_df['rain_probability'] * 100).round(2).astype(str) + '%'
        display_df = display_df[['forecast_date_vn', 'rain_probability', 'rain_label', 'location']]
        display_df.columns = ['Thời Gian Dự Đoán', 'Xác Suất Mưa', 'Nhãn', 'Địa Điểm']
        st.dataframe(display_df, use_container_width=True)

def create_comparison_chart(df):
    """Biểu đồ so sánh dự đoán vs thực tế"""
    if df.empty or 'predicted_temperature' not in df.columns:
        st.warning("Không có dữ liệu để so sánh.")
        return
    
    fig = go.Figure()

    # Chuyển cột thời gian sang giờ Việt Nam nếu có
    if "date" in df.columns:
        df = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df["date"]):
            df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
        if df["date"].dt.tz is None:
            df["date"] = df["date"].dt.tz_localize("UTC")
        df["date_vn"] = df["date"].dt.tz_convert(VIETNAM_TZ)
        x_col = "date_vn"
    else:
        x_col = df.index
    
    fig.add_trace(go.Scatter(
        x=df[x_col],
        y=df['predicted_temperature'],
        mode='lines+markers',
        name='Nhiệt Độ Dự Đoán',
        line=dict(color='#4A90E2', dash='dash', width=2),
        marker=dict(size=6)
    ))
    
    if 'actual_temperature' in df.columns:
        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=df['actual_temperature'],
            mode='lines+markers',
            name='Nhiệt Độ Thực Tế',
            line=dict(color='#FF6B6B', width=2),
            marker=dict(size=6)
        ))
    
    fig.update_layout(
        title="📊 So Sánh Nhiệt Độ Dự Đoán vs Thực Tế",
        xaxis_title="Thời Gian",
        yaxis_title="Nhiệt Độ (°C)",
        height=500,
        hovermode='x unified',
        template='plotly_white',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_weather_metrics_cards(df, data_type="daily"):
    """Hiển thị các thẻ metrics"""
    if df.empty:
        return

    if data_type == "daily":
        # Bảng daily dùng các cột *_mean / *_max / *_min
        temp_mean_col = None
        for c in ["nr_temperature_2m_mean", "nr_temperature_2m"]:
            if c in df.columns:
                temp_mean_col = c
                break

        temp_max_col = "nr_temperature_2m_max" if "nr_temperature_2m_max" in df.columns else temp_mean_col
        temp_min_col = "nr_temperature_2m_min" if "nr_temperature_2m_min" in df.columns else temp_mean_col

        rain_col = None
        for c in ["nr_rain_sum", "nr_precipitation_sum"]:
            if c in df.columns:
                rain_col = c
                break

        humidity_col = "nr_relative_humidity_2m_mean" if "nr_relative_humidity_2m_mean" in df.columns else None

        cols = st.columns(4)
        with cols[0]:
            if temp_mean_col is not None:
                avg_temp = df[temp_mean_col].mean()
                st.metric("🌡️ Nhiệt độ trung bình", f"{avg_temp:.1f}°C")
            else:
                st.metric("🌡️ Nhiệt độ trung bình", "N/A")
        with cols[1]:
            if temp_max_col is not None:
                max_temp = df[temp_max_col].max()
                st.metric("🔥 Nhiệt độ cao nhất", f"{max_temp:.1f}°C")
            else:
                st.metric("🔥 Nhiệt độ cao nhất", "N/A")
        with cols[2]:
            if rain_col is not None:
                total_rain = df[rain_col].sum()
                st.metric("🌧️ Tổng lượng mưa", f"{total_rain:.1f} mm")
            else:
                st.metric("🌧️ Tổng lượng mưa", "N/A")
        with cols[3]:
            if humidity_col is not None:
                avg_humidity = df[humidity_col].mean()
                st.metric("💧 Độ ẩm trung bình", f"{avg_humidity:.0f}%")
            else:
                st.metric("📊 Số ngày", len(df))
    
    elif data_type == "hourly":
        cols = st.columns(4)
        with cols[0]:
            if 'nr_temperature_2m' in df.columns:
                avg_temp = df['nr_temperature_2m'].mean()
                st.metric("🌡️ Nhiệt Độ TB", f"{avg_temp:.1f}°C")
        with cols[1]:
            if 'nr_humidity' in df.columns:
                avg_humidity = df['nr_humidity'].mean()
                st.metric("💧 Độ Ẩm TB", f"{avg_humidity:.1f}%")
        with cols[2]:
            if 'nr_wind_speed' in df.columns:
                avg_wind = df['nr_wind_speed'].mean()
                st.metric("💨 Tốc Độ Gió TB", f"{avg_wind:.1f} km/h")
        with cols[3]:
            st.metric("⏰ Số Giờ", len(df))

def create_multi_city_comparison(cities_data):
    """So sánh dữ liệu giữa các thành phố"""
    if not cities_data:
        return
    
    fig = go.Figure()
    
    for city, df in cities_data.items():
        if not df.empty and 'nr_temperature_2m' in df.columns:
            # Không dùng nhiều trong app hiện tại, nhưng vẫn đồng bộ về giờ VN nếu có cột date
            time_x = df.index
            if "date" in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df["date"]):
                    df = df.copy()
                    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
                if df["date"].dt.tz is None:
                    df["date"] = df["date"].dt.tz_localize("UTC")
                time_x = df["date"].dt.tz_convert(VIETNAM_TZ)

            fig.add_trace(go.Scatter(
                x=time_x,
                y=df['nr_temperature_2m'],
                mode='lines+markers',
                name=city,
                marker=dict(size=6)
            ))
    
    fig.update_layout(
        title="🏙️ So Sánh Nhiệt Độ Giữa Các Thành Phố",
        xaxis_title="Thời Gian",
        yaxis_title="Nhiệt Độ (°C)",
        height=500,
        hovermode='x unified',
        template='plotly_white'
    )
    
    st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# MAIN APPLICATION
# ============================================================================
def main():
    st.title("🌤️ Weather Forecast Dashboard - Việt Nam")
    st.markdown("Dashboard theo dõi và dự đoán thời tiết cho các thành phố lớn tại Việt Nam")
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Cài Đặt")
        
        # Chọn thành phố (hiển thị tiếng Việt, dùng giá trị map sang tên trong Databricks)
        selected_city = st.selectbox(
            "📍 Chọn thành phố:",
            options=list(CITIES.keys()),
            index=0
        )
        db_city = CITIES.get(selected_city, selected_city)
        
        st.markdown("---")
        
        # Chọn loại dữ liệu
        st.header("📑 Loại Dữ Liệu")
        data_type = st.radio(
            "Chọn loại dữ liệu:",
            ["🏠 Trang Chủ", "📅 Thời Tiết Hàng Ngày", "⏰ Thời Tiết Hàng Giờ", 
             "🌡️ Dự Đoán Nhiệt Độ 24h", "🌧️ Xác Suất Mưa", "📊 So Sánh Dự Đoán"]
        )
        
        st.markdown("---")
        
        # Tùy chọn bổ sung
        st.header("🔧 Tùy Chọn")
        if data_type in ["📅 Thời Tiết Hàng Ngày", "⏰ Thời Tiết Hàng Giờ", "📊 So Sánh Dự Đoán"]:
            days = st.slider("Số ngày hiển thị:", 1, 30, 7)
        else:
            days = 7
    
    # Xử lý và hiển thị dữ liệu
    try:
        if data_type == "🏠 Trang Chủ":
            st.header(f"🏠 Trang Chủ - {selected_city}")
            
            # Lấy dữ liệu tổng hợp
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🌡️ Dự Đoán Nhiệt Độ 24h")
                temp_forecast = get_temperature_forecast_24h(db_city)
                if not temp_forecast.empty:
                    create_temperature_forecast_chart(temp_forecast)
                else:
                    st.info("Đang tải dữ liệu dự đoán...")
            
            with col2:
                st.subheader("🌧️ Xác Suất Mưa Hôm Nay")
                rain_prob = get_rain_probability_today(db_city)
                if not rain_prob.empty:
                    create_rain_probability_gauge(rain_prob)
                else:
                    st.info("Đang tải dữ liệu xác suất mưa...")
            
            st.markdown("---")
            
            # Metrics tổng quan
            st.subheader("📊 Thông Tin Tổng Quan")
            daily_data = get_daily_weather(db_city)
            if not daily_data.empty:
                create_weather_metrics_cards(daily_data, "daily")
            
            st.markdown("---")
            
            # So sánh dự đoán vs thực tế
            st.subheader("📊 So Sánh Dự Đoán vs Thực Tế")
            comparison_data = get_temperature_comparison(db_city, days=3)
            if not comparison_data.empty:
                create_comparison_chart(comparison_data)
        
        elif data_type == "📅 Thời Tiết Hàng Ngày":
            st.header(f"📅 Thời Tiết Hàng Ngày - {selected_city}")
            
            with st.spinner('Đang tải dữ liệu...'):
                df = get_daily_weather(db_city)
            
            if not df.empty:
                create_weather_metrics_cards(df, "daily")
                st.markdown("---")
                
                # Biểu đồ nhiệt độ (dùng cột *_mean của bảng daily)
                temp_col = None
                for c in ["nr_temperature_2m_mean", "nr_temperature_2m", "nr_temperature_2m_max", "nr_temperature_2m_min"]:
                    if c in df.columns:
                        temp_col = c
                        break

                if temp_col is not None:
                    # Đảm bảo cột thời gian là datetime & chuyển sang giờ Việt Nam
                    df = df.copy()
                    if not pd.api.types.is_datetime64_any_dtype(df["dt_date_record"]):
                        df["dt_date_record"] = pd.to_datetime(df["dt_date_record"], utc=True, errors="coerce")
                    if df["dt_date_record"].dt.tz is None:
                        df["dt_date_record"] = df["dt_date_record"].dt.tz_localize("UTC")
                    df["dt_date_record_vn"] = df["dt_date_record"].dt.tz_convert(VIETNAM_TZ)

                    fig = px.line(
                        df.sort_values("dt_date_record_vn"),
                        x="dt_date_record_vn",
                        y=temp_col,
                        title="🌡️ Nhiệt Độ Hàng Ngày",
                        labels={temp_col: "Nhiệt Độ (°C)", "dt_date_record_vn": "Ngày (UTC+7)"},
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Không tìm thấy cột nhiệt độ phù hợp trong dữ liệu hàng ngày.")
                
                st.markdown("---")
                st.subheader("📋 Bảng Dữ Liệu")
                st.dataframe(df, use_container_width=True)
            else:
                st.warning("Không có dữ liệu cho thành phố này.")
        
        elif data_type == "⏰ Thời Tiết Hàng Giờ":
            st.header(f"⏰ Thời Tiết Hàng Giờ - {selected_city}")
            
            with st.spinner('Đang tải dữ liệu...'):
                df = get_hourly_weather(db_city, days=days)
            
            if not df.empty:
                create_weather_metrics_cards(df, "hourly")
                st.markdown("---")
                
                # Biểu đồ nhiệt độ theo giờ (chuyển sang giờ Việt Nam)
                if 'nr_temperature_2m' in df.columns and 'dt_date_record' in df.columns:
                    df = df.copy()
                    if not pd.api.types.is_datetime64_any_dtype(df["dt_date_record"]):
                        df["dt_date_record"] = pd.to_datetime(df["dt_date_record"], utc=True, errors="coerce")
                    if df["dt_date_record"].dt.tz is None:
                        df["dt_date_record"] = df["dt_date_record"].dt.tz_localize("UTC")
                    df["dt_date_record_vn"] = df["dt_date_record"].dt.tz_convert(VIETNAM_TZ)

                    fig = px.line(
                        df.sort_values("dt_date_record_vn").head(168),
                        x='dt_date_record_vn',
                        y='nr_temperature_2m',
                        title="🌡️ Nhiệt Độ Theo Giờ (UTC+7, 7 Ngày Gần Nhất)",
                        labels={'nr_temperature_2m': 'Nhiệt Độ (°C)', 
                                'dt_date_record_vn': 'Thời Gian (UTC+7)'}
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                
                st.markdown("---")
                st.subheader("📋 Bảng Dữ Liệu")
                st.dataframe(df.head(100), use_container_width=True)
            else:
                st.warning("Không có dữ liệu cho thành phố này.")
        
        elif data_type == "🌡️ Dự Đoán Nhiệt Độ 24h":
            st.header(f"🌡️ Dự Đoán Nhiệt Độ 24 Giờ - {selected_city}")
            
            with st.spinner('Đang tải dữ liệu dự đoán...'):
                df = get_temperature_forecast_24h(db_city)
            
            if not df.empty:
                # Metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("🌡️ Nhiệt Độ TB", f"{df['predicted_temperature'].mean():.1f}°C")
                with col2:
                    st.metric("🔥 Cao Nhất", f"{df['predicted_temperature'].max():.1f}°C")
                with col3:
                    st.metric("❄️ Thấp Nhất", f"{df['predicted_temperature'].min():.1f}°C")
                
                st.markdown("---")
                create_temperature_forecast_chart(df)
                
                st.markdown("---")
                st.subheader("📋 Chi Tiết Dự Đoán")
                st.dataframe(df, use_container_width=True)
            else:
                st.warning("Không có dữ liệu dự đoán cho thành phố này.")
        
        elif data_type == "🌧️ Xác Suất Mưa":
            st.header(f"🌧️ Xác Suất Mưa - {selected_city}")
            
            with st.spinner('Đang tải dữ liệu xác suất mưa...'):
                df = get_rain_probability_today(db_city)
            
            if not df.empty:
                # Giá trị gốc 0–1, chuyển sang %
                rain_prob_raw = df['rain_probability'].iloc[0]
                rain_prob = rain_prob_raw * 100
                
                # Hiển thị metric lớn
                col1, col2, col3 = st.columns([1, 2, 1])
                with col2:
                    if rain_prob < 30:
                        st.metric("🌧️ Xác Suất Mưa", f"{rain_prob:.1f}%", 
                                 delta=f"{rain_prob - 50:.1f}% so với ngưỡng 50%",
                                 delta_color="inverse")
                        st.info("☀️ Ít khả năng mưa - Thời tiết khô ráo")
                    elif rain_prob < 50:
                        st.metric("🌧️ Xác Suất Mưa", f"{rain_prob:.1f}%", 
                                 delta=f"{rain_prob - 50:.1f}% so với ngưỡng 50%",
                                 delta_color="off")
                        st.warning("⛅ Có thể mưa - Nên mang theo ô")
                    elif rain_prob < 70:
                        st.metric("🌧️ Xác Suất Mưa", f"{rain_prob:.1f}%", 
                                 delta=f"{rain_prob - 50:.1f}% so với ngưỡng 50%",
                                 delta_color="normal")
                        st.warning("🌦️ Khả năng mưa cao - Nên mang theo ô")
                    else:
                        st.metric("🌧️ Xác Suất Mưa", f"{rain_prob:.1f}%", 
                                 delta=f"{rain_prob - 50:.1f}% so với ngưỡng 50%",
                                 delta_color="normal")
                        st.error("🌧️ Rất có khả năng mưa - Nhớ mang theo ô!")
                
                st.markdown("---")
                
                # Gauge chart
                create_rain_probability_gauge(df)
                
                st.markdown("---")
                st.subheader("📋 Chi Tiết")
                
                # Xử lý và hiển thị chi tiết
                try:
                    forecast_date = pd.to_datetime(df['forecast_date'].iloc[0], utc=True)
                    forecast_date_vn = forecast_date.tz_convert(VIETNAM_TZ).strftime('%d-%m-%Y %H:%M')
                    
                    model_run_time = pd.to_datetime(df['model_run_time'].iloc[0], utc=True)
                    model_run_time_vn = model_run_time.tz_convert(VIETNAM_TZ).strftime('%d-%m-%Y %H:%M:%S')
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Ngày dự đoán:** {forecast_date_vn}")
                        st.write(f"**Thành phố:** {df['location'].iloc[0]}")
                    with col2:
                        st.write(f"**Xác suất mưa:** {rain_prob:.2f}%")
                        st.write(f"**Thời gian model chạy:** {model_run_time_vn}")
                    
                    if 'rain_label' in df.columns:
                        st.write(f"**Nhãn dự đoán:** {'Có mưa' if df['rain_label'].iloc[0] == 1 else 'Không mưa'}")
                except Exception as e:
                    st.error(f"Lỗi xử lý dữ liệu chi tiết: {e}")
            else:
                st.warning("Không có dữ liệu xác suất mưa cho ngày hôm nay.")
        
        elif data_type == "📊 So Sánh Dự Đoán":
            st.header(f"📊 So Sánh Dự Đoán vs Thực Tế - {selected_city}")
            
            with st.spinner('Đang tải dữ liệu so sánh...'):
                df = get_temperature_comparison(db_city, days=days)
            
            if not df.empty:
                # Metrics độ chính xác
                if 'predicted_temperature' in df.columns and 'actual_temperature' in df.columns:
                    mae = abs(df['predicted_temperature'] - df['actual_temperature']).mean()
                    rmse = ((df['predicted_temperature'] - df['actual_temperature'])**2).mean()**0.5
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📊 Số Điểm Dữ Liệu", len(df))
                    with col2:
                        st.metric("📉 MAE (Sai Số Trung Bình)", f"{mae:.2f}°C")
                    with col3:
                        st.metric("📈 RMSE", f"{rmse:.2f}°C")
                
                st.markdown("---")
                create_comparison_chart(df)
                
                st.markdown("---")
                st.subheader("📋 Bảng Dữ Liệu So Sánh")
                st.dataframe(df, use_container_width=True)
            else:
                st.warning("Không có dữ liệu để so sánh cho thành phố này.")
    
    except Exception as e:
        st.error(f"❌ Có lỗi xảy ra: {e}")
        st.exception(e)

if __name__ == "__main__":
    main()
